"""
Unit tests for pipeline_trigger.py — the SQS-driven Lambda that
serializes Step Functions executions.

The two behaviours we care most about:
  1. When no execution is running, the Lambda starts a fresh one.
  2. When an execution IS running, the Lambda refuses (raises) so SQS
     holds the message and redelivers later.

moto provides in-process mocks for Step Functions and SQS.
"""

import json
import os

import boto3
import pytest
from moto import mock_aws


# ---------------------------------------------------------------------------
# AWS credentials placeholder — moto uses these but they're never sent.
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"


# A minimal state machine definition. We only test entry behaviour, so a
# trivial PASS state is enough — the moto state machine just needs to
# accept StartExecution.
TRIVIAL_DEFINITION = json.dumps({
    "StartAt": "Done",
    "States": {"Done": {"Type": "Pass", "End": True}},
})


@pytest.fixture
def trigger_env():
    """
    Spin up moto + create a fake state machine + IAM role for it, set
    the STATE_MACHINE_ARN env var, then fresh-import pipeline_trigger.
    """
    with mock_aws():
        # State machines require a role ARN even if moto doesn't enforce
        # the trust policy. Create a stub IAM role to satisfy the API.
        iam = boto3.client("iam", region_name="eu-west-1")
        role = iam.create_role(
            RoleName="sfn-test-role",
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "states.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )

        sfn = boto3.client("stepfunctions", region_name="eu-west-1")
        sm = sfn.create_state_machine(
            name="test-pipeline",
            definition=TRIVIAL_DEFINITION,
            roleArn=role["Role"]["Arn"],
        )
        state_machine_arn = sm["stateMachineArn"]

        # pipeline_trigger reads STATE_MACHINE_ARN at handler call time
        # (good — easier to test than module-import time).
        os.environ["STATE_MACHINE_ARN"] = state_machine_arn

        # Fresh import inside the mock so the boto3 client is moto-backed.
        import importlib
        import sys
        if "pipeline_trigger" in sys.modules:
            del sys.modules["pipeline_trigger"]
        import pipeline_trigger

        yield {
            "sfn": sfn,
            "state_machine_arn": state_machine_arn,
            "module": pipeline_trigger,
        }


def make_sqs_event(body):
    """Construct the SQS-event-shaped payload Lambda will receive."""
    return {
        "Records": [{
            "messageId": "abc-123",
            "body": json.dumps(body) if not isinstance(body, str) else body,
            "attributes": {},
            "messageAttributes": {},
        }]
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_starts_execution_when_idle(trigger_env):
    """No executions running → Lambda calls StartExecution and returns."""
    pt = trigger_env["module"]
    sfn = trigger_env["sfn"]
    sm_arn = trigger_env["state_machine_arn"]

    eb_event = {
        "detail": {
            "bucket": {"name": "raw-bucket"},
            "object": {"key": "streams/x.csv"},
        }
    }

    result = pt.lambda_handler(make_sqs_event(eb_event), None)

    assert result["status"] == "started"
    assert "executionArn" in result

    # Verify Step Functions actually received a start call.
    history = sfn.list_executions(stateMachineArn=sm_arn)
    assert len(history["executions"]) == 1


def test_passes_eventbridge_event_as_input(trigger_env):
    """The EventBridge event must arrive verbatim as the SFN input."""
    pt = trigger_env["module"]
    sfn = trigger_env["sfn"]

    eb_event = {
        "detail": {
            "bucket": {"name": "raw-bucket"},
            "object": {"key": "streams/specific-file.csv"},
        }
    }
    result = pt.lambda_handler(make_sqs_event(eb_event), None)

    described = sfn.describe_execution(executionArn=result["executionArn"])
    input_json = json.loads(described["input"])
    assert input_json["detail"]["object"]["key"] == "streams/specific-file.csv"


# ---------------------------------------------------------------------------
# Serialization guard
# ---------------------------------------------------------------------------

def test_refuses_when_execution_already_running(trigger_env):
    """If a run is already in flight, the Lambda raises — SQS will retry."""
    pt = trigger_env["module"]
    sfn = trigger_env["sfn"]
    sm_arn = trigger_env["state_machine_arn"]

    # Start one execution manually and KEEP it running. moto Pass states
    # finish immediately, so we need a trick: most moto versions report
    # the first execution as SUCCEEDED right away, defeating the test.
    # Instead, monkeypatch list_executions on the module's sfn client to
    # report a fake running execution.
    pt.sfn.list_executions = lambda **kw: {
        "executions": [{
            "executionArn": f"{sm_arn}:execution:already-running",
            "status": "RUNNING",
        }]
    }

    eb_event = {
        "detail": {
            "bucket": {"name": "raw-bucket"},
            "object": {"key": "streams/y.csv"},
        }
    }

    with pytest.raises(RuntimeError, match="busy"):
        pt.lambda_handler(make_sqs_event(eb_event), None)


# ---------------------------------------------------------------------------
# Defensive coding
# ---------------------------------------------------------------------------

def test_rejects_multi_record_batch(trigger_env):
    """batch_size=1 is enforced infra-side, but we double-check here."""
    pt = trigger_env["module"]

    event = {
        "Records": [
            {"body": "{}"},
            {"body": "{}"},  # second record violates batch_size=1
        ]
    }

    with pytest.raises(RuntimeError, match="exactly 1 SQS record"):
        pt.lambda_handler(event, None)


def test_rejects_malformed_json_body(trigger_env):
    """A non-JSON SQS body should fail fast with a clear error."""
    pt = trigger_env["module"]

    event = {"Records": [{"body": "this is not json {{{"}]}

    with pytest.raises(RuntimeError, match="not valid JSON"):
        pt.lambda_handler(event, None)
