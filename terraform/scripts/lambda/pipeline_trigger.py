"""
pipeline_trigger.py — Serialized Step Functions launcher

Sits between SQS FIFO and Step Functions. Its job is to make sure
ONLY ONE pipeline execution runs at a time, no matter how many
stream files land in S3 in quick succession.

Why this Lambda exists
----------------------
Without it, two files landing seconds apart would each trigger a
parallel pipeline run. Both `Load` jobs would then race to upsert
the SAME (genre, date) keys in DynamoDB — last writer wins, the
losing run's contribution silently disappears.

How serialization works
-----------------------
Three layers of defense, all of which must be configured in
Terraform alongside this code:

  1. SQS FIFO queue with all messages sharing one MessageGroupId.
     FIFO guarantees only ONE message in flight per group at any
     time.
  2. Lambda reserved concurrency = 1. AWS Lambda will never run
     two copies of this function simultaneously.
  3. Defense-in-depth check below: before starting an execution,
     we ListExecutions and bail if any are still RUNNING. If
     someone starts a pipeline run manually via the console,
     we'll detect and defer.

Flow
----
  SQS message arrives
    → check if any execution is RUNNING (against state machine)
      → yes: raise → SQS holds message → redelivered after
              visibility timeout (set in Terraform to ~10 min)
      → no:  start a fresh execution with the SQS message body
              (which is the original EventBridge event) as input
    → return success → SQS deletes the message
"""

import json
import logging
import os

import boto3

# Logger setup — Lambda runtime writes everything here to CloudWatch.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reuse the boto3 client across warm invocations.
sfn = boto3.client("stepfunctions")

# Injected by Terraform via the function's environment variables.
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def lambda_handler(event, context):
    """Process exactly one SQS FIFO message (batch_size is enforced to 1)."""
    records = event.get("Records", [])
    if len(records) != 1:
        # Defensive — batch_size=1 should make this impossible. If we ever
        # see it, fail loudly rather than silently mishandle messages.
        raise RuntimeError(
            f"Expected exactly 1 SQS record (batch_size=1), got {len(records)}"
        )

    sqs_record = records[0]

    # The SQS message body is the original EventBridge event we routed in,
    # serialized as JSON. Step Functions wants it as the execution input.
    try:
        eb_event = json.loads(sqs_record["body"])
    except json.JSONDecodeError as e:
        raise RuntimeError(f"SQS message body is not valid JSON: {e}") from e

    logger.info("Received EventBridge event from SQS: %s", eb_event)

    # ------------------------------------------------------------------
    # Defense-in-depth: don't start a new execution if one is running.
    # SQS FIFO + reserved_concurrency=1 already guarantee this, but a
    # manual console execution OR a manual `aws stepfunctions start-execution`
    # call would slip past those guards. This check catches that.
    # ------------------------------------------------------------------
    running = sfn.list_executions(
        stateMachineArn=STATE_MACHINE_ARN,
        statusFilter="RUNNING",
        maxResults=1,
    )["executions"]

    if running:
        running_arn = running[0]["executionArn"]
        logger.info(
            "Pipeline already running (%s) — deferring this trigger. "
            "SQS will redeliver after the visibility timeout.",
            running_arn,
        )
        # Raising forces SQS to redeliver. With visibility timeout set
        # appropriately in Terraform (~10 min), the next attempt should
        # find a clean slate.
        raise RuntimeError(
            f"Pipeline is busy with execution {running_arn}; deferring."
        )

    # ------------------------------------------------------------------
    # Start a new execution. We pass the original EventBridge event as
    # input so $.detail.bucket.name and $.detail.object.key still resolve
    # correctly inside the state machine.
    # ------------------------------------------------------------------
    start_resp = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(eb_event),
    )
    execution_arn = start_resp["executionArn"]

    logger.info("Started Step Functions execution: %s", execution_arn)

    # Return immediately — we do NOT wait for the pipeline to finish.
    # SQS will treat this message as successfully processed and remove
    # it from the queue. The next message can only be delivered after
    # the visibility timeout, by which time the current execution will
    # either be done or far enough along that the RUNNING check above
    # will catch overlap.
    return {
        "status": "started",
        "executionArn": execution_arn,
    }
