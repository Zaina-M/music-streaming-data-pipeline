"""
dispatcher.py — Zero-latency next-message dispatcher

Triggered by EventBridge whenever a Step Functions execution reaches a
terminal state (SUCCEEDED, FAILED, TIMED_OUT, ABORTED). Its job is to
immediately drain the next queued SQS message instead of waiting for
the SQS visibility-timeout cycle.

Why this exists
---------------
Without this Lambda, when the trigger Lambda is "busy" and raises, SQS
hides the message for the full visibility_timeout (60s in our config)
before redelivering it. That means a queued file waits up to 60s after
the previous pipeline ends.

With this Lambda, the moment Step Functions reports the pipeline done,
EventBridge invokes us. We long-poll SQS for up to 20 seconds — if any
hidden message's visibility timeout expires during that window, we
catch it instantly and dispatch the next pipeline.

The standard SQS Lambda event-source mapping is still attached to the
queue; this dispatcher is *additional*. They never fight over the same
message because SQS guarantees a single in-flight delivery per FIFO
message group.
"""

import json
import logging
import os

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Explicit boto3 timeouts — read_timeout must be longer than the SQS
# long-poll wait, or the SDK will give up before SQS responds.
_BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=25,
    retries={"max_attempts": 3, "mode": "standard"},
)

sfn = boto3.client("stepfunctions", config=_BOTO_CONFIG)
sqs = boto3.client("sqs", config=_BOTO_CONFIG)

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
QUEUE_URL = os.environ["QUEUE_URL"]

# How long to wait for a message to become visible. SQS long-poll caps
# at 20 seconds; we use the maximum because the dispatcher's whole
# purpose is to catch a message the instant SQS releases it.
LONG_POLL_SECONDS = 20


def lambda_handler(event, context):
    """Wake up, look for the next queued message, dispatch it if found."""
    logger.info(
        "Dispatcher invoked. detail-type=%s status=%s",
        event.get("detail-type"),
        event.get("detail", {}).get("status"),
    )

    # Defense-in-depth: don't double-dispatch. If another path already
    # kicked off the next execution (e.g., the SQS event-source mapping
    # beat us by milliseconds), respect that and return.
    running = sfn.list_executions(
        stateMachineArn=STATE_MACHINE_ARN,
        statusFilter="RUNNING",
        maxResults=1,
    )["executions"]
    if running:
        logger.info(
            "A pipeline is already running (%s) — nothing to dispatch.",
            running[0]["executionArn"],
        )
        return {"status": "skipped", "reason": "pipeline_already_running"}

    # Long-poll for up to LONG_POLL_SECONDS. If a queued message's
    # visibility timeout expires during that window, SQS hands it to us
    # immediately. If nothing shows up, the standard event-source
    # mapping will still pick it up on its own polling cadence.
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=LONG_POLL_SECONDS,
    )
    messages = resp.get("Messages", [])

    if not messages:
        logger.info(
            "No queued messages within %ds long-poll window. Standard "
            "SQS redelivery will handle whatever's next.",
            LONG_POLL_SECONDS,
        )
        return {"status": "no_messages"}

    msg = messages[0]
    msg_id = msg.get("MessageId", "<unknown>")

    try:
        eb_event = json.loads(msg["Body"])
    except json.JSONDecodeError:
        # Don't delete the message — re-raise so SQS counts the failure
        # toward maxReceiveCount. The message will eventually land in
        # the DLQ where a human can inspect it.
        logger.exception("Malformed SQS body (msg %s) — leaving for DLQ", msg_id)
        raise

    # Start the next pipeline. We DO NOT delete the message until
    # StartExecution succeeds — if SFN is briefly unreachable, the
    # message stays in flight and SQS will redeliver after the
    # visibility timeout.
    start_resp = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(eb_event),
    )

    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=msg["ReceiptHandle"],
    )

    logger.info(
        "Dispatched queued message %s -> execution %s",
        msg_id, start_resp["executionArn"],
    )
    return {
        "status": "dispatched",
        "messageId": msg_id,
        "executionArn": start_resp["executionArn"],
    }
