###############################################################################
# modules/sqs/main.tf
#
# Creates an SQS FIFO queue that sits between EventBridge and the trigger
# Lambda. The queue's job is to SERIALIZE incoming S3 events so the
# downstream pipeline only ever processes one file at a time — preventing
# the race condition where parallel Load jobs would overwrite each other's
# KPI rows in DynamoDB.
#
# Two queues:
#   1. Main FIFO queue (.fifo suffix is REQUIRED by AWS for FIFO queues).
#   2. Dead-letter queue — standard (not FIFO) by convention. Captures
#      messages that fail too many times so a human can investigate.
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# Dead-letter queue.
#
# MUST be FIFO because AWS requires the DLQ to match the source queue's
# type — and our main queue is FIFO. The .fifo suffix is REQUIRED by AWS
# for FIFO queues; without it, CreateQueue rejects fifo_queue=true.
#
# content_based_deduplication is enabled so messages that SQS re-routes
# from the main queue carry forward without us needing to compute new
# deduplication IDs.
#
# Retention 14 days gives operators time to notice and triage.
# -----------------------------------------------------------------------------
resource "aws_sqs_queue" "dlq" {
  name                        = "${local.name_prefix}-pipeline-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  message_retention_seconds   = 1209600 # 14 days, the maximum
}

# -----------------------------------------------------------------------------
# Main FIFO queue.
#
# Key settings:
#   - fifo_queue = true → enables FIFO semantics (exactly-once, ordered).
#   - content_based_deduplication = true → SQS hashes the body for the
#       deduplication ID. Lets EventBridge send without computing one.
#   - visibility_timeout → long enough that retries don't fire while the
#       previous pipeline run is still in progress (see variables.tf).
#   - redrive_policy → after N failed receives, move to DLQ.
# -----------------------------------------------------------------------------
resource "aws_sqs_queue" "pipeline_trigger" {
  name = "${local.name_prefix}-pipeline-trigger.fifo"

  fifo_queue                  = true
  content_based_deduplication = true

  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = 345600 # 4 days
  receive_wait_time_seconds  = 20     # long-polling: cheaper than short polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

# -----------------------------------------------------------------------------
# Queue resource policy — grants EventBridge permission to send messages
# directly to this queue. We use a resource policy instead of an IAM role
# because EventBridge → SQS is a "push" target and AWS recommends
# resource policies for cross-service push permissions.
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "allow_eventbridge_send" {
  statement {
    sid    = "AllowEventBridgeSend"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.pipeline_trigger.arn]
  }
}

resource "aws_sqs_queue_policy" "allow_eventbridge_send" {
  queue_url = aws_sqs_queue.pipeline_trigger.url
  policy    = data.aws_iam_policy_document.allow_eventbridge_send.json
}
