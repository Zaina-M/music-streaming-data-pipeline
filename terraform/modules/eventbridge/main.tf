###############################################################################
# modules/eventbridge/main.tf
#
# Wires S3 PutObject events on the raw bucket to the SQS FIFO trigger
# queue. The queue serializes incoming events; the pipeline_trigger
# Lambda then consumes one at a time and starts Step Functions runs.
#
# Event flow:
#   S3 PutObject (raw bucket)
#     → AWS publishes "Object Created" event to the default EventBridge bus
#     → our rule matches the event by bucket name + streams/ prefix
#     → rule's target = SQS FIFO queue, with a shared MessageGroupId so
#       FIFO ordering applies across ALL incoming files
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# Rule — matches "Object Created" events from our raw bucket only, and
# only for files under streams/ (reference data uploads are ignored).
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "raw_object_created" {
  name        = "${local.name_prefix}-raw-object-created"
  description = "Fires when a new stream file lands in the raw bucket; enqueues it for serialized processing."

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        key = [{
          prefix = "streams/"
        }]
      }
    }
  })
}

# -----------------------------------------------------------------------------
# Target — SQS FIFO queue.
#
# All messages share the same MessageGroupId ("pipeline"). For FIFO this
# means every message is processed strictly in order, one at a time.
# Using different group IDs would allow parallel processing across
# groups — exactly what we DON'T want here.
#
# EventBridge → SQS uses the queue's resource policy for authorization
# (granted in the sqs module), so no role_arn is needed here.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_target" "to_sqs_fifo" {
  rule = aws_cloudwatch_event_rule.raw_object_created.name
  arn  = var.sqs_queue_arn

  sqs_target {
    message_group_id = "pipeline"
  }
}
