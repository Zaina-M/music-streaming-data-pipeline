###############################################################################
# modules/pipeline_trigger/main.tf
#
# The "serialization Lambda" — receives one SQS FIFO message at a time
# and starts a Step Functions execution. Combined with the FIFO queue's
# single-message-in-flight guarantee and reserved_concurrent_executions=1,
# this enforces that the ETL pipeline only ever runs one execution at a
# time.
###############################################################################

locals {
  name_prefix   = "${var.project_name}-${var.environment}"
  function_name = "${local.name_prefix}-pipeline-trigger"
}

# Zip the source at apply time — same pattern as the archive Lambda.
data "archive_file" "trigger_zip" {
  type        = "zip"
  source_file = var.source_file
  output_path = "${path.module}/build/pipeline_trigger.zip"
}

# Explicit log group so retention is controlled (default would be "forever").
resource "aws_cloudwatch_log_group" "trigger" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = 14
}

# -----------------------------------------------------------------------------
# The Lambda function itself.
#
# Serialization layers in this architecture:
#   1. SQS FIFO + shared MessageGroupId — only one message delivered at a
#      time per group. (Configured in the sqs / eventbridge modules.)
#   2. (Optional) reserved_concurrent_executions = 1 — AWS-level guarantee
#      that no two copies of the function run at once. Off by default
#      because new / sandbox AWS accounts cannot reserve concurrency
#      (account quota of 10 < AWS-enforced minimum unreserved of 10).
#      Set var.reserved_concurrency = 1 if your account quota >= 11.
#   3. ListExecutions check inside the Lambda code — catches manual
#      triggers from the console / CLI. Always on.
#
# Layer 1 alone is sufficient for SQS-driven traffic. Layer 2 only adds
# value as a hypothetical guard against AWS itself spinning up parallel
# Lambda instances for separate FIFO messages — which won't happen at
# batch_size=1 on a FIFO source.
# -----------------------------------------------------------------------------
resource "aws_lambda_function" "trigger" {
  function_name = local.function_name
  description   = "Receives one SQS FIFO message at a time and starts the Step Functions pipeline. Enforces serial execution."
  role          = var.trigger_role_arn

  filename         = data.archive_file.trigger_zip.output_path
  source_code_hash = data.archive_file.trigger_zip.output_base64sha256

  handler = "pipeline_trigger.lambda_handler"
  runtime = "python3.12"
  timeout = 30   # we don't wait for the pipeline — just start it. 30s is plenty.

  memory_size = 128

  # See variable docs. Default -1 = no reservation; opt in by setting to 1
  # when the account's Lambda concurrency quota is >= 11.
  reserved_concurrent_executions = var.reserved_concurrency

  environment {
    variables = {
      STATE_MACHINE_ARN = var.state_machine_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.trigger]
}

# -----------------------------------------------------------------------------
# Event source mapping — connects SQS to the Lambda.
#
# batch_size = 1 is critical: with FIFO, AWS will only deliver one message
# at a time per MessageGroupId anyway, but explicitly setting batch_size=1
# means even if we ever introduce multiple groups, each Lambda invocation
# handles exactly one message and decides exactly one execution.
# -----------------------------------------------------------------------------
resource "aws_lambda_event_source_mapping" "from_sqs" {
  event_source_arn = var.queue_arn
  function_name    = aws_lambda_function.trigger.arn

  batch_size                         = 1
  maximum_batching_window_in_seconds = 0  # don't wait to batch — start as soon as a message arrives

  # report_batch_item_failures lets Lambda mark individual records as
  # failed without re-delivering the whole batch. With batch_size=1 it
  # makes no difference, but it's free hygiene.
  function_response_types = ["ReportBatchItemFailures"]
}

# =============================================================================
# DISPATCHER LAMBDA — fires on Step Functions execution complete
# =============================================================================
#
# Why: when the trigger Lambda raises "busy", SQS hides the message for the
# full visibility_timeout. Without anything else, a queued file waits up to
# that long after the previous pipeline finishes. The dispatcher closes
# that gap — EventBridge wakes it the instant SFN ends, and it long-polls
# SQS for the next message.
#
# Shares the trigger Lambda's IAM role: identical permissions (SQS
# receive/delete/changeVis + SFN start/list/describe + Logs).
# =============================================================================

data "archive_file" "dispatcher_zip" {
  type        = "zip"
  source_file = var.dispatcher_source_file
  output_path = "${path.module}/build/dispatcher.zip"
}

resource "aws_cloudwatch_log_group" "dispatcher" {
  name              = "/aws/lambda/${local.name_prefix}-dispatcher"
  retention_in_days = 14
}

resource "aws_lambda_function" "dispatcher" {
  function_name = "${local.name_prefix}-dispatcher"
  description   = "On Step Functions execution complete, immediately dispatches the next queued SQS message."
  role          = var.trigger_role_arn

  filename         = data.archive_file.dispatcher_zip.output_path
  source_code_hash = data.archive_file.dispatcher_zip.output_base64sha256

  handler = "dispatcher.lambda_handler"
  runtime = "python3.12"

  # 30s: 20s SQS long-poll + ~5s headroom for SFN StartExecution + delete.
  # If something stalls past this, raising and letting EventBridge retry
  # is the right call — the dispatcher is idempotent.
  timeout     = 30
  memory_size = 128

  environment {
    variables = {
      STATE_MACHINE_ARN = var.state_machine_arn
      QUEUE_URL         = var.queue_url
    }
  }

  depends_on = [aws_cloudwatch_log_group.dispatcher]
}

# -----------------------------------------------------------------------------
# EventBridge rule — fires only on terminal Step Functions status changes
# for OUR state machine. Scoped tightly so this rule doesn't pick up
# unrelated state machines in the same account.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "sfn_execution_complete" {
  name        = "${local.name_prefix}-sfn-execution-complete"
  description = "Fires when our state machine execution reaches SUCCEEDED / FAILED / TIMED_OUT / ABORTED."

  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [var.state_machine_arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "dispatcher" {
  rule      = aws_cloudwatch_event_rule.sfn_execution_complete.name
  target_id = "dispatcher"
  arn       = aws_lambda_function.dispatcher.arn
}

# EventBridge needs explicit permission to invoke the Lambda — Lambda
# resource policies are separate from IAM roles.
resource "aws_lambda_permission" "eventbridge_invoke_dispatcher" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dispatcher.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sfn_execution_complete.arn
}
