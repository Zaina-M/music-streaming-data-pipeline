###############################################################################
# modules/monitoring/main.tf
#
# Two layers of failure detection, both feeding the same SNS topic:
#
#   1. Per-Glue-job alarm on the `glue.driver.aggregate.numFailedTasks`
#      metric. Catches failures inside an individual job.
#
#   2. State-machine-level alarm on Step Functions `ExecutionsFailed`.
#      Catches Lambda failures, timeouts, retry exhaustion — anything
#      the orchestrator marks as failed.
#
# Subscribed email confirms the union of both.
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# SNS topic — single fan-out point for all pipeline alerts.
# -----------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
}

# Email subscription. AWS sends a confirmation email to alert_email — the
# subscription stays in "Pending confirmation" until the recipient clicks
# the link. Terraform cannot do that for you.
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# -----------------------------------------------------------------------------
# Per-Glue-job failure alarm.
# for_each creates one alarm per job name. Using a map (job_name => job_name)
# rather than a list because for_each on lists with computed values can
# cause "for_each value cannot be known" errors during plan.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  for_each = toset(var.glue_job_names)

  alarm_name          = "${each.value}-failure"
  alarm_description   = "Fires when the Glue job ${each.value} reports failed tasks."
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  statistic           = "Sum"
  period              = 300  # 5-minute eval window
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  # treat_missing_data = "notBreaching" — most of the time the job isn't
  # running, so missing data is the normal state. Without this, the alarm
  # would flap into INSUFFICIENT_DATA constantly.
  treat_missing_data = "notBreaching"

  dimensions = {
    JobName = each.value
    # JobRunId = "ALL" — aggregates across every run of the job.
    JobRunId = "ALL"
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}

# -----------------------------------------------------------------------------
# State-machine-level failure alarm.
# Catches anything Step Functions marks as failed (timeouts, Lambda errors,
# Glue errors that retries didn't recover from).
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "sfn_execution_failed" {
  alarm_name          = "${local.name_prefix}-pipeline-failed"
  alarm_description   = "Fires when a Step Functions execution of the ETL pipeline fails."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.state_machine_name}"
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}

# Look up region + account so we can construct the state machine ARN
# without forcing the caller to pass it in.
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------
# Dead-letter queue depth alarm.
#
# The DLQ is the "drain" for messages that the trigger Lambda can't
# successfully process after maxReceiveCount retries (see sqs module).
# Healthy state: empty. Any message landing here means a real failure
# that no automatic mechanism can recover from — a human must look.
#
# We alarm on ApproximateNumberOfMessagesVisible > 0, sampled every
# minute. That's intentionally sensitive: one stuck message warrants
# investigation in a serial-execution pipeline like this one.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name          = "${local.name_prefix}-dlq-not-empty"
  alarm_description   = "Fires when the trigger DLQ has any messages — indicates a pipeline trigger that failed beyond retries."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"  # Max is more reliable than Average for DLQ depth
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  # SQS publishes metrics every 5 min, so periods between data points are
  # routine. notBreaching keeps the alarm calm during normal idle stretches.
  treat_missing_data = "notBreaching"

  dimensions = {
    QueueName = var.dlq_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]  # also notify when it drains
}
