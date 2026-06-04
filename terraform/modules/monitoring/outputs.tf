###############################################################################
# modules/monitoring/outputs.tf
###############################################################################

output "sns_topic_arn" {
  description = "ARN of the alerts SNS topic — surfaced as a root output."
  value       = aws_sns_topic.alerts.arn
}
