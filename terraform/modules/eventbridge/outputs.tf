###############################################################################
# modules/eventbridge/outputs.tf
###############################################################################

output "rule_arn" {
  description = "ARN of the EventBridge rule that triggers the pipeline."
  value       = aws_cloudwatch_event_rule.raw_object_created.arn
}

output "rule_name" {
  description = "Name of the EventBridge rule."
  value       = aws_cloudwatch_event_rule.raw_object_created.name
}
