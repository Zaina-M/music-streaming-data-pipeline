###############################################################################
# modules/sqs/outputs.tf
###############################################################################

output "queue_arn" {
  description = "ARN of the main FIFO trigger queue. EventBridge targets this; trigger Lambda consumes from it."
  value       = aws_sqs_queue.pipeline_trigger.arn
}

output "queue_url" {
  description = "URL of the trigger FIFO queue."
  value       = aws_sqs_queue.pipeline_trigger.url
}

output "queue_name" {
  description = "Name of the trigger FIFO queue (.fifo suffix included)."
  value       = aws_sqs_queue.pipeline_trigger.name
}

output "dlq_arn" {
  description = "ARN of the dead-letter queue — useful for surfacing in monitoring dashboards."
  value       = aws_sqs_queue.dlq.arn
}

output "dlq_name" {
  description = "Name of the dead-letter queue — required for CloudWatch metric dimensions (which use QueueName, not ARN)."
  value       = aws_sqs_queue.dlq.name
}
