###############################################################################
# modules/pipeline_trigger/outputs.tf
###############################################################################

output "function_arn" {
  description = "ARN of the pipeline-trigger Lambda."
  value       = aws_lambda_function.trigger.arn
}

output "function_name" {
  description = "Function name — useful for log lookups and CLI debugging."
  value       = aws_lambda_function.trigger.function_name
}

output "dispatcher_function_arn" {
  description = "ARN of the dispatcher Lambda (invoked by EventBridge on Step Functions execution complete)."
  value       = aws_lambda_function.dispatcher.arn
}

output "dispatcher_function_name" {
  description = "Dispatcher function name — useful for log lookups."
  value       = aws_lambda_function.dispatcher.function_name
}
