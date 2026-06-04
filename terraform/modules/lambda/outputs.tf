###############################################################################
# modules/lambda/outputs.tf
###############################################################################

output "function_arn" {
  description = "ARN of the archive Lambda — passed to Step Functions so it can invoke it."
  value       = aws_lambda_function.archive.arn
}

output "function_name" {
  description = "Function name — useful for CloudWatch metric filters and CLI invocation."
  value       = aws_lambda_function.archive.function_name
}
