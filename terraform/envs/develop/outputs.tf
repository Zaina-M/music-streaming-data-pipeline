###############################################################################
# outputs.tf
#
# Surface key resource identifiers after `terraform apply`. These are useful
# for: (1) debugging, (2) CI/CD pipelines that need ARNs, (3) sanity-checking
# what was actually created.
###############################################################################

output "raw_bucket_name" {
  description = "S3 bucket where incoming stream CSV files are dropped. Drop a file here to trigger the pipeline."
  value       = module.s3.raw_bucket_name
}

output "processed_bucket_name" {
  description = "S3 bucket holding the Parquet output of the Transform job."
  value       = module.s3.processed_bucket_name
}

output "archive_bucket_name" {
  description = "S3 bucket where successfully-processed stream files are moved."
  value       = module.s3.archive_bucket_name
}

output "scripts_bucket_name" {
  description = "S3 bucket hosting the Glue job Python/PySpark scripts."
  value       = module.s3.scripts_bucket_name
}

output "step_function_arn" {
  description = "ARN of the orchestrating Step Functions state machine. Trigger manually with: aws stepfunctions start-execution --state-machine-arn <this>"
  value       = module.step_functions.state_machine_arn
}

output "sns_topic_arn" {
  description = "SNS topic ARN for Glue failure alerts. Confirm your email subscription before relying on it."
  value       = module.monitoring.sns_topic_arn
}

output "dynamodb_table_names" {
  description = "Names of the 3 KPI DynamoDB tables written by the Load Glue job."
  value       = module.dynamodb.table_names
}

output "trigger_queue_url" {
  description = "URL of the SQS FIFO queue that serializes pipeline triggers. Useful for direct CLI sends during testing."
  value       = module.sqs.queue_url
}

output "trigger_dlq_arn" {
  description = "Dead-letter queue ARN — inspect this if messages stop getting processed."
  value       = module.sqs.dlq_arn
}
