###############################################################################
# modules/iam/variables.tf
#
# IAM module accepts the ARNs of resources it needs to grant access to.
# We don't hardcode bucket/table ARNs because they're constructed by other
# modules — passing them in keeps this module reusable and decoupled.
###############################################################################

variable "project_name" {
  description = "Project prefix used to name roles and policies."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod). Embedded in role names."
  type        = string
}

# S3 bucket ARNs the various services need access to. Passed in from the
# root module after the S3 module creates them.
variable "raw_bucket_arn" {
  description = "ARN of the raw stream bucket (read + delete by Lambda, read by Glue)."
  type        = string
}

variable "processed_bucket_arn" {
  description = "ARN of the processed Parquet bucket (read+write by Glue)."
  type        = string
}

variable "archive_bucket_arn" {
  description = "ARN of the archive bucket (write by Lambda)."
  type        = string
}

variable "scripts_bucket_arn" {
  description = "ARN of the scripts bucket (read by Glue jobs to fetch their .py files)."
  type        = string
}

variable "dynamodb_table_arns" {
  description = "List of the 3 KPI DynamoDB table ARNs the Load Glue job writes to."
  type        = list(string)
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS FIFO trigger queue. The trigger Lambda needs receive/delete permissions on it."
  type        = string
}
