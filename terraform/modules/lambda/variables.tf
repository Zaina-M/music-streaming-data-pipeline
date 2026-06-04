###############################################################################
# modules/lambda/variables.tf
###############################################################################

variable "project_name" {
  description = "Project prefix for the function name."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
}

variable "lambda_role_arn" {
  description = "ARN of the Lambda execution role (created by the IAM module)."
  type        = string
}

variable "source_file" {
  description = "Local path to the archive.py source file. Terraform zips this at apply time."
  type        = string
}
