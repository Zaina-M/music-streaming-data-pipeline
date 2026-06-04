###############################################################################
# variables.tf
#
# Root-level input variables. Everything that could conceivably change
# between environments (dev/staging/prod) lives here, never hardcoded in
# resource blocks. Defaults are sensible-for-dev; override in tfvars files.
###############################################################################

variable "aws_region" {
  description = "AWS region where all resources will be deployed."
  type        = string
  default     = "eu-west-1"
}

variable "aws_profile" {
  description = "Named AWS CLI profile used for authentication. Leave empty to use the default credential chain (env vars, instance role, etc.)."
  type        = string
  default     = "default"
}

variable "project_name" {
  description = "Short project identifier used as a prefix for all resource names. Keep it lowercase and hyphen-separated — S3 buckets and many other resources are case-sensitive."
  type        = string
  default     = "music-streaming"

  # Guardrail: enforce naming convention so we don't accidentally create
  # resources that violate AWS naming rules (especially S3 buckets).
  validation {
    condition     = can(regex("^[a-z0-9-]{3,30}$", var.project_name))
    error_message = "project_name must be 3-30 chars, lowercase letters, digits, and hyphens only."
  }
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod). Used in tags and resource names to keep environments isolated."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "owner" {
  description = "Team or individual responsible for these resources. Surfaces in default tags for cost allocation."
  type        = string
  default     = "data-engineering"
}

variable "alert_email" {
  description = "Email address that receives SNS alerts when a Glue job fails. Must be confirmed manually after Terraform applies (AWS sends a subscription email)."
  type        = string
  # No default — the user must supply this so we don't accidentally spam an
  # old address. Terraform will prompt if it's missing.
}

variable "reference_data_path" {
  description = "Local directory containing songs.csv and users.csv reference files. Resolved relative to envs/develop/, so three levels up to LAB_1/. Uploaded to S3 during apply."
  type        = string
  default     = "../../../Project 1 -- ETL with s3, dynamo and glue/data"
}
