###############################################################################
# modules/glue/variables.tf
###############################################################################

variable "project_name" {
  description = "Project prefix for Glue resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
}

variable "glue_role_arn" {
  description = "ARN of the Glue execution role (created by the IAM module)."
  type        = string
}

variable "scripts_bucket" {
  description = "Name of the S3 bucket that will host the .py script files."
  type        = string
}

variable "scripts_local_path" {
  description = "Local filesystem path containing validate.py, transform.py, load.py."
  type        = string
}
