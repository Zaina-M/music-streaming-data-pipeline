###############################################################################
# modules/dynamodb/variables.tf
###############################################################################

variable "project_name" {
  description = "Project prefix for table names."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
}
