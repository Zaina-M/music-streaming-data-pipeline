###############################################################################
# modules/s3/variables.tf
###############################################################################

variable "project_name" {
  description = "Project prefix used to compose bucket names. Must be globally unique once combined with environment + suffix."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
}

variable "reference_data_path" {
  description = "Local path to the directory containing songs.csv and users.csv. Files are uploaded to the raw bucket under a reference/ prefix during apply."
  type        = string
}
