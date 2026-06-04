###############################################################################
# modules/step_functions/variables.tf
###############################################################################

variable "project_name" {
  type        = string
  description = "Project prefix used in the state machine name."
}

variable "environment" {
  type        = string
  description = "Deployment environment."
}

variable "sfn_role_arn" {
  type        = string
  description = "ARN of the Step Functions execution role."
}

# --- Glue job references ---------------------------------------------------
variable "validate_job_name" {
  type        = string
  description = "Name of the validate Glue job."
}

variable "transform_job_name" {
  type        = string
  description = "Name of the transform Glue job."
}

variable "load_job_name" {
  type        = string
  description = "Name of the load Glue job."
}

# --- Lambda + bucket references for the archive step -----------------------
variable "archive_lambda_arn" {
  type        = string
  description = "ARN of the archive Lambda function."
}

variable "raw_bucket" {
  type        = string
  description = "Raw bucket name — passed to Glue jobs and to the archive Lambda."
}

variable "processed_bucket" {
  type        = string
  description = "Processed bucket name — passed to Transform + Load."
}

variable "archive_bucket" {
  type        = string
  description = "Archive bucket name — passed to the archive Lambda."
}

variable "songs_reference_key" {
  type        = string
  description = "S3 key of the songs.csv reference file."
}

variable "users_reference_key" {
  type        = string
  description = "S3 key of the users.csv reference file."
}

# --- DynamoDB table names passed to the Load job ---------------------------
variable "dynamodb_table_names" {
  type        = list(string)
  description = "[daily_genre, top_songs, top_genres] — in that order."
}
