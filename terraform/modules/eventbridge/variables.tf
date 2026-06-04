###############################################################################
# modules/eventbridge/variables.tf
###############################################################################

variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "raw_bucket_name" {
  type        = string
  description = "Name of the raw bucket whose PutObject events should trigger the pipeline."
}

variable "sqs_queue_arn" {
  type        = string
  description = "ARN of the SQS FIFO queue events should be pushed to. The pipeline_trigger Lambda consumes from this queue."
}
