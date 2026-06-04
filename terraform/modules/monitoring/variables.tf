###############################################################################
# modules/monitoring/variables.tf
###############################################################################

variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "alert_email" {
  description = "Email address subscribed to the SNS topic. After apply, AWS sends a confirmation email — you must click the link before alerts are delivered."
  type        = string
}

variable "glue_job_names" {
  description = "List of all Glue job names. One CloudWatch alarm is created per job to detect FAILED runs."
  type        = list(string)
}

variable "state_machine_name" {
  description = "Step Functions state machine name — used for the pipeline-level ExecutionsFailed alarm."
  type        = string
}

variable "dlq_name" {
  description = "Name of the SQS dead-letter queue. A CloudWatch alarm fires when any message lands in it — the strongest signal that the trigger pipeline is stuck."
  type        = string
}
