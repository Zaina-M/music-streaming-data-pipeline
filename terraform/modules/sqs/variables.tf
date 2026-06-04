###############################################################################
# modules/sqs/variables.tf
###############################################################################

variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "visibility_timeout_seconds" {
  description = <<-EOT
    How long a received message stays invisible to other consumers.

    This is also the maximum wait between "busy" retry cycles for the
    trigger Lambda. Lower = snappier dispatch when the previous pipeline
    finishes, more retry calls during long runs (each retry costs
    well under a cent). Higher = fewer retries, longer tail latency.

    With the dispatcher Lambda (EventBridge on SFN execution complete)
    handling the "fast path" out of busy state, 60s is a good middle
    ground — keeps retry pressure manageable while ensuring the
    dispatcher's 20s long-poll has a decent chance of catching the
    message right when SQS releases it.
  EOT
  type        = number
  default     = 60
}

variable "max_receive_count" {
  description = "Number of times a message can be received before being moved to the dead-letter queue. High enough to absorb back-pressure but low enough that truly-stuck messages get flagged."
  type        = number
  default     = 20
}
