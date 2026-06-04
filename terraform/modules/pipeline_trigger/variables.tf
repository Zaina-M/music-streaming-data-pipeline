###############################################################################
# modules/pipeline_trigger/variables.tf
###############################################################################

variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "trigger_role_arn" {
  description = "Lambda execution role ARN (from IAM module) — grants StartExecution + ListExecutions + SQS receive + logs."
  type        = string
}

variable "source_file" {
  description = "Local path to pipeline_trigger.py."
  type        = string
}

variable "state_machine_arn" {
  description = "ARN of the Step Functions state machine the Lambda should trigger."
  type        = string
}

variable "queue_arn" {
  description = "ARN of the SQS FIFO queue this Lambda consumes from."
  type        = string
}

variable "queue_url" {
  description = "URL of the SQS FIFO queue. Required by the dispatcher Lambda for ReceiveMessage / DeleteMessage calls — those APIs take URL, not ARN."
  type        = string
}

variable "dispatcher_source_file" {
  description = "Local path to dispatcher.py. Zipped at apply time and deployed as a second Lambda that fires on Step Functions execution complete."
  type        = string
}

variable "reserved_concurrency" {
  description = <<-EOT
    Reserved concurrent executions for the trigger Lambda. -1 means "no
    reservation, use the shared unreserved pool" (Terraform default).

    Setting this to 1 adds a third belt-and-braces serialization
    guarantee, but AWS enforces that your account's UnreservedConcurrentExecutions
    stays >= 10. Accounts with a default Lambda quota of 10 (new / sandbox
    accounts) cannot reserve any concurrency — keep this at -1 there.

    Accounts with the standard quota of 1000 can safely set this to 1.

    Layer 2 isn't strictly needed for correctness: SQS FIFO with a shared
    MessageGroupId already guarantees one message in flight at a time, and
    the Lambda's ListExecutions check catches manual triggers. Layer 2
    only guards against AWS itself spinning up parallel Lambda instances,
    which won't happen with batch_size=1 on a FIFO source anyway.
  EOT
  type        = number
  default     = -1
}
