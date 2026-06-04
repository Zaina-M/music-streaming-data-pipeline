###############################################################################
# modules/step_functions/outputs.tf
###############################################################################

output "state_machine_arn" {
  description = "ARN of the pipeline state machine — used as the EventBridge target."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "state_machine_name" {
  description = "State machine name — used by CloudWatch alarms in the monitoring module."
  value       = aws_sfn_state_machine.pipeline.name
}
