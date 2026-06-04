###############################################################################
# modules/iam/outputs.tf
#
# Expose role ARNs so other modules (glue, step_functions, eventbridge,
# lambda, pipeline_trigger) can reference them when creating their own
# resources.
###############################################################################

output "glue_role_arn" {
  description = "ARN of the Glue execution role. Used by all 3 Glue jobs."
  value       = aws_iam_role.glue.arn
}

output "step_functions_role_arn" {
  description = "ARN of the Step Functions execution role."
  value       = aws_iam_role.step_functions.arn
}

# Note: no EventBridge role is exposed any more. With the SQS-serialized
# architecture, EventBridge pushes directly to SQS using the queue's
# resource policy, not an IAM role.

output "lambda_archive_role_arn" {
  description = "ARN of the archive Lambda execution role."
  value       = aws_iam_role.lambda_archive.arn
}

output "lambda_trigger_role_arn" {
  description = "ARN of the pipeline-trigger Lambda execution role."
  value       = aws_iam_role.lambda_trigger.arn
}
