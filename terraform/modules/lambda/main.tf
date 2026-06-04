###############################################################################
# modules/lambda/main.tf
#
# Provisions the archive Lambda function. The archive_file data source
# zips the local archive.py at apply time — no separate build step, no
# stale artifacts, and the zip is regenerated automatically whenever the
# source file changes (filebase64sha256 is the trigger).
###############################################################################

locals {
  name_prefix   = "${var.project_name}-${var.environment}"
  function_name = "${local.name_prefix}-archive"
}

# Zip the source file into a deployment package. The output path lives
# under .terraform/ so it's excluded from version control via .gitignore.
data "archive_file" "archive_zip" {
  type        = "zip"
  source_file = var.source_file
  output_path = "${path.module}/build/archive.zip"
}

# CloudWatch log group, created explicitly so we can control retention.
# If Lambda creates the log group implicitly on first invoke, it defaults
# to "never expire" — a quiet long-term cost.
resource "aws_cloudwatch_log_group" "archive" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "archive" {
  function_name = local.function_name
  description   = "Moves processed stream files from the raw bucket to the archive bucket."
  role          = var.lambda_role_arn

  # Source archive — Terraform tracks the hash so any change to archive.py
  # triggers a new deployment.
  filename         = data.archive_file.archive_zip.output_path
  source_code_hash = data.archive_file.archive_zip.output_base64sha256

  # Handler format: <filename-without-.py>.<function-name>
  handler = "archive.lambda_handler"

  # Python 3.12 is current AWS-supported LTS at time of writing.
  # boto3 is included automatically in the Lambda runtime.
  runtime = "python3.12"

  # Default 3-second timeout is too short — copying a multi-MB CSV plus
  # the delete can take longer. 60s gives generous headroom.
  timeout = 60

  # 128 MB is the floor and is plenty for an S3 copy/delete. RAM only
  # affects CPU/network for Lambda, and we're I/O bound, not compute bound.
  memory_size = 128

  # Ensure the log group exists before the function — otherwise the first
  # invocation will race-create it without our retention policy.
  depends_on = [aws_cloudwatch_log_group.archive]
}
