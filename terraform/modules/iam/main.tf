###############################################################################
# modules/iam/main.tf
#
# Creates four distinct IAM roles — one per service — following least
# privilege. Reusing a single role across services is a common antipattern
# because it grants every service the union of every other service's
# permissions, defeating the purpose of IAM.
#
# Roles created:
#   1. Glue execution role         (run validate/transform/load jobs)
#   2. Step Functions role         (start Glue jobs, invoke Lambda)
#   3. EventBridge role            (start Step Functions execution)
#   4. Lambda execution role       (archive — move file from raw → archive)
###############################################################################

locals {
  # Convenience prefix so every resource name follows the same pattern,
  # e.g. "music-streaming-dev-glue-role"
  name_prefix = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# 1. GLUE EXECUTION ROLE
# -----------------------------------------------------------------------------
# Assumed by AWS Glue when running validate/transform/load jobs.
# Needs:
#   - S3 read on scripts (to fetch the .py file)
#   - S3 read on raw + reference (input data)
#   - S3 write on processed (Parquet output)
#   - DynamoDB write on the 3 KPI tables
#   - CloudWatch Logs (via AWS-managed AWSGlueServiceRole)
# -----------------------------------------------------------------------------

# Trust policy: only the Glue service can assume this role.
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  description        = "Execution role for the validate/transform/load Glue jobs."
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

# Attach the AWS-managed policy that grants the baseline permissions every
# Glue job needs (CloudWatch Logs, Glue catalog reads, etc.). Building this
# from scratch is error-prone; the managed policy is the standard approach.
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom inline policy for project-specific S3 + DynamoDB access. Scoped
# strictly to our buckets and tables — no wildcard "s3:*" or "dynamodb:*".
data "aws_iam_policy_document" "glue_custom" {
  # S3 — read scripts, read raw + reference, read+write processed.
  statement {
    sid    = "S3ReadScripts"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      var.scripts_bucket_arn,
      "${var.scripts_bucket_arn}/*"
    ]
  }

  statement {
    sid    = "S3ReadRaw"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      var.raw_bucket_arn,
      "${var.raw_bucket_arn}/*"
    ]
  }

  # Read + write the processed bucket. Covers TWO things:
  #   1. enriched_streams/  — Parquet Transform writes and Load reads.
  #   2. manifests/         — the dates-touched JSON Transform writes and
  #                           Load reads (the partition-pruning hand-off).
  # If you ever narrow this to a prefix, keep BOTH paths in scope.
  statement {
    sid    = "S3WriteProcessed"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = [
      var.processed_bucket_arn,
      "${var.processed_bucket_arn}/*"
    ]
  }

  # DynamoDB — only the actions Load needs. No Scan/Query/DeleteItem.
  statement {
    sid    = "DynamoDBWrite"
    effect = "Allow"
    actions = [
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DescribeTable"
    ]
    resources = var.dynamodb_table_arns
  }
}

resource "aws_iam_policy" "glue_custom" {
  name        = "${local.name_prefix}-glue-custom-policy"
  description = "Project-specific S3 + DynamoDB permissions for Glue jobs."
  policy      = data.aws_iam_policy_document.glue_custom.json
}

resource "aws_iam_role_policy_attachment" "glue_custom" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_custom.arn
}

# -----------------------------------------------------------------------------
# 2. STEP FUNCTIONS EXECUTION ROLE
# -----------------------------------------------------------------------------
# Assumed by Step Functions to orchestrate Glue jobs and invoke the
# archive Lambda. Cannot use a managed policy here — Step Functions doesn't
# ship one that fits this exact need.
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${local.name_prefix}-sfn-role"
  description        = "Allows Step Functions to start Glue jobs and invoke the archive Lambda."
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
}

# Step Functions needs to:
#   - Start, monitor, and stop Glue jobs
#   - Invoke Lambda (for the archive step)
#   - Write logs and send to CloudWatch
data "aws_iam_policy_document" "sfn_custom" {
  statement {
    sid    = "InvokeGlueJobs"
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun"
    ]
    # Glue job ARNs are constructed at apply time; '*' is acceptable here
    # because the role is already tightly scoped via its trust policy, but
    # in production you'd want to pass the job ARNs in and constrain this.
    resources = ["*"]
  }

  statement {
    sid    = "InvokeArchiveLambda"
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    # Restrict to project-prefixed Lambdas only. Cleaner than '*'.
    resources = [
      "arn:aws:lambda:*:*:function:${local.name_prefix}-*"
    ]
  }

  # CloudWatch Logs delivery for Step Functions' own execution history.
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "sfn_custom" {
  name   = "${local.name_prefix}-sfn-policy"
  policy = data.aws_iam_policy_document.sfn_custom.json
}

resource "aws_iam_role_policy_attachment" "sfn_custom" {
  role       = aws_iam_role.step_functions.name
  policy_arn = aws_iam_policy.sfn_custom.arn
}

# -----------------------------------------------------------------------------
# 3. EVENTBRIDGE → SQS
# -----------------------------------------------------------------------------
# In the SQS-serialized architecture, EventBridge no longer needs an IAM
# role — it pushes to SQS using a resource policy on the queue (granted
# in the sqs module). No EventBridge role is created here.

# -----------------------------------------------------------------------------
# 4. LAMBDA EXECUTION ROLE (Archive function)
# -----------------------------------------------------------------------------
# The archive Lambda copies the raw stream file to the archive bucket and
# deletes the original. Needs:
#   - S3 GetObject + DeleteObject on raw
#   - S3 PutObject on archive
#   - CloudWatch Logs (via managed policy)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_archive" {
  name               = "${local.name_prefix}-lambda-archive-role"
  description        = "Execution role for the archive Lambda (move file raw -> archive)."
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Baseline Lambda logging permissions — using the AWS-managed policy
# is the documented best practice.
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_archive.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_archive_custom" {
  # Read + delete from raw bucket (the "move" is really copy-then-delete).
  statement {
    sid    = "S3ReadDeleteRaw"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:DeleteObject"
    ]
    resources = ["${var.raw_bucket_arn}/*"]
  }

  # Write to archive bucket.
  statement {
    sid    = "S3WriteArchive"
    effect = "Allow"
    actions = [
      "s3:PutObject"
    ]
    resources = ["${var.archive_bucket_arn}/*"]
  }
}

resource "aws_iam_policy" "lambda_archive_custom" {
  name   = "${local.name_prefix}-lambda-archive-policy"
  policy = data.aws_iam_policy_document.lambda_archive_custom.json
}

resource "aws_iam_role_policy_attachment" "lambda_archive_custom" {
  role       = aws_iam_role.lambda_archive.name
  policy_arn = aws_iam_policy.lambda_archive_custom.arn
}

# -----------------------------------------------------------------------------
# 5. LAMBDA EXECUTION ROLE (Pipeline trigger function)
# -----------------------------------------------------------------------------
# The trigger Lambda consumes SQS FIFO messages and starts Step Functions
# executions. Needs:
#   - SQS Receive / Delete / GetQueueAttributes / ChangeMessageVisibility
#     on the FIFO queue (event source mapping requires these).
#   - states:StartExecution + states:ListExecutions on the state machine.
#   - CloudWatch Logs (via managed AWSLambdaBasicExecutionRole).
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda_trigger" {
  name               = "${local.name_prefix}-lambda-trigger-role"
  description        = "Execution role for the pipeline-trigger Lambda."
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Baseline logging (same managed policy as the archive Lambda).
resource "aws_iam_role_policy_attachment" "lambda_trigger_basic_execution" {
  role       = aws_iam_role.lambda_trigger.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_trigger_custom" {
  # SQS — these are the actions the Lambda event source mapping requires.
  # AWS will silently refuse to attach the trigger if any are missing.
  statement {
    sid    = "SQSConsume"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [var.sqs_queue_arn]
  }

  # Step Functions — start new executions and list current ones (the
  # Lambda checks for running executions before starting a new one).
  statement {
    sid    = "StepFunctions"
    effect = "Allow"
    actions = [
      "states:StartExecution",
      "states:ListExecutions",
      "states:DescribeExecution",
    ]
    # '*' is acceptable here because the role's trust policy already
    # restricts to Lambda. For stricter prod-grade scoping, pass in the
    # state machine ARN and constrain this resource list.
    resources = ["*"]
  }
}

resource "aws_iam_policy" "lambda_trigger_custom" {
  name   = "${local.name_prefix}-lambda-trigger-policy"
  policy = data.aws_iam_policy_document.lambda_trigger_custom.json
}

resource "aws_iam_role_policy_attachment" "lambda_trigger_custom" {
  role       = aws_iam_role.lambda_trigger.name
  policy_arn = aws_iam_policy.lambda_trigger_custom.arn
}
