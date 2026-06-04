###############################################################################
# envs/develop/main.tf — Develop environment root
#
# This is the entry point you run `terraform apply` from for the develop
# environment. It composes the shared sub-modules (under ../../modules/)
# into a serialized, event-driven pipeline.
#
# Module sources use `../../modules/...` because Terraform resolves them
# relative to THIS file. Local script paths use `${path.module}/../../`
# for the same reason.
#
# Terraform builds the dependency graph from each module's inputs, so
# this file's order is for HUMAN reading, not execution.
#
# Order (read top-to-bottom for a logical story):
#   1. S3              — storage layer
#   2. DynamoDB        — KPI sinks
#   3. SQS             — FIFO serialization queue (between EventBridge and pipeline)
#   4. IAM             — least-privilege roles (depends on S3, DynamoDB, SQS)
#   5. Lambda (archive) — moves processed files raw → archive
#   6. Glue            — 3 jobs that do the ETL work
#   7. Step Functions  — orchestrates the 3 Glue jobs + archive Lambda
#   8. Pipeline Trigger — Lambda that consumes SQS and starts Step Functions
#                          (this is what enforces serial execution)
#   9. EventBridge     — S3 events → SQS FIFO queue
#   10. Monitoring     — CloudWatch alarms + SNS
###############################################################################

# --- 1. S3 buckets ----------------------------------------------------------
module "s3" {
  source = "../../modules/s3"

  project_name        = var.project_name
  environment         = var.environment
  reference_data_path = var.reference_data_path
}

# --- 2. DynamoDB tables -----------------------------------------------------
module "dynamodb" {
  source = "../../modules/dynamodb"

  project_name = var.project_name
  environment  = var.environment
}

# --- 3. SQS FIFO queue + DLQ -----------------------------------------------
# Created BEFORE iam so the iam module can scope the trigger Lambda's
# permissions to this specific queue ARN.
module "sqs" {
  source = "../../modules/sqs"

  project_name = var.project_name
  environment  = var.environment
}

# --- 4. IAM roles -----------------------------------------------------------
module "iam" {
  source = "../../modules/iam"

  project_name = var.project_name
  environment  = var.environment

  raw_bucket_arn       = module.s3.raw_bucket_arn
  processed_bucket_arn = module.s3.processed_bucket_arn
  archive_bucket_arn   = module.s3.archive_bucket_arn
  scripts_bucket_arn   = module.s3.scripts_bucket_arn
  dynamodb_table_arns  = module.dynamodb.table_arns
  sqs_queue_arn        = module.sqs.queue_arn
}

# --- 5. Lambda (archive function) ------------------------------------------
module "lambda" {
  source = "../../modules/lambda"

  project_name    = var.project_name
  environment     = var.environment
  lambda_role_arn = module.iam.lambda_archive_role_arn
  source_file     = "${path.module}/../../scripts/lambda/archive.py"
}

# --- 6. Glue jobs + catalog ------------------------------------------------
module "glue" {
  source = "../../modules/glue"

  project_name       = var.project_name
  environment        = var.environment
  glue_role_arn      = module.iam.glue_role_arn
  scripts_bucket     = module.s3.scripts_bucket_name
  scripts_local_path = "${path.module}/../../scripts/glue"
}

# --- 7. Step Functions state machine ---------------------------------------
module "step_functions" {
  source = "../../modules/step_functions"

  project_name = var.project_name
  environment  = var.environment

  sfn_role_arn       = module.iam.step_functions_role_arn
  validate_job_name  = module.glue.validate_job_name
  transform_job_name = module.glue.transform_job_name
  load_job_name      = module.glue.load_job_name

  archive_lambda_arn   = module.lambda.function_arn
  raw_bucket           = module.s3.raw_bucket_name
  processed_bucket     = module.s3.processed_bucket_name
  archive_bucket       = module.s3.archive_bucket_name
  songs_reference_key  = module.s3.songs_reference_key
  users_reference_key  = module.s3.users_reference_key
  dynamodb_table_names = module.dynamodb.table_names
}

# --- 8. Pipeline trigger Lambda --------------------------------------------
# Bridges SQS FIFO and Step Functions. reserved_concurrent_executions=1
# (set inside the module) is the third layer of our serial-execution
# guarantee (after FIFO + shared MessageGroupId).
module "pipeline_trigger" {
  source = "../../modules/pipeline_trigger"

  project_name           = var.project_name
  environment            = var.environment
  trigger_role_arn       = module.iam.lambda_trigger_role_arn
  source_file            = "${path.module}/../../scripts/lambda/pipeline_trigger.py"
  dispatcher_source_file = "${path.module}/../../scripts/lambda/dispatcher.py"
  state_machine_arn      = module.step_functions.state_machine_arn
  queue_arn              = module.sqs.queue_arn
  queue_url              = module.sqs.queue_url
}

# --- 9. EventBridge rule ---------------------------------------------------
# Targets the SQS FIFO queue (not Step Functions directly).
module "eventbridge" {
  source = "../../modules/eventbridge"

  project_name    = var.project_name
  environment     = var.environment
  raw_bucket_name = module.s3.raw_bucket_name
  sqs_queue_arn   = module.sqs.queue_arn
}

# --- 10. Monitoring (CloudWatch alarms + SNS) ------------------------------
module "monitoring" {
  source = "../../modules/monitoring"

  project_name       = var.project_name
  environment        = var.environment
  alert_email        = var.alert_email
  glue_job_names     = module.glue.all_job_names
  state_machine_name = module.step_functions.state_machine_name
  dlq_name           = module.sqs.dlq_name
}
