###############################################################################
# modules/step_functions/main.tf
#
# Provisions the orchestrating Standard state machine. Flow:
#
#   [Validate Glue] → [Transform Glue] → [Load Glue] → [Archive Lambda]
#                ↓                  ↓             ↓              ↓
#               Fail              Fail          Fail            Fail
#
# Each step's `Catch` block routes to a NotifyFailure state. CloudWatch
# alarms in the monitoring module pick up Glue job failures via Glue's
# own metrics — Step Functions failure routing is the safety net.
#
# State machine type: STANDARD (vs EXPRESS).
#   Standard = at-least-once, durable, up to 1 year duration. Right for
#              long ETL with Glue waits.
#   Express  = at-most-once, lower latency, 5-minute cap. Wrong fit here.
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  # DynamoDB table name shorthands — destructure the list for readability
  # in the state machine definition below.
  table_daily_genre = var.dynamodb_table_names[0]
  table_top_songs   = var.dynamodb_table_names[1]
  table_top_genres  = var.dynamodb_table_names[2]
}

# -----------------------------------------------------------------------------
# CloudWatch log group for the state machine. Explicit creation lets us
# control retention; the alternative is letting Step Functions auto-create
# a "never expire" log group.
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-pipeline"
  retention_in_days = 14
}

# -----------------------------------------------------------------------------
# State machine definition — JSON document, built with jsonencode() so we
# get Terraform variable interpolation and no quoting headaches.
#
# Input to the state machine (provided by EventBridge):
#   {
#     "detail": {
#       "bucket": { "name": "<raw bucket>" },
#       "object": { "key":  "<uploaded file key>" }
#     }
#   }
#
# We thread bucket+key through every step using JSONPath ($.) selectors.
# -----------------------------------------------------------------------------
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name_prefix}-pipeline"
  role_arn = var.sfn_role_arn

  # Send execution history to CloudWatch for debugging.
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  definition = jsonencode({
    Comment = "Music streaming ETL: Validate → Transform → Load → Archive"
    StartAt = "Validate"
    States = {

      # ------------------------------------------------------------------
      # STATE: Validate
      # Runs the validate Glue job synchronously (.sync waits for completion).
      # Passes the source bucket + key as Glue --arg values.
      # ------------------------------------------------------------------
      Validate = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.validate_job_name
          Arguments = {
            "--raw_bucket.$" = "$.detail.bucket.name"
            "--object_key.$" = "$.detail.object.key"
          }
        }
        # ResultPath: discard Glue's output but keep the original event so
        # downstream states still see the bucket/key in the same place.
        ResultPath = null
        Next       = "Transform"
        Retry = [
          # Concurrent-runs error: Glue defaults to max 1 run per job. If a
          # previous execution is still wrapping up when we StartJobRun,
          # we get this. Retry with exponential backoff up to 5 times
          # (covers >5 min of overlap) before giving up.
          {
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 30
            MaxAttempts     = 5
            BackoffRate     = 2.0
          },
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts     = 1
            BackoffRate     = 2.0
          }
        ]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # ------------------------------------------------------------------
      # STATE: Transform
      # Runs the transform Glue job — joins streams with reference data
      # and writes Parquet.
      #
      # --manifest_key is built from the inbound object_key with
      # States.Format. Both Transform and Load receive the SAME value;
      # Transform writes the manifest, Load reads it. Format:
      #     manifests/<object_key>.json
      # e.g. object_key "streams/streams1.csv"
      #      → manifest "manifests/streams/streams1.csv.json"
      # ------------------------------------------------------------------
      Transform = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.transform_job_name
          Arguments = {
            "--raw_bucket.$"      = "$.detail.bucket.name"
            "--object_key.$"      = "$.detail.object.key"
            "--songs_key"         = var.songs_reference_key
            "--users_key"         = var.users_reference_key
            "--processed_bucket"  = var.processed_bucket
            "--manifest_key.$"    = "States.Format('manifests/{}.json', $.detail.object.key)"
          }
        }
        ResultPath = null
        Next       = "Load"
        Retry = [
          {
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 30
            MaxAttempts     = 5
            BackoffRate     = 2.0
          },
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 1
            BackoffRate     = 2.0
          }
        ]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # ------------------------------------------------------------------
      # STATE: Load
      # Reads the manifest written by Transform, filters the enriched
      # Parquet to ONLY the dates that file contributed, and upserts
      # KPIs for each into DynamoDB. Partition pruning keeps Load's
      # runtime constant as historical data grows.
      # ------------------------------------------------------------------
      Load = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.load_job_name
          Arguments = {
            "--processed_bucket"  = var.processed_bucket
            "--manifest_key.$"    = "States.Format('manifests/{}.json', $.detail.object.key)"
            "--table_daily_genre" = local.table_daily_genre
            "--table_top_songs"   = local.table_top_songs
            "--table_top_genres"  = local.table_top_genres
          }
        }
        ResultPath = null
        Next       = "Archive"
        Retry = [
          {
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 30
            MaxAttempts     = 5
            BackoffRate     = 2.0
          },
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 1
            BackoffRate     = 2.0
          }
        ]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # ------------------------------------------------------------------
      # STATE: Archive
      # Invokes the archive Lambda to move the original raw file into
      # the archive bucket. Only reached if all Glue jobs succeeded.
      # ------------------------------------------------------------------
      Archive = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.archive_lambda_arn
          Payload = {
            "raw_bucket"        = var.raw_bucket
            "archive_bucket"    = var.archive_bucket
            "object_key.$"      = "$.detail.object.key"
          }
        }
        ResultPath = null
        End        = true
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.TooManyRequestsException"]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NotifyFailure"
          ResultPath  = "$.error"
        }]
      }

      # ------------------------------------------------------------------
      # STATE: NotifyFailure (terminal)
      # Marks the execution as Failed. CloudWatch metric
      # ExecutionsFailed on this state machine drives the SNS alert in
      # the monitoring module.
      # ------------------------------------------------------------------
      NotifyFailure = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One of the ETL steps failed — see CloudWatch logs for details."
      }
    }
  })
}
