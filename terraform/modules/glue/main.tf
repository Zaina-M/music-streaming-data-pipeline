###############################################################################
# modules/glue/main.tf
#
# Provisions:
#   - A Glue Data Catalog database (required for PySpark jobs even if we
#     don't register any tables in it — it gives Glue a default namespace).
#   - S3 uploads of the 3 Python scripts.
#   - The 3 Glue jobs themselves (validate / transform / load).
#
# Job command types:
#   * validate.py → command "pythonshell"  (lightweight Python runtime,
#                                           cheaper, no Spark cluster)
#   * transform.py → command "glueetl"     (PySpark, scales horizontally)
#   * load.py     → command "glueetl"      (PySpark, reads Parquet at scale)
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# Glue Data Catalog database
# Even though our PySpark scripts write directly to S3 paths (not catalog
# tables), Glue ETL jobs still expect a default database to exist.
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "this" {
  name        = replace("${local.name_prefix}-catalog", "-", "_")
  description = "Default Glue catalog database for ${local.name_prefix} ETL pipeline."
}

# -----------------------------------------------------------------------------
# Upload the 3 scripts to S3. Terraform watches the local file content
# via `etag = filemd5(...)` — if we edit validate.py locally and re-apply,
# the new version uploads automatically and the next Glue job run picks
# it up.
# -----------------------------------------------------------------------------
resource "aws_s3_object" "validate_script" {
  bucket = var.scripts_bucket
  key    = "glue/validate.py"
  source = "${var.scripts_local_path}/validate.py"
  etag   = filemd5("${var.scripts_local_path}/validate.py")
}

resource "aws_s3_object" "transform_script" {
  bucket = var.scripts_bucket
  key    = "glue/transform.py"
  source = "${var.scripts_local_path}/transform.py"
  etag   = filemd5("${var.scripts_local_path}/transform.py")
}

resource "aws_s3_object" "load_script" {
  bucket = var.scripts_bucket
  key    = "glue/load.py"
  source = "${var.scripts_local_path}/load.py"
  etag   = filemd5("${var.scripts_local_path}/load.py")
}

# -----------------------------------------------------------------------------
# 1. Validate job (Python shell — cheap, no Spark needed)
# Python shell jobs charge per DPU-hour at a fraction of ETL cost. Perfect
# for short fail-fast validation logic.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "validate" {
  name        = "${local.name_prefix}-validate"
  description = "Validates the incoming stream CSV against the expected schema."
  role_arn    = var.glue_role_arn

  glue_version      = "3.0"  # Required for pythonshell + Python 3.9
  max_retries       = 1
  timeout           = 10     # minutes — validation should be near-instant

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.scripts_bucket}/${aws_s3_object.validate_script.key}"
  }

  # max_capacity for Python shell jobs is measured in DPU (Data Processing
  # Units). 0.0625 is the smallest available — equivalent to 1/16 of a DPU.
  max_capacity = 0.0625

  # Defense-in-depth: Glue defaults to max_concurrent_runs = 1 PER JOB.
  # If two Step Functions executions ever overlap (a Step Functions retry,
  # a manual console run during debugging, or a brief race in the SQS
  # serializer), the second StartJobRun call gets
  # Glue.ConcurrentRunsExceededException — a real production failure
  # we already observed once. 5 is generous enough that no realistic
  # overlap can hit the ceiling but low enough to surface runaway loops.
  execution_property {
    max_concurrent_runs = 5
  }

  # Sane defaults — these can be overridden per-execution by Step Functions
  # if needed. Empty defaults are fine since we always inject values.
  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# -----------------------------------------------------------------------------
# 2. Transform job (PySpark)
# Heavier compute — joins streams with songs/users and writes Parquet.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "transform" {
  name        = "${local.name_prefix}-transform"
  description = "Joins stream data with songs/users reference and writes Parquet."
  role_arn    = var.glue_role_arn

  glue_version      = "4.0"
  max_retries       = 1
  timeout           = 30   # minutes

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.scripts_bucket}/${aws_s3_object.transform_script.key}"
  }

  # G.1X = 4 vCPU, 16 GB RAM per worker. 2 workers is enough for our data
  # volume but scales by changing this number, no code changes needed.
  worker_type       = "G.1X"
  number_of_workers = 2

  # See the validate job for the rationale on max_concurrent_runs = 5.
  execution_property {
    max_concurrent_runs = 5
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    # job-bookmark-disable: we explicitly process one file per execution,
    # passed in by Step Functions. Enabling bookmarks would double-track.
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
  }
}

# -----------------------------------------------------------------------------
# 3. Load job (PySpark)
# Reads the Parquet partition for the day and upserts KPIs to DynamoDB.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "load" {
  name        = "${local.name_prefix}-load"
  description = "Computes KPIs from Parquet and upserts to DynamoDB."
  role_arn    = var.glue_role_arn

  glue_version      = "4.0"
  max_retries       = 1
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.scripts_bucket}/${aws_s3_object.load_script.key}"
  }

  worker_type       = "G.1X"
  number_of_workers = 2

  # See the validate job for the rationale on max_concurrent_runs = 5.
  execution_property {
    max_concurrent_runs = 5
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
  }
}
