###############################################################################
# modules/s3/main.tf
#
# Creates all 4 S3 buckets used by the pipeline:
#   1. raw       — landing zone for incoming stream files (triggers pipeline)
#   2. processed — Parquet output of the Transform job
#   3. archive   — destination for successfully-processed stream files
#   4. scripts   — holds Glue .py scripts; uploaded by the glue module
#
# Reference data (songs.csv, users.csv) lives under a `reference/` prefix
# inside the raw bucket — they're conceptually part of the same data
# landing zone, just static instead of event-driven.
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# Random 4-char suffix appended to every bucket name. S3 bucket names must
# be globally unique across all AWS accounts; without a suffix, two people
# running this Terraform would collide.
resource "random_id" "bucket_suffix" {
  byte_length = 2
}

# -----------------------------------------------------------------------------
# RAW BUCKET — landing zone, fires EventBridge on PutObject
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "raw" {
  bucket = "${local.name_prefix}-raw-${random_id.bucket_suffix.hex}"

  # Lifecycle protection: this bucket holds production data. Set to `true`
  # only in dev where you want `terraform destroy` to actually clean up.
  force_destroy = var.environment == "dev"
}

# Block all public access — critical default. S3 has caused more data
# breaches than any other AWS service; never leave public access ambiguous.
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable EventBridge notifications — when a file lands in this bucket,
# AWS will publish an event to the default EventBridge bus. The
# eventbridge module then has a rule that catches those events and
# starts the Step Functions execution.
resource "aws_s3_bucket_notification" "raw" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}

# Enable server-side encryption at rest (SSE-S3 / AES-256). Free, on by
# default for new buckets since 2023 — declaring it explicitly makes
# intent visible to anyone reading the IaC.
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning protects against accidental overwrites/deletes during the
# ETL run (e.g. if Lambda accidentally writes to raw instead of archive).
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -----------------------------------------------------------------------------
# PROCESSED BUCKET — holds Parquet output of Transform
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "processed" {
  bucket        = "${local.name_prefix}-processed-${random_id.bucket_suffix.hex}"
  force_destroy = var.environment == "dev"
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle policy: Parquet output can grow indefinitely. Transition to
# cheaper storage after 30 days, expire after a year. Tune for your needs.
resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "archive-old-parquet"
    status = "Enabled"

    # An empty filter block means "apply to all objects" — required since
    # AWS provider v4.x.
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# -----------------------------------------------------------------------------
# ARCHIVE BUCKET — successful raw files end up here
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "archive" {
  bucket        = "${local.name_prefix}-archive-${random_id.bucket_suffix.hex}"
  force_destroy = var.environment == "dev"
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Archive is cold data by definition — push it straight to Glacier.
resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "to-glacier"
    status = "Enabled"
    filter {}

    transition {
      days          = 30
      storage_class = "GLACIER"
    }
  }
}

# -----------------------------------------------------------------------------
# SCRIPTS BUCKET — Glue jobs fetch their Python scripts from here
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "scripts" {
  bucket        = "${local.name_prefix}-scripts-${random_id.bucket_suffix.hex}"
  force_destroy = true # Scripts are reproducible from Terraform; safe to wipe
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning on scripts so we can roll back to a previous Glue job
# version without re-running Terraform.
resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -----------------------------------------------------------------------------
# REFERENCE DATA UPLOAD — songs.csv and users.csv
# -----------------------------------------------------------------------------
# These files are uploaded once at apply time. They live under the
# `reference/` prefix of the raw bucket so they're stored alongside
# stream data but clearly segregated. Glue jobs read them by full S3 key.
#
# Using aws_s3_object means Terraform owns the lifecycle of these files —
# if they change locally, the next `terraform apply` re-uploads them.
# -----------------------------------------------------------------------------
resource "aws_s3_object" "songs_reference" {
  bucket = aws_s3_bucket.raw.id
  key    = "reference/songs.csv"
  source = "${var.reference_data_path}/songs/songs.csv"
  # etag forces Terraform to detect content changes (not just file modtime).
  etag = filemd5("${var.reference_data_path}/songs/songs.csv")

  content_type = "text/csv"
}

resource "aws_s3_object" "users_reference" {
  bucket       = aws_s3_bucket.raw.id
  key          = "reference/users.csv"
  source       = "${var.reference_data_path}/users/users.csv"
  etag         = filemd5("${var.reference_data_path}/users/users.csv")
  content_type = "text/csv"
}
