###############################################################################
# modules/dynamodb/main.tf
#
# Creates the 3 KPI tables the Load Glue job writes to.
#
# Key design notes:
#   * billing_mode = PAY_PER_REQUEST (on-demand). Reasons:
#     - ETL writes are spiky (3 batches/day, then idle) — provisioned
#       capacity would be wasteful or throttled.
#     - No need to tune RCU/WCU or set up autoscaling.
#     For predictable, high-traffic prod tables you'd revisit this.
#
#   * Composite primary keys (partition + sort) on every table. This
#     supports the "upsert" pattern in Load — if the same KPI for the
#     same day runs twice, PutItem overwrites the existing item rather
#     than creating a duplicate. The annotation on the architecture
#     diagram corresponds to this design decision.
#
#   * Point-in-time recovery enabled — protects against accidental
#     bad-data overwrites from a buggy Load job.
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# -----------------------------------------------------------------------------
# 1. daily-genre-kpis
# Partition key: genre   (string)
# Sort key:      date    (YYYY-MM-DD, string)
# Stores per-day, per-genre aggregates (listen count, unique listeners, etc.)
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "daily_genre_kpis" {
  name         = "${local.name_prefix}-daily-genre-kpis"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "genre"
  range_key = "date"

  attribute {
    name = "genre"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  # Server-side encryption with AWS-owned key (free, default).
  server_side_encryption {
    enabled = true
  }
}

# -----------------------------------------------------------------------------
# 2. top-songs-per-genre  (top-3 songs per (genre, date))
#
# Partition key: genre      (string)
# Sort key:      date_rank  (e.g. "2024-06-25#01", string)
#
# Why date_rank? It packs the day and the rank position into one key:
#   - Same (genre, date_rank) on rerun → clean overwrite, no stale rows
#   - Multiple days coexist in the same partition — query a single day
#     with KeyConditionExpression `genre = :g AND begins_with(date_rank, :d)`
#   - The "01", "02", ... padding keeps results sorted by rank naturally
#
# Previous design used `<rank>#<track_id>` as the sort key, which leaked
# stale rows when the top-N ranking shifted between runs.
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "top_songs_per_genre" {
  name         = "${local.name_prefix}-top-songs-per-genre"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "genre"
  range_key = "date_rank"

  attribute {
    name = "genre"
    type = "S"
  }

  attribute {
    name = "date_rank"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}

# -----------------------------------------------------------------------------
# 3. top-genres-daily
# Partition key: date    (YYYY-MM-DD, string)
# Sort key:      rank    (zero-padded int as string, e.g. "01", "02")
# Stores the ranked list of top genres for each day. Partitioning by date
# means each day is one logical row group — efficient daily lookups.
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "top_genres_daily" {
  name         = "${local.name_prefix}-top-genres-daily"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "date"
  range_key = "rank"

  attribute {
    name = "date"
    type = "S"
  }

  attribute {
    name = "rank"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}
