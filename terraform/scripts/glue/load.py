"""
load.py — Load Glue Job (PySpark / Glue ETL)

Step 3 of the ETL pipeline. Reads the enriched Parquet produced by
Transform, computes 3 KPI aggregates, and upserts each into its
corresponding DynamoDB table.

KPIs computed:
  1. daily-genre-kpis    — per (genre, date): listen_count, unique_listeners,
                           total_listening_time_ms, avg_listening_time_per_user_ms
  2. top-songs-per-genre — per (genre, date): top 3 songs by listen_count
  3. top-genres-daily    — per (date):        top 5 genres by listen_count

Upsert semantics: every write is a PutItem with the table's composite
primary key as defined in Terraform. Re-running this job for the same
day overwrites the previous values rather than creating duplicates —
that's the "upsert" annotation on the architecture diagram.

Structure
---------
The KPI math is split into three pure functions that take a DataFrame
in and return a DataFrame out. They have no AWS dependencies, so each
can be unit-tested with a local SparkSession.

  * compute_daily_genre_kpis(enriched)
  * compute_top_songs_per_genre(enriched, listen_date, n=3)
  * compute_top_genres_daily(enriched, listen_date, n=5)
  * read_dates_manifest(s3, bucket, key)  — partition-pruning hand-off

main() handles the I/O around them: read manifest, narrow the Parquet
scan to the dates Transform just touched, call the three KPI functions,
batch-write the results to DynamoDB.

Partition-pruning hand-off
--------------------------
Transform writes a tiny JSON manifest listing the listen_dates it just
produced. Load reads that manifest and applies the dates as a partition
filter, so Load only ever scans the folders that changed — regardless of
how much history sits in the processed/ tree.

Job parameters:
  --processed_bucket   Bucket holding the Parquet output from Transform
  --manifest_key       S3 key (under processed_bucket) of the manifest
                       Transform wrote for this run
  --table_daily_genre  DynamoDB table name for daily-genre-kpis
  --table_top_songs    DynamoDB table name for top-songs-per-genre
  --table_top_genres   DynamoDB table name for top-genres-daily
"""

import json
import sys
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def read_dates_manifest(s3_client, bucket, key, *, expected_owner=None):
    """
    Read the dates-touched manifest written by Transform.

    Returns the list of date strings (e.g. ["2024-06-25", "2024-06-26"])
    that Load should process. Anything else in processed/ is ignored.

    The manifest body is `{"dates": [...]}`; missing or malformed payload
    raises so the Glue job fails loudly instead of silently scanning
    everything.
    """
    get_args = {"Bucket": bucket, "Key": key}
    if expected_owner:
        get_args["ExpectedBucketOwner"] = expected_owner

    obj = s3_client.get_object(**get_args)
    payload = json.loads(obj["Body"].read())

    dates = payload.get("dates")
    if not isinstance(dates, list):
        raise ValueError(
            f"manifest s3://{bucket}/{key} missing 'dates' list: {payload!r}"
        )
    return [str(d) for d in dates]


# -----------------------------------------------------------------------------
# Pure KPI computations — testable without AWS.
# -----------------------------------------------------------------------------

def compute_daily_genre_kpis(enriched):
    """
    KPI 1: per (genre, date), the four daily metrics the spec requires.

    Output columns:
        genre                          — partition key in DynamoDB
        date (str)                     — sort key in DynamoDB
        listen_count                   — total plays for this genre on this day
        unique_listeners               — distinct user_id count
        total_listening_time_ms        — SUM of song duration_ms across plays
        avg_listening_time_per_user_ms — total_listening_time_ms / unique_listeners

    Notes
    -----
    `duration_ms` comes from the joined songs.csv reference data, so it's
    only available because Transform broadcast-joined songs into the
    enriched DataFrame. If the source data ever loses that column,
    these KPIs will fail loudly at agg time.
    """
    aggregated = (
        enriched.groupBy("track_genre", "listen_date")
        .agg(
            F.count("*").alias("listen_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.sum("duration_ms").alias("total_listening_time_ms"),
        )
    )

    # avg_listening_time_per_user is derived. We can't compute it inside
    # the agg() call directly because countDistinct can't be referenced
    # by a SUM/divide in the same projection.
    with_avg = aggregated.withColumn(
        "avg_listening_time_per_user_ms",
        F.col("total_listening_time_ms") / F.col("unique_listeners"),
    )

    return (
        with_avg
        .withColumnRenamed("track_genre", "genre")
        .withColumn("date", F.col("listen_date").cast("string"))
        .drop("listen_date")
    )


def compute_top_songs_per_genre(enriched, listen_date, n=3):
    """
    KPI 2: top N songs per (genre, date) by listen_count.

    Default n=3 matches the project spec ("Top 3 Songs per Genre per Day").

    Output columns: genre, date_rank (sort key), track_id, track_name,
                    listen_count, rank, date

    Sort-key design
    ---------------
    The DynamoDB sort key is `date_rank` = "<date>#<rank>" — for example
    "2024-06-25#01". This shape:

      1. Cleanly OVERWRITES on rerun. If we re-process the same day and
         the ranking changes, the new rank-1 song overwrites the old
         rank-1 row at the same key, instead of leaving a stale row.
      2. Supports multiple days in the SAME table. Each day occupies its
         own contiguous sort-key range (`begins_with("2024-06-25#")`),
         so per-day queries are efficient and per-genre history is
         preserved across days.

    The old design encoded `<rank>#<track_id>` in the sort key, which
    accidentally allowed multiple days only when the same rank happened
    to fall on different tracks — and lost data when it didn't.
    """
    genre_song_counts = enriched.groupBy(
        "track_genre", "track_id", "track_name"
    ).agg(F.count("*").alias("listen_count"))

    window = Window.partitionBy("track_genre").orderBy(F.col("listen_count").desc())

    return (
        genre_song_counts.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= n)
        .withColumn(
            "date_rank",
            # "<date>#<rank>" — sortable, overwrite-safe, per-day scoped.
            F.concat(
                F.lit(listen_date),
                F.lit("#"),
                F.lpad(F.col("rank").cast("string"), 2, "0"),
            ),
        )
        .withColumnRenamed("track_genre", "genre")
        .withColumn("date", F.lit(listen_date))
    )


def compute_top_genres_daily(enriched, listen_date, n=5):
    """
    KPI 3: top N genres by listen_count for one specific date.

    Default n=5 matches the project spec ("Top 5 Genres per Day").

    Output columns: date, rank (2-digit padded str), genre, listen_count
    """
    genre_totals = enriched.groupBy("track_genre").agg(
        F.count("*").alias("listen_count")
    )
    genre_window = Window.orderBy(F.col("listen_count").desc())

    return (
        genre_totals.withColumn("rank_int", F.row_number().over(genre_window))
        .filter(F.col("rank_int") <= n)
        .withColumn("rank", F.lpad(F.col("rank_int").cast("string"), 2, "0"))
        .withColumn("date", F.lit(listen_date))
        .withColumnRenamed("track_genre", "genre")
        .drop("rank_int")
    )


# -----------------------------------------------------------------------------
# Helpers for the DynamoDB write path (only called from main()).
# -----------------------------------------------------------------------------

def row_to_dynamo_item(row, *, casts):
    """
    Convert a Spark Row to a dict ready for DynamoDB.

    DynamoDB rejects Python floats; numeric values must be Decimal.
    `casts` is a {column: target_type} dict that says how to coerce each
    field — e.g. {'genre': str, 'listen_count': Decimal}.
    """
    item = {}
    for col, dtype in casts.items():
        value = row[col]
        if value is None:
            continue
        if dtype is Decimal:
            item[col] = Decimal(str(value))
        elif dtype is str:
            item[col] = str(value)
        else:
            item[col] = dtype(value)
    return item


def batch_upsert(table, items):
    """
    Upsert a list of items into a DynamoDB table.

    boto3's batch_writer handles the 25-item API limit and retries on
    UnprocessedItems automatically.
    """
    with table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=item)


# -----------------------------------------------------------------------------
# Glue ETL entry point.
# -----------------------------------------------------------------------------

def main():
    """Glue runtime entry — reads Parquet, computes KPIs, writes to DynamoDB."""
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "processed_bucket",
            "manifest_key",
            "table_daily_genre",
            "table_top_songs",
            "table_top_genres",
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    s3 = boto3.client("s3")
    dynamodb = boto3.resource("dynamodb")

    # ExpectedBucketOwner guards the manifest read against confused-deputy
    # / bucket-takeover scenarios. Resolved once at cold start.
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    # -----------------------------------------------------------------------------
    # Read the manifest Transform wrote — the authoritative list of
    # listen_dates this run should refresh. The manifest is the contract
    # between Transform and Load; Load never guesses which dates to touch.
    # -----------------------------------------------------------------------------
    listen_dates = read_dates_manifest(
        s3,
        bucket=args["processed_bucket"],
        key=args["manifest_key"],
        expected_owner=account_id,
    )
    if not listen_dates:
        # Empty manifest — Transform produced zero rows (an upstream Validate
        # bug, but defensively handled here). Returning normally (not
        # sys.exit) lets Glue mark the run SUCCEEDED.
        print(
            f"Manifest s3://{args['processed_bucket']}/{args['manifest_key']} "
            "is empty — nothing to load."
        )
        job.commit()
        return

    print(f"Processing {len(listen_dates)} date partition(s) from manifest: {listen_dates}")

    # -----------------------------------------------------------------------------
    # Read ONLY the Parquet partitions named in the manifest. Spark
    # recognises listen_date as a partition column (Transform wrote with
    # partitionBy("listen_date")), so the .filter(isin(...)) below prunes
    # the scan at the S3-listing layer — other days' folders are never
    # opened. Load runtime stays O(touched-days), not O(history).
    # -----------------------------------------------------------------------------
    parquet_path = f"s3://{args['processed_bucket']}/enriched_streams/"
    print(f"Reading enriched data from {parquet_path}")

    enriched = (
        spark.read.parquet(parquet_path)
        .filter(F.col("listen_date").cast("string").isin(listen_dates))
    )

    if enriched.rdd.isEmpty():
        # Manifest listed dates, but no rows match — should not happen unless
        # someone deleted the Parquet between Transform and Load. Don't
        # sys.exit; return so Glue marks the run SUCCEEDED.
        print("Manifest dates resolved to zero rows — nothing to load.")
        job.commit()
        return

    daily_genre_table = dynamodb.Table(args["table_daily_genre"])
    top_songs_table = dynamodb.Table(args["table_top_songs"])
    top_genres_table = dynamodb.Table(args["table_top_genres"])

    for listen_date in listen_dates:
        print(f"--- Processing listen_date={listen_date} ---")
        day_data = enriched.filter(F.col("listen_date").cast("string") == listen_date)

        # ---- KPI 1: daily-genre-kpis ---------------------------------
        # Four daily metrics per (genre, date): listen_count, unique_listeners,
        # total_listening_time_ms, avg_listening_time_per_user_ms.
        daily_genre = compute_daily_genre_kpis(day_data)
        daily_genre_items = [
            row_to_dynamo_item(
                r,
                casts={
                    "genre": str,
                    "date": str,
                    "listen_count": Decimal,
                    "unique_listeners": Decimal,
                    "total_listening_time_ms": Decimal,
                    "avg_listening_time_per_user_ms": Decimal,
                },
            )
            for r in daily_genre.collect()
        ]
        batch_upsert(daily_genre_table, daily_genre_items)
        print(f"  Wrote {len(daily_genre_items)} items to {args['table_daily_genre']}")

        # ---- KPI 2: top-3 songs per genre per day --------------------
        # Sort key is `date_rank` = "<date>#<rank>" — clean overwrite
        # semantics on rerun, supports multi-day in the same table.
        top_songs = compute_top_songs_per_genre(day_data, listen_date, n=3)
        top_songs_items = [
            row_to_dynamo_item(
                r,
                casts={
                    "genre": str,
                    "date_rank": str,
                    "track_id": str,
                    "track_name": str,
                    "listen_count": Decimal,
                    "date": str,
                    "rank": Decimal,
                },
            )
            for r in top_songs.collect()
        ]
        batch_upsert(top_songs_table, top_songs_items)
        print(f"  Wrote {len(top_songs_items)} items to {args['table_top_songs']}")

        # ---- KPI 3: top-5 genres per day -----------------------------
        top_genres = compute_top_genres_daily(day_data, listen_date, n=5)
        top_genres_items = [
            row_to_dynamo_item(
                r,
                casts={
                    "date": str,
                    "rank": str,
                    "genre": str,
                    "listen_count": Decimal,
                },
            )
            for r in top_genres.collect()
        ]
        batch_upsert(top_genres_table, top_genres_items)
        print(f"  Wrote {len(top_genres_items)} items to {args['table_top_genres']}")

    print(f"Load complete — processed {len(listen_dates)} date(s) across all 3 KPI tables.")
    job.commit()


if __name__ == "__main__":
    main()
