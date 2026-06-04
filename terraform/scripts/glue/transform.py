"""
transform.py — Transform Glue Job (PySpark / Glue ETL)

Step 2 of the ETL pipeline. Joins the validated stream file with the
static songs.csv and users.csv reference data, enriches each listen
event with genre / song / user attributes, derives a date partition,
and writes the result as Parquet to the processed bucket.

Structure
---------
Same split as validate.py:

  * enrich_streams(streams_df, songs_df, users_df) — PURE function.
    Takes three DataFrames, returns one enriched DataFrame. No I/O,
    no Glue boilerplate. Unit-testable with any local SparkSession.

  * write_dates_manifest(s3_client, bucket, key, dates) — small S3
    helper, unit-testable with moto.

  * main() — runs only inside Glue, does I/O and calls enrich_streams.

Why PySpark here?
  - Stream files can grow to tens/hundreds of MB.
  - songs.csv is ~120k rows; a Spark broadcast join handles this
    efficiently regardless of stream size.
  - Parquet writes are parallel and column-pruned by default.

Partition-pruning hand-off
--------------------------
After writing Parquet, Transform collects the distinct listen_dates it
just produced and writes them as a JSON manifest at:

    s3://<processed_bucket>/<manifest_key>

Load reads that manifest and filters its Parquet scan to only those
partitions — turning Load's runtime from O(history) into O(touched-days).
See docs/sample-queries.md and WALKTHROUGH.md for the full design.

Job parameters:
  --raw_bucket        Bucket holding stream file + reference data
  --object_key        Key of the validated stream file
  --songs_key         S3 key of the reference songs.csv
  --users_key         S3 key of the reference users.csv
  --processed_bucket  Destination bucket for Parquet output
  --manifest_key      S3 key under processed_bucket where this run
                      should write its "dates touched" manifest
"""

import json
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def write_dates_manifest(s3_client, bucket, key, dates, *, expected_owner=None):
    """
    Write a small JSON manifest listing the listen_dates this run touched.

    The manifest body looks like:
        {"dates": ["2024-06-25", "2024-06-26"]}

    Load reads this file to drive partition pruning — it only reads the
    Parquet folders for the dates listed here, instead of scanning the
    whole processed/ tree.

    Parameters
    ----------
    s3_client : boto3 S3 client
    bucket    : destination bucket (the processed bucket)
    key       : object key for the manifest (passed in by Step Functions)
    dates     : iterable of date-like values (str or datetime.date)
    expected_owner : optional AWS account ID for the confused-deputy
                     guard (ExpectedBucketOwner on PutObject).
    """
    # Coerce date values to strings, dedupe, and sort so the manifest is
    # deterministic regardless of Spark partition ordering.
    normalized = sorted({str(d) for d in dates if d is not None})
    payload = json.dumps({"dates": normalized}, separators=(",", ":"))

    put_args = {
        "Bucket": bucket,
        "Key": key,
        "Body": payload.encode("utf-8"),
        "ContentType": "application/json",
    }
    if expected_owner:
        put_args["ExpectedBucketOwner"] = expected_owner

    s3_client.put_object(**put_args)
    return normalized


def enrich_streams(streams_df, songs_df, users_df):
    """
    Join streams with songs and users and add a listen_date partition.

    This is the entire business logic of the Transform step, expressed
    without any AWS/Glue dependencies so it's directly testable with a
    plain SparkSession in pytest.

    Parameters
    ----------
    streams_df : DataFrame
        Columns: user_id, track_id, listen_time
    songs_df : DataFrame
        Columns: track_id (+ song attributes, e.g. track_genre, artists, ...)
    users_df : DataFrame
        Columns: user_id (+ user attributes, e.g. user_age, user_country, ...)

    Returns
    -------
    DataFrame
        streams + songs + users joined on track_id and user_id, with a
        derived `listen_date` (date) column.

    Notes
    -----
    Broadcast joins are used because songs.csv and users.csv combined are
    well under the default 10 MB broadcast threshold for realistic data
    sizes. A broadcast avoids the shuffle step entirely, which is the
    single biggest cost in any Spark join.
    """
    enriched = (
        streams_df
        .join(broadcast(songs_df), on="track_id", how="inner")
        .join(broadcast(users_df), on="user_id", how="inner")
    )

    # Derive the date partition column from listen_time. Downstream Load
    # filters by this column, so partitioning by it makes reads cheap.
    enriched = enriched.withColumn(
        "listen_date",
        F.to_date(F.col("listen_time")),
    )

    return enriched


def main():
    """Glue ETL entry point — only runs inside the managed Spark runtime."""
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    # -----------------------------------------------------------------------------
    # Boilerplate Glue ETL setup — these 4 lines are required for every Glue
    # Spark job. They wire up the Glue + Spark contexts and register the job
    # bookmark mechanism (which we don't use here, but Glue requires the call).
    # -----------------------------------------------------------------------------
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "raw_bucket",
            "object_key",
            "songs_key",
            "users_key",
            "processed_bucket",
            "manifest_key",
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # -----------------------------------------------------------------------------
    # Read inputs from S3. CSVs all have a header row; `inferSchema=true` is
    # fine for this size of reference data. For huge files we'd hand-specify
    # the schema to skip the inference scan.
    # -----------------------------------------------------------------------------
    streams_path = f"s3://{args['raw_bucket']}/{args['object_key']}"
    songs_path = f"s3://{args['raw_bucket']}/{args['songs_key']}"
    users_path = f"s3://{args['raw_bucket']}/{args['users_key']}"

    print(f"Reading streams: {streams_path}")
    print(f"Reading songs:   {songs_path}")
    print(f"Reading users:   {users_path}")

    streams_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(streams_path)
    )

    songs_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(songs_path)
    )

    users_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(users_path)
    )

    # ---- Business logic, isolated for testability -------------------------
    enriched_df = enrich_streams(streams_df, songs_df, users_df)

    # -----------------------------------------------------------------------------
    # Write Parquet output. Partitioned by date for efficient downstream reads.
    # `mode("append")` ensures multiple stream files for the same day merge
    # rather than overwriting each other.
    # -----------------------------------------------------------------------------
    output_path = f"s3://{args['processed_bucket']}/enriched_streams/"
    print(f"Writing Parquet: {output_path}")

    (
        enriched_df.write
        .mode("append")
        .partitionBy("listen_date")
        .parquet(output_path)
    )

    # -----------------------------------------------------------------------------
    # Emit the dates-touched manifest. Load reads this file to drive
    # partition pruning. We collect distinct listen_dates from THIS run's
    # DataFrame (not from the processed/ bucket) — that's what makes the
    # hand-off honest: only the dates this file contributed end up in the
    # manifest, not whatever happens to be sitting in the bucket already.
    # -----------------------------------------------------------------------------
    date_rows = enriched_df.select("listen_date").distinct().collect()
    dates_touched = [row["listen_date"] for row in date_rows]

    s3 = boto3.client("s3")
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    normalized = write_dates_manifest(
        s3,
        bucket=args["processed_bucket"],
        key=args["manifest_key"],
        dates=dates_touched,
        expected_owner=account_id,
    )
    print(f"Manifest written: s3://{args['processed_bucket']}/{args['manifest_key']}")
    print(f"Dates touched ({len(normalized)}): {normalized}")

    print(f"Transform complete — rows written: {enriched_df.count()}")

    job.commit()


if __name__ == "__main__":
    main()
