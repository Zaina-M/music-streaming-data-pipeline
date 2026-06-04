"""
Unit tests for the pure KPI-computation functions in load.py:

  * compute_daily_genre_kpis
  * compute_top_songs_per_genre
  * compute_top_genres_daily

Plus tests for the helpers:
  * row_to_dynamo_item     (Decimal coercion, None-skipping)
  * read_dates_manifest    (partition-pruning hand-off from Transform)

These tests use the session-scoped Spark fixture from conftest.py.
"""

import datetime as dt
import json
import os
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from load import (
    compute_daily_genre_kpis,
    compute_top_genres_daily,
    compute_top_songs_per_genre,
    read_dates_manifest,
    row_to_dynamo_item,
)

pytestmark = pytest.mark.spark

# Region for moto's in-memory S3 client — env-sourced to avoid the
# SonarLint S6262 "hardcoded region" warning (the rule is aimed at
# production code, but it fires on test code too).
TEST_AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")


# ---------------------------------------------------------------------------
# Sample enriched DataFrame — what Load receives from Transform.
#
# Schema includes `duration_ms` (joined from songs.csv by Transform) so
# the KPI computations can compute total + average listening time.
# Durations chosen so the arithmetic is easy to eyeball:
#
#   t1 → 200_000 ms   t2 → 300_000 ms
#   t3 → 250_000 ms   t4 → 400_000 ms
#
# Per-day, per-genre breakdown for 2024-06-25:
#
#   pop  : 4 plays = t1×3 + t2×1
#        total_ms = 3·200_000 + 1·300_000 = 900_000
#        unique_users {u1, u2, u3} = 3
#        avg = 900_000 / 3 = 300_000
#
#   rock : 2 plays = t3×2
#        total_ms = 2·250_000 = 500_000
#        unique_users {u4, u5} = 2
#        avg = 500_000 / 2 = 250_000
#
#   jazz : 1 play  = t4×1
#        total_ms = 400_000
#        unique_users {u6} = 1
#        avg = 400_000
#
# For 2024-06-26: 1 play of t1 by u1 → pop total 200_000, avg 200_000.
# ---------------------------------------------------------------------------

@pytest.fixture
def enriched_df(spark):
    return spark.createDataFrame(
        [
            # date 2024-06-25
            ("u1", "t1", "2024-06-25 10:00:00", "Pop Hit",  "pop",  200_000, dt.date(2024, 6, 25)),
            ("u2", "t1", "2024-06-25 10:30:00", "Pop Hit",  "pop",  200_000, dt.date(2024, 6, 25)),
            ("u3", "t1", "2024-06-25 11:00:00", "Pop Hit",  "pop",  200_000, dt.date(2024, 6, 25)),
            ("u1", "t2", "2024-06-25 12:00:00", "Pop Two",  "pop",  300_000, dt.date(2024, 6, 25)),
            ("u4", "t3", "2024-06-25 13:00:00", "Rock One", "rock", 250_000, dt.date(2024, 6, 25)),
            ("u5", "t3", "2024-06-25 14:00:00", "Rock One", "rock", 250_000, dt.date(2024, 6, 25)),
            ("u6", "t4", "2024-06-25 15:00:00", "Jazz One", "jazz", 400_000, dt.date(2024, 6, 25)),
            # date 2024-06-26 — distinct partition
            ("u1", "t1", "2024-06-26 10:00:00", "Pop Hit",  "pop",  200_000, dt.date(2024, 6, 26)),
        ],
        [
            "user_id", "track_id", "listen_time",
            "track_name", "track_genre", "duration_ms", "listen_date",
        ],
    )


# ---------------------------------------------------------------------------
# compute_daily_genre_kpis — all four daily metrics
# ---------------------------------------------------------------------------

def test_daily_genre_kpis_counts(enriched_df):
    """listen_count is total plays; unique_listeners is distinct user_ids."""
    result = {
        (r["genre"], r["date"]): r
        for r in compute_daily_genre_kpis(enriched_df).collect()
    }

    pop_25 = result[("pop", "2024-06-25")]
    assert pop_25["listen_count"] == 4
    assert pop_25["unique_listeners"] == 3

    rock_25 = result[("rock", "2024-06-25")]
    assert rock_25["listen_count"] == 2
    assert rock_25["unique_listeners"] == 2

    pop_26 = result[("pop", "2024-06-26")]
    assert pop_26["listen_count"] == 1
    assert pop_26["unique_listeners"] == 1


def test_daily_genre_total_listening_time(enriched_df):
    """SUM(duration_ms) across all plays for a (genre, date)."""
    result = {
        (r["genre"], r["date"]): r
        for r in compute_daily_genre_kpis(enriched_df).collect()
    }

    # pop 2024-06-25: 3·200_000 + 1·300_000 = 900_000
    assert result[("pop", "2024-06-25")]["total_listening_time_ms"] == 900_000
    # rock 2024-06-25: 2·250_000 = 500_000
    assert result[("rock", "2024-06-25")]["total_listening_time_ms"] == 500_000
    # jazz 2024-06-25: 400_000
    assert result[("jazz", "2024-06-25")]["total_listening_time_ms"] == 400_000
    # pop 2024-06-26: 200_000
    assert result[("pop", "2024-06-26")]["total_listening_time_ms"] == 200_000


def test_daily_genre_avg_listening_time_per_user(enriched_df):
    """avg = total_listening_time_ms / unique_listeners (per the spec)."""
    result = {
        (r["genre"], r["date"]): r
        for r in compute_daily_genre_kpis(enriched_df).collect()
    }

    # pop 25: 900_000 / 3 = 300_000
    assert result[("pop", "2024-06-25")]["avg_listening_time_per_user_ms"] == 300_000
    # rock 25: 500_000 / 2 = 250_000
    assert result[("rock", "2024-06-25")]["avg_listening_time_per_user_ms"] == 250_000
    # jazz 25: 400_000 / 1 = 400_000
    assert result[("jazz", "2024-06-25")]["avg_listening_time_per_user_ms"] == 400_000


def test_daily_genre_kpis_columns(enriched_df):
    """Output columns must match the DynamoDB schema (incl. new KPIs) exactly."""
    result = compute_daily_genre_kpis(enriched_df)
    assert set(result.columns) == {
        "genre",
        "date",
        "listen_count",
        "unique_listeners",
        "total_listening_time_ms",
        "avg_listening_time_per_user_ms",
    }


# ---------------------------------------------------------------------------
# compute_top_songs_per_genre — default n=3 per the spec
# ---------------------------------------------------------------------------

def test_top_songs_ranked_by_listens(enriched_df):
    """Within pop on 2024-06-25, t1 (3 listens) ranks above t2 (1 listen)."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_songs_per_genre(one_day, "2024-06-25").collect()

    pop_rows = [r for r in result if r["genre"] == "pop"]
    pop_rows_sorted = sorted(pop_rows, key=lambda r: r["rank"])

    assert pop_rows_sorted[0]["track_id"] == "t1"
    assert pop_rows_sorted[0]["listen_count"] == 3
    assert pop_rows_sorted[0]["rank"] == 1

    assert pop_rows_sorted[1]["track_id"] == "t2"
    assert pop_rows_sorted[1]["listen_count"] == 1


def test_top_songs_date_rank_format(enriched_df):
    """
    Sort key is `date_rank` = "<date>#<rank>" — that's the new design.
    The old `<rank>#<track_id>` shape was abandoned because it leaked
    stale rows when the ranking shifted between runs.
    """
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_songs_per_genre(one_day, "2024-06-25").collect()

    for row in result:
        # Always "2024-06-25#" + 2-digit-padded rank.
        assert row["date_rank"] == f"2024-06-25#{row['rank']:02d}"


def test_top_songs_default_n_matches_spec(enriched_df):
    """The brief says 'Top 3 Songs per Genre per Day' — that's the default."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_songs_per_genre(one_day, "2024-06-25").collect()

    # Pop has 2 distinct songs (t1, t2), so we get 2 — not capped by n=3.
    pop_rows = [r for r in result if r["genre"] == "pop"]
    assert len(pop_rows) == 2

    # No genre should ever exceed n=3 rows.
    from collections import Counter
    counts = Counter(r["genre"] for r in result)
    for genre, count in counts.items():
        assert count <= 3, f"{genre} has {count} rows, expected at most 3"


def test_top_songs_limit_n(enriched_df):
    """n=1 should cap each genre to exactly its top song."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_songs_per_genre(one_day, "2024-06-25", n=1).collect()

    genres = [r["genre"] for r in result]
    assert sorted(genres) == ["jazz", "pop", "rock"]
    assert len(result) == 3


def test_top_songs_columns(enriched_df):
    """Output must include date_rank — the new DynamoDB sort key."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_songs_per_genre(one_day, "2024-06-25")

    cols = set(result.columns)
    assert "date_rank" in cols
    assert "rank_song_id" not in cols, "The old sort key should no longer be emitted"
    # Other expected attributes still present.
    for required in ("genre", "track_id", "track_name", "listen_count", "rank", "date"):
        assert required in cols


# ---------------------------------------------------------------------------
# compute_top_genres_daily — default n=5 per the spec
# ---------------------------------------------------------------------------

def test_top_genres_ordered_by_listens(enriched_df):
    """For 2024-06-25 — pop (4) > rock (2) > jazz (1)."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = sorted(
        compute_top_genres_daily(one_day, "2024-06-25").collect(),
        key=lambda r: r["rank"],
    )

    assert result[0]["genre"] == "pop"
    assert result[0]["listen_count"] == 4
    assert result[0]["rank"] == "01"

    assert result[1]["genre"] == "rock"
    assert result[1]["listen_count"] == 2
    assert result[1]["rank"] == "02"

    assert result[2]["genre"] == "jazz"
    assert result[2]["rank"] == "03"


def test_top_genres_default_n_matches_spec(enriched_df):
    """The brief says 'Top 5 Genres per Day' — that's the default."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_genres_daily(one_day, "2024-06-25").collect()

    # Test fixture has 3 distinct genres for the day — fewer than 5, so we
    # get all 3. The cap matters when there are more than 5; we verify
    # the function never emits more than 5 in that case.
    assert len(result) == 3

    # Synthetic case with > 5 genres to confirm the cap.
    many_genres = enriched_df.unionByName(
        enriched_df.sparkSession.createDataFrame(
            [
                ("u100", f"tx{i}", "2024-06-25 09:00:00",
                 f"Song {i}", f"genre{i}", 100_000, dt.date(2024, 6, 25))
                for i in range(10)
            ],
            enriched_df.columns,
        )
    ).filter("listen_date = '2024-06-25'")

    capped = compute_top_genres_daily(many_genres, "2024-06-25").collect()
    assert len(capped) == 5


def test_top_genres_columns(enriched_df):
    """Output columns must match the DynamoDB schema exactly."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_genres_daily(one_day, "2024-06-25")
    assert set(result.columns) == {"date", "rank", "genre", "listen_count"}


def test_top_genres_date_column_constant(enriched_df):
    """Every row's date field should equal the listen_date arg."""
    one_day = enriched_df.filter("listen_date = '2024-06-25'")
    result = compute_top_genres_daily(one_day, "2024-06-25").collect()
    assert all(r["date"] == "2024-06-25" for r in result)


# ---------------------------------------------------------------------------
# row_to_dynamo_item helper
# ---------------------------------------------------------------------------

def test_row_to_dynamo_item_coerces_to_decimal(spark):
    """DynamoDB rejects floats; numeric values must end up as Decimal."""
    df = spark.createDataFrame([("pop", 42, 3.14)], ["genre", "count", "ratio"])
    row = df.collect()[0]

    item = row_to_dynamo_item(
        row,
        casts={"genre": str, "count": Decimal, "ratio": Decimal},
    )

    assert item["genre"] == "pop"
    assert isinstance(item["count"], Decimal)
    assert isinstance(item["ratio"], Decimal)
    assert item["count"] == Decimal("42")
    assert item["ratio"] == Decimal("3.14")


def test_row_to_dynamo_item_skips_none(spark):
    """None values are omitted, not written as null (DynamoDB-friendly)."""
    # Provide an explicit schema — Spark can't infer the type of a column
    # whose only sample value is None.
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType([
        StructField("genre", StringType(), True),
        StructField("count", LongType(), True),
    ])
    df = spark.createDataFrame([("pop", None)], schema)
    row = df.collect()[0]

    item = row_to_dynamo_item(row, casts={"genre": str, "count": Decimal})

    assert "genre" in item
    assert "count" not in item  # None was dropped


# ---------------------------------------------------------------------------
# read_dates_manifest — partition-pruning hand-off
#
# Load uses this to know which listen_date partitions Transform just
# wrote. It MUST be deterministic, and it MUST fail loudly when the
# manifest is missing or malformed (silent fall-through to a full scan
# would defeat the whole point of partition pruning).
# ---------------------------------------------------------------------------

@pytest.fixture
def manifest_s3():
    """Moto-backed S3 client + bucket name with the test bucket pre-created."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_AWS_REGION)
        bucket = "processed-bucket-test"
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
        )
        yield s3, bucket


def test_read_dates_manifest_returns_dates(manifest_s3):
    """Happy path — manifest JSON parses and returns the dates list."""
    s3, bucket = manifest_s3
    key = "manifests/streams/streams1.csv.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps({"dates": ["2024-06-25", "2024-06-26"]}).encode("utf-8"),
    )

    dates = read_dates_manifest(s3, bucket, key)
    assert dates == ["2024-06-25", "2024-06-26"]


def test_read_dates_manifest_empty_list_ok(manifest_s3):
    """An empty manifest is valid — Transform produced zero rows."""
    s3, bucket = manifest_s3
    key = "manifests/empty.json"
    s3.put_object(Bucket=bucket, Key=key, Body=b'{"dates": []}')

    assert read_dates_manifest(s3, bucket, key) == []


def test_read_dates_manifest_missing_dates_key_raises(manifest_s3):
    """Manifest without 'dates' key must fail loudly (no silent full-scan)."""
    s3, bucket = manifest_s3
    key = "manifests/malformed.json"
    s3.put_object(Bucket=bucket, Key=key, Body=b'{"oops": []}')

    with pytest.raises(ValueError, match="dates"):
        read_dates_manifest(s3, bucket, key)


def test_read_dates_manifest_missing_object_raises(manifest_s3):
    """A missing manifest is a programming error — surface it, don't swallow."""
    s3, bucket = manifest_s3
    with pytest.raises(s3.exceptions.NoSuchKey):
        read_dates_manifest(s3, bucket, "manifests/does-not-exist.json")


def test_read_dates_manifest_stringifies_values(manifest_s3):
    """All returned values are str — defensive against numeric dates in JSON."""
    s3, bucket = manifest_s3
    key = "manifests/numeric.json"
    # JSON can't store dates natively; if a producer ever serialises one as
    # a number by mistake, str() coerces it consistently.
    s3.put_object(Bucket=bucket, Key=key, Body=b'{"dates": ["2024-06-25", 20240626]}')

    dates = read_dates_manifest(s3, bucket, key)
    assert dates == ["2024-06-25", "20240626"]
    assert all(isinstance(d, str) for d in dates)
