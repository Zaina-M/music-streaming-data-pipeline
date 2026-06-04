"""
Unit tests for transform.py:

  * enrich_streams        — the join + listen_date derivation (pure Spark)
  * write_dates_manifest  — the partition-pruning hand-off to Load (moto S3)

Most tests use a local PySpark session (provided by the spark fixture
in conftest.py). They're slow to start because of Spark cold-start —
~10s on most laptops — but each individual assertion is fast.

Run only these with:
    pytest tests/glue_tests/test_transform.py -m spark
"""

import datetime as dt
import json
import os

import boto3
import pytest
from moto import mock_aws

from transform import enrich_streams, write_dates_manifest

# All tests here require Spark — mark them so they can be opted out of.
pytestmark = pytest.mark.spark

# Region passed to moto's in-memory S3 client. Sourced from env so it
# isn't a hardcoded literal (SonarLint S6262); falls back to a sensible
# default for local runs where AWS_DEFAULT_REGION isn't set.
TEST_AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")


# ---------------------------------------------------------------------------
# DataFrame fixtures — small, hand-crafted samples that exercise the join.
# ---------------------------------------------------------------------------

@pytest.fixture
def streams_df(spark):
    return spark.createDataFrame(
        [
            ("u1", "t1", "2024-06-25 10:00:00"),
            ("u2", "t2", "2024-06-25 11:30:00"),
            ("u1", "t1", "2024-06-26 09:00:00"),  # different day, same user/track
            ("u3", "t-nonexistent", "2024-06-25 12:00:00"),  # no matching song
        ],
        ["user_id", "track_id", "listen_time"],
    )


@pytest.fixture
def songs_df(spark):
    return spark.createDataFrame(
        [
            ("t1", "Song One", "pop", "Artist A"),
            ("t2", "Song Two", "rock", "Artist B"),
            # t-nonexistent intentionally absent — inner join must drop it.
        ],
        ["track_id", "track_name", "track_genre", "artists"],
    )


@pytest.fixture
def users_df(spark):
    return spark.createDataFrame(
        [
            ("u1", "Alice", 25, "UK"),
            ("u2", "Bob", 40, "US"),
            # u3 intentionally absent — inner join must drop the streams/u3 row
            # if its song was present (it isn't, but defense-in-depth).
        ],
        ["user_id", "user_name", "user_age", "user_country"],
    )


# ---------------------------------------------------------------------------
# Join correctness
# ---------------------------------------------------------------------------

def test_inner_join_drops_unmatched_rows(streams_df, songs_df, users_df):
    """Rows whose track_id has no song OR user_id has no user must vanish."""
    result = enrich_streams(streams_df, songs_df, users_df)

    # Out of 4 input rows, only 3 have matching song+user: 2 from u1, 1 from u2.
    assert result.count() == 3


def test_song_attributes_attached(streams_df, songs_df, users_df):
    """Joined rows should carry song fields (genre, name, artist)."""
    result = enrich_streams(streams_df, songs_df, users_df).collect()

    genres = {row["track_genre"] for row in result}
    assert genres == {"pop", "rock"}

    names = {row["track_name"] for row in result}
    assert "Song One" in names
    assert "Song Two" in names


def test_user_attributes_attached(streams_df, songs_df, users_df):
    """Joined rows should carry user fields (name, age, country)."""
    result = enrich_streams(streams_df, songs_df, users_df).collect()

    countries = {row["user_country"] for row in result}
    assert countries == {"UK", "US"}


# ---------------------------------------------------------------------------
# Date partition derivation
# ---------------------------------------------------------------------------

def test_listen_date_column_added(streams_df, songs_df, users_df):
    """The output should include a listen_date column for partitioning."""
    result = enrich_streams(streams_df, songs_df, users_df)
    assert "listen_date" in result.columns


def test_listen_date_correctly_derived(streams_df, songs_df, users_df):
    """listen_date should be the calendar date part of listen_time."""
    result = enrich_streams(streams_df, songs_df, users_df).collect()

    dates = {row["listen_date"] for row in result}
    # u1+t1 listens on both 2024-06-25 and 2024-06-26; u2+t2 on 25th.
    assert dt.date(2024, 6, 25) in dates
    assert dt.date(2024, 6, 26) in dates


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_streams_returns_empty(spark, songs_df, users_df):
    """An empty stream file should produce zero output rows, not crash."""
    empty_streams = spark.createDataFrame(
        [],
        schema="user_id string, track_id string, listen_time string",
    )
    result = enrich_streams(empty_streams, songs_df, users_df)
    assert result.count() == 0
    # The listen_date column should still exist on the schema.
    assert "listen_date" in result.columns


# ---------------------------------------------------------------------------
# write_dates_manifest — Transform's partition-pruning hand-off to Load.
#
# These tests mock S3 with moto; they don't need Spark.
# ---------------------------------------------------------------------------

@pytest.fixture
def manifest_s3():
    """Moto-backed S3 with a pre-created processed bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_AWS_REGION)
        bucket = "processed-bucket-test"
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
        )
        yield s3, bucket


def test_write_dates_manifest_writes_json(manifest_s3):
    """Manifest payload should be JSON with a sorted 'dates' list."""
    s3, bucket = manifest_s3
    key = "manifests/streams/streams1.csv.json"

    returned = write_dates_manifest(
        s3, bucket, key, dates=[dt.date(2024, 6, 26), dt.date(2024, 6, 25)],
    )

    # Function returns the normalised list it wrote — useful for logging.
    assert returned == ["2024-06-25", "2024-06-26"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(obj["Body"].read())
    assert payload == {"dates": ["2024-06-25", "2024-06-26"]}


def test_write_dates_manifest_dedupes(manifest_s3):
    """Duplicate dates collapse — Spark partitions can repeat per worker."""
    s3, bucket = manifest_s3
    key = "manifests/dedupe.json"

    write_dates_manifest(
        s3, bucket, key, dates=["2024-06-25", "2024-06-25", "2024-06-26"],
    )

    payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    assert payload["dates"] == ["2024-06-25", "2024-06-26"]


def test_write_dates_manifest_drops_none(manifest_s3):
    """None values (defensive) should be filtered out."""
    s3, bucket = manifest_s3
    key = "manifests/drop-none.json"

    write_dates_manifest(s3, bucket, key, dates=["2024-06-25", None])

    payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    assert payload["dates"] == ["2024-06-25"]


def test_write_dates_manifest_empty_input(manifest_s3):
    """Empty input still produces a valid manifest (Load handles empty gracefully)."""
    s3, bucket = manifest_s3
    key = "manifests/empty.json"

    write_dates_manifest(s3, bucket, key, dates=[])

    payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    assert payload == {"dates": []}


def test_write_dates_manifest_sets_content_type(manifest_s3):
    """Stored as application/json so the S3 console renders it nicely."""
    s3, bucket = manifest_s3
    key = "manifests/content-type.json"

    write_dates_manifest(s3, bucket, key, dates=["2024-06-25"])

    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentType"] == "application/json"
