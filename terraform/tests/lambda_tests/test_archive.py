"""
Unit tests for archive.py — the Lambda that moves processed stream
files from the raw bucket to the archive bucket.

Strategy: moto mocks the entire boto3 S3 layer in-process so the tests
run in milliseconds and never touch real AWS.

Note: archive.py initializes its S3 client and looks up the account ID
at module import time, so we need to start the moto mocks BEFORE the
module is imported. The auto-applied fixture below handles this.
"""

import os

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws


# ---------------------------------------------------------------------------
# Module-level setup: moto must be active before archive.py imports, since
# archive.py creates a boto3 client and calls sts.get_caller_identity()
# at import time. autouse=True + scope="module" gives us exactly that.
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    """Force boto3 to use fake credentials so it never reads real ones."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"


@pytest.fixture
def s3_buckets():
    """
    Spin up moto with a raw and an archive bucket pre-seeded.
    The archive module is imported INSIDE the mock context — moto
    intercepts the module-level boto3.client('s3') call too.
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-1")
        s3.create_bucket(
            Bucket="raw-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        s3.create_bucket(
            Bucket="archive-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

        # Force a fresh import each test so the module-level clients pick
        # up moto's mocked endpoint, not a cached real client.
        import importlib
        import sys
        if "archive" in sys.modules:
            del sys.modules["archive"]
        import archive

        yield s3, archive


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------

def test_archives_object_successfully(s3_buckets):
    """The canonical case — file exists in raw, ends up in archive only."""
    s3, archive = s3_buckets

    # Seed: put a file in the raw bucket.
    s3.put_object(Bucket="raw-bucket", Key="streams/x.csv", Body=b"hello,world\n")

    event = {
        "raw_bucket": "raw-bucket",
        "archive_bucket": "archive-bucket",
        "object_key": "streams/x.csv",
    }

    result = archive.lambda_handler(event, None)

    # Archive bucket now has the file.
    archived = s3.get_object(Bucket="archive-bucket", Key="streams/x.csv")
    assert archived["Body"].read() == b"hello,world\n"

    # Raw bucket no longer has it.
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket="raw-bucket", Key="streams/x.csv")
    assert exc.value.response["Error"]["Code"] in ("404", "NoSuchKey")

    # Return value reports both paths.
    assert result["status"] == "archived"
    assert "raw-bucket/streams/x.csv" in result["from"]
    assert "archive-bucket/streams/x.csv" in result["to"]


def test_preserves_key_layout(s3_buckets):
    """Nested keys land at the same path in the archive bucket."""
    s3, archive = s3_buckets

    s3.put_object(
        Bucket="raw-bucket",
        Key="streams/2024/06/25/streams1.csv",
        Body=b"data",
    )

    archive.lambda_handler(
        {
            "raw_bucket": "raw-bucket",
            "archive_bucket": "archive-bucket",
            "object_key": "streams/2024/06/25/streams1.csv",
        },
        None,
    )

    # Same nested key in archive.
    s3.head_object(Bucket="archive-bucket", Key="streams/2024/06/25/streams1.csv")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_missing_event_field_raises_value_error(s3_buckets):
    """Step Functions sending a half-formed payload should fail fast."""
    _, archive = s3_buckets

    incomplete_event = {
        "raw_bucket": "raw-bucket",
        # archive_bucket missing
        "object_key": "streams/x.csv",
    }

    with pytest.raises(ValueError, match="archive_bucket"):
        archive.lambda_handler(incomplete_event, None)


def test_nonexistent_source_raises(s3_buckets):
    """If the raw object doesn't exist, copy fails — we re-raise."""
    _, archive = s3_buckets

    event = {
        "raw_bucket": "raw-bucket",
        "archive_bucket": "archive-bucket",
        "object_key": "streams/does-not-exist.csv",
    }

    with pytest.raises(ClientError):
        archive.lambda_handler(event, None)


def test_idempotency_safe_to_rerun(s3_buckets):
    """
    Re-running on the same file once it's already archived should not
    raise (the copy is idempotent), but the delete would 404. The
    current archive.py treats delete failure as fatal — we document
    that here. If this ever needs to change, this test will tell you.
    """
    s3, archive = s3_buckets

    s3.put_object(Bucket="raw-bucket", Key="streams/x.csv", Body=b"data")
    event = {
        "raw_bucket": "raw-bucket",
        "archive_bucket": "archive-bucket",
        "object_key": "streams/x.csv",
    }

    # First run succeeds.
    archive.lambda_handler(event, None)

    # Second run: source already gone. moto's delete_object on a missing
    # key returns success (matching real S3 behavior), so this should
    # succeed even though the copy step is now copying nothing useful.
    # If S3 ever raises on missing source for copy_object, this test will
    # catch the regression.
    with pytest.raises(ClientError):
        archive.lambda_handler(event, None)
