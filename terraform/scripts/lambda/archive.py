"""
archive.py — Archive Lambda

Invoked by the Step Functions state machine as its final step, after the
Load Glue job has successfully written KPIs to DynamoDB. Its single
responsibility is to move the processed stream file from the raw bucket
to the archive bucket.

S3 has no native "move" operation — we copy the object then delete the
original. If the copy succeeds but the delete fails, we log and re-raise
so Step Functions can retry; the copy is idempotent (same destination
key overwrites).

Expected event payload (passed in by Step Functions):
    {
        "raw_bucket":     "<source bucket>",
        "archive_bucket": "<destination bucket>",
        "object_key":     "<key of the processed file>"
    }
"""

import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Logger setup — Lambda runtime sends anything written here to CloudWatch.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Explicit boto3 timeouts. Defaults are 60s connect + 60s read, which is
# way too long for an S3 control-plane call inside a Lambda — a hung
# connection would burn the full Terraform-side 60s function timeout
# before raising. With these settings, a slow S3 call fails fast and
# the standard retry mode applies sane backoff.
_BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=10,
    retries={"max_attempts": 3, "mode": "standard"},
)

# Initialize the S3 client outside the handler so it's reused across warm
# invocations. Cold-start cost is paid once; warm invocations get a free
# pre-built TCP connection pool.
s3 = boto3.client("s3", config=_BOTO_CONFIG)

# Resolve our own account ID once at cold start. Used as ExpectedBucketOwner
# on every S3 call — confused-deputy guardrail that ensures we only ever
# read/write buckets in OUR account, even if a bad event payload points us
# at someone else's bucket name.
_ACCOUNT_ID = boto3.client("sts", config=_BOTO_CONFIG).get_caller_identity()["Account"]


def lambda_handler(event, context):
    """Move one object from raw → archive."""
    logger.info("Archive Lambda invoked with event: %s", event)

    # Extract the three required fields. If any are missing, we want to
    # fail fast with a clear error rather than silently misroute data.
    try:
        raw_bucket = event["raw_bucket"]
        archive_bucket = event["archive_bucket"]
        object_key = event["object_key"]
    except KeyError as missing:
        raise ValueError(f"Required event field missing: {missing}") from missing

    copy_source = {"Bucket": raw_bucket, "Key": object_key}

    # Step 1: copy to archive bucket. We preserve the original key so the
    # archive mirrors the raw layout, which makes auditing trivial.
    # Both buckets are verified to belong to our account — Expected*BucketOwner
    # blocks bucket-sniping / confused-deputy attacks.
    try:
        logger.info(
            "Copying s3://%s/%s -> s3://%s/%s",
            raw_bucket, object_key, archive_bucket, object_key,
        )
        s3.copy_object(
            CopySource=copy_source,
            Bucket=archive_bucket,
            Key=object_key,
            ExpectedBucketOwner=_ACCOUNT_ID,
            ExpectedSourceBucketOwner=_ACCOUNT_ID,
        )
    except ClientError:
        # logger.exception captures the active exception automatically —
        # no need to bind it to a name.
        logger.exception("Copy failed — aborting before delete")
        raise  # Re-raise so Step Functions records the failure.

    # Step 2: delete from raw bucket. Only runs if copy succeeded, so we
    # never lose data even if this Lambda dies mid-execution.
    try:
        logger.info("Deleting s3://%s/%s", raw_bucket, object_key)
        s3.delete_object(
            Bucket=raw_bucket,
            Key=object_key,
            ExpectedBucketOwner=_ACCOUNT_ID,
        )
    except ClientError:
        # If delete fails after copy succeeded, the archive holds the
        # canonical copy. The raw bucket will be cleaned up on retry —
        # the copy_object call above is idempotent.
        logger.exception("Delete from raw failed (copy was successful)")
        raise

    return {
        "status": "archived",
        "from": f"s3://{raw_bucket}/{object_key}",
        "to": f"s3://{archive_bucket}/{object_key}",
    }
