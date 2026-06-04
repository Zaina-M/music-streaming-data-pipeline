"""
validate.py — Validate Glue Job (Python shell)

Step 1 of the ETL pipeline. Validates the incoming stream CSV against a
known schema BEFORE we spin up an expensive Spark cluster for Transform.
Fail-fast saves money and gives clearer error messages.

Structure
---------
The file is split into two layers so the validation logic is testable
without any AWS or Glue runtime in scope:

  * validate_csv(csv_text) — PURE function. Takes the raw CSV string,
    returns (ok: bool, message: str, row_count: int). Has zero
    dependencies on boto3, awsglue, sys.argv, or environment.

  * main() — IO wrapper. Parses Glue job args, downloads the object
    from S3, hands the text to validate_csv, exits non-zero on failure.

Validation rules:
  1. Header matches the expected columns exactly.
  2. No row has null/empty user_id, track_id, or listen_time.
  3. listen_time parses as ISO datetime.
  4. File has at least one data row.

Job parameters (passed in by Step Functions via --arg=value):
  --raw_bucket   S3 bucket containing the stream file
  --object_key   Key of the stream file to validate
"""

import csv
import io
import sys
from datetime import datetime

# The expected schema for stream files. Any divergence is a validation
# failure — schemas evolve through deliberate code changes, not surprise.
EXPECTED_COLUMNS = ["user_id", "track_id", "listen_time"]


def validate_csv(csv_text):
    """
    Pure validation of a stream CSV.

    Parameters
    ----------
    csv_text : str
        The full CSV body as a string (header included).

    Returns
    -------
    (ok, message, row_count) : (bool, str, int)
        ok=True means every rule passed. message is human-readable.
        row_count is the number of DATA rows checked (header excluded).
    """
    reader = csv.reader(io.StringIO(csv_text))

    # 1. Header check — must exist and exactly match the expected columns.
    try:
        header = next(reader)
    except StopIteration:
        return (False, "file is empty", 0)

    if header != EXPECTED_COLUMNS:
        return (
            False,
            f"header mismatch: expected {EXPECTED_COLUMNS}, got {header}",
            0,
        )

    # 2. Row-level checks. start=2 so line numbers in error messages
    # match what a human sees in a text editor (header is line 1).
    row_count = 0
    for line_no, row in enumerate(reader, start=2):
        if len(row) != len(EXPECTED_COLUMNS):
            return (
                False,
                f"line {line_no}: column count mismatch — {row}",
                row_count,
            )

        user_id, track_id, listen_time = row

        if not user_id or not track_id or not listen_time:
            return (
                False,
                f"line {line_no}: empty value in required field — {row}",
                row_count,
            )

        # 3. Datetime parse — catches malformed timestamps upstream.
        try:
            datetime.fromisoformat(listen_time)
        except ValueError:
            return (
                False,
                f"line {line_no}: listen_time '{listen_time}' is not ISO datetime",
                row_count,
            )

        row_count += 1

    # 4. Must have at least one data row — a header-only file is a bug.
    if row_count == 0:
        return (False, "file contains only a header, no data rows", 0)

    return (True, f"PASSED — {row_count} rows checked", row_count)


def main():
    """IO wrapper — only invoked inside the Glue runtime."""
    # Imports are local so the module stays importable for unit tests
    # without awsglue / boto3 installed.
    import boto3
    from awsglue.utils import getResolvedOptions  # noqa: E402

    args = getResolvedOptions(sys.argv, ["raw_bucket", "object_key"])
    raw_bucket = args["raw_bucket"]
    object_key = args["object_key"]

    print(f"Validating s3://{raw_bucket}/{object_key}")

    s3 = boto3.client("s3")

    # Resolve our own account ID once and pass it as ExpectedBucketOwner to
    # every S3 call. This is the "confused-deputy" guardrail: if someone
    # ever sniped our bucket name in another account or reused this code
    # against the wrong account, the call fails fast instead of leaking data.
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    try:
        obj = s3.get_object(
            Bucket=raw_bucket,
            Key=object_key,
            ExpectedBucketOwner=account_id,
        )
    except s3.exceptions.NoSuchKey:
        print(
            f"VALIDATION FAILED: object {object_key} not found in bucket {raw_bucket}",
            file=sys.stderr,
        )
        sys.exit(1)

    body = obj["Body"].read().decode("utf-8")

    ok, message, _ = validate_csv(body)
    if not ok:
        print(f"VALIDATION FAILED: {message}", file=sys.stderr)
        sys.exit(1)

    print(f"Validation {message}")


if __name__ == "__main__":
    main()
