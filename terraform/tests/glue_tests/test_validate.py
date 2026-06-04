"""
Unit tests for the pure validate_csv() function in validate.py.

This test module has zero AWS or Glue dependencies — validate_csv is a
plain string-in / tuple-out function. The tests just feed CSV literals
and assert on the (ok, message, row_count) return value.

If a future change makes validate_csv depend on boto3 / awsglue,
these tests will fail at import time and that should be treated as a
regression in testability, not just a missing dependency.
"""

import pytest

from validate import EXPECTED_COLUMNS, validate_csv


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_valid_csv_passes():
    csv_text = (
        "user_id,track_id,listen_time\n"
        "1,abc,2024-06-25 17:43:13\n"
        "2,def,2024-06-25 17:43:14\n"
    )
    ok, message, rows = validate_csv(csv_text)
    assert ok is True
    assert rows == 2
    assert "2 rows" in message


def test_iso_datetime_with_T_separator_accepted():
    """ISO 8601 also allows 'T' between date and time."""
    csv_text = (
        "user_id,track_id,listen_time\n"
        "1,abc,2024-06-25T17:43:13\n"
    )
    ok, _, rows = validate_csv(csv_text)
    assert ok is True
    assert rows == 1


# ---------------------------------------------------------------------------
# Schema failures
# ---------------------------------------------------------------------------

def test_empty_file_fails():
    ok, message, _ = validate_csv("")
    assert ok is False
    assert "empty" in message.lower()


def test_header_only_file_fails():
    csv_text = "user_id,track_id,listen_time\n"
    ok, message, rows = validate_csv(csv_text)
    assert ok is False
    assert rows == 0
    assert "header" in message.lower()


def test_wrong_header_fails():
    csv_text = (
        "wrong,columns,here\n"
        "1,abc,2024-06-25 17:43:13\n"
    )
    ok, message, _ = validate_csv(csv_text)
    assert ok is False
    assert "header mismatch" in message


def test_column_count_mismatch_fails():
    """Extra commas in a row → wrong field count."""
    csv_text = (
        "user_id,track_id,listen_time\n"
        "1,abc,2024-06-25 17:43:13,extra-field\n"
    )
    ok, message, _ = validate_csv(csv_text)
    assert ok is False
    assert "column count mismatch" in message


# ---------------------------------------------------------------------------
# Per-field failures
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_row,description", [
    (",abc,2024-06-25 17:43:13",        "empty user_id"),
    ("1,,2024-06-25 17:43:13",          "empty track_id"),
    ("1,abc,",                          "empty listen_time"),
])
def test_empty_required_field_fails(bad_row, description):
    csv_text = f"user_id,track_id,listen_time\n{bad_row}\n"
    ok, message, _ = validate_csv(csv_text)
    assert ok is False, f"expected failure for: {description}"
    assert "empty value" in message


@pytest.mark.parametrize("bad_timestamp", [
    "not-a-date",
    "2024/06/25",
    "06-25-2024 17:43:13",
    "",
])
def test_malformed_timestamp_fails(bad_timestamp):
    if bad_timestamp == "":
        # The empty-field check catches this before the datetime check.
        # Skip to avoid double-counting.
        pytest.skip("empty handled by earlier rule")

    csv_text = (
        "user_id,track_id,listen_time\n"
        f"1,abc,{bad_timestamp}\n"
    )
    ok, message, _ = validate_csv(csv_text)
    assert ok is False
    assert "not ISO datetime" in message


def test_error_reports_correct_line_number():
    """Line numbers in error messages should match human-counted lines."""
    csv_text = (
        "user_id,track_id,listen_time\n"  # line 1 (header)
        "1,abc,2024-06-25 17:43:13\n"      # line 2 (valid)
        "2,def,2024-06-25 17:43:14\n"      # line 3 (valid)
        "3,ghi,not-a-date\n"               # line 4 (BAD)
    )
    ok, message, _ = validate_csv(csv_text)
    assert ok is False
    assert "line 4" in message


def test_partial_progress_reported_on_failure():
    """When row 4 fails, row_count should still report the 2 valid rows seen."""
    csv_text = (
        "user_id,track_id,listen_time\n"
        "1,abc,2024-06-25 17:43:13\n"
        "2,def,2024-06-25 17:43:14\n"
        "3,ghi,bad-timestamp\n"
    )
    _, _, rows = validate_csv(csv_text)
    assert rows == 2


# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

def test_expected_columns_unchanged():
    """
    Guardrail: if someone changes the expected schema, this test forces
    them to also update the Glue transform job that relies on these
    column names downstream.
    """
    assert EXPECTED_COLUMNS == ["user_id", "track_id", "listen_time"]
