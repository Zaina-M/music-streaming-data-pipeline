# Unit Tests

Tests for the Lambda functions and Glue scripts that ship with this
pipeline. All tests run **locally** with no AWS account required —
`moto` mocks boto3 for the Lambdas, and a local PySpark session mocks
Glue for the Spark jobs.

---

## Test layout

```
tests/
├── conftest.py            # Shared fixtures (sys.path setup, SparkSession)
├── pytest.ini             # Test discovery + markers
├── requirements-test.txt  # pytest, moto, pyspark
├── lambda_tests/
│   ├── test_archive.py            # archive Lambda (S3 copy + delete)
│   └── test_pipeline_trigger.py   # SQS-driven Step Functions trigger
└── glue_tests/
    ├── test_validate.py    # validate_csv pure function (no Spark)
    ├── test_transform.py   # enrich_streams (Spark)
    └── test_load.py        # KPI computation functions (Spark)
```

> The folders are named `lambda_tests` and `glue_tests` — not `lambda`
> and `glue` — because `lambda` is a Python reserved keyword and using
> it as a package name breaks imports.

---

## Setup (one-time)

From the `terraform/` directory:

```powershell
# Optional but recommended: a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install test dependencies
pip install -r tests/requirements-test.txt
```

PySpark requires **Java 11+** on PATH. Check with:

```powershell
java -version
```

Install Temurin / Microsoft OpenJDK if missing.

### Windows + PySpark: pick one of three paths

PySpark on Windows expects a Hadoop-style filesystem and fails at runtime if `winutils.exe` isn't available. You'll see errors like `py4j.protocol.Py4JJavaError ... collectToPython` on every Spark operation. This has nothing to do with the production pipeline (which runs on Glue / Linux) — it's a local-tooling gap.

You have three options:

**Option A — Skip Spark tests (recommended for fast iteration).**

The Lambda + `validate_csv` tests cover most of the truly-testable logic. Skip the Spark suite locally and rely on the PySpark CI runner (Linux) to validate transform/load:

```powershell
pytest tests -m "not spark"
```

You get green coverage of the Lambdas and validation in ~3 seconds.

**Option B — Install `winutils` (real fix).**

Pick a `winutils` build matching your Hadoop client version (pyspark 3.5.x bundles Hadoop 3.3.x). Steps:

1. Download `winutils.exe` + `hadoop.dll` for Hadoop 3.3.x from one of the maintained mirrors (search `winutils 3.3` — github.com/cdarlint/winutils is the canonical one).
2. Place them at `C:\hadoop\bin\winutils.exe` and `C:\hadoop\bin\hadoop.dll`.
3. Set environment variables (User → System Properties → Environment Variables):
   - `HADOOP_HOME` = `C:\hadoop`
   - Append `%HADOOP_HOME%\bin` to `Path`
4. Restart your terminal and re-run `pytest tests`.

**Option C — Run tests inside WSL (cleanest, slower setup).**

WSL gives you a Linux environment where PySpark works without any of this. From PowerShell:

```powershell
wsl --install   # one-time, requires reboot
wsl             # opens a bash shell
# Then inside WSL:
sudo apt install python3-pip default-jre
pip install -r terraform/tests/requirements-test.txt
pytest terraform/tests
```

After this, both Spark and non-Spark tests pass identically to how they'd run in CI.

---

## Running tests

From `terraform/`:

```powershell
# Everything
pytest tests

# Lambda tests only (fast — no Spark startup)
pytest tests/lambda_tests

# Only the non-Spark Glue test
pytest tests/glue_tests/test_validate.py

# Skip Spark tests (e.g. in CI without Java)
pytest tests -m "not spark"

# Only Spark tests
pytest tests -m spark

# Verbose with stdout shown for passing tests too
pytest tests -v -s
```

---

## What each suite covers

### `test_archive.py`
- Happy path: file moves raw → archive, raw key is deleted afterwards.
- Nested keys preserved.
- Missing event fields raise `ValueError` immediately.
- Non-existent source object raises `ClientError`.

### `test_pipeline_trigger.py`
- Idle case: no execution running → starts a new one with the EB event as input.
- Busy case: an execution is RUNNING → Lambda raises so SQS will retry.
- Defensive: multi-record batches and malformed JSON bodies fail fast.

### `test_validate.py`
- Schema and header rules.
- Per-row empty-field rules.
- ISO datetime parsing (space + `T` separator both accepted).
- Error messages include line numbers matching what a human counts.
- The expected-columns constant is locked.

### `test_transform.py`
- Inner join semantics: unmatched track_id / user_id rows are dropped.
- Song and user attributes attached to joined rows.
- `listen_date` column derived from `listen_time`.
- Empty input → empty output, schema preserved.

### `test_load.py`
- `compute_daily_genre_kpis`: listen_count + unique_listeners math.
- `compute_top_songs_per_genre`: ordering, `rank_song_id` formatting, top-N cap.
- `compute_top_genres_daily`: ordering, padded rank, date column.
- `row_to_dynamo_item`: float → Decimal coercion, None values dropped.

---

## CI tips

For GitHub Actions or similar:

```yaml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: "3.12"

- name: Set up Java (for PySpark)
  uses: actions/setup-java@v4
  with:
    distribution: temurin
    java-version: "17"

- name: Install test deps
  run: pip install -r terraform/tests/requirements-test.txt

- name: Run Lambda tests (fast)
  run: pytest terraform/tests/lambda_tests

- name: Run Glue tests (Spark)
  run: pytest terraform/tests/glue_tests
```

Splitting the two `pytest` invocations means a Lambda regression fails
the CI job in ~30 seconds instead of waiting for Spark to warm up.

---

## Adding new tests

1. Put the file under `lambda_tests/` or `glue_tests/`.
2. Name it `test_<something>.py` (pytest discovery).
3. If it needs PySpark, add `pytestmark = pytest.mark.spark` at the top
   so it's filterable by marker.
4. Import production code directly (`from archive import lambda_handler`).
   The path is set up by `conftest.py`.
