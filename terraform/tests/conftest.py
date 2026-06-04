"""
conftest.py — pytest fixtures shared across all test modules.

Three responsibilities:
  1. Put the production scripts/ folder on sys.path so tests can
     `import archive`, `import validate`, etc. directly.
  2. Provide a session-scoped SparkSession for the PySpark tests so
     we only pay the ~10s Spark startup cost once per test run.
  3. Make the SparkSession as Windows-friendly as possible. Real fix
     for Windows is installing winutils — see tests/README.md — but
     the config tweaks below remove a couple of the rougher edges.
"""

import os
import sys
import tempfile

import pytest

# ---------------------------------------------------------------------------
# Make production scripts importable as top-level modules.
# ---------------------------------------------------------------------------
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TESTS_DIR)
SCRIPTS_LAMBDA = os.path.join(PROJECT_ROOT, "scripts", "lambda")
SCRIPTS_GLUE = os.path.join(PROJECT_ROOT, "scripts", "glue")

# Prepend rather than append so our scripts win over anything else with
# the same module names on the user's machine.
for path in (SCRIPTS_LAMBDA, SCRIPTS_GLUE):
    if path not in sys.path:
        sys.path.insert(0, path)


# ---------------------------------------------------------------------------
# Session-scoped SparkSession. The first test that needs Spark pays the
# startup cost; subsequent tests reuse it.
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Local SparkSession suitable for unit tests."""
    from pyspark.sql import SparkSession

    # Per-session warehouse + temp dirs. Default Spark warehouse lives in
    # the current working directory ("spark-warehouse/"), which:
    #   - on Windows, often hits "permission denied" when chmod-ing
    #   - on shared CI runners, can collide between parallel jobs
    # Using a fresh temp dir per session sidesteps both issues.
    warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
    local_dir = tempfile.mkdtemp(prefix="spark-local-")

    builder = (
        SparkSession.builder
        # local[1] = single-threaded execution. On Windows this avoids
        # several shuffle/temp-file races that local[*] can trigger.
        # Tests are tiny — single-threaded is plenty fast.
        .master("local[1]")
        .appName("etl-unit-tests")
        # 1 shuffle partition — anything more is wasted overhead for our
        # tiny test DataFrames.
        .config("spark.sql.shuffle.partitions", "1")
        # Use in-memory catalog — no Derby, no Hive metastore on disk.
        .config("spark.sql.catalogImplementation", "in-memory")
        # Quieter logs.
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        # Custom temp/warehouse locations (see comment above).
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.local.dir", local_dir)
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    spark.stop()
