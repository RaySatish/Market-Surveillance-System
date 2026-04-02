"""
SPARK UTILITIES
===============
Provides helper functions for creating a SparkSession and reading local Parquet.

Why this exists:
  - pandas cannot read Parquet partitioned directories as efficiently as Spark.
  - Spark reads local Parquet natively (no HDFS required on Phase 1/2).
  - This replaces the old hdfs_utils.py — same interface, no HDFS dependency.

On AWS (Phase 3):
  - The same code works — just change parquet_dir in config.py to s3a://...
  - Add the hadoop-aws JAR via spark.jars.packages.

Performance note:
  - .toPandas() pulls all data into driver RAM. Fine for detection algorithms
    that need full-dataset analysis on a single node (our dataset fits in memory).
  - In a true distributed deployment, detection logic would run in Spark directly.

Fault tolerance:
  - Spark session creation and Parquet reads are wrapped with retry/backoff.
"""

import os

# ── Spark environment fix ────────────────────────────────────────────────────
# If SPARK_HOME points to a standalone Spark install (e.g. 3.x), it conflicts
# with the PySpark 4.x bundled JARs in the venv.  Unset it so PySpark uses
# its own bundled JARs (the correct approach when installing via pip).
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# Prefer Java 17 if available (PySpark 4.x requires Java 17+).
_java17 = "/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home"
if os.path.isdir(_java17):
    os.environ["JAVA_HOME"] = _java17
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession

from config import get_config, MODE
from utils.fault_tolerance import get_logger, retry

log = get_logger("spark_utils")


def get_or_create_spark(app_name: str = "MarketSurveillance") -> SparkSession:
    """
    Get an existing SparkSession or create a new one.

    Why getOrCreate?
      - If run_all_detections.py already started a Spark session for ETL,
        this reuses it (no duplicate JVM startup).
      - If a detector runs standalone, this creates a fresh session.
    """
    cfg = get_config()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(cfg["spark_master"])
        # Suppress verbose Spark/Hadoop INFO logs
        .config("spark.sql.shuffle.partitions", "4")  # sensible default for laptop
    )

    # AWS mode: add S3 filesystem connector
    if MODE == "aws":
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session ready (master=%s, app=%s)", cfg["spark_master"], app_name)
    return spark


@retry(max_retries=3, base_delay=2.0, exceptions=(Exception,))
def read_parquet(path: str = None):
    """
    Read a Parquet dataset from local disk (or S3 in AWS mode) and return
    a pandas DataFrame.

    Args:
        path: Path to the Parquet folder. Defaults to cfg["parquet_dir"].

    Returns:
        pandas DataFrame with all columns from the Parquet.

    Fault tolerance:
      - Retries up to 3 times with exponential back-off on read failures.
    """
    cfg = get_config()
    if path is None:
        path = cfg["parquet_dir"]

    log.info("Reading Parquet from: %s", path)

    spark = get_or_create_spark()
    spark_df = spark.read.parquet(path)

    record_count = spark_df.count()
    log.info("Records in Parquet: %s", f"{record_count:,}")

    # Convert distributed Spark DataFrame → local pandas DataFrame
    pdf = spark_df.toPandas()
    return pdf
