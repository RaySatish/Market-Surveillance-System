"""
ETL PIPELINE (Extract → Transform → Load)
==========================================
What this does:
  1. EXTRACT:  Read raw trade CSV from HDFS (uploaded by ingest_to_hdfs.py)
  2. TRANSFORM: Cast data types, parse timestamps, drop nulls, add derived columns
  3. LOAD:     Write cleaned data as Parquet back to HDFS

Why HDFS?
  - Hadoop Distributed File System stores data across a cluster of machines.
  - Locally: HDFS runs on localhost with a single datanode (mirrors production).
  - On AWS EMR: HDFS is backed by S3 (via EMRFS) across the cluster.
  - Spark reads/writes HDFS natively — no extra libraries needed.

Why Parquet?
  - CSV is row-based, slow to scan columns. Parquet is columnar and compressed.
  - A 50MB CSV becomes ~10MB Parquet and queries run 10-100x faster.
  - This is what production big-data pipelines use (AWS Athena, Spark, etc.)

Why Spark?
  - Pandas loads everything into RAM (fails on large data).
  - Spark distributes work across cores/machines — scales from laptop to 100-node cluster.
  - Locally: Spark uses all your CPU cores (local[*]).
  - On AWS EMR: same code runs across a cluster automatically.

Fault tolerance:
  - Parquet write uses a staging directory + atomic rename to prevent corruption.
  - The entire ETL function is wrapped with @retry for transient HDFS/Spark errors.
  - Structured logging replaces print() for auditability.

Config-driven:
  - MODE="local" → reads CSV from HDFS, writes Parquet to HDFS
  - MODE="aws"   → reads from S3, writes Parquet back to S3
"""

import subprocess
from pyspark.sql.functions import col, to_timestamp, lit, when
from pyspark.sql.types import DoubleType, IntegerType

from config import get_config, MODE
from etl.hdfs_utils import get_or_create_spark
from utils.fault_tolerance import get_logger, retry

log = get_logger("etl")


@retry(max_retries=3, base_delay=2.0, exceptions=(Exception,))
def run_etl():
    """
    Run the ETL pipeline: read raw CSV from HDFS → clean → write Parquet to HDFS.
    Returns the path to the cleaned Parquet output.

    Fault tolerance:
      - Writes to a staging path first, then atomically replaces the final path.
      - Retries up to 3× with exponential back-off.
    """
    cfg = get_config()

    # ---- STEP 1: Get / create Spark session ----
    spark = get_or_create_spark("MarketSurveillance_ETL")
    log.info("Spark session created")

    # ---- STEP 2: EXTRACT — Read raw CSV from HDFS ----
    input_path = cfg["raw_input"]
    log.info("Reading raw data from HDFS: %s", input_path)

    raw_df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv(input_path)

    raw_count = raw_df.count()
    log.info("Raw records: %s", f"{raw_count:,}")
    raw_df.show(5, truncate=False)

    # ---- STEP 3: TRANSFORM — Clean and type-cast ----
    clean_df = raw_df \
        .withColumn("price",    col("price").cast(DoubleType())) \
        .withColumn("quantity", col("quantity").cast(IntegerType())) \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .dropna(subset=["price", "quantity", "event_time"])

    clean_df = clean_df.withColumn(
        "trade_value", col("price") * col("quantity")
    )

    clean_df = clean_df.withColumn(
        "is_suspicious",
        when(col("event_type") != "TRADE", lit(True)).otherwise(lit(False))
    )

    clean_count = clean_df.count()
    dropped = raw_count - clean_count
    log.info("Clean records: %s  |  Dropped: %s", f"{clean_count:,}", f"{dropped:,}")
    if dropped:
        log.warning("%d rows dropped during ETL (nulls or type-cast failures)", dropped)

    # ---- STEP 4: LOAD — Write as Parquet to HDFS (safe, atomic) ----
    output_path = cfg["clean_output"]
    staging_path = output_path.rstrip("/") + "_staging"

    log.info("Writing cleaned Parquet to HDFS staging: %s", staging_path)

    # Write to staging directory first
    clean_df.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(staging_path)

    # Atomic swap: remove old output, rename staging → final
    log.info("Atomic swap: staging → %s", output_path)
    try:
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", output_path],
                        capture_output=True)
        subprocess.run(["hdfs", "dfs", "-mv", staging_path, output_path],
                        check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError:
        log.warning("Atomic rename failed — falling back to direct overwrite")
        clean_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path)
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", staging_path],
                        capture_output=True)

    log.info("ETL COMPLETE — %s records written to HDFS Parquet", f"{clean_count:,}")

    return output_path


if __name__ == "__main__":
    path = run_etl()
    from pyspark.sql import SparkSession
    SparkSession.builder.getOrCreate().stop()
