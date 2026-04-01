"""
ETL PIPELINE (Extract → Transform → Load)
==========================================
What this does:
  1. EXTRACT:  Read raw trade CSV from local disk (produced by fetch_binance.py
               or generate_trades.py)
  2. TRANSFORM: Cast data types, parse timestamps, drop nulls, add derived columns
  3. LOAD:     Write cleaned data as Parquet to local disk (partitioned by symbol)

Why Parquet?
  - CSV is row-based, slow to scan columns. Parquet is columnar and compressed.
  - A 50MB CSV becomes ~10MB Parquet and queries run 10–100x faster.
  - This is what production big-data pipelines use (AWS Athena, Spark, etc.)

Why Spark?
  - Pandas loads everything into RAM (fails on large data).
  - Spark distributes work across cores/machines — scales from laptop to cluster.
  - Locally: Spark uses all your CPU cores (local[*]).
  - On AWS EMR: same code runs across a cluster automatically.

Fault tolerance:
  - Parquet write uses a staging directory + atomic rename to prevent corruption.
  - The entire ETL function is wrapped with @retry for transient errors.
  - Structured logging replaces print() for auditability.

Config-driven:
  - MODE="local" → reads CSV from local disk, writes Parquet to local disk
  - MODE="aws"   → reads from S3, writes Parquet back to S3
"""

import os
import shutil

from pyspark.sql.functions import col, to_timestamp, lit, when
from pyspark.sql.types import DoubleType, IntegerType

from config import get_config, MODE
from etl.spark_utils import get_or_create_spark
from utils.fault_tolerance import get_logger, retry

log = get_logger("etl")


@retry(max_retries=3, base_delay=2.0, exceptions=(Exception,))
def run_etl():
    """
    Run the ETL pipeline: read raw CSV → clean → write Parquet locally.
    Returns the path to the cleaned Parquet output directory.

    Fault tolerance:
      - Writes to a staging path first, then atomically replaces the final path.
      - Retries up to 3× with exponential back-off.
    """
    cfg = get_config()

    # ---- STEP 1: Get / create Spark session ----
    spark = get_or_create_spark("MarketSurveillance_ETL")
    log.info("Spark session created")

    # ---- STEP 2: EXTRACT — Read raw CSV from local disk ----
    input_path = cfg["trades_csv"]
    log.info("Reading raw CSV: %s", input_path)

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
        .withColumn("quantity", col("quantity").cast(DoubleType())) \
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

    # ---- STEP 4: LOAD — Write as Parquet locally (safe, atomic) ----
    output_path = cfg["parquet_dir"]
    staging_path = output_path.rstrip("/") + "_staging"

    # Ensure parent directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    log.info("Writing cleaned Parquet to staging: %s", staging_path)

    # Write to staging directory first
    clean_df.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(staging_path)

    # Atomic swap: remove old output, rename staging → final
    log.info("Atomic swap: staging → %s", output_path)
    try:
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        shutil.move(staging_path, output_path)
    except Exception as exc:
        log.warning("Atomic rename failed (%s) — falling back to direct overwrite", exc)
        clean_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path)
        if os.path.exists(staging_path):
            shutil.rmtree(staging_path)

    log.info("ETL COMPLETE — %s records written to Parquet at %s",
             f"{clean_count:,}", output_path)

    return output_path


if __name__ == "__main__":
    path = run_etl()
    from pyspark.sql import SparkSession
    SparkSession.builder.getOrCreate().stop()
