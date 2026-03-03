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

Config-driven:
  - MODE="local" → reads CSV from HDFS, writes Parquet to HDFS
  - MODE="aws"   → reads from S3, writes Parquet back to S3
"""

from pyspark.sql.functions import col, to_timestamp, lit, when
from pyspark.sql.types import DoubleType, IntegerType

from config import get_config, MODE
from hdfs_utils import get_or_create_spark


def run_etl():
    """
    Run the ETL pipeline: read raw CSV from HDFS → clean → write Parquet to HDFS.
    Returns the path to the cleaned Parquet output.
    """
    cfg = get_config()

    # ---- STEP 1: Get / create Spark session ----
    # Uses get_or_create_spark() from hdfs_utils so that the same session
    # is reused if run_all_detections.py already started one.
    spark = get_or_create_spark("MarketSurveillance_ETL")
    print("Spark session created")

    # ---- STEP 2: EXTRACT — Read raw CSV from HDFS ----
    input_path = cfg["raw_input"]
    print(f"Reading raw data from HDFS: {input_path}")

    raw_df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv(input_path)

    raw_count = raw_df.count()
    print(f"  Raw records: {raw_count:,}")
    raw_df.show(5, truncate=False)

    # ---- STEP 3: TRANSFORM — Clean and type-cast ----
    # Cast string columns to proper types for efficient storage and querying
    clean_df = raw_df \
        .withColumn("price",    col("price").cast(DoubleType())) \
        .withColumn("quantity", col("quantity").cast(IntegerType())) \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .dropna(subset=["price", "quantity", "event_time"])

    # Add a derived column: trade_value = price × quantity
    # Useful for detecting large-value suspicious trades
    clean_df = clean_df.withColumn(
        "trade_value", col("price") * col("quantity")
    )

    # Add a flag column: is_suspicious (anything that's not a normal TRADE)
    clean_df = clean_df.withColumn(
        "is_suspicious",
        when(col("event_type") != "TRADE", lit(True)).otherwise(lit(False))
    )

    clean_count = clean_df.count()
    print(f"  Clean records: {clean_count:,}")
    print(f"  Dropped: {raw_count - clean_count:,}")

    # ---- STEP 4: LOAD — Write as Parquet to HDFS ----
    output_path = cfg["clean_output"]
    print(f"Writing cleaned Parquet to HDFS: {output_path}")

    # .partitionBy("symbol") creates sub-folders per symbol (BTCUSDT/, ETHUSDT/, etc.)
    # This makes queries filtering by symbol MUCH faster (partition pruning)
    # HDFS handles directory creation automatically — no os.makedirs needed.
    clean_df.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(output_path)

    print(f"ETL COMPLETE — {clean_count:,} records written to HDFS Parquet")

    # NOTE: We do NOT call spark.stop() here.
    # The orchestrator (run_all_detections.py) will stop Spark after all
    # detectors have finished reading from HDFS.
    return output_path


if __name__ == "__main__":
    path = run_etl()
    # When run standalone, stop Spark
    from pyspark.sql import SparkSession
    SparkSession.builder.getOrCreate().stop()
