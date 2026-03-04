"""
HDFS UTILITIES
==============
Provides helper functions for reading data from HDFS.

Why this exists:
  - pandas cannot read files from HDFS directly (it only knows local paths).
  - Spark CAN read HDFS natively (it's built on Hadoop).
  - This utility creates a Spark session, reads Parquet from HDFS,
    converts it to a pandas DataFrame, and returns it.
  - Detection scripts and the dashboard use this instead of pd.read_parquet().

On AWS EMR:
  - The same code works — just change the path from hdfs:// to s3a://
  - Spark on EMR already has the S3 connector configured.

Performance note:
  - .toPandas() pulls all data into the driver's RAM. Fine for detection
    algorithms that need full-dataset analysis on a single node.
  - In a true distributed deployment, detection logic would also run in Spark
    (e.g., using groupBy + UDF). We use pandas here for clarity and because
    our dataset fits in memory.

Fault tolerance:
  - Spark session creation and Parquet reads are wrapped with retry/backoff.
  - HDFS replication factor is set to 3 (configurable in config.py).
"""

from pyspark.sql import SparkSession

from config import get_config, MODE, HDFS_REPLICATION_FACTOR
from utils.fault_tolerance import get_logger, retry

log = get_logger("hdfs_utils")


def get_or_create_spark(app_name="MarketSurveillance"):
    """
    Get an existing SparkSession or create a new one.

    Why getOrCreate?
      - If run_all_detections.py already started a Spark session for ETL,
        this reuses it (no duplicate JVM startup).
      - If a detector runs standalone, this creates a fresh session.

    Fault tolerance:
      - Sets HDFS replication factor to HDFS_REPLICATION_FACTOR (default 3).
    """
    cfg = get_config()

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(cfg["spark_master"]) \
        .config("spark.hadoop.dfs.replication", str(HDFS_REPLICATION_FACTOR))

    # AWS needs the S3 filesystem connector
    if MODE == "aws":
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session ready (master=%s, replication=%d)",
             cfg["spark_master"], HDFS_REPLICATION_FACTOR)
    return spark


@retry(max_retries=3, base_delay=2.0, exceptions=(Exception,))
def read_parquet_from_hdfs(path=None):
    """
    Read a Parquet dataset from HDFS (or S3) and return a pandas DataFrame.

    Args:
        path: HDFS/S3 path to the Parquet folder. Defaults to cfg["clean_output"].

    Returns:
        pandas DataFrame with all columns from the Parquet.

    Fault tolerance:
      - Retries up to 3 times with exponential back-off on HDFS read failures.
    """
    cfg = get_config()
    if path is None:
        path = cfg["clean_output"]

    log.info("Reading Parquet from HDFS: %s", path)

    spark = get_or_create_spark()
    spark_df = spark.read.parquet(path)

    record_count = spark_df.count()
    log.info("Records in HDFS Parquet: %s", f"{record_count:,}")

    # Convert distributed Spark DataFrame → local pandas DataFrame
    pdf = spark_df.toPandas()
    return pdf
