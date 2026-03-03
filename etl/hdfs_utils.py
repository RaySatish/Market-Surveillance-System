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
"""

from pyspark.sql import SparkSession

from config import get_config, MODE


def get_or_create_spark(app_name="MarketSurveillance"):
    """
    Get an existing SparkSession or create a new one.

    Why getOrCreate?
      - If run_all_detections.py already started a Spark session for ETL,
        this reuses it (no duplicate JVM startup).
      - If a detector runs standalone, this creates a fresh session.
    """
    cfg = get_config()

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(cfg["spark_master"])

    # AWS needs the S3 filesystem connector
    if MODE == "aws":
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_parquet_from_hdfs(path=None):
    """
    Read a Parquet dataset from HDFS (or S3) and return a pandas DataFrame.

    Args:
        path: HDFS/S3 path to the Parquet folder. Defaults to cfg["clean_output"].

    Returns:
        pandas DataFrame with all columns from the Parquet.

    How it works:
      1. SparkSession.read.parquet(hdfs://...) → Spark DataFrame (distributed)
      2. .toPandas() → pulls data to driver memory as a pandas DataFrame
      3. Detection scripts then run normal pandas logic on it.
    """
    cfg = get_config()
    if path is None:
        path = cfg["clean_output"]

    print(f"  Reading Parquet from HDFS: {path}")

    spark = get_or_create_spark()
    spark_df = spark.read.parquet(path)

    record_count = spark_df.count()
    print(f"  Records in HDFS Parquet: {record_count:,}")

    # Convert distributed Spark DataFrame → local pandas DataFrame
    pdf = spark_df.toPandas()
    return pdf
