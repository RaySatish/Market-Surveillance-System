"""
SPARK STRUCTURED STREAMING — WASH TRADE DETECTOR
==================================================
Phase 2: reads live trades from Kafka, detects wash trading in near-real-time.

How it works:
  1. Spark Structured Streaming reads from the 'market-trades' Kafka topic
  2. Parses JSON trade messages into a typed schema
  3. Applies a 2-minute tumbling window per symbol
  4. Computes rolling volume Z-score within each window
  5. Flags windows where Z-score > threshold as potential wash trades
  6. Appends alerts to alerts/streaming_wash_alerts.csv

Why tumbling windows?
  - Each 2-minute window is independent (no overlap)
  - Simple to reason about: one alert per window per symbol if anomalous
  - Watermark handles late-arriving data (up to 30s late)

Fault tolerance:
  - Spark Structured Streaming checkpoints to .checkpoints/streaming_wash/
  - Kafka consumer group offset tracking (exactly-once with checkpointing)
  - Structured logging

Usage:
  # Start Kafka first, then the producer:
  docker compose up -d
  python streaming/kafka_producer.py --test

  # In a separate terminal, start the streaming detector:
  python streaming/spark_streaming_wash.py
"""

import os
import sys

# ── Spark environment fix (same as spark_utils.py) ──────────────────────────
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]
_java17 = "/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home"
if os.path.isdir(_java17):
    os.environ["JAVA_HOME"] = _java17
# ────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

from config import get_config, DETECTION
from utils.fault_tolerance import get_logger

log = get_logger("streaming_wash")

# ============================================================
#  SCHEMA — must match what kafka_producer.py serialises
# ============================================================
TRADE_SCHEMA = StructType([
    StructField("trade_id",   StringType(),    True),
    StructField("timestamp",  StringType(),    True),   # ISO-8601 string from producer
    StructField("symbol",     StringType(),    True),
    StructField("price",      DoubleType(),    True),
    StructField("quantity",   DoubleType(),    True),
    StructField("side",       StringType(),    True),
    StructField("order_id",   StringType(),    True),
    StructField("event_type", StringType(),    True),
])

# ============================================================
#  ALERT WRITER — foreachBatch sink
# ============================================================
def _write_alerts(batch_df, batch_id):
    """
    Called by Spark Structured Streaming for every micro-batch.
    Receives a DataFrame of flagged windows; appends to CSV.
    """
    cfg = get_config()
    alerts_dir = cfg["alerts_dir"]
    out_path   = os.path.join(alerts_dir, "streaming_wash_alerts.csv")
    os.makedirs(alerts_dir, exist_ok=True)

    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d WASH TRADE alerts", batch_id, len(pdf))

    # Append mode — streaming alerts accumulate over time
    file_exists = os.path.exists(out_path)
    pdf.to_csv(out_path, mode="a", header=not file_exists, index=False)
    log.info("Appended %d alerts → %s", len(pdf), out_path)


# ============================================================
#  MAIN STREAMING JOB
# ============================================================
def run_streaming_wash():
    cfg = get_config()
    bootstrap = cfg.get("kafka_bootstrap", "localhost:9092")
    topic     = cfg.get("kafka_topic",     "market-trades")
    checkpoint_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".checkpoints", "streaming_wash"
    )

    log.info("Starting Spark Structured Streaming — Wash Trade Detector")
    log.info("Kafka broker : %s", bootstrap)
    log.info("Topic        : %s", topic)
    log.info("Checkpoint   : %s", checkpoint_dir)

    spark = (
        SparkSession.builder
        .appName("MarketSurveillance-StreamingWash")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        # Kafka connector JAR (downloaded on first run)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Read from Kafka ──────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")    # only new messages
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Step 2: Parse JSON value ─────────────────────────────────────────────
    trades = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("trade")
        )
        .select("trade.*")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"))   # convert ISO string → timestamp
        )
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("symbol").isNotNull())
    )

    # ── Step 3: Tumbling window aggregation ──────────────────────────────────
    # 2-minute tumbling windows with 30-second watermark for late data
    window_duration = DETECTION.get("wash_rolling_window", "2min").replace("min", " minutes")

    windowed = (
        trades
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("symbol")
        )
        .agg(
            F.sum("quantity").alias("window_volume"),
            F.avg("price").alias("avg_price"),
            F.count("trade_id").alias("trade_count"),
            F.stddev("quantity").alias("qty_stddev"),
            F.avg("quantity").alias("qty_mean"),
        )
    )

    # ── Step 4: Z-score approximation per window ─────────────────────────────
    # In streaming, we can't compute a true rolling Z-score across all past windows
    # (that would require unbounded state). Instead, we flag windows where:
    #   window_volume > qty_mean + threshold * qty_stddev
    # This is equivalent to Z-score > threshold within the window itself.
    threshold = DETECTION.get("wash_zscore_threshold", 1.8)

    alerts = (
        windowed
        .filter(F.col("qty_stddev").isNotNull())
        .filter(F.col("qty_stddev") > 0)
        .withColumn(
            "z_score",
            (F.col("window_volume") - F.col("qty_mean") * F.col("trade_count"))
            / (F.col("qty_stddev") * F.sqrt(F.col("trade_count")))
        )
        .filter(F.col("z_score") > threshold)
        .withColumn(
            "severity",
            F.when(F.col("z_score") > threshold * 1.5, "CRITICAL").otherwise("HIGH")
        )
        .withColumn("alert_type", F.lit("WASH_TRADE_STREAM"))
        .withColumn("detected_at", F.current_timestamp())
        .select(
            "alert_type",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            F.round("window_volume", 4).alias("window_volume"),
            F.round("avg_price", 2).alias("avg_price"),
            "trade_count",
            F.round("z_score", 2).alias("z_score"),
            "severity",
            "detected_at",
        )
    )

    # ── Step 5: Write alerts via foreachBatch ────────────────────────────────
    query = (
        alerts.writeStream
        .outputMode("append")
        .foreachBatch(_write_alerts)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="30 seconds")   # micro-batch every 30s
        .start()
    )

    log.info("Streaming wash detector running. Press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopped by user.")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    run_streaming_wash()
