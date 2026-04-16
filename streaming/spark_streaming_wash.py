"""
SPARK STRUCTURED STREAMING — WASH TRADE DETECTOR
==================================================
Dual-mode: reads live trades from Kafka, detects wash trading in near-real-time.

Phase 2 (MODE = "local_streaming"):
  - Writes alerts to alerts/streaming_wash_alerts.csv via foreachBatch

Phase 3 (MODE = "streaming" or "aws"):
  - Writes alerts to Kafka topic "wash-alerts" as JSON
  - alert_consumer.py picks them up and persists to PostgreSQL

How it works:
  1. Reads JSON trade messages from Kafka topic 'market-trades'
  2. Parses into typed schema
  3. Applies 2-minute tumbling windows per symbol
  4. Computes volume Z-score within each window
  5. Flags anomalous windows as WASH_TRADE alerts
  6. Sinks to CSV (Phase 2) or Kafka alert topic (Phase 3)

Output mode: complete (no watermark needed — emits all window results every trigger)
"""

import os
import sys
import json
from datetime import datetime

# ── Project root path fix ──────────────────────────────────────────
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)
# ──────────────────────────────────────────────────────────────────────────────

# ── Spark / Java environment ───────────────────────────────────────────────
import subprocess

def _find_java_home():
    for ver in ["@11", "@17", "@21", ""]:
        try:
            p = subprocess.run(
                ["brew", "--prefix", f"openjdk{ver}"],
                capture_output=True, text=True, timeout=5
            )
            path = p.stdout.strip()
            if path and os.path.isdir(path):
                return path
        except Exception:
            pass
    return None

_java = _find_java_home()
if _java:
    os.environ.setdefault("JAVA_HOME", _java)
    java_bin = os.path.join(_java, "bin")
    os.environ["PATH"] = java_bin + ":" + os.environ.get("PATH", "")

import pyspark
_spark_home = os.path.dirname(os.path.dirname(pyspark.__file__))
os.environ.setdefault("SPARK_HOME", _spark_home)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# ──────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

from config import get_config, MODE, DETECTION
from utils.fault_tolerance import get_logger

log = get_logger("spark_streaming_wash")
cfg = get_config()

# ── Determine sink mode ──────────────────────────────────────────────────────
# Phase 2: CSV sink via foreachBatch
# Phase 3+: Kafka alert topic sink
PHASE3 = MODE in ("streaming", "aws")

# ── Kafka / output config ──────────────────────────────────────────
KAFKA_BROKER  = cfg.get("kafka_bootstrap", cfg.get("kafka_broker", "localhost:9092"))
KAFKA_TOPIC   = cfg.get("kafka_topic",   "market-trades")
CHECKPOINT    = cfg.get("checkpoint_wash",
                        os.path.join(cfg.get("checkpoint_dir", ".checkpoints"), "streaming_wash"))
ALERTS_DIR    = cfg.get("alerts_dir", "alerts")
OUTPUT_PATH   = os.path.join(ALERTS_DIR, "streaming_wash_alerts.csv")
ZSCORE_THRESH = float(DETECTION.get("wash_zscore_threshold", 1.8))

# Phase 3: Kafka alert topic
WASH_ALERTS_TOPIC = cfg.get("kafka_wash_alerts_topic", "wash-alerts")

# ── Trade schema ───────────────────────────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("trade_id",   StringType(),  True),
    StructField("timestamp",  StringType(),  True),   # ISO string → cast later
    StructField("symbol",     StringType(),  True),
    StructField("price",      DoubleType(),  True),
    StructField("quantity",   DoubleType(),  True),
    StructField("side",       StringType(),  True),
    StructField("order_id",   StringType(),  True),
    StructField("event_type", StringType(),  True),
    StructField("trader_id",  StringType(),  True),
])


# ── foreachBatch writer (Phase 2: CSV sink) ──────────────────────────────────
def _write_alerts_csv(batch_df, batch_id):
    """Phase 2: Called every trigger. Writes alerts to CSV."""
    if batch_df.rdd.isEmpty():
        log.info("Batch %d: empty — no data yet", batch_id)
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d window rows received", batch_id, len(pdf))

    # ── Cross-window Z-score (same as Phase 3 logic)
    for sym in pdf["symbol"].unique():
        mask = pdf["symbol"] == sym
        sym_vols = pdf.loc[mask, "total_volume"]
        mu  = sym_vols.mean()
        std = sym_vols.std()
        if std and std > 0:
            pdf.loc[mask, "z_score"] = (sym_vols - mu) / std
        else:
            pdf.loc[mask, "z_score"] = 0.0

    # Apply Z-score threshold filter
    alerts = pdf[pdf["z_score"].abs() > ZSCORE_THRESH].copy()
    if alerts.empty:
        log.info("Batch %d: no alerts above threshold %.2f", batch_id, ZSCORE_THRESH)
        return

    # Severity
    alerts["severity"] = alerts["z_score"].abs().apply(
        lambda z: "CRITICAL" if z > ZSCORE_THRESH * 3.0
                  else "HIGH"  if z > ZSCORE_THRESH * 2.0
                  else "MEDIUM"
    )
    alerts["alert_type"]  = "WASH_TRADE"
    alerts["detected_at"] = datetime.utcnow().isoformat()

    # Select final columns
    out = alerts[[
        "window_start", "window_end", "symbol",
        "trade_count", "total_volume", "mean_volume", "std_volume",
        "z_score", "severity", "alert_type", "detected_at"
    ]]

    os.makedirs(ALERTS_DIR, exist_ok=True)
    file_exists = os.path.exists(OUTPUT_PATH)
    out.to_csv(OUTPUT_PATH, mode="a", header=not file_exists, index=False)
    log.info("Batch %d: wrote %d alerts → %s", batch_id, len(out), OUTPUT_PATH)


# ── foreachBatch writer (Phase 3: Kafka alert topic sink) ────────────────────
def _write_alerts_kafka(batch_df, batch_id):
    """
    Phase 3: Called every trigger. Filters alerts and publishes to Kafka
    alert topic as JSON. alert_consumer.py picks them up for PostgreSQL.
    """
    if batch_df.rdd.isEmpty():
        log.info("Batch %d: empty — no data yet", batch_id)
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d window rows received", batch_id, len(pdf))

    # ── Cross-window Z-score: compare each window's total_volume against
    #    the distribution of total_volume across ALL windows in this batch.
    #    This answers: "is this window's volume unusually high compared to
    #    other windows?" — which is the actual wash-trade signal.
    for sym in pdf["symbol"].unique():
        mask = pdf["symbol"] == sym
        sym_vols = pdf.loc[mask, "total_volume"]
        mu  = sym_vols.mean()
        std = sym_vols.std()
        if std and std > 0:
            pdf.loc[mask, "z_score"] = (sym_vols - mu) / std
        else:
            pdf.loc[mask, "z_score"] = 0.0

    log.info("Batch %d: z_scores — %s",
             batch_id,
             {row["symbol"]: round(row["z_score"], 4)
              for _, row in pdf.iterrows()})

    # Apply Z-score threshold filter
    alerts = pdf[pdf["z_score"].abs() > ZSCORE_THRESH].copy()
    if alerts.empty:
        log.info("Batch %d: no alerts above threshold %.2f", batch_id, ZSCORE_THRESH)
        return

    # Severity
    alerts["severity"] = alerts["z_score"].abs().apply(
        lambda z: "CRITICAL" if z > ZSCORE_THRESH * 3.0
                  else "HIGH"  if z > ZSCORE_THRESH * 2.0
                  else "MEDIUM"
    )
    alerts["alert_type"]  = "WASH_TRADE"
    alerts["detected_at"] = datetime.utcnow().isoformat()

    log.info("Batch %d: %d alerts above threshold %.2f", batch_id, len(alerts), ZSCORE_THRESH)

    # Produce each alert to Kafka topic
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    count = 0
    for _, row in alerts.iterrows():
        alert_dict = {
            "window_start":  str(row["window_start"]),
            "window_end":    str(row["window_end"]),
            "symbol":        row["symbol"],
            "trade_count":   int(row["trade_count"]),
            "total_volume":  float(row["total_volume"]),
            "mean_volume":   float(row["mean_volume"]),
            "std_volume":    float(row["std_volume"]),
            "z_score":       float(row["z_score"]),
            "severity":      row["severity"],
            "alert_type":    row["alert_type"],
            "detected_at":   row["detected_at"],
        }
        producer.send(WASH_ALERTS_TOPIC, value=alert_dict)
        count += 1

    producer.flush()
    producer.close()
    log.info("Batch %d: published %d wash alerts → Kafka topic '%s'",
             batch_id, count, WASH_ALERTS_TOPIC)


# ── Main streaming job ─────────────────────────────────────────────────────
def run():
    sink_label = "Kafka topic" if PHASE3 else "CSV"
    log.info("Creating Spark session... (sink: %s, MODE=%s)", sink_label, MODE)

    spark = (
        SparkSession.builder
        .appName("WashTradeDetector")
        .master("local[2]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session ready. Connecting to Kafka %s topic=%s", KAFKA_BROKER, KAFKA_TOPIC)

    # ── Step 1: Read from Kafka ────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Step 2: Parse JSON ─────────────────────────────────────────────────
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("symbol").isin("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    )

    # ── Step 3: 2-minute tumbling windows (complete mode — no watermark needed) ──
    windowed = (
        parsed
        .groupBy(
            F.window(F.col("event_time"), "2 minutes"),
            F.col("symbol")
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("quantity").alias("total_volume"),
            F.avg("quantity").alias("mean_volume"),
            F.stddev("quantity").alias("std_volume"),
        )
        .withColumn("std_volume",  F.coalesce(F.col("std_volume"),  F.lit(0.0)))
        .withColumn("mean_volume", F.coalesce(F.col("mean_volume"), F.lit(0.0)))
        .withColumn("z_score", F.lit(0.0))  # placeholder — real z_score computed in foreachBatch
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # ── Step 4: Write via foreachBatch ─────────────────────────────────────
    # Phase 2: CSV sink | Phase 3+: Kafka alert topic sink
    batch_fn = _write_alerts_kafka if PHASE3 else _write_alerts_csv

    query = (
        windowed
        .writeStream
        .outputMode("complete")
        .foreachBatch(batch_fn)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    log.info("Streaming query started (sink=%s). Waiting for data... (Ctrl+C to stop)", sink_label)
    query.awaitTermination()


if __name__ == "__main__":
    run()
