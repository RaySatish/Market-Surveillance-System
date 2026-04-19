"""
SPARK STRUCTURED STREAMING — WASH TRADE DETECTOR
==================================================
Reads live trades from Kafka, detects wash trading in near-real-time.

How it works:
  1. Reads JSON trade messages from Kafka topic 'market-trades'
  2. Parses into typed schema
  3. Applies 2-minute tumbling windows per symbol
  4. Computes cross-window volume Z-score in foreachBatch
  5. Flags anomalous windows as WASH_TRADE alerts
  6. Publishes alerts to Kafka topic 'wash-alerts' as JSON
  7. Runs sensitivity sweep across multiple thresholds → PostgreSQL

alert_consumer.py picks up alerts from Kafka and persists to PostgreSQL.

Output mode: complete (no watermark needed — emits all window results every trigger)
"""

import os
import sys
import json
from datetime import datetime

# ── Project root path fix ──────────────────────────────────
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)
# ──────────────────────────────────────────────────────────────────────────

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
# ──────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

from config import get_config, DETECTION
from utils.fault_tolerance import get_logger

log = get_logger("spark_streaming_wash")
cfg = get_config()

# ── Kafka / output config ──────────────────────────────────────────────────
KAFKA_BROKER  = cfg.get("kafka_bootstrap", "localhost:9092")
KAFKA_TOPIC   = cfg.get("kafka_topic",   "market-trades")
CHECKPOINT    = cfg.get("checkpoint_wash",
                        os.path.join(_root, "checkpoints", "streaming_wash"))
ZSCORE_THRESH = float(DETECTION.get("wash_zscore_threshold", 1.8))

# Kafka alert topic
WASH_ALERTS_TOPIC = cfg.get("kafka_wash_alerts_topic", "wash-alerts")

# ── Threshold sensitivity sweep (for paper Table 2) ──────────────────────
# Evaluated in _write_alerts_kafka alongside the normal alert flow.
# Results written directly to PostgreSQL wash_sensitivity table.
SENSITIVITY_THRESHOLDS = [0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]


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


# ── foreachBatch writer: Kafka alert topic sink ──────────────────────────
def _write_alerts_kafka(batch_df, batch_id):
    """
    Called every trigger. Filters alerts and publishes to Kafka
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
    else:
        # Severity
        alerts["severity"] = alerts["z_score"].abs().apply(
            lambda z: "CRITICAL" if z > ZSCORE_THRESH * 5.0
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

    # ── Threshold sensitivity sweep (paper Table 2) ──────────────────
    # Evaluate ALL windows (not just alerts) against each threshold.
    # Write directly to PostgreSQL — bypasses Kafka for sweep data.
    try:
        from streaming.db import get_connection, insert_wash_sensitivity
        sweep_conn = get_connection()
        sweep_count = 0
        for _, row in pdf.iterrows():
            abs_z = abs(row["z_score"])
            for thresh in SENSITIVITY_THRESHOLDS:
                flagged = bool(abs_z > thresh)
                severity = None
                if flagged:
                    severity = ("CRITICAL" if abs_z > thresh * 5.0
                                else "HIGH" if abs_z > thresh * 2.0
                                else "MEDIUM")
                insert_wash_sensitivity({
                    "window_start":  str(row["window_start"]),
                    "window_end":    str(row["window_end"]),
                    "symbol":        row["symbol"],
                    "trade_count":   int(row["trade_count"]),
                    "total_volume":  float(row["total_volume"]),
                    "z_score":       float(row["z_score"]),
                    "threshold":     thresh,
                    "flagged":       flagged,
                    "severity":      severity,
                    "detected_at":   datetime.utcnow().isoformat(),
                }, conn=sweep_conn)
                sweep_count += 1
        sweep_conn.close()
        log.info("Batch %d: sensitivity sweep wrote %d rows across %d thresholds",
                 batch_id, sweep_count, len(SENSITIVITY_THRESHOLDS))
    except Exception as e:
        log.warning("Batch %d: sensitivity sweep failed (non-fatal): %s", batch_id, e)



# ── Main streaming job ─────────────────────────────────────────────────────
def run():
    log.info("Creating Spark session...")

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

    # ── Step 1: Read from Kafka ────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Step 2: Parse JSON ─────────────────────────────────────────────
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("symbol").isin("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    )

    # ── Step 3: 2-minute tumbling windows (complete mode — no watermark needed)
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

    # ── Step 4: Write via foreachBatch → Kafka alert topic ─────────────
    query = (
        windowed
        .writeStream
        .outputMode("complete")
        .foreachBatch(_write_alerts_kafka)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    log.info("Streaming query started. Waiting for data... (Ctrl+C to stop)")
    query.awaitTermination()


if __name__ == "__main__":
    run()
