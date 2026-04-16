"""
SPARK STRUCTURED STREAMING — PUMP & DUMP DETECTOR
==================================================
Dual-mode: reads live trades from Kafka, detects pump & dump in near-real-time.

Phase 2 (MODE = "local_streaming"):
  - Writes alerts to alerts/streaming_pump_dump_alerts.csv via foreachBatch

Phase 3 (MODE = "streaming" or "aws"):
  - Writes alerts to Kafka topic "pump-dump-alerts" as JSON
  - alert_consumer.py picks them up and persists to PostgreSQL

How it works:
  1. Reads JSON trade messages from Kafka topic 'market-trades'
  2. Builds 1-minute OHLCV bars per symbol using tumbling windows
  3. In foreachBatch: detects PUMP (price spike + volume surge) then DUMP
  4. Sinks to CSV (Phase 2) or Kafka alert topic (Phase 3)

Output mode: complete (emits all window results every trigger — no watermark stall)
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
import subprocess as _subprocess

def _find_java_home():
    for ver in ["@11", "@17", "@21", ""]:
        try:
            p = _subprocess.run(
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

log = get_logger("spark_streaming_pump_dump")
cfg = get_config()

# ── Determine sink mode ──────────────────────────────────────────────────────
PHASE3 = MODE in ("streaming", "aws")

# ── Config ─────────────────────────────────────────────────────────────────
KAFKA_BROKER   = cfg.get("kafka_bootstrap", cfg.get("kafka_broker", "localhost:9092"))
KAFKA_TOPIC    = cfg.get("kafka_topic",   "market-trades")
CHECKPOINT     = cfg.get("checkpoint_pump_dump",
                         os.path.join(cfg.get("checkpoint_dir", ".checkpoints"), "streaming_pump_dump"))
ALERTS_DIR     = cfg.get("alerts_dir", "alerts")
OUTPUT_PATH    = os.path.join(ALERTS_DIR, "streaming_pump_dump_alerts.csv")

PUMP_THRESH    = float(DETECTION.get("pd_pump_threshold",   0.01))   # 1% price rise
DUMP_THRESH    = float(DETECTION.get("pd_dump_threshold",  -0.01))   # 1% price drop
VOL_RATIO      = float(DETECTION.get("pd_volume_ratio",     1.5))    # 1.5× baseline
PD_WINDOW_MIN  = int(DETECTION.get("pd_window_minutes",     3))

# Phase 3: Kafka alert topic
PD_ALERTS_TOPIC = cfg.get("kafka_pd_alerts_topic", "pump-dump-alerts")

# ── Trade schema ───────────────────────────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("trade_id",   StringType(), True),
    StructField("timestamp",  StringType(), True),
    StructField("symbol",     StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("quantity",   DoubleType(), True),
    StructField("side",       StringType(), True),
    StructField("order_id",   StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("trader_id",  StringType(), True),
])

# ── Stateful pump tracker ──────────────────────────────────────────────────
# { symbol -> {"window_start": datetime, "open_price": float, "close_price": float} }
_pump_state: dict = {}


def _detect_alerts(pdf, batch_id):
    """
    Core detection logic shared by both Phase 2 and Phase 3 sinks.
    Returns a list of alert dicts.
    """
    alerts = []
    now = datetime.utcnow()

    # Sort bars by window_start per symbol
    for symbol, grp in pdf.groupby("symbol"):
        bars = grp.sort_values("window_start").reset_index(drop=True)

        # Compute baseline volume (mean across all bars for this symbol)
        baseline_vol = bars["total_volume"].mean() if len(bars) > 1 else bars["total_volume"].iloc[0]

        for _, bar in bars.iterrows():
            open_p  = bar["open_price"]
            close_p = bar["close_price"]
            if open_p <= 0:
                continue

            price_chg = (close_p - open_p) / open_p
            vol_ratio = bar["total_volume"] / baseline_vol if baseline_vol > 0 else 1.0

            bar_start = bar["window_start"]
            if hasattr(bar_start, "to_pydatetime"):
                bar_start = bar_start.to_pydatetime()

            # ── PUMP detection ─────────────────────────────────────────
            if price_chg >= PUMP_THRESH and vol_ratio >= VOL_RATIO:
                _pump_state[symbol] = {
                    "window_start": bar_start,
                    "price_chg":    price_chg,
                    "vol_ratio":    vol_ratio,
                    "close_price":  close_p,
                }
                log.info("PUMP detected: %s +%.2f%% vol×%.2f", symbol, price_chg*100, vol_ratio)

            # ── DUMP detection (only if prior PUMP within window) ──────
            elif price_chg <= DUMP_THRESH and symbol in _pump_state:
                pump = _pump_state[symbol]
                pump_dt = pump["window_start"]
                diff_min = (bar_start - pump_dt).total_seconds() / 60 if bar_start > pump_dt else 999

                if 0 < diff_min <= PD_WINDOW_MIN:
                    severity = (
                        "CRITICAL" if abs(price_chg) > abs(DUMP_THRESH) * 3
                        else "HIGH" if abs(price_chg) > abs(DUMP_THRESH) * 2
                        else "MEDIUM"
                    )
                    alerts.append({
                        "window_start":      pump_dt.isoformat() if hasattr(pump_dt, "isoformat") else str(pump_dt),
                        "window_end":        bar_start.isoformat() if hasattr(bar_start, "isoformat") else str(bar_start),
                        "symbol":            symbol,
                        "phase":             "DUMP",
                        "pump_price_chg_pct": round(pump["price_chg"] * 100, 4),
                        "dump_price_chg_pct": round(price_chg * 100, 4),
                        "price_change_pct":  round(price_chg * 100, 4),
                        "volume_ratio":       round(vol_ratio, 4),
                        "severity":           severity,
                        "alert_type":         "PUMP_DUMP",
                        "detected_at":        now.isoformat(),
                    })
                    log.info("PUMP+DUMP confirmed: %s severity=%s", symbol, severity)
                    del _pump_state[symbol]

    # Expire stale pump states
    stale = [s for s, p in _pump_state.items()
             if (now - (p["window_start"] if not hasattr(p["window_start"], "total_seconds")
                        else p["window_start"])).total_seconds() / 60 > PD_WINDOW_MIN * 2]
    for s in stale:
        del _pump_state[s]

    return alerts


# ── foreachBatch writer (Phase 2: CSV sink) ──────────────────────────────────
def _process_batch_csv(batch_df, batch_id):
    """Phase 2: Receives OHLCV bars, detects P&D, writes alerts to CSV."""
    if batch_df.rdd.isEmpty():
        log.info("Batch %d: empty", batch_id)
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d OHLCV bars", batch_id, len(pdf))

    alerts = _detect_alerts(pdf, batch_id)

    if not alerts:
        log.info("Batch %d: no P&D alerts", batch_id)
        return

    import pandas as pd
    alerts_df = pd.DataFrame(alerts)
    os.makedirs(ALERTS_DIR, exist_ok=True)
    file_exists = os.path.exists(OUTPUT_PATH)
    alerts_df.to_csv(OUTPUT_PATH, mode="a", header=not file_exists, index=False)
    log.info("Batch %d: wrote %d P&D alerts → %s", batch_id, len(alerts_df), OUTPUT_PATH)


# ── foreachBatch writer (Phase 3: Kafka alert topic sink) ────────────────────
def _process_batch_kafka(batch_df, batch_id):
    """
    Phase 3: Receives OHLCV bars, detects P&D, publishes alerts to Kafka
    alert topic. alert_consumer.py picks them up for PostgreSQL.
    """
    if batch_df.rdd.isEmpty():
        log.info("Batch %d: empty", batch_id)
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d OHLCV bars", batch_id, len(pdf))

    alerts = _detect_alerts(pdf, batch_id)

    if not alerts:
        log.info("Batch %d: no P&D alerts", batch_id)
        return

    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    count = 0
    for alert_dict in alerts:
        producer.send(PD_ALERTS_TOPIC, value=alert_dict)
        count += 1

    producer.flush()
    producer.close()
    log.info("Batch %d: published %d P&D alerts → Kafka topic '%s'",
             batch_id, count, PD_ALERTS_TOPIC)


# ── Main streaming job ─────────────────────────────────────────────────────
def run():
    sink_label = "Kafka topic" if PHASE3 else "CSV"
    log.info("Creating Spark session... (sink: %s, MODE=%s)", sink_label, MODE)

    spark = (
        SparkSession.builder
        .appName("PumpDumpDetector")
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

    # ── Step 3: 1-minute OHLCV tumbling windows (complete mode) ───────────
    ohlcv = (
        parsed
        .groupBy(
            F.window(F.col("event_time"), "1 minute"),
            F.col("symbol")
        )
        .agg(
            F.first("price").alias("open_price"),
            F.last("price").alias("close_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.sum("quantity").alias("total_volume"),
            F.count("*").alias("trade_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # ── Step 4: Write via foreachBatch (stateful P&D matching) ────────────
    # Phase 2: CSV sink | Phase 3+: Kafka alert topic sink
    batch_fn = _process_batch_kafka if PHASE3 else _process_batch_csv

    query = (
        ohlcv
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
