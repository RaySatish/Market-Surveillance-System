"""
SPARK STRUCTURED STREAMING — PUMP & DUMP DETECTOR
==================================================
Reads live trades from Kafka, detects pump & dump in near-real-time.

How it works:
  1. Reads JSON trade messages from Kafka topic 'market-trades'
  2. Builds 1-minute OHLCV bars per symbol using tumbling windows
  3. In foreachBatch: detects PUMP (price spike + volume surge) then DUMP
  4. Publishes alerts to Kafka topic 'pump-dump-alerts' as JSON
  5. Runs P&D sensitivity sweep across threshold combos → PostgreSQL

alert_consumer.py picks up alerts from Kafka and persists to PostgreSQL.

Output mode: complete (emits all window results every trigger — no watermark stall)
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
# ──────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

from config import get_config, DETECTION
from utils.fault_tolerance import get_logger

log = get_logger("spark_streaming_pump_dump")
cfg = get_config()

# ── Config ─────────────────────────────────────────────────────────────────
KAFKA_BROKER   = cfg.get("kafka_bootstrap", "localhost:9092")
KAFKA_TOPIC    = cfg.get("kafka_topic",   "market-trades")
CHECKPOINT     = cfg.get("checkpoint_pump_dump",
                         os.path.join(_root, "checkpoints", "streaming_pump_dump"))

PUMP_THRESH    = float(DETECTION.get("pd_pump_threshold",   0.01))   # 1% price rise
DUMP_THRESH    = float(DETECTION.get("pd_dump_threshold",  -0.01))   # 1% price drop
VOL_RATIO      = float(DETECTION.get("pd_volume_ratio",     1.5))    # 1.5× baseline
PD_WINDOW_MIN  = int(DETECTION.get("pd_window_minutes",     3))

# Kafka alert topic
PD_ALERTS_TOPIC = cfg.get("kafka_pd_alerts_topic", "pump-dump-alerts")

# ── P&D Sensitivity sweep thresholds (for paper Table 3) ─────────────────
# Each combo of (price_threshold, vol_threshold) is evaluated per bar.
PD_SENSITIVITY_COMBOS = [
    (0.0005, 0.5),   # Very aggressive: 0.05% price, 0.5× volume
    (0.001,  0.8),   # Aggressive: 0.1% price, 0.8× volume
    (0.001,  1.1),   # Current default: 0.1% price, 1.1× volume
    (0.002,  1.5),   # Moderate: 0.2% price, 1.5× volume
    (0.005,  2.0),   # Conservative: 0.5% price, 2.0× volume
]


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

# Track the latest bar timestamp per symbol to only process new bars
# Key: symbol -> latest window_start datetime seen
_last_bar_ts: dict = {}

# Track already-alerted PUMP->DUMP pairs to avoid duplicate alerts
# Key: (symbol, pump_window_start_str, dump_window_start_str)
_alerted_pairs: set = set()


def _detect_alerts(pdf, batch_id):
    """
    Core detection logic. Returns a list of alert dicts.

    Strategy for Spark's complete output mode:
    - Track the latest bar timestamp per symbol (_last_bar_ts)
    - Only process bars NEWER than the last-seen timestamp
    - _pump_state persists across batches so PUMP→DUMP matching works
    - _alerted_pairs prevents duplicate alert generation
    """
    alerts = []
    now = datetime.utcnow()

    for symbol, grp in pdf.groupby("symbol"):
        bars = grp.sort_values("window_start").reset_index(drop=True)

        for idx, bar in bars.iterrows():
            open_p  = bar["open_price"]
            close_p = bar["close_price"]
            if open_p <= 0:
                continue

            bar_start = bar["window_start"]
            if hasattr(bar_start, "to_pydatetime"):
                bar_start = bar_start.to_pydatetime()

            # Only process bars newer than the last one we saw for this symbol
            last_ts = _last_bar_ts.get(symbol)
            if last_ts is not None and bar_start <= last_ts:
                continue

            price_chg = float((close_p - open_p) / open_p)

            # Baseline volume: exclude current bar to avoid self-comparison
            other_bars = bars.drop(idx)
            if len(other_bars) > 0:
                baseline_vol = other_bars["total_volume"].mean()
            else:
                baseline_vol = bar["total_volume"]
            vol_ratio = bar["total_volume"] / baseline_vol if baseline_vol > 0 else 1.0

            log.debug("Bar %s %s: price_chg=%.4f%% vol_ratio=%.2f (need pump>=%.4f%% dump<=%.4f%% vol>=%.1f)",
                      symbol, bar_start, price_chg*100, vol_ratio,
                      PUMP_THRESH*100, DUMP_THRESH*100, VOL_RATIO)

            # -- PUMP detection --
            if price_chg >= PUMP_THRESH and vol_ratio >= VOL_RATIO:
                _pump_state[symbol] = {
                    "window_start": bar_start,
                    "price_chg":    price_chg,
                    "vol_ratio":    vol_ratio,
                    "close_price":  close_p,
                }
                log.info("PUMP detected: %s +%.2f%% vol*%.2f (bar %s)",
                         symbol, price_chg*100, vol_ratio, bar_start)

            # -- DUMP detection (independent of PUMP check) --
            if price_chg <= DUMP_THRESH and vol_ratio >= VOL_RATIO and symbol in _pump_state:
                pump = _pump_state[symbol]
                pump_dt = pump["window_start"]
                diff_min = (bar_start - pump_dt).total_seconds() / 60 if bar_start > pump_dt else 999

                if 0 < diff_min <= PD_WINDOW_MIN:
                    pair_key = (symbol, str(pump_dt), str(bar_start))
                    if pair_key in _alerted_pairs:
                        continue
                    _alerted_pairs.add(pair_key)

                    severity = (
                        "CRITICAL" if abs(price_chg) > abs(DUMP_THRESH) * 5
                        else "HIGH" if abs(price_chg) > abs(DUMP_THRESH) * 3
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
                    log.info("PUMP+DUMP confirmed: %s severity=%s (pump=%s dump=%s diff=%.1fmin)",
                             symbol, severity, pump_dt, bar_start, diff_min)
                    del _pump_state[symbol]

        # Update last-seen timestamp for this symbol to the latest bar
        if len(bars) > 0:
            latest = bars["window_start"].max()
            if hasattr(latest, "to_pydatetime"):
                latest = latest.to_pydatetime()
            _last_bar_ts[symbol] = latest

    # Expire stale pump states
    stale = [s for s, p in _pump_state.items()
             if (now - p["window_start"]).total_seconds() / 60 > PD_WINDOW_MIN * 2]
    for s in stale:
        log.debug("Expiring stale PUMP state for %s", s)
        del _pump_state[s]

    return alerts


# ── foreachBatch writer: Kafka alert topic sink ──────────────────────────
def _process_batch_kafka(batch_df, batch_id):
    """
    Receives OHLCV bars, detects P&D, publishes alerts to Kafka alert topic.
    Also runs P&D sensitivity sweep for paper Table 3.
    """
    if batch_df.rdd.isEmpty():
        log.info("Batch %d: empty", batch_id)
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d OHLCV bars", batch_id, len(pdf))

    # ── P&D Sensitivity sweep (paper Table 3) ────────────────────────────
    # Runs BEFORE alert detection so it executes even when no alerts fire.
    # Evaluates every new bar against all threshold combos.
    try:
        from streaming.db import get_connection, insert_pd_sensitivity, init_pd_sensitivity_schema
        sweep_conn = get_connection()
        init_pd_sensitivity_schema(conn=sweep_conn)
        sweep_count = 0

        for symbol, grp in pdf.groupby("symbol"):
            bars = grp.sort_values("window_start").reset_index(drop=True)

            # Only sweep the LATEST bar per symbol (new data only)
            if len(bars) == 0:
                continue
            latest_bar = bars.iloc[-1]
            open_p  = latest_bar["open_price"]
            close_p = latest_bar["close_price"]
            if open_p <= 0:
                continue

            price_chg = float((close_p - open_p) / open_p)

            # Baseline volume: all bars except latest
            if len(bars) > 1:
                baseline_vol = bars.iloc[:-1]["total_volume"].mean()
            else:
                baseline_vol = latest_bar["total_volume"]
            vol_ratio = float(latest_bar["total_volume"] / baseline_vol) if baseline_vol > 0 else 1.0

            for price_thresh, vol_thresh in PD_SENSITIVITY_COMBOS:
                is_pump = bool(price_chg >= price_thresh and vol_ratio >= vol_thresh)
                is_dump = bool(price_chg <= -price_thresh and vol_ratio >= vol_thresh)

                for phase, flagged in [("PUMP", is_pump), ("DUMP", is_dump)]:
                    severity = None
                    if flagged:
                        abs_pchg = abs(price_chg)
                        severity = ("CRITICAL" if abs_pchg > price_thresh * 5.0
                                    else "HIGH" if abs_pchg > price_thresh * 2.0
                                    else "MEDIUM")
                    insert_pd_sensitivity({
                        "window_start":     str(latest_bar["window_start"]),
                        "window_end":       str(latest_bar["window_end"]),
                        "symbol":           symbol,
                        "price_change_pct": round(price_chg * 100, 6),
                        "volume_ratio":     round(vol_ratio, 4),
                        "price_threshold":  price_thresh,
                        "vol_threshold":    vol_thresh,
                        "phase":            phase,
                        "flagged":          flagged,
                        "severity":         severity,
                        "detected_at":      datetime.utcnow().isoformat(),
                    }, conn=sweep_conn)
                    sweep_count += 1

        sweep_conn.close()
        log.info("Batch %d: P&D sensitivity sweep wrote %d rows", batch_id, sweep_count)
    except Exception as e:
        log.warning("Batch %d: P&D sensitivity sweep failed (non-fatal): %s", batch_id, e)

    # ── Normal alert detection + Kafka publish ────────────────────────────
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
    log.info("Creating Spark session...")

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

    # ── Step 3: 1-minute OHLCV tumbling windows (complete mode) ────────
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

    # ── Step 4: Write via foreachBatch → Kafka alert topic ─────────────
    query = (
        ohlcv
        .writeStream
        .outputMode("complete")
        .foreachBatch(_process_batch_kafka)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    log.info("Streaming query started. Waiting for data... (Ctrl+C to stop)")
    query.awaitTermination()


if __name__ == "__main__":
    run()
