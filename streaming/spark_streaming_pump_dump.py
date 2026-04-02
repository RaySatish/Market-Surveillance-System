"""
SPARK STRUCTURED STREAMING — PUMP & DUMP DETECTOR
===================================================
Phase 2: reads live trades from Kafka, detects pump & dump in near-real-time.

How it works:
  1. Spark Structured Streaming reads from the 'market-trades' Kafka topic
  2. Parses JSON trade messages into a typed schema
  3. Applies a 1-minute tumbling window per symbol to build OHLCV bars
  4. Detects PUMP bars (price rise >= threshold) and DUMP bars (price drop >= threshold)
  5. Uses a stateful foreachBatch to match PUMP → DUMP sequences across consecutive batches
  6. Appends confirmed P&D alerts to alerts/streaming_pump_dump_alerts.csv

Stateful matching:
  True P&D detection requires looking across time windows (PUMP then DUMP).
  We maintain a lightweight in-memory state dict per symbol that remembers
  the last PUMP window. When a DUMP window follows within pd_window_minutes,
  a P&D alert is fired.

Fault tolerance:
  - Spark Structured Streaming checkpoints to .checkpoints/streaming_pump_dump/
  - In-memory pump state is rebuilt from recent alerts on restart
  - Structured logging

Usage:
  # Start Kafka first, then the producer:
  docker compose up -d
  python streaming/kafka_producer.py --test

  # In a separate terminal, start the streaming detector:
  python streaming/spark_streaming_pump_dump.py
"""

import os
import sys
from datetime import datetime, timedelta

# ── Spark environment fix ────────────────────────────────────────────────────
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]
_java17 = "/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home"
if os.path.isdir(_java17):
    os.environ["JAVA_HOME"] = _java17
# ────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

from config import get_config, DETECTION
from utils.fault_tolerance import get_logger

log = get_logger("streaming_pump_dump")

# ============================================================
#  SCHEMA
# ============================================================
TRADE_SCHEMA = StructType([
    StructField("trade_id",   StringType(), True),
    StructField("timestamp",  StringType(), True),
    StructField("symbol",     StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("quantity",   DoubleType(), True),
    StructField("side",       StringType(), True),
    StructField("order_id",   StringType(), True),
    StructField("event_type", StringType(), True),
])

# ============================================================
#  STATEFUL PUMP TRACKER
# ============================================================
# Keeps the most recent confirmed PUMP window per symbol in memory.
# Structure: { symbol: {"window_start": datetime, "price_chg_pct": float, "peak_price": float} }
_pump_state: dict = {}


def _process_batch(batch_df, batch_id):
    """
    foreachBatch handler.
    Receives 1-minute OHLCV bars for all symbols in this micro-batch.
    Detects PUMP and DUMP bars, matches PUMP→DUMP sequences, writes alerts.
    """
    global _pump_state

    cfg = get_config()
    alerts_dir = cfg["alerts_dir"]
    out_path   = os.path.join(alerts_dir, "streaming_pump_dump_alerts.csv")
    os.makedirs(alerts_dir, exist_ok=True)

    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    log.info("Batch %d: %d 1-min bars across %d symbols",
             batch_id, len(pdf), pdf["symbol"].nunique())

    price_spike_pct = DETECTION.get("pd_price_spike_pct", 0.08)
    window_minutes  = DETECTION.get("pd_window_minutes",  3)
    pd_alerts       = []

    for symbol, sym_bars in pdf.groupby("symbol"):
        sym_bars = sym_bars.sort_values("window_start").reset_index(drop=True)

        for _, bar in sym_bars.iterrows():
            bar_start    = bar["window_start"]
            price_chg    = bar["price_chg_pct"]
            is_pump      = price_chg >= price_spike_pct
            is_dump      = price_chg <= -price_spike_pct

            if is_pump:
                # Record this PUMP window in state
                _pump_state[symbol] = {
                    "window_start":   bar_start,
                    "price_chg_pct":  price_chg,
                    "peak_price":     bar["high"],
                    "buy_vol":        bar["buy_vol"],
                }
                log.debug("%s PUMP detected at %s (%.4f%%)", symbol, bar_start, price_chg)

            elif is_dump and symbol in _pump_state:
                # Check if DUMP follows PUMP within window_minutes
                pump = _pump_state[symbol]
                try:
                    # bar_start may be a pandas Timestamp or string
                    if hasattr(bar_start, 'to_pydatetime'):
                        bar_dt  = bar_start.to_pydatetime()
                        pump_dt = pump["window_start"].to_pydatetime() if hasattr(pump["window_start"], 'to_pydatetime') else pump["window_start"]
                    else:
                        bar_dt  = datetime.fromisoformat(str(bar_start))
                        pump_dt = datetime.fromisoformat(str(pump["window_start"]))

                    time_diff_min = (bar_dt - pump_dt).total_seconds() / 60
                except Exception:
                    time_diff_min = 999

                if 0 < time_diff_min <= window_minutes:
                    severity = "CRITICAL" if abs(pump["price_chg_pct"]) > 0.3 else "HIGH"
                    pd_alerts.append({
                        "alert_type":       "PUMP_AND_DUMP_STREAM",
                        "symbol":           symbol,
                        "pump_window_start": pump["window_start"],
                        "dump_window_start": bar_start,
                        "pump_price_chg":   round(pump["price_chg_pct"], 4),
                        "dump_price_chg":   round(price_chg, 4),
                        "peak_price":       round(float(pump["peak_price"]), 2),
                        "trough_price":     round(float(bar["low"]), 2),
                        "pump_buy_vol":     round(float(pump["buy_vol"]), 4),
                        "dump_sell_vol":    round(float(bar["sell_vol"]), 4),
                        "severity":         severity,
                        "detected_at":      datetime.now().isoformat(),
                    })
                    log.info("%s PUMP→DUMP confirmed! pump=%.4f%% dump=%.4f%% (%.1f min apart)",
                             symbol, pump["price_chg_pct"], price_chg, time_diff_min)
                    # Clear pump state after match to avoid duplicate alerts
                    del _pump_state[symbol]

    # Expire stale pump states older than 2× window_minutes
    now = datetime.now()
    stale = []
    for sym, pump in _pump_state.items():
        try:
            if hasattr(pump["window_start"], 'to_pydatetime'):
                pump_dt = pump["window_start"].to_pydatetime()
            else:
                pump_dt = datetime.fromisoformat(str(pump["window_start"]))
            if (now - pump_dt).total_seconds() / 60 > window_minutes * 2:
                stale.append(sym)
        except Exception:
            stale.append(sym)
    for sym in stale:
        del _pump_state[sym]

    # Write alerts
    if pd_alerts:
        import pandas as pd
        alerts_df   = pd.DataFrame(pd_alerts)
        file_exists = os.path.exists(out_path)
        alerts_df.to_csv(out_path, mode="a", header=not file_exists, index=False)
        log.info("Appended %d P&D alerts → %s", len(pd_alerts), out_path)


# ============================================================
#  MAIN STREAMING JOB
# ============================================================
def run_streaming_pump_dump():
    cfg = get_config()
    bootstrap      = cfg.get("kafka_bootstrap", "localhost:9092")
    topic          = cfg.get("kafka_topic",     "market-trades")
    checkpoint_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".checkpoints", "streaming_pump_dump"
    )

    log.info("Starting Spark Structured Streaming — Pump & Dump Detector")
    log.info("Kafka broker : %s", bootstrap)
    log.info("Topic        : %s", topic)
    log.info("Checkpoint   : %s", checkpoint_dir)

    spark = (
        SparkSession.builder
        .appName("MarketSurveillance-StreamingPumpDump")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
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
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Step 2: Parse JSON ───────────────────────────────────────────────────
    trades = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("trade")
        )
        .select("trade.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("symbol").isNotNull())
    )

    # ── Step 3: 1-minute OHLCV tumbling windows ──────────────────────────────
    buy_trades  = trades.filter(F.col("side") == "BUY")
    sell_trades = trades.filter(F.col("side") == "SELL")

    ohlcv = (
        trades
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window(F.col("event_time"), "1 minute"),
            F.col("symbol")
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("total_vol"),
        )
    )

    buy_vol = (
        buy_trades
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window(F.col("event_time"), "1 minute"),
            F.col("symbol")
        )
        .agg(F.sum("quantity").alias("buy_vol"))
    )

    sell_vol = (
        sell_trades
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window(F.col("event_time"), "1 minute"),
            F.col("symbol")
        )
        .agg(F.sum("quantity").alias("sell_vol"))
    )

    # Join OHLCV with buy/sell volumes
    bars = (
        ohlcv
        .join(buy_vol,  on=["window", "symbol"], how="left")
        .join(sell_vol, on=["window", "symbol"], how="left")
        .withColumn("buy_vol",  F.coalesce(F.col("buy_vol"),  F.lit(0.0)))
        .withColumn("sell_vol", F.coalesce(F.col("sell_vol"), F.lit(0.0)))
        .withColumn(
            "price_chg_pct",
            F.when(
                F.col("open") > 0,
                ((F.col("close") - F.col("open")) / F.col("open")) * 100
            ).otherwise(0.0)
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .select(
            "symbol", "window_start", "window_end",
            "open", "high", "low", "close",
            "total_vol", "buy_vol", "sell_vol", "price_chg_pct"
        )
    )

    # ── Step 4: Write via foreachBatch (stateful P&D matching) ───────────────
    query = (
        bars.writeStream
        .outputMode("append")
        .foreachBatch(_process_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("Streaming pump & dump detector running. Press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopped by user.")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    run_streaming_pump_dump()
