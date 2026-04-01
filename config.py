"""
PIPELINE CONFIGURATION
======================
Central config that controls WHERE data lives and HOW it's processed.

PHASE 1 (now):  MODE = "local"
  - Batch pull from Binance REST API (fetch_binance.py) — real market data
  - Spark reads/writes local filesystem (no HDFS)
  - Alerts written locally (small CSVs from pandas)
  - Detectors: Wash Trade (statistical Z-score), Pump & Dump

PHASE 2 (laptop streaming):  MODE = "local" + Kafka
  - Same local paths; Spark Structured Streaming reads from Kafka topic
  - Kafka runs in Docker (KRaft single-broker, no Zookeeper)

PHASE 3 (AWS):  MODE = "aws"
  - Binance REST API → S3; Spark on EMR; same code, just change MODE

NOTE: Spoofing detection has been permanently removed.
  Binance public aggTrades API does not expose CANCELLED order events.
  Spoofing detection is not possible on real exchange data without
  private order-book access (e.g. exchange co-location feeds).

Just change MODE to "aws" and fill in the AWS settings — everything adapts.
"""

import os

# ============================================================
#  SWITCH THIS TO "aws" WHEN DEPLOYING TO CLOUD
# ============================================================
MODE = "local"  # "local" or "aws"

# Project root (directory containing this file)
_ROOT = os.path.dirname(os.path.abspath(__file__))


# ============================================================
#  LOCAL SETTINGS (Phase 1 & 2 — laptop, local filesystem)
# ============================================================
LOCAL = {
    "spark_master": "local[*]",

    # Raw CSV produced by fetch_binance.py (real data)
    # or generate_trades.py (synthetic data for development/testing)
    "trades_csv": os.path.join(_ROOT, "trades.csv"),

    # Parquet output from Spark ETL (partitioned by symbol)
    "parquet_dir": os.path.join(_ROOT, "data", "parquet"),

    # Alerts (written by detectors, read by dashboard)
    "alerts_dir":       os.path.join(_ROOT, "alerts"),
    "alerts_wash":      os.path.join(_ROOT, "alerts", "alerts_wash.csv"),
    "alerts_pump_dump": os.path.join(_ROOT, "alerts", "alerts_pump_dump.csv"),
    "alerts_combined":  os.path.join(_ROOT, "alerts", "all_alerts.csv"),

    # Kafka (Phase 2 streaming — Docker KRaft broker)
    "kafka_bootstrap": "localhost:9092",
    "kafka_topic":     "market-trades",
}


# ============================================================
#  AWS SETTINGS (Phase 3 — EMR + S3)
# ============================================================
AWS = {
    "spark_master": "yarn",  # EMR manages Spark via YARN

    # S3 paths (replace with your bucket)
    "trades_csv":       "s3a://your-bucket/market/raw/trades.csv",
    "parquet_dir":      "s3a://your-bucket/market/clean/trades",

    # Alert outputs on S3
    "alerts_dir":       "s3a://your-bucket/market/alerts/",
    "alerts_wash":      "s3a://your-bucket/market/alerts/alerts_wash.csv",
    "alerts_pump_dump": "s3a://your-bucket/market/alerts/alerts_pump_dump.csv",
    "alerts_combined":  "s3a://your-bucket/market/alerts/all_alerts.csv",

    # Binance REST API
    "binance_symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],

    # Kafka / Kinesis (Phase 3 streaming)
    "kafka_bootstrap": "your-kafka-broker:9092",
    "kafka_topic":     "market-trades",
}


# ============================================================
#  HELPER: Get the active config based on MODE
# ============================================================
def get_config():
    """Returns the active configuration dict based on MODE."""
    if MODE == "aws":
        return AWS
    return LOCAL


# Detection thresholds (same for all modes — centralised here, never hardcoded in detectors)
DETECTION = {
    # Wash trade — statistical Z-score (no real trader_id from Binance public API)
    # Group-based detection is used only when trader_id is present (synthetic/dev data)
    "wash_zscore_threshold": 3.0,       # flag windows where volume Z-score > this
    "wash_rolling_window":   "5min",    # rolling window size for volume baseline

    # Pump & dump
    "pd_window_minutes":   5,
    "pd_price_spike_pct":  5,
    "pd_volume_ratio":     3.0,

    # NOTE: Spoofing thresholds removed — detection not possible on Binance public data.
    # Binance aggTrades API only returns executed trades, never CANCELLED orders.
}
