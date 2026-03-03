"""
PIPELINE CONFIGURATION
======================
Central config that controls WHERE data lives and HOW it's processed.

PHASE 1 (now):  MODE = "local"
  - Synthetic data from generate_trades.py
  - Spark reads/writes via HDFS (Hadoop on localhost)
  - Raw CSV is uploaded to HDFS, Spark ETL writes Parquet to HDFS
  - Alerts written locally (small CSVs from pandas)

PHASE 2 (later): MODE = "aws"
  - Real-time data from Binance WebSocket API
  - Spark on AWS EMR (Hadoop/YARN managed by EMR)
  - HDFS/EMRFS backed by S3 (s3a://)
  - Streaming via Kafka / Kinesis

Just change MODE to "aws" and fill in the AWS settings — everything adapts.
"""

import os

# ============================================================
#  SWITCH THIS TO "aws" WHEN DEPLOYING TO CLOUD
# ============================================================
MODE = "local"  # "local" or "aws"

# HDFS namenode URI (from your core-site.xml)
HDFS_NAMENODE = "hdfs://localhost:9000"

# Local file that generate_trades.py produces (on disk, before HDFS upload)
LOCAL_CSV = os.path.join(os.path.dirname(__file__), "trades.csv")


# ============================================================
#  LOCAL SETTINGS (Phase 1 — Hadoop + Spark on your machine)
# ============================================================
LOCAL = {
    "spark_master": "local[*]",

    # HDFS path: raw CSV uploaded here by ingest_to_hdfs.py
    "raw_input": f"{HDFS_NAMENODE}/market/raw/trades.csv",

    # HDFS path: Spark ETL writes cleaned Parquet here
    "clean_output": f"{HDFS_NAMENODE}/market/clean/trades",

    # Alerts stay local (pandas writes small CSVs, dashboard reads them)
    "alerts_dir": "alerts",
    "alerts_wash": "alerts/alerts_wash.csv",
    "alerts_pump_dump": "alerts/alerts_pump_dump.csv",
    "alerts_spoofing": "alerts/alerts_spoofing.csv",
    "alerts_combined": "alerts/all_alerts.csv",
}


# ============================================================
#  AWS SETTINGS (Phase 2 — EMR production on cloud)
# ============================================================
AWS = {
    "spark_master": "yarn",  # EMR manages Spark via YARN

    # S3 paths (replace with your bucket)
    "raw_input": "s3a://your-bucket/market/raw/trades/",
    "clean_output": "s3a://your-bucket/market/clean/trades/",

    # Alert outputs on S3
    "alerts_dir": "s3a://your-bucket/market/alerts/",
    "alerts_wash": "s3a://your-bucket/market/alerts/alerts_wash.csv",
    "alerts_pump_dump": "s3a://your-bucket/market/alerts/alerts_pump_dump.csv",
    "alerts_spoofing": "s3a://your-bucket/market/alerts/alerts_spoofing.csv",
    "alerts_combined": "s3a://your-bucket/market/alerts/all_alerts.csv",

    # Binance API (Phase 2)
    "binance_ws_url": "wss://stream.binance.com:9443/ws",
    "binance_symbols": ["btcusdt@trade", "ethusdt@trade", "solusdt@trade"],

    # Kafka / Kinesis (Phase 2 — streaming ingestion)
    "kafka_bootstrap": "your-kafka-broker:9092",
    "kafka_topic": "market-trades",
}


# ============================================================
#  HELPER: Get the active config based on MODE
# ============================================================
def get_config():
    """Returns the active configuration dict based on MODE."""
    if MODE == "aws":
        return AWS
    return LOCAL


# Detection thresholds (same for both modes)
DETECTION = {
    # Wash trade
    "wash_min_group_size": 2,

    # Pump & dump
    "pd_window_minutes": 5,
    "pd_price_spike_pct": 0.05,
    "pd_volume_ratio": 3.0,

    # Spoofing
    "spoof_cancel_rate": 0.5,
    "spoof_min_orders": 3,
    "spoof_size_multiplier": 2.0,
}
