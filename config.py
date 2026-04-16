"""
PIPELINE CONFIGURATION
======================
Central config that controls WHERE data lives and HOW it's processed.

PHASE 1:  MODE = "local"
  - Batch pull from Binance REST API (fetch_binance.py) — real market data
  - Spark reads/writes local filesystem (no HDFS)
  - Alerts written locally (small CSVs from pandas)
  - Detectors: Wash Trade (statistical Z-score), Pump & Dump

PHASE 2:  MODE = "local_streaming"
  - Same local paths; Spark Structured Streaming reads from Kafka topic
  - Kafka runs in Docker (KRaft single-broker, no Zookeeper)
  - Alerts written to local CSV files via foreachBatch

PHASE 3:  MODE = "streaming"
  - Kafka alert topics replace CSV sinks
  - alert_consumer.py persists to PostgreSQL (Docker)
  - Dashboard queries PostgreSQL live

PHASE 4:  MODE = "aws"
  - Amazon MSK + EMR + RDS + S3 — same code, just config changes

NOTE: Spoofing detection has been permanently removed.
  Binance public aggTrades API does not expose CANCELLED order events.
  Spoofing detection is not possible on real exchange data without
  private order-book access (e.g. exchange co-location feeds).

Just change MODE and everything adapts.
"""

import os

# ============================================================
#  SWITCH THIS TO CHANGE DEPLOYMENT PHASE
# ============================================================
# Options: "local" (Phase 1), "local_streaming" (Phase 2),
#          "streaming" (Phase 3), "aws" (Phase 4)
MODE = "streaming"

# Project root (directory containing this file)
_ROOT = os.path.dirname(os.path.abspath(__file__))


# ============================================================
#  LOCAL SETTINGS (Phase 1 — batch, local filesystem)
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
#  LOCAL STREAMING SETTINGS (Phase 2 — Kafka + CSV sinks)
# ============================================================
LOCAL_STREAMING = {
    **LOCAL,  # inherit all local paths

    "spark_master": "local[2]",  # 2 cores for streaming

    # Kafka topics
    "kafka_bootstrap": "localhost:9092",
    "kafka_topic":     "market-trades",

    # Streaming checkpoint dirs (Spark Structured Streaming)
    "checkpoint_wash":      os.path.join(_ROOT, "checkpoints", "streaming_wash"),
    "checkpoint_pump_dump": os.path.join(_ROOT, "checkpoints", "streaming_pump_dump"),
}


# ============================================================
#  STREAMING SETTINGS (Phase 3 — Kafka alert topics + PostgreSQL)
# ============================================================
STREAMING = {
    **LOCAL_STREAMING,  # inherit local + streaming paths

    # Kafka alert topics (Phase 3: Spark writes alerts here)
    "kafka_wash_alerts_topic":  "wash-alerts",
    "kafka_pd_alerts_topic":    "pump-dump-alerts",

    # PostgreSQL (Docker, Phase 3)
    "pg_host":     "localhost",
    "pg_port":     5432,
    "pg_database": "surveillance",
    "pg_user":     "surveillance",
    "pg_password": os.environ.get("PG_PASSWORD", "surveillance_local"),

    # Optional admin credentials used only as a fallback when the configured
    # `pg_user` role is missing (common when a Docker volume was created
    # previously with different POSTGRES_USER values).
    #
    # For local Docker dev, `postgres` is usually a safe default and may work
    # with the same password stored in the image's existing cluster.
    "pg_admin_user": os.environ.get("PG_ADMIN_USER", "postgres"),
    "pg_admin_password": os.environ.get("PG_ADMIN_PASSWORD", os.environ.get("PG_PASSWORD", "surveillance_local")),
    "pg_admin_database": os.environ.get("PG_ADMIN_DATABASE", "postgres"),

    # Streaming checkpoint dirs (separate from Phase 2 to avoid conflicts)
    "checkpoint_wash":      os.path.join(_ROOT, "checkpoints", "phase3_wash"),
    "checkpoint_pump_dump": os.path.join(_ROOT, "checkpoints", "phase3_pump_dump"),
}


# ============================================================
#  AWS SETTINGS (Phase 4 — MSK + EMR + RDS + S3)
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

    # Kafka (Amazon MSK)
    "kafka_bootstrap":          "your-msk-broker-1:9092,your-msk-broker-2:9092,your-msk-broker-3:9092",
    "kafka_topic":              "market-trades",
    "kafka_wash_alerts_topic":  "wash-alerts",
    "kafka_pd_alerts_topic":    "pump-dump-alerts",

    # PostgreSQL (Amazon RDS)
    "pg_host":     os.environ.get("RDS_ENDPOINT", "your-rds-endpoint.rds.amazonaws.com"),
    "pg_port":     5432,
    "pg_database": "surveillance",
    "pg_user":     os.environ.get("RDS_USER", "surveillance"),
    "pg_password": os.environ.get("RDS_PASSWORD", "change-me-in-production"),

    # SNS (Critical alert notifications — Phase 4 only)
    "sns_critical_topic_arn": os.environ.get("SNS_CRITICAL_ARN", ""),

    # Streaming checkpoint dirs (S3)
    "checkpoint_wash":      "s3a://your-bucket/market/checkpoints/wash",
    "checkpoint_pump_dump": "s3a://your-bucket/market/checkpoints/pump_dump",
}


# ============================================================
#  HELPER: Get the active config based on MODE
# ============================================================
def get_config():
    """Returns the active configuration dict based on MODE."""
    configs = {
        "local":           LOCAL,
        "local_streaming": LOCAL_STREAMING,
        "streaming":       STREAMING,
        "aws":             AWS,
    }
    if MODE not in configs:
        raise ValueError(f"Unknown MODE '{MODE}'. Valid: {list(configs.keys())}")
    return configs[MODE]


# Detection thresholds (same for all modes — centralised here, never hardcoded in detectors)
#
# TUNING NOTE:
#   These thresholds control the sensitivity vs. specificity tradeoff.
#   Lower values → higher recall (catches more suspicious activity, more false positives).
#   Higher values → higher precision (fewer alerts, but each is more confident).
#   In a production system, these would be calibrated against labelled historical data.
#   Current values are tuned for real Binance public data (30–60 min windows).
#
DETECTION = {
    # Wash trade — statistical Z-score (no real trader_id from Binance public API)
    # Group-based detection is used only when trader_id is present (synthetic/dev data)
    "wash_zscore_threshold": 0.1,       # Z-score threshold for streaming wash detection
                                        # Tuned for real Binance data: volume variance is tight
                                        # on major pairs; 0.1 catches natural micro-spikes
                                        # (history: 3.0 → 0.5 → 0.1)
    "wash_rolling_window":   "2min",    # rolling window size for volume baseline

    # Pump & dump — resampled 1-min OHLCV windows
    "pd_window_minutes":   5,           # rolling window size (in 1-min bars) for P&D detection
                                        # Wider window gives more time for dump to follow pump
    "pd_price_spike_pct":  0.08,        # minimum % price change to flag as spike (batch)
    "pd_pump_threshold":   0.001,       # 0.1% rise triggers PUMP in streaming
    "pd_dump_threshold":  -0.001,       # 0.1% drop triggers DUMP in streaming
                                        # Real BTC/ETH easily move 0.1% per minute
                                        # (history: 5.0 → 0.15 → 0.005 → 0.001)
    "pd_volume_ratio":     1.1,         # volume ratio to confirm P&D phase
                                        # (history: 3.0 → 1.5 → 1.3 → 1.1)
}
