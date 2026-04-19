"""
PIPELINE CONFIGURATION
======================
Central config for the market surveillance streaming pipeline.

Architecture:
  Binance WebSocket → Kafka → Spark Structured Streaming → Kafka alert topics
  → alert_consumer.py → PostgreSQL → Streamlit dashboard

Infrastructure (Docker Compose):
  - Kafka (KRaft single-broker, no Zookeeper)
  - PostgreSQL 16

NOTE: Spoofing detection is not supported.
  Binance public aggTrades API does not expose CANCELLED order events.
  Spoofing detection requires private order-book access (e.g. exchange
  co-location feeds).
"""

import os

# Project root (directory containing this file)
_ROOT = os.path.dirname(os.path.abspath(__file__))


# ============================================================
#  INFRASTRUCTURE
# ============================================================

# Kafka (Docker KRaft broker)
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "market-trades"
KAFKA_WASH_ALERTS_TOPIC = "wash-alerts"
KAFKA_PD_ALERTS_TOPIC = "pump-dump-alerts"

# PostgreSQL (Docker)
PG_HOST = "localhost"
PG_PORT = 5432
PG_DATABASE = "surveillance"
PG_USER = "surveillance"
PG_PASSWORD = os.environ.get("PG_PASSWORD", "surveillance_local")

# Admin credentials — fallback when the configured role is missing
# (common when a Docker volume was created with different POSTGRES_USER)
PG_ADMIN_USER = os.environ.get("PG_ADMIN_USER", "postgres")
PG_ADMIN_PASSWORD = os.environ.get(
    "PG_ADMIN_PASSWORD", os.environ.get("PG_PASSWORD", "surveillance_local")
)
PG_ADMIN_DATABASE = os.environ.get("PG_ADMIN_DATABASE", "postgres")

# Spark
SPARK_MASTER = "local[2]"

# Spark Structured Streaming checkpoint dirs
CHECKPOINT_WASH = os.path.join(_ROOT, "checkpoints", "streaming_wash")
CHECKPOINT_PUMP_DUMP = os.path.join(_ROOT, "checkpoints", "streaming_pump_dump")


# ============================================================
#  VALID SYMBOLS
# ============================================================
VALID_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


# ============================================================
#  DETECTION THRESHOLDS
# ============================================================
# These control the sensitivity vs. specificity tradeoff.
#   Lower values → higher recall (more alerts, more false positives).
#   Higher values → higher precision (fewer alerts, each more confident).
# In a production system, calibrate against labelled historical data.
# Current values are tuned for real Binance public data (30–60 min windows).

DETECTION = {
    # Wash trade — statistical Z-score on rolling volume windows
    # (no trader_id from Binance public API; purely volume-based)
    "wash_zscore_threshold": 0.1,       # Z-score threshold for wash detection
                                        # Tuned for real Binance data: volume variance
                                        # is tight on major pairs; 0.1 catches natural
                                        # micro-spikes
    "wash_rolling_window":   "2min",    # tumbling window size for volume baseline

    # Pump & dump — resampled 1-min OHLCV bars
    "pd_window_minutes":   5,           # rolling window size (in 1-min bars)
                                        # Wider window gives more time for dump to
                                        # follow pump
    "pd_pump_threshold":   0.001,       # 0.1% rise triggers PUMP
    "pd_dump_threshold":  -0.001,       # 0.1% drop triggers DUMP
                                        # Real BTC/ETH easily move 0.1% per minute
    "pd_volume_ratio":     1.1,         # volume ratio to confirm P&D phase
}


# ============================================================
#  HELPER: get_config() — returns a flat dict for backward compat
# ============================================================
def get_config():
    """Returns the active configuration as a dict.

    Streaming modules can import individual constants directly
    (e.g. `from config import KAFKA_BOOTSTRAP`), but some legacy
    code paths still call `get_config()["key"]`.
    """
    return {
        "spark_master":           SPARK_MASTER,
        "kafka_bootstrap":        KAFKA_BOOTSTRAP,
        "kafka_topic":            KAFKA_TOPIC,
        "kafka_wash_alerts_topic": KAFKA_WASH_ALERTS_TOPIC,
        "kafka_pd_alerts_topic":  KAFKA_PD_ALERTS_TOPIC,
        "pg_host":                PG_HOST,
        "pg_port":                PG_PORT,
        "pg_database":            PG_DATABASE,
        "pg_user":                PG_USER,
        "pg_password":            PG_PASSWORD,
        "pg_admin_user":          PG_ADMIN_USER,
        "pg_admin_password":      PG_ADMIN_PASSWORD,
        "pg_admin_database":      PG_ADMIN_DATABASE,
        "checkpoint_wash":        CHECKPOINT_WASH,
        "checkpoint_pump_dump":   CHECKPOINT_PUMP_DUMP,
    }
