"""
STREAMING PIPELINE ORCHESTRATOR
================================
Dual-mode orchestrator for Phase 2 and Phase 3 streaming pipelines.

Phase 2 (--mode phase2, default):
  1. Checks Kafka is reachable
  2. Ensures 'market-trades' topic exists
  3. Starts Kafka producer (Binance WebSocket OR synthetic test data)
  4. Starts both Spark Structured Streaming detectors → CSV sinks
  5. Monitors all processes; shuts down cleanly on Ctrl+C

Phase 3 (--mode phase3):
  1. Checks Kafka is reachable
  2. Ensures 'market-trades', 'wash-alerts', 'pump-dump-alerts' topics exist
  3. Checks PostgreSQL is reachable; initialises schema if needed
  4. Starts alert_consumer.py (Kafka alert topics → PostgreSQL)
  5. Starts Kafka producer (Binance WebSocket OR synthetic test data)
  6. Starts both Spark Structured Streaming detectors → Kafka alert topic sinks
  7. Monitors all processes; shuts down cleanly on Ctrl+C

Usage:
  docker compose up -d

  # Phase 2 (CSV sinks):
  python streaming/run_streaming_pipeline.py --test
  python streaming/run_streaming_pipeline.py --live

  # Phase 3 (Kafka + PostgreSQL):
  python streaming/run_streaming_pipeline.py --mode phase3 --test
  python streaming/run_streaming_pipeline.py --mode phase3 --live

Architecture (Phase 3):
  run_streaming_pipeline.py
    ├── Subprocess 1: alert_consumer.py (Kafka → PostgreSQL)
    ├── Subprocess 2: spark_streaming_wash.py (→ Kafka: wash-alerts)
    ├── Subprocess 3: spark_streaming_pump_dump.py (→ Kafka: pump-dump-alerts)
    └── Subprocess 4: kafka_producer.py (Binance WS or synthetic)

NOTE: Running two Spark sessions in the same process is not supported.
The orchestrator launches all components as subprocesses.
"""

import argparse
import subprocess
import sys
import time
import os
import signal
import shutil

# ── Project root path fix ──────────────────────────────────────────
import sys as _sys, os as _os
_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _root not in _sys.path:
    _sys.path.insert(0, _root)
# ───────────────────────────────────────────────────────────────────────────
from utils.fault_tolerance import get_logger
from config import get_config

log = get_logger("streaming_pipeline")

# ============================================================
#  KAFKA HEALTH CHECK
# ============================================================
def wait_for_kafka(bootstrap: str, retries: int = 15, delay: float = 3.0) -> bool:
    """
    Poll Kafka until it responds or we run out of retries.
    Uses kafka-python's simple connection check.
    """
    try:
        from kafka import KafkaAdminClient
        from kafka.errors import NoBrokersAvailable
    except ImportError:
        log.warning("kafka-python not installed — skipping Kafka health check")
        return True

    log.info("Waiting for Kafka at %s ...", bootstrap)
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
            admin.close()
            log.info("Kafka is ready! (attempt %d/%d)", attempt, retries)
            return True
        except Exception as exc:
            log.warning("Kafka not ready yet (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay)

    log.error("Kafka did not become ready after %d attempts.", retries)
    return False


def ensure_topic(bootstrap: str, topic: str, num_partitions: int = 3):
    """Create the Kafka topic if it doesn't already exist."""
    try:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError:
        log.warning("kafka-python not installed — skipping topic creation")
        return

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=1
        )])
        log.info("Created Kafka topic '%s' (%d partitions)", topic, num_partitions)
    except TopicAlreadyExistsError:
        log.info("Topic '%s' already exists.", topic)
    except Exception as exc:
        log.warning("Could not create topic '%s': %s", topic, exc)
    finally:
        admin.close()


# ============================================================
#  POSTGRESQL HEALTH CHECK (Phase 3)
# ============================================================
def wait_for_postgres(cfg: dict, retries: int = 10, delay: float = 3.0) -> bool:
    """Poll PostgreSQL until it responds or we run out of retries."""
    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 not installed — skipping PostgreSQL health check")
        return True

    log.info("Waiting for PostgreSQL at %s:%s ...",
             cfg.get("pg_host", "localhost"), cfg.get("pg_port", 5432))

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=cfg.get("pg_host", "localhost"),
                port=cfg.get("pg_port", 5432),
                dbname=cfg.get("pg_database", "surveillance"),
                user=cfg.get("pg_user", "surveillance"),
                password=cfg.get("pg_password", "surveillance_local"),
                connect_timeout=5,
            )
            conn.close()
            log.info("PostgreSQL is ready! (attempt %d/%d)", attempt, retries)
            return True
        except Exception as exc:
            log.warning("PostgreSQL not ready yet (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay)

    log.error("PostgreSQL did not become ready after %d attempts.", retries)
    return False


def init_db_schema():
    """Initialise PostgreSQL schema (Phase 3)."""
    try:
        from streaming.db import init_schema
        init_schema()
        log.info("PostgreSQL schema initialised successfully.")
    except Exception as exc:
        log.error("Failed to initialise PostgreSQL schema: %s", exc)
        raise


# ============================================================
#  SUBPROCESS LAUNCHERS
# ============================================================
def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def start_producer(live: bool = False, num_messages: int = 2000) -> subprocess.Popen:
    """Launch kafka_producer.py as a subprocess."""
    root = _project_root()
    cmd  = [sys.executable, "-m", "streaming.kafka_producer"]
    if live:
        cmd.append("--live")
    else:
        cmd += ["--test", "--messages", str(num_messages)]

    log.info("Starting Kafka producer: %s", " ".join(cmd))
    return subprocess.Popen(cmd, cwd=root)


def start_wash_detector() -> subprocess.Popen:
    """Launch spark_streaming_wash.py as a subprocess."""
    root = _project_root()
    cmd  = [sys.executable, "-m", "streaming.spark_streaming_wash"]
    log.info("Starting streaming wash detector: %s", " ".join(cmd))
    return subprocess.Popen(cmd, cwd=root)


def start_pump_dump_detector() -> subprocess.Popen:
    """Launch spark_streaming_pump_dump.py as a subprocess."""
    root = _project_root()
    cmd  = [sys.executable, "-m", "streaming.spark_streaming_pump_dump"]
    log.info("Starting streaming pump & dump detector: %s", " ".join(cmd))
    return subprocess.Popen(cmd, cwd=root)


def start_alert_consumer() -> subprocess.Popen:
    """Launch alert_consumer.py as a subprocess (Phase 3 only)."""
    root = _project_root()
    cmd  = [sys.executable, "-m", "streaming.alert_consumer"]
    log.info("Starting alert consumer (Kafka → PostgreSQL): %s", " ".join(cmd))
    return subprocess.Popen(cmd, cwd=root)


# ============================================================
#  MAIN ORCHESTRATOR
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Streaming pipeline orchestrator (Phase 2 & Phase 3)"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--live",  action="store_true",
                       help="Use real Binance WebSocket as data source")
    group.add_argument("--test",  action="store_true", default=True,
                       help="Use synthetic data (default)")
    parser.add_argument("--messages", type=int, default=5000,
                        help="Number of synthetic messages (test mode, default: 5000)")
    parser.add_argument("--no-producer", action="store_true",
                        help="Skip starting the producer (if already running)")
    parser.add_argument("--detectors-only", action="store_true",
                        help="Start only the Spark streaming detectors")
    parser.add_argument("--mode", choices=["phase2", "phase3"], default="phase2",
                        help="Pipeline mode: phase2 (CSV sinks) or phase3 (Kafka + PostgreSQL)")
    args = parser.parse_args()

    cfg       = get_config()
    bootstrap = cfg.get("kafka_bootstrap", "localhost:9092")
    topic     = cfg.get("kafka_topic",     "market-trades")
    is_phase3 = args.mode == "phase3"

    phase_label = "Phase 3 (Kafka alerts + PostgreSQL)" if is_phase3 else "Phase 2 (CSV sinks)"

    log.info("=" * 60)
    log.info("Streaming Pipeline — %s", phase_label)
    log.info("Mode      : %s", "LIVE (Binance WebSocket)" if args.live else "TEST (synthetic)")
    log.info("Kafka     : %s", bootstrap)
    log.info("Topic     : %s", topic)
    if is_phase3:
        log.info("PostgreSQL: %s:%s/%s",
                 cfg.get("pg_host", "localhost"),
                 cfg.get("pg_port", 5432),
                 cfg.get("pg_database", "surveillance"))
    log.info("=" * 60)

    # ── Step 1: Check Kafka is up ────────────────────────────────────────────
    if not wait_for_kafka(bootstrap):
        log.error("Kafka is not reachable. Start it with: docker compose up -d")
        sys.exit(1)

    # ── Step 2: Ensure topics exist ──────────────────────────────────────────
    ensure_topic(bootstrap, topic)
    if is_phase3:
        # Phase 3: also create alert topics
        wash_alerts_topic = cfg.get("kafka_wash_alerts_topic", "wash-alerts")
        pd_alerts_topic   = cfg.get("kafka_pd_alerts_topic", "pump-dump-alerts")
        ensure_topic(bootstrap, wash_alerts_topic)
        ensure_topic(bootstrap, pd_alerts_topic)

    # ── Step 3 (Phase 3 only): Check PostgreSQL and init schema ──────────────
    if is_phase3:
        if not wait_for_postgres(cfg):
            log.error("PostgreSQL is not reachable. Start it with: docker compose up -d")
            sys.exit(1)
        init_db_schema()

    processes = []

    # ── Step 4: Clear stale streaming checkpoints ────────────────────────────
    if is_phase3:
        checkpoint_dirs = [
            cfg.get("checkpoint_wash", "checkpoints/phase3_wash"),
            cfg.get("checkpoint_pump_dump", "checkpoints/phase3_pump_dump"),
        ]
    else:
        checkpoint_dirs = [
            ".checkpoints/streaming_wash",
            ".checkpoints/streaming_pump_dump",
        ]

    for _ck_dir in checkpoint_dirs:
        # Handle both absolute and relative paths
        if os.path.isabs(_ck_dir):
            _full = _ck_dir
        else:
            _full = os.path.join(_project_root(), _ck_dir)
        if os.path.isdir(_full):
            shutil.rmtree(_full)
            log.info("Cleared stale streaming checkpoint: %s", _full)

    # ── Step 5 (Phase 3 only): Start alert consumer ─────────────────────────
    if is_phase3 and not args.detectors_only:
        consumer_proc = start_alert_consumer()
        processes.append(consumer_proc)
        log.info("Alert consumer started. Waiting 3s for it to connect...")
        time.sleep(3)

    # ── Step 6: Start Spark streaming detectors ──────────────────────────────
    log.info("Starting Spark Structured Streaming detectors...")
    wash_proc     = start_wash_detector()
    pd_proc       = start_pump_dump_detector()
    processes.extend([wash_proc, pd_proc])

    # Give Spark a moment to initialise before the producer starts flooding
    log.info("Waiting 15s for Spark to initialise...")
    time.sleep(15)

    # ── Step 7: Start Kafka producer ─────────────────────────────────────────
    if not args.no_producer and not args.detectors_only:
        producer_proc = start_producer(
            live=args.live,
            num_messages=args.messages
        )
        processes.append(producer_proc)

    log.info("All components started. Press Ctrl+C to stop.")
    if is_phase3:
        log.info("Alerts flow: Spark → Kafka alert topics → alert_consumer → PostgreSQL")
        log.info("Dashboard: streamlit run streaming/stream_alerts_dashboard.py")
    else:
        log.info("Streaming alerts will appear in: alerts/streaming_*.csv")

    # ── Step 8: Monitor processes ────────────────────────────────────────────
    def _shutdown(signum=None, frame=None):
        log.info("Shutting down streaming pipeline...")
        for proc in processes:
            try:
                proc.terminate()
            except Exception:
                pass
        for proc in processes:
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
        log.info("All processes stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            for proc in processes:
                ret = proc.poll()
                if ret is not None and ret != 0:
                    log.warning("Process %s exited with code %d", proc.args, ret)
            time.sleep(5)
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    main()
