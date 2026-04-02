"""
STREAMING PIPELINE ORCHESTRATOR
================================
Phase 2: Starts all streaming components in the correct order.

What this script does:
  1. Checks that Kafka (Docker) is reachable
  2. Ensures the 'market-trades' topic exists (creates it if not)
  3. Starts the Kafka producer (Binance WebSocket OR synthetic test data)
  4. Starts both Spark Structured Streaming detectors in background threads
  5. Monitors all processes; shuts everything down cleanly on Ctrl+C

Usage:
  # Start Kafka first:
  docker compose up -d

  # Then run the full streaming pipeline:
  python streaming/run_streaming_pipeline.py            # test mode (synthetic data)
  python streaming/run_streaming_pipeline.py --live     # real Binance WebSocket
  python streaming/run_streaming_pipeline.py --test --messages 10000

Architecture:
  run_streaming_pipeline.py
    ├── Thread 1: kafka_producer (Binance WS or synthetic)
    ├── Thread 2: spark_streaming_wash.run_streaming_wash()
    └── Thread 3: spark_streaming_pump_dump.run_streaming_pump_dump()

NOTE: Running two Spark sessions in the same process is not supported.
The orchestrator launches the Spark streaming jobs as subprocesses instead.
"""

import argparse
import subprocess
import sys
import time
import os
import signal
import threading

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


# ============================================================
#  MAIN ORCHESTRATOR
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Phase 2 streaming pipeline orchestrator"
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
    args = parser.parse_args()

    cfg       = get_config()
    bootstrap = cfg.get("kafka_bootstrap", "localhost:9092")
    topic     = cfg.get("kafka_topic",     "market-trades")

    log.info("=" * 60)
    log.info("Phase 2 Streaming Pipeline")
    log.info("Mode      : %s", "LIVE (Binance WebSocket)" if args.live else "TEST (synthetic)")
    log.info("Kafka     : %s", bootstrap)
    log.info("Topic     : %s", topic)
    log.info("=" * 60)

    # ── Step 1: Check Kafka is up ────────────────────────────────────────────
    if not wait_for_kafka(bootstrap):
        log.error("Kafka is not reachable. Start it with: docker compose up -d")
        sys.exit(1)

    # ── Step 2: Ensure topic exists ──────────────────────────────────────────
    ensure_topic(bootstrap, topic)

    processes = []

    # ── Step 3: Start Spark streaming detectors ──────────────────────────────
    log.info("Starting Spark Structured Streaming detectors...")
    wash_proc     = start_wash_detector()
    pd_proc       = start_pump_dump_detector()
    processes.extend([wash_proc, pd_proc])

    # Give Spark a moment to initialise before the producer starts flooding
    log.info("Waiting 15s for Spark to initialise...")
    time.sleep(15)

    # ── Step 4: Start Kafka producer ─────────────────────────────────────────
    if not args.no_producer and not args.detectors_only:
        producer_proc = start_producer(
            live=args.live,
            num_messages=args.messages
        )
        processes.append(producer_proc)

    log.info("All components started. Press Ctrl+C to stop.")
    log.info("Streaming alerts will appear in: alerts/streaming_*.csv")

    # ── Step 5: Monitor processes ────────────────────────────────────────────
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
