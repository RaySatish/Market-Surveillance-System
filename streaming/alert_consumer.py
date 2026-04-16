"""
streaming/alert_consumer.py — Kafka Alert Consumer → PostgreSQL (Phase 3+)
===========================================================================

Long-running process that:
  1. Subscribes to Kafka topics: wash-alerts, pump-dump-alerts
  2. Deserializes JSON alert messages
  3. Persists to PostgreSQL via db.py (UPSERT — idempotent)
  4. Logs CRITICAL severity alerts (Phase 3) / publishes to SNS (Phase 4)
  5. Commits Kafka offsets after successful DB write (at-least-once delivery)

Usage:
  python streaming/alert_consumer.py              # run consumer
  python streaming/alert_consumer.py --test       # publish one test alert and exit

Architecture:
  Spark Streaming detectors → Kafka alert topics → THIS CONSUMER → PostgreSQL
                                                                  → SNS (Phase 4)

Delivery guarantee: at-least-once
  - Kafka offset committed AFTER successful DB write
  - DB uses ON CONFLICT DO NOTHING for deduplication
  - If consumer crashes mid-batch, Kafka replays from last committed offset
  - Replayed alerts are safely deduplicated by the UNIQUE constraint
"""

import sys
import os
import json
import signal
import time
import argparse
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

from config import get_config, MODE
from utils.fault_tolerance import get_logger
from streaming.db import (
    get_connection,
    init_schema,
    insert_wash_alert,
    insert_pump_dump_alert,
)

logger = get_logger("alert_consumer")

# ============================================================
#  Graceful shutdown
# ============================================================

_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name} — shutting down gracefully...")
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ============================================================
#  Alert processing
# ============================================================

def _process_alert(topic, alert_dict, conn):
    """
    Route an alert to the correct DB insert function based on topic.

    Args:
        topic:      Kafka topic name (wash-alerts or pump-dump-alerts)
        alert_dict: Deserialized JSON alert
        conn:       psycopg2 connection (reused across messages)
    """
    cfg = get_config()
    wash_topic = cfg.get("kafka_wash_alerts_topic", "wash-alerts")
    pd_topic = cfg.get("kafka_pd_alerts_topic", "pump-dump-alerts")

    if topic == wash_topic:
        insert_wash_alert(alert_dict, conn=conn)
        severity = alert_dict.get("severity", "UNKNOWN")
        symbol = alert_dict.get("symbol", "?")
        logger.info(f"Wash alert persisted: {symbol} [{severity}]")

    elif topic == pd_topic:
        insert_pump_dump_alert(alert_dict, conn=conn)
        severity = alert_dict.get("severity", "UNKNOWN")
        symbol = alert_dict.get("symbol", "?")
        phase = alert_dict.get("phase", "?")
        logger.info(f"P&D alert persisted: {symbol} {phase} [{severity}]")

    else:
        logger.warning(f"Unknown topic '{topic}' — skipping message")
        return

    # ---- Critical severity handling ----
    severity = alert_dict.get("severity", "")
    if severity == "CRITICAL":
        _handle_critical_alert(alert_dict)


def _handle_critical_alert(alert_dict):
    """
    Handle CRITICAL severity alerts.
    Phase 3: log.critical() only
    Phase 4 (AWS): publish to SNS topic
    """
    symbol = alert_dict.get("symbol", "?")
    alert_type = alert_dict.get("alert_type", "?")
    msg = (f"CRITICAL ALERT: {alert_type} on {symbol} — "
           f"{json.dumps(alert_dict, default=str)}")

    logger.critical(msg)

    # Phase 4: SNS publish
    if MODE == "aws":
        cfg = get_config()
        sns_arn = cfg.get("sns_critical_topic_arn", "")
        if sns_arn:
            try:
                import boto3
                sns = boto3.client("sns")
                sns.publish(
                    TopicArn=sns_arn,
                    Subject=f"CRITICAL: {alert_type} on {symbol}",
                    Message=msg,
                )
                logger.info(f"SNS notification sent for {symbol}")
            except Exception as e:
                logger.error(f"Failed to publish SNS notification: {e}")


# ============================================================
#  Consumer loop
# ============================================================

def run_consumer():
    """
    Main consumer loop. Subscribes to alert topics, processes messages,
    writes to PostgreSQL, commits offsets.
    """
    cfg = get_config()
    bootstrap = cfg["kafka_bootstrap"]
    wash_topic = cfg.get("kafka_wash_alerts_topic", "wash-alerts")
    pd_topic = cfg.get("kafka_pd_alerts_topic", "pump-dump-alerts")
    topics = [wash_topic, pd_topic]

    logger.info(f"Starting alert consumer — topics: {topics}")
    logger.info(f"Kafka bootstrap: {bootstrap}")

    # ---- Ensure DB schema exists ----
    logger.info("Ensuring database schema exists...")
    init_schema()

    # ---- Connect to PostgreSQL (reuse connection) ----
    conn = get_connection()
    logger.info("PostgreSQL connection established")

    # ---- Create Kafka consumer ----
    consumer = None
    retry_count = 0
    max_retries = 10

    while not _shutdown_requested and retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap,
                group_id="alert-consumer-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # manual commit after DB write
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,  # poll returns after 1s if no messages
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("Kafka consumer connected successfully")
            break
        except NoBrokersAvailable:
            retry_count += 1
            wait = min(2 ** retry_count, 30)
            logger.warning(f"No Kafka brokers available — retry {retry_count}/{max_retries} "
                           f"in {wait}s...")
            time.sleep(wait)

    if consumer is None:
        logger.error("Failed to connect to Kafka after max retries — exiting")
        conn.close()
        return

    # ---- Main processing loop ----
    logger.info("Consumer loop started — waiting for alerts...")
    messages_processed = 0
    errors = 0

    try:
        while not _shutdown_requested:
            try:
                # Poll for messages (returns after consumer_timeout_ms if none)
                message_batch = consumer.poll(timeout_ms=1000)

                if not message_batch:
                    continue

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if _shutdown_requested:
                            break

                        topic = message.topic
                        alert_dict = message.value

                        try:
                            _process_alert(topic, alert_dict, conn)
                            messages_processed += 1

                            # Log progress every 100 messages
                            if messages_processed % 100 == 0:
                                logger.info(f"Processed {messages_processed} alerts "
                                            f"({errors} errors)")

                        except Exception as e:
                            errors += 1
                            logger.error(
                                f"Failed to process alert from {topic}: {e} — "
                                f"payload: {json.dumps(alert_dict, default=str)[:500]}"
                            )
                            # Try to reconnect to DB if connection is broken
                            try:
                                conn.close()
                            except Exception:
                                pass
                            try:
                                conn = get_connection()
                                logger.info("Reconnected to PostgreSQL")
                            except Exception as reconn_err:
                                logger.error(f"DB reconnect failed: {reconn_err}")
                                time.sleep(5)
                                continue

                # Commit offsets after processing the batch
                try:
                    consumer.commit()
                except Exception as e:
                    logger.error(f"Failed to commit Kafka offsets: {e}")

            except KafkaError as e:
                logger.error(f"Kafka error: {e}")
                time.sleep(2)

    finally:
        # ---- Cleanup ----
        logger.info(f"Shutting down — processed {messages_processed} alerts, "
                    f"{errors} errors")
        try:
            consumer.close()
            logger.info("Kafka consumer closed")
        except Exception:
            pass
        try:
            conn.close()
            logger.info("PostgreSQL connection closed")
        except Exception:
            pass


# ============================================================
#  Test mode
# ============================================================

def run_test():
    """Publish a test alert to Kafka for verification."""
    cfg = get_config()
    bootstrap = cfg["kafka_bootstrap"]
    wash_topic = cfg.get("kafka_wash_alerts_topic", "wash-alerts")

    # Ensure schema exists
    init_schema()

    # Produce a test alert to Kafka
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    now = datetime.now(timezone.utc).isoformat()
    test_alert = {
        "window_start":  now,
        "window_end":    now,
        "symbol":        "BTCUSDT",
        "trade_count":   99,
        "total_volume":  5678.90,
        "mean_volume":   200.0,
        "std_volume":    75.0,
        "zscore":        4.2,
        "severity":      "HIGH",
        "alert_type":    "WASH_TRADE",
        "detected_at":   now,
    }

    producer.send(wash_topic, value=test_alert)
    producer.flush()
    producer.close()
    print(f"Test alert published to Kafka topic '{wash_topic}'")
    print("  Run the consumer to persist it to PostgreSQL:")
    print("  python streaming/alert_consumer.py")


# ============================================================
#  CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Kafka alert consumer -> PostgreSQL (Phase 3+)"
    )
    parser.add_argument("--test", action="store_true",
                        help="Publish a test alert to Kafka and exit")
    args = parser.parse_args()

    if args.test:
        run_test()
    else:
        run_consumer()


if __name__ == "__main__":
    main()
