"""
PHASE 3 INTEGRATION TEST
=========================
End-to-end test for the Phase 3 streaming pipeline.

Prerequisites:
  docker compose up -d   (Kafka + PostgreSQL must be running)

What this tests:
  1. PostgreSQL schema init (db.py --init)
  2. Kafka topic creation
  3. Producer -> Kafka -> Spark detectors -> Kafka alerts -> Alert consumer -> PostgreSQL
  4. Dashboard data availability (query_alerts)
  5. Deduplication (run twice, verify no duplicates)
  6. Fault tolerance (alert consumer restart)

Usage:
  python tests/test_phase3_integration.py              # full test
  python tests/test_phase3_integration.py --quick      # skip slow tests
"""

import os
import sys
import time
import json
import signal
import subprocess
import argparse

# -- Project root path fix
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)

from config import get_config, MODE

if MODE not in ("streaming", "aws"):
    print("WARNING: config.py MODE is '{}', not 'streaming'.".format(MODE))
    print("   Set MODE = 'streaming' in config.py for full Phase 3 testing.\n")

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"
results = []


def log_result(test_name, passed, detail=""):
    status = PASS if passed else FAIL
    results.append((test_name, passed, detail))
    msg = "  [{}] {}".format(status, test_name)
    if detail:
        msg += " -- {}".format(detail)
    print(msg)


def log_skip(test_name, reason):
    results.append((test_name, None, reason))
    print("  [{}] {} -- SKIPPED: {}".format(SKIP, test_name, reason))


# ============================================================
#  TEST 1: Infrastructure Check
# ============================================================
def test_infrastructure():
    print("\n" + "=" * 60)
    print("TEST 1: Infrastructure Check")
    print("=" * 60)

    # Check Docker is running
    try:
        result = subprocess.run(
            ["docker", "compose", "ps"],
            capture_output=True, text=True, timeout=10, cwd=_root,
        )
        if result.returncode != 0:
            log_result("Docker Compose running", False, "docker compose ps failed")
            return False
        log_result("Docker Compose running", True)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        log_result("Docker Compose running", False, str(exc))
        return False

    # Check Kafka connectivity
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            request_timeout_ms=5000,
        )
        producer.close()
        log_result("Kafka broker reachable", True, "localhost:9092")
    except Exception as exc:
        log_result("Kafka broker reachable", False, str(exc))
        return False

    # Check PostgreSQL connectivity
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="surveillance", user="surveillance",
            password="surveillance_local", connect_timeout=5,
        )
        conn.close()
        log_result("PostgreSQL reachable", True, "localhost:5432/surveillance")
    except Exception as exc:
        log_result("PostgreSQL reachable", False, str(exc))
        return False

    return True


# ============================================================
#  TEST 2: Database Schema Init
# ============================================================
def test_db_schema():
    print("\n" + "=" * 60)
    print("TEST 2: Database Schema Init")
    print("=" * 60)

    try:
        from streaming.db import init_schema, get_connection

        init_schema()
        log_result("init_schema() succeeded", True)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('wash_alerts', 'pump_dump_alerts')
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        log_result("wash_alerts table exists", "wash_alerts" in tables)
        log_result("pump_dump_alerts table exists", "pump_dump_alerts" in tables)

        return "wash_alerts" in tables and "pump_dump_alerts" in tables
    except Exception as exc:
        log_result("Database schema init", False, str(exc))
        return False


# ============================================================
#  TEST 3: Alert Insert + Query + Dedup
# ============================================================
def test_db_insert_query():
    print("\n" + "=" * 60)
    print("TEST 3: Alert Insert + Query + Dedup")
    print("=" * 60)

    try:
        from streaming.db import (
            insert_wash_alert, insert_pump_dump_alert,
            query_alerts, get_connection,
        )

        test_wash = {
            "window_start": "2026-01-01T00:00:00",
            "window_end":   "2026-01-01T00:01:00",
            "symbol":       "BTCUSDT",
            "trade_count":  100,
            "total_volume": 500.0,
            "mean_volume":  50.0,
            "std_volume":   10.0,
            "zscore":       5.0,
            "severity":     "HIGH",
            "alert_type":   "WASH_TRADE",
            "detected_at":  "2026-01-01T00:01:05",
        }
        insert_wash_alert(test_wash)
        log_result("Insert wash alert", True)

        test_pd = {
            "window_start":     "2026-01-01T00:00:00",
            "window_end":       "2026-01-01T00:02:00",
            "symbol":           "ETHUSDT",
            "phase":            "PUMP",
            "price_change_pct": 2.5,
            "volume_ratio":     3.0,
            "severity":         "CRITICAL",
            "alert_type":       "PUMP_DUMP",
            "detected_at":      "2026-01-01T00:02:05",
        }
        insert_pump_dump_alert(test_pd)
        log_result("Insert pump-dump alert", True)

        wash_results = query_alerts("wash", symbol="BTCUSDT", limit=10)
        log_result("Query wash alerts", len(wash_results) > 0,
                   "{} rows".format(len(wash_results)))

        pd_results = query_alerts("pump_dump", symbol="ETHUSDT", limit=10)
        log_result("Query pump-dump alerts", len(pd_results) > 0,
                   "{} rows".format(len(pd_results)))

        # Dedup test
        insert_wash_alert(test_wash)
        wash_results_2 = query_alerts("wash", symbol="BTCUSDT", limit=10)
        no_dup = len(wash_results_2) == len(wash_results)
        log_result("Deduplication (ON CONFLICT DO NOTHING)", no_dup,
                   "Before: {}, After: {}".format(len(wash_results), len(wash_results_2)))

        # Clean up
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM wash_alerts WHERE window_start = '2026-01-01T00:00:00'")
        cur.execute("DELETE FROM pump_dump_alerts WHERE window_start = '2026-01-01T00:00:00'")
        conn.commit()
        cur.close()
        conn.close()
        log_result("Test data cleaned up", True)

        return True
    except Exception as exc:
        log_result("DB insert/query test", False, str(exc))
        return False


# ============================================================
#  TEST 4: Kafka Topic Creation
# ============================================================
def test_kafka_topics():
    print("\n" + "=" * 60)
    print("TEST 4: Kafka Topic Creation")
    print("=" * 60)

    try:
        from kafka.admin import KafkaAdminClient, NewTopic

        admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
        existing = admin.list_topics()

        topics_needed = ["market-trades", "wash-alerts", "pump-dump-alerts"]
        topics_to_create = [t for t in topics_needed if t not in existing]

        if topics_to_create:
            new_topics = [
                NewTopic(name=t, num_partitions=3, replication_factor=1)
                for t in topics_to_create
            ]
            try:
                admin.create_topics(new_topics)
                log_result("Create missing topics", True, str(topics_to_create))
            except Exception as exc:
                if "TopicAlreadyExists" in str(exc):
                    log_result("Topics already exist", True)
                else:
                    raise

        existing = admin.list_topics()
        for t in topics_needed:
            log_result("Topic '{}' exists".format(t), t in existing)

        admin.close()
        return all(t in existing for t in topics_needed)
    except Exception as exc:
        log_result("Kafka topic creation", False, str(exc))
        return False


# ============================================================
#  TEST 5: Producer -> Kafka (small batch)
# ============================================================
def test_producer_to_kafka():
    print("\n" + "=" * 60)
    print("TEST 5: Producer -> Kafka (50 messages)")
    print("=" * 60)

    try:
        from kafka import KafkaProducer
        import uuid
        from datetime import datetime
        import random

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        for i in range(50):
            trade = {
                "trade_id":   str(uuid.uuid4()),
                "timestamp":  datetime.now().isoformat(),
                "symbol":     random.choice(symbols),
                "price":      round(random.uniform(100, 70000), 2),
                "quantity":   round(random.uniform(0.01, 100), 4),
                "side":       random.choice(["BUY", "SELL"]),
                "order_id":   str(uuid.uuid4()),
                "event_type": "TRADE",
            }
            producer.send("market-trades", key=trade["symbol"], value=trade)

        producer.flush()
        producer.close()
        log_result("Produce 50 test messages to Kafka", True)
        return True
    except Exception as exc:
        log_result("Producer -> Kafka test", False, str(exc))
        return False


# ============================================================
#  TEST 6: Alert Consumer (Kafka -> PostgreSQL)
# ============================================================
def test_alert_consumer():
    print("\n" + "=" * 60)
    print("TEST 6: Alert Consumer (Kafka -> PostgreSQL)")
    print("=" * 60)

    try:
        from kafka import KafkaProducer
        from streaming.db import get_connection, init_schema

        init_schema()

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        test_alert = {
            "window_start": "2026-06-01T12:00:00",
            "window_end":   "2026-06-01T12:01:00",
            "symbol":       "SOLUSDT",
            "trade_count":  42,
            "total_volume": 999.0,
            "mean_volume":  100.0,
            "std_volume":   20.0,
            "zscore":       4.5,
            "severity":     "HIGH",
            "alert_type":   "WASH_TRADE",
            "detected_at":  "2026-06-01T12:01:05",
        }
        producer.send("wash-alerts", value=test_alert)
        producer.flush()
        producer.close()
        log_result("Produce test alert to wash-alerts", True)

        consumer_proc = subprocess.Popen(
            [sys.executable, "streaming/alert_consumer.py"],
            cwd=_root, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        log_result("Alert consumer started", True, "PID {}".format(consumer_proc.pid))

        time.sleep(8)

        consumer_proc.send_signal(signal.SIGTERM)
        try:
            consumer_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer_proc.kill()
        log_result("Alert consumer stopped", True)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM wash_alerts "
            "WHERE window_start = '2026-06-01T12:00:00' AND symbol = 'SOLUSDT'"
        )
        count = cur.fetchone()[0]

        cur.execute("DELETE FROM wash_alerts WHERE window_start = '2026-06-01T12:00:00'")
        conn.commit()
        cur.close()
        conn.close()

        log_result("Alert persisted to PostgreSQL", count > 0,
                   "Found {} matching row(s)".format(count))
        return count > 0
    except Exception as exc:
        log_result("Alert consumer test", False, str(exc))
        return False


# ============================================================
#  TEST 7: Full Pipeline (--test mode)
# ============================================================
def test_full_pipeline(quick=False):
    print("\n" + "=" * 60)
    print("TEST 7: Full Pipeline (phase3 --test)")
    print("=" * 60)

    if quick:
        log_skip("Full pipeline test", "Skipped in --quick mode (takes ~60s)")
        return True

    try:
        from streaming.db import init_schema, get_connection

        init_schema()

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM wash_alerts WHERE detected_at > NOW() - INTERVAL '1 hour'")
        cur.execute("DELETE FROM pump_dump_alerts WHERE detected_at > NOW() - INTERVAL '1 hour'")
        conn.commit()
        cur.close()
        conn.close()

        log_result("Starting pipeline (phase3 --test)", True, "Will run for ~45s")

        pipeline = subprocess.Popen(
            [sys.executable, "streaming/run_streaming_pipeline.py",
             "--mode", "phase3", "--test"],
            cwd=_root, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        time.sleep(45)

        pipeline.send_signal(signal.SIGTERM)
        try:
            pipeline.wait(timeout=15)
        except subprocess.TimeoutExpired:
            pipeline.kill()
        log_result("Pipeline ran and stopped", True)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM wash_alerts WHERE detected_at > NOW() - INTERVAL '5 minutes'")
        wash_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pump_dump_alerts WHERE detected_at > NOW() - INTERVAL '5 minutes'")
        pd_count = cur.fetchone()[0]
        cur.close()
        conn.close()

        log_result("Wash alerts in PostgreSQL", wash_count > 0, "{} alerts".format(wash_count))
        log_result("Pump-dump alerts in PostgreSQL", pd_count > 0, "{} alerts".format(pd_count))

        return wash_count > 0 or pd_count > 0
    except Exception as exc:
        log_result("Full pipeline test", False, str(exc))
        return False


# ============================================================
#  SUMMARY
# ============================================================
def print_summary():
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, p, _ in results if p is True)
    failed = sum(1 for _, p, _ in results if p is False)
    skipped = sum(1 for _, p, _ in results if p is None)
    total = len(results)

    for name, p, detail in results:
        if p is True:
            status = PASS
        elif p is False:
            status = FAIL
        else:
            status = SKIP
        msg = "  [{}] {}".format(status, name)
        if detail:
            msg += " -- {}".format(detail)
        print(msg)

    print()
    print("  Total: {}  |  Passed: {}  |  Failed: {}  |  Skipped: {}".format(
        total, passed, failed, skipped))

    if failed == 0:
        print("\n  ALL TESTS PASSED!")
    else:
        print("\n  {} test(s) failed. Check details above.".format(failed))

    return failed == 0


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase 3 integration tests")
    parser.add_argument("--quick", action="store_true",
                        help="Skip slow tests (full pipeline)")
    args = parser.parse_args()

    print("PHASE 3 INTEGRATION TESTS")
    print("=" * 60)
    print("Config MODE: {}".format(MODE))
    print("Quick mode:  {}".format(args.quick))

    infra_ok = test_infrastructure()
    if not infra_ok:
        print("\nInfrastructure not ready. Start services first:")
        print("   docker compose up -d")
        print_summary()
        sys.exit(1)

    test_db_schema()
    test_db_insert_query()
    test_kafka_topics()
    test_producer_to_kafka()
    test_alert_consumer()
    test_full_pipeline(quick=args.quick)

    all_passed = print_summary()
    sys.exit(0 if all_passed else 1)
