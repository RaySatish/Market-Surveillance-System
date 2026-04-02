"""
KAFKA PRODUCER — MARKET TRADES
================================
Phase 2: Binance WebSocket → Kafka topic (Docker KRaft broker)

What this does:
  1. Connects to the Binance WebSocket stream for BTC, ETH, SOL
  2. Parses each incoming trade event into our standard schema
  3. Validates every row (row-level quality gate)
  4. Serialises valid trades as JSON and produces them to the
     'market-trades' Kafka topic
  5. Invalid rows go to the dead-letter queue

Fault tolerance:
  - Auto-reconnect with exponential back-off if WebSocket drops
  - Kafka producer retries (kafka-python built-in)
  - Row-level validation with dead-letter queue
  - Structured logging

Usage:
  # Start Kafka first:
  docker compose up -d

  # Then run the producer:
  python streaming/kafka_producer.py --live     # real Binance WebSocket
  python streaming/kafka_producer.py --test     # synthetic data (no internet needed)
  python streaming/kafka_producer.py --test --messages 5000
"""

import json
import uuid
import time
import random
import argparse
from datetime import datetime

from config import get_config
from utils.fault_tolerance import get_logger, validate_trade, write_dead_letter

log = get_logger("kafka_producer")

# ============================================================
#  CONFIGURATION
# ============================================================
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# WebSocket reconnect settings
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_BASE_DELAY   = 1.0   # seconds
RECONNECT_BACKOFF      = 2.0   # multiplier per failed attempt


def _make_producer():
    """
    Create and return a KafkaProducer configured to talk to the Docker broker.
    JSON serialisation — every message is a UTF-8 JSON string.
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        log.error("kafka-python not installed. Run: pip install kafka-python")
        raise

    cfg = get_config()
    bootstrap = cfg.get("kafka_bootstrap", "localhost:9092")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Reliability settings
        acks="all",           # wait for all in-sync replicas to ack
        retries=5,
        retry_backoff_ms=300,
        # Throughput settings
        batch_size=16384,     # 16 KB batches
        linger_ms=10,         # wait up to 10ms to fill a batch
        compression_type="gzip",
    )
    log.info("KafkaProducer connected to %s", bootstrap)
    return producer


def _parse_binance_trade(msg: dict) -> dict:
    """
    Map a raw Binance WebSocket trade event → our pipeline schema.

    Binance aggTrade fields:
      T = trade time (ms epoch)
      s = symbol
      p = price
      q = quantity
      m = is_buyer_maker (True → buyer is market maker → SELL side)
      a = aggregate trade ID
    """
    return {
        "trade_id":   str(uuid.uuid4()),
        "timestamp":  datetime.fromtimestamp(msg["T"] / 1000).isoformat(),
        "symbol":     msg["s"],
        "price":      float(msg["p"]),
        "quantity":   float(msg["q"]),
        "side":       "SELL" if msg["m"] else "BUY",
        "order_id":   str(msg.get("a", uuid.uuid4())),
        "event_type": "TRADE",
    }


# ============================================================
#  LIVE STREAM (real Binance WebSocket → Kafka)
# ============================================================
def run_live(topic: str = None):
    """
    Connect to the Binance WebSocket and stream live trades into Kafka.
    Requires: pip install websocket-client kafka-python
    """
    try:
        import websocket
    except ImportError:
        log.error("websocket-client not installed. Run: pip install websocket-client")
        return

    cfg = get_config()
    if topic is None:
        topic = cfg.get("kafka_topic", "market-trades")

    streams = "/".join(f"{s.lower()}@aggTrade" for s in SYMBOLS)
    ws_url  = f"wss://stream.binance.com:9443/stream?streams={streams}"

    log.info("LIVE MODE — connecting to Binance WebSocket")
    log.info("Streams : %s", streams)
    log.info("Kafka   : %s → topic '%s'", cfg.get('kafka_bootstrap'), topic)

    producer = _make_producer()
    stats    = {"produced": 0, "rejected": 0, "errors": 0}
    reconnect_attempts = 0
    reconnect_delay    = RECONNECT_BASE_DELAY

    def _connect():
        nonlocal reconnect_attempts, reconnect_delay

        def on_message(ws, message):
            try:
                outer = json.loads(message)
                # Combined stream wraps each event in {"stream":..., "data":{...}}
                msg = outer.get("data", outer)
                trade = _parse_binance_trade(msg)

                ok, reason = validate_trade(trade)
                if ok:
                    # Use symbol as partition key for locality
                    producer.send(topic, key=trade["symbol"], value=trade)
                    stats["produced"] += 1
                    if stats["produced"] % 500 == 0:
                        log.info("Produced %d trades so far", stats["produced"])
                else:
                    write_dead_letter(trade, reason)
                    stats["rejected"] += 1
                    log.debug("Rejected trade: %s", reason)

            except (KeyError, ValueError, TypeError) as exc:
                stats["errors"] += 1
                log.error("Parse error: %s — %s", exc, message[:200])

        def on_error(ws, error):
            log.error("WebSocket error: %s", error)

        def on_close(ws, code, msg):
            nonlocal reconnect_attempts, reconnect_delay
            log.warning("WebSocket closed (code=%s). Produced: %d, Rejected: %d",
                        code, stats["produced"], stats["rejected"])
            producer.flush()

            if reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
                reconnect_attempts += 1
                log.warning("Reconnecting in %.1fs (attempt %d/%d)",
                            reconnect_delay, reconnect_attempts, MAX_RECONNECT_ATTEMPTS)
                time.sleep(reconnect_delay)
                reconnect_delay *= RECONNECT_BACKOFF
                _connect()
            else:
                log.error("Max reconnect attempts reached — giving up.")

        def on_open(ws):
            nonlocal reconnect_attempts, reconnect_delay
            reconnect_attempts = 0
            reconnect_delay    = RECONNECT_BASE_DELAY
            log.info("Connected! Streaming live trades into Kafka...")

        ws_app = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        try:
            ws_app.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            log.info("Stopped by user. Final stats: %s", stats)
            producer.flush()
            producer.close()

    _connect()


# ============================================================
#  SYNTHETIC / TEST MODE (no Binance needed)
# ============================================================
def run_test(num_messages: int = 2000, topic: str = None):
    """
    Generate synthetic trade events and produce them to Kafka.
    Useful for testing the streaming pipeline without internet access.

    Injects ~5% wash-trade-like volume spikes and ~3% price spike sequences
    to verify the streaming detectors fire.
    """
    cfg = get_config()
    if topic is None:
        topic = cfg.get("kafka_topic", "market-trades")

    log.info("TEST MODE — producing %d synthetic trades to topic '%s'", num_messages, topic)

    producer  = _make_producer()
    base_prices = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0, "SOLUSDT": 175.0}
    stats       = {"produced": 0, "rejected": 0}

    for i in range(num_messages):
        symbol = random.choice(SYMBOLS)
        base   = base_prices[symbol]

        # Inject occasional volume spike (wash-trade simulation)
        qty = random.randint(1, 10)
        if random.random() < 0.05:   # 5% chance of volume spike
            qty = random.randint(200, 500)

        # Inject occasional price spike (pump simulation)
        price_delta = random.gauss(0, base * 0.0005)  # ±0.05% normal noise
        if random.random() < 0.03:   # 3% chance of price spike
            price_delta = base * random.uniform(0.002, 0.005)  # +0.2–0.5% spike

        base_prices[symbol] = max(base + price_delta, base * 0.5)  # don't go negative

        trade = {
            "trade_id":   str(uuid.uuid4()),
            "timestamp":  datetime.now().isoformat(),
            "symbol":     symbol,
            "price":      round(base_prices[symbol], 2),
            "quantity":   float(qty),
            "side":       random.choice(["BUY", "SELL"]),
            "order_id":   str(uuid.uuid4()),
            "event_type": "TRADE",
        }

        ok, reason = validate_trade(trade)
        if ok:
            producer.send(topic, key=trade["symbol"], value=trade)
            stats["produced"] += 1
        else:
            write_dead_letter(trade, reason)
            stats["rejected"] += 1

        # Simulate ~100 trades/sec
        if i % 100 == 0:
            producer.flush()
            log.info("Progress: %d / %d messages sent", i + 1, num_messages)
            time.sleep(0.05)   # slight throttle so Spark can keep up

    producer.flush()
    producer.close()
    log.info("TEST MODE complete. Stats: %s", stats)


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka producer: Binance trades → 'market-trades' topic"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--live",  action="store_true",
                       help="Stream from real Binance WebSocket")
    group.add_argument("--test",  action="store_true", default=True,
                       help="Produce synthetic trades (default)")
    parser.add_argument("--messages", type=int, default=2000,
                        help="Number of messages in test mode (default: 2000)")
    parser.add_argument("--topic", type=str, default=None,
                        help="Kafka topic name (default: from config.py)")
    args = parser.parse_args()

    if args.live:
        run_live(topic=args.topic)
    else:
        run_test(num_messages=args.messages, topic=args.topic)
