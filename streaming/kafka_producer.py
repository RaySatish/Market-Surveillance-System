"""
KAFKA PRODUCER — MARKET TRADES
================================
Binance WebSocket → Kafka topic (Docker KRaft broker)

What this does:
  1. Connects to the Binance WebSocket stream for BTC, ETH, SOL
  2. Parses each incoming trade event into our standard schema
  3. Validates every row (row-level quality gate)
  4. Serialises valid trades as JSON and produces them to the
     'market-trades' Kafka topic
  5. Invalid rows go to the dead-letter queue

REST backfill on reconnect:
  - Tracks last_trade_timestamp from WebSocket stream
  - On reconnect, if gap > GAP_THRESHOLD seconds:
    - Calls Binance REST API for the gap window
    - Deduplicates on trade_id before publishing to Kafka
    - Rate-limit-aware: reads X-MBX-USED-WEIGHT-1M header
  - Handles HTTP 429 (Retry-After) and HTTP 418 (IP ban, 5 min sleep)

Fault tolerance:
  - Auto-reconnect with exponential back-off if WebSocket drops
  - REST backfill fills any data gap on reconnect
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
import threading
from datetime import datetime, timezone, timedelta

import requests

# ── Project root path fix ──────────────────────────────────────────────
import sys as _sys, os as _os
_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _root not in _sys.path:
    _sys.path.insert(0, _root)
# ───────────────────────────────────────────────────────────────────────
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

# REST backfill settings
BINANCE_REST_BASE      = "https://api.binance.com"
AGG_TRADES_ENDPOINT    = "/api/v3/aggTrades"
REST_MAX_LIMIT         = 1000          # Binance hard cap per request
REST_REQUEST_PAUSE     = 0.2           # seconds between paginated requests
GAP_THRESHOLD          = 5.0           # seconds — backfill if gap > this
RATE_LIMIT_THRESHOLD   = 850           # back off at 85% of 1200 weight/min
IP_BAN_SLEEP           = 300           # 5 minutes on HTTP 418


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
#  REST BACKFILL (fills gaps on WebSocket reconnect)
# ============================================================
def _rest_fetch_page(symbol: str, start_ms: int, end_ms: int,
                     from_id: int = None) -> tuple:
    """
    Fetch one page of aggTrades from Binance REST API.
    Returns (trades_list, used_weight) tuple.
    Handles HTTP 429 (rate limit) and HTTP 418 (IP ban).
    """
    params = {"symbol": symbol, "limit": REST_MAX_LIMIT}

    if from_id is not None:
        params["fromId"] = from_id
    else:
        params["startTime"] = start_ms
        params["endTime"]   = end_ms

    resp = requests.get(
        BINANCE_REST_BASE + AGG_TRADES_ENDPOINT,
        params=params,
        timeout=10,
    )

    # Read rate-limit weight from response headers
    used_weight = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", "0"))

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "60"))
        log.warning("REST rate limited (429). Sleeping %d seconds.", retry_after)
        time.sleep(retry_after)
        return [], used_weight

    if resp.status_code == 418:
        log.error("REST IP ban (418). Sleeping %d seconds.", IP_BAN_SLEEP)
        time.sleep(IP_BAN_SLEEP)
        return [], used_weight

    resp.raise_for_status()
    return resp.json(), used_weight


def _map_rest_trade(raw: dict, symbol: str) -> dict:
    """Map a Binance REST aggTrade record to our pipeline schema."""
    ts_ms = int(raw["T"])
    ts_iso = datetime.fromtimestamp(
        ts_ms / 1000.0, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%S")

    trade_id = str(raw["a"])
    return {
        "trade_id":   trade_id,
        "timestamp":  ts_iso,
        "symbol":     symbol,
        "price":      float(raw["p"]),
        "quantity":   float(raw["q"]),
        "side":       "BUY" if raw["m"] else "SELL",
        "order_id":   trade_id,
        "event_type": "TRADE",
    }


def _backfill_gap(producer, topic: str, gap_start_ms: int, gap_end_ms: int,
                  seen_ids: set) -> int:
    """
    Fetch trades from Binance REST API for the gap window and publish
    to Kafka, deduplicating against already-seen trade IDs.

    Returns the number of backfilled trades published.
    """
    gap_seconds = (gap_end_ms - gap_start_ms) / 1000.0
    log.info("REST BACKFILL: gap of %.1f seconds (%s → %s)",
             gap_seconds,
             datetime.fromtimestamp(gap_start_ms / 1000, tz=timezone.utc).isoformat(),
             datetime.fromtimestamp(gap_end_ms / 1000, tz=timezone.utc).isoformat())

    total_backfilled = 0

    for symbol in SYMBOLS:
        from_id = None
        symbol_count = 0

        while True:
            try:
                trades, used_weight = _rest_fetch_page(
                    symbol, gap_start_ms, gap_end_ms, from_id=from_id
                )
            except Exception as exc:
                log.error("[%s] REST backfill page failed: %s", symbol, exc)
                break

            if not trades:
                break

            # Rate-limit awareness: back off at 85% capacity
            if used_weight > RATE_LIMIT_THRESHOLD:
                pause = max(2.0, REST_REQUEST_PAUSE * 5)
                log.warning("REST weight at %d/%d — throttling %.1fs",
                            used_weight, 1200, pause)
                time.sleep(pause)

            # Filter to time window when paginating by ID
            if from_id is not None:
                trades = [t for t in trades
                          if gap_start_ms <= int(t["T"]) <= gap_end_ms]
                if not trades:
                    break

            for raw in trades:
                agg_id = str(raw["a"])
                if agg_id in seen_ids:
                    continue  # deduplicate

                mapped = _map_rest_trade(raw, symbol)
                ok, reason = validate_trade(mapped)
                if ok:
                    producer.send(topic, key=mapped["symbol"], value=mapped)
                    seen_ids.add(agg_id)
                    symbol_count += 1
                else:
                    write_dead_letter(mapped, reason)

            # Pagination
            last_id = int(trades[-1]["a"])
            last_ts = int(trades[-1]["T"])

            if last_ts >= gap_end_ms or len(trades) < REST_MAX_LIMIT:
                break

            from_id = last_id + 1
            time.sleep(REST_REQUEST_PAUSE)

        log.info("[%s] Backfilled %d trades", symbol, symbol_count)
        total_backfilled += symbol_count

    producer.flush()
    log.info("REST BACKFILL complete: %d total trades published", total_backfilled)
    return total_backfilled


# ============================================================
#  LIVE STREAM (real Binance WebSocket → Kafka)
# ============================================================
def run_live(topic: str = None):
    """
    Connect to the Binance WebSocket and stream live trades into Kafka.
    On reconnect, backfills any gap via REST API.
    Requires: pip install websocket-client kafka-python requests
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
    stats    = {"produced": 0, "rejected": 0, "errors": 0, "backfilled": 0}
    reconnect_attempts = 0
    reconnect_delay    = RECONNECT_BASE_DELAY

    # Track last trade timestamp for gap detection
    last_trade_ts_ms = [0]  # mutable container for closure access
    seen_ids = set()        # dedup set (bounded — cleared periodically)
    _id_clear_lock = threading.Lock()

    def _maybe_clear_seen_ids():
        """Keep seen_ids bounded — clear if > 100k entries."""
        with _id_clear_lock:
            if len(seen_ids) > 100_000:
                seen_ids.clear()
                log.debug("Cleared seen_ids set (was > 100k entries)")

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

                    # Track timestamp for gap detection
                    trade_ts_ms = int(msg.get("T", 0))
                    if trade_ts_ms > last_trade_ts_ms[0]:
                        last_trade_ts_ms[0] = trade_ts_ms

                    # Track aggregate trade ID for dedup
                    agg_id = str(msg.get("a", ""))
                    if agg_id:
                        seen_ids.add(agg_id)

                    if stats["produced"] % 500 == 0:
                        log.info("Produced %d trades so far", stats["produced"])
                        _maybe_clear_seen_ids()
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

                # ── REST BACKFILL on reconnect ──────────────────────
                if last_trade_ts_ms[0] > 0:
                    now_ms = int(time.time() * 1000)
                    gap_seconds = (now_ms - last_trade_ts_ms[0]) / 1000.0
                    if gap_seconds > GAP_THRESHOLD:
                        try:
                            backfilled = _backfill_gap(
                                producer, topic,
                                gap_start_ms=last_trade_ts_ms[0],
                                gap_end_ms=now_ms,
                                seen_ids=seen_ids,
                            )
                            stats["backfilled"] += backfilled
                            log.info("Backfill stats: %d trades recovered for %.1fs gap",
                                     backfilled, gap_seconds)
                        except Exception as exc:
                            log.error("REST backfill failed: %s — continuing without backfill", exc)
                # ────────────────────────────────────────────────────

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

    # ── Watermark-flush sentinel messages ─────────────────────────────
    # Spark Structured Streaming only closes a window when it sees a message
    # with event_time > window_end + watermark.  After the main batch finishes,
    # no more messages arrive, so windows never close.  We send a small burst of
    # sentinel trades timestamped 5 minutes in the future to advance the
    # watermark and force all pending windows to emit results.
    log.info("Sending watermark-flush sentinels (t+5min) to close pending windows...")
    future_ts = (datetime.now() + timedelta(minutes=5)).isoformat()
    symbols   = list(base_prices.keys())
    for _ in range(10):                        # 10 per symbol is enough
        for sym in symbols:
            sentinel = {
                "trade_id":   str(uuid.uuid4()),
                "timestamp":  future_ts,
                "symbol":     sym,
                "price":      round(base_prices[sym], 2),
                "quantity":   1.0,
                "side":       "BUY",
                "order_id":   str(uuid.uuid4()),
                "event_type": "TRADE",
            }
            producer.send(topic, key=sentinel["symbol"], value=sentinel)
    producer.flush()
    log.info("Watermark-flush sentinels sent (%d messages).", 10 * len(symbols))
    # ──────────────────────────────────────────────────────────────────

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
