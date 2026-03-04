"""
BINANCE REAL-TIME TRADE INGESTION (Phase 2)
=============================================
This script connects to the Binance WebSocket API and streams
LIVE trades into the pipeline.

What is a WebSocket?
  Unlike HTTP (request → response), a WebSocket keeps a persistent connection
  open. Binance pushes every trade to you the INSTANT it happens.
  No polling, no delays — true real-time.

How it works:
  1. Connect to Binance WebSocket (wss://stream.binance.com)
  2. Subscribe to trade streams for BTC, ETH, SOL
  3. Each incoming trade is:
     a. Validated (row-level data quality check).
     b. Written to a CSV buffer (batch file)
     c. When buffer reaches BATCH_SIZE, flush to disk
     d. The ETL pipeline picks up new files and processes them

Fault tolerance:
  - Auto-reconnect with exponential back-off if the WebSocket drops.
  - Row-level validation; invalid messages go to the dead-letter queue.
  - Structured logging (rotating file + console).

Phase 2 (AWS) flow:
  Binance WebSocket → This script → Kafka/Kinesis → Spark Streaming → S3 → Detectors

Current (Phase 1) usage:
  python stream_binance.py --test
  This runs a SIMULATED stream using synthetic data (no Binance API needed).

Production usage:
  python stream_binance.py --live
  Connects to real Binance API (requires internet, no API key needed for public trades).

NOTE: pip install websocket-client  (needed for production mode)
"""

import json
import csv
import uuid
import time
import random
import os
import argparse
from datetime import datetime

from config import get_config, MODE
from utils.fault_tolerance import get_logger, validate_trade, write_dead_letter

log = get_logger("stream_binance")

# ============================================================
#  CONFIGURATION
# ============================================================
BATCH_SIZE = 1000          # Flush to disk every N trades
OUTPUT_DIR = "data/streaming"  # Where batch files land

# Auto-reconnect settings
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_BASE_DELAY = 1.0  # seconds
RECONNECT_BACKOFF = 2.0     # multiplier


def parse_binance_trade(msg):
    """
    Convert a Binance WebSocket trade message into our standard format.
    This normalizes Binance's format to match our pipeline schema.
    """
    return {
        "trade_id": str(uuid.uuid4()),
        "timestamp": datetime.fromtimestamp(msg["T"] / 1000).isoformat(),
        "symbol": msg["s"],
        "price": float(msg["p"]),
        "quantity": int(float(msg["q"])),
        "side": "SELL" if msg["m"] else "BUY",
        "trader_id": f"B{msg['b']}",
        "order_id": str(msg.get("a", uuid.uuid4())),
        "event_type": "TRADE"
    }


def flush_batch(batch, batch_num):
    """
    Write a batch of trades to a CSV file.
    The ETL pipeline will pick up these files.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"batch_{batch_num:06d}.csv")

    fieldnames = [
        "trade_id", "timestamp", "symbol",
        "price", "quantity", "side",
        "trader_id", "order_id", "event_type"
    ]

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(batch)

    log.info("Flushed batch %d: %d trades → %s", batch_num, len(batch), filename)
    return filename


# ============================================================
#  SIMULATED STREAM (Phase 1 — no Binance needed)
# ============================================================
def run_simulated_stream(num_batches=5):
    """
    Simulate a Binance-like stream using synthetic data.
    Useful for testing the pipeline without internet/API access.
    """
    log.info("SIMULATED STREAM MODE")
    log.info("Generating %d batches of %d trades each", num_batches, BATCH_SIZE)
    log.info("Output: %s/", OUTPUT_DIR)

    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    base_prices = {"BTCUSDT": 42000, "ETHUSDT": 2300, "SOLUSDT": 95}
    batch = []
    batch_num = 0
    rejected = 0

    total_trades = num_batches * BATCH_SIZE

    for i in range(total_trades):
        symbol = random.choice(symbols)
        trade = {
            "trade_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "price": round(base_prices[symbol] + random.gauss(0, 5), 2),
            "quantity": random.randint(1, 50),
            "side": random.choice(["BUY", "SELL"]),
            "trader_id": f"T{random.randint(1, 500):04d}",
            "order_id": str(uuid.uuid4()),
            "event_type": "TRADE"
        }

        # Validate before accepting
        ok, reason = validate_trade(trade)
        if ok:
            batch.append(trade)
        else:
            write_dead_letter(trade, reason)
            rejected += 1

        if len(batch) >= BATCH_SIZE:
            flush_batch(batch, batch_num)
            batch = []
            batch_num += 1
            time.sleep(0.5)

    if batch:
        flush_batch(batch, batch_num)

    log.info("Simulation complete: %d trades in %d batches (%d rejected → DLQ)",
             total_trades, batch_num + 1, rejected)


# ============================================================
#  LIVE STREAM (Phase 2 — real Binance WebSocket)
# ============================================================
def run_live_stream():
    """
    Connect to the real Binance WebSocket API and stream live trades.
    Requires: pip install websocket-client

    Fault tolerance:
      - Auto-reconnects up to MAX_RECONNECT_ATTEMPTS with exponential back-off.
      - Invalid messages are logged and sent to the dead-letter queue.
    """
    try:
        import websocket
    except ImportError:
        log.error("Install websocket-client first:  pip install websocket-client")
        return

    cfg = get_config()
    symbols = cfg.get("binance_symbols", ["btcusdt@trade", "ethusdt@trade", "solusdt@trade"])
    ws_url = cfg.get("binance_ws_url", "wss://stream.binance.com:9443/ws")
    stream_url = f"{ws_url}/{'/'.join(symbols)}"

    log.info("LIVE STREAM MODE")
    log.info("Connecting to: %s", stream_url)
    log.info("Symbols: %s", symbols)
    log.info("Batch size: %d", BATCH_SIZE)

    batch = []
    batch_num = 0
    reconnect_attempts = 0
    reconnect_delay = RECONNECT_BASE_DELAY

    def _connect():
        nonlocal reconnect_attempts, reconnect_delay

        def on_message(ws, message):
            nonlocal batch, batch_num
            try:
                msg = json.loads(message)
                trade = parse_binance_trade(msg)

                ok, reason = validate_trade(trade)
                if ok:
                    batch.append(trade)
                else:
                    write_dead_letter(trade, reason)
                    log.warning("Invalid trade rejected: %s", reason)

                if len(batch) >= BATCH_SIZE:
                    flush_batch(batch, batch_num)
                    batch = []
                    batch_num += 1
            except (KeyError, ValueError, TypeError) as exc:
                log.error("Malformed WebSocket message: %s — %s", exc, message[:200])
                write_dead_letter({"raw_message": message[:500]}, f"parse_error:{exc}")

        def on_error(ws, error):
            log.error("WebSocket error: %s", error)

        def on_close(ws, close_status, close_msg):
            nonlocal reconnect_attempts, reconnect_delay
            log.warning("WebSocket closed: %s %s", close_status, close_msg)
            if batch:
                flush_batch(batch, batch_num)

            # ---- AUTO-RECONNECT with exponential back-off ----
            if reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
                reconnect_attempts += 1
                log.warning("Reconnecting in %.1fs… (attempt %d/%d)",
                            reconnect_delay, reconnect_attempts, MAX_RECONNECT_ATTEMPTS)
                time.sleep(reconnect_delay)
                reconnect_delay *= RECONNECT_BACKOFF
                _connect()  # recursive reconnect
            else:
                log.error("Max reconnect attempts (%d) reached — giving up.",
                          MAX_RECONNECT_ATTEMPTS)

        def on_open(ws):
            nonlocal reconnect_attempts, reconnect_delay
            reconnect_attempts = 0          # reset on successful connect
            reconnect_delay = RECONNECT_BASE_DELAY
            log.info("Connected to Binance WebSocket!")
            log.info("Streaming trades… (Ctrl+C to stop)")

        ws = websocket.WebSocketApp(
            stream_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        try:
            ws.run_forever()
        except KeyboardInterrupt:
            log.info("Stopped by user. Total batches: %d", batch_num)
            if batch:
                flush_batch(batch, batch_num)

    _connect()


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream trades from Binance")
    parser.add_argument(
        "--live", action="store_true",
        help="Connect to real Binance WebSocket API"
    )
    parser.add_argument(
        "--test", action="store_true", default=True,
        help="Run simulated stream (default)"
    )
    parser.add_argument(
        "--batches", type=int, default=5,
        help="Number of batches for simulated mode (default: 5)"
    )
    args = parser.parse_args()

    if args.live:
        run_live_stream()
    else:
        run_simulated_stream(num_batches=args.batches)
