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
     a. Written to a CSV buffer (batch file)
     b. When buffer reaches BATCH_SIZE, flush to disk
     c. The ETL pipeline picks up new files and processes them

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


# ============================================================
#  CONFIGURATION
# ============================================================
BATCH_SIZE = 1000          # Flush to disk every N trades
OUTPUT_DIR = "data/streaming"  # Where batch files land

# Binance WebSocket trade message format (what you receive):
# {
#   "e": "trade",        ← event type
#   "s": "BTCUSDT",      ← symbol
#   "p": "42000.50",     ← price
#   "q": "0.5",          ← quantity
#   "b": 12345,          ← buyer order ID
#   "a": 67890,          ← seller order ID
#   "T": 1672531200000,  ← trade time (ms since epoch)
#   "m": true            ← is buyer the market maker?
# }


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
        "side": "SELL" if msg["m"] else "BUY",  # m=true means buyer is maker → taker sold
        "trader_id": f"B{msg['b']}",  # Use buyer order ID as pseudo-trader
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

    print(f"  Flushed batch {batch_num}: {len(batch)} trades → {filename}")
    return filename


# ============================================================
#  SIMULATED STREAM (Phase 1 — no Binance needed)
# ============================================================
def run_simulated_stream(num_batches=5):
    """
    Simulate a Binance-like stream using synthetic data.
    Useful for testing the pipeline without internet/API access.
    """
    print("SIMULATED STREAM MODE")
    print(f"  Generating {num_batches} batches of {BATCH_SIZE} trades each")
    print(f"  Output: {OUTPUT_DIR}/")
    print()

    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    base_prices = {"BTCUSDT": 42000, "ETHUSDT": 2300, "SOLUSDT": 95}
    batch = []
    batch_num = 0

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
        batch.append(trade)

        if len(batch) >= BATCH_SIZE:
            flush_batch(batch, batch_num)
            batch = []
            batch_num += 1

            # Simulate real-time delay between batches
            time.sleep(0.5)

    # Flush remaining
    if batch:
        flush_batch(batch, batch_num)

    print(f"\nSimulation complete: {total_trades} trades in {batch_num + 1} batches")


# ============================================================
#  LIVE STREAM (Phase 2 — real Binance WebSocket)
# ============================================================
def run_live_stream():
    """
    Connect to the real Binance WebSocket API and stream live trades.
    Requires: pip install websocket-client
    """
    try:
        import websocket
    except ImportError:
        print("ERROR: Install websocket-client first:")
        print("  pip install websocket-client")
        return

    cfg = get_config()
    symbols = cfg.get("binance_symbols", ["btcusdt@trade", "ethusdt@trade", "solusdt@trade"])
    ws_url = cfg.get("binance_ws_url", "wss://stream.binance.com:9443/ws")

    # Combine multiple streams into one connection
    stream_url = f"{ws_url}/{'/'.join(symbols)}"

    print("LIVE STREAM MODE")
    print(f"  Connecting to: {stream_url}")
    print(f"  Symbols: {symbols}")
    print(f"  Batch size: {BATCH_SIZE}")
    print()

    batch = []
    batch_num = 0

    def on_message(ws, message):
        nonlocal batch, batch_num

        msg = json.loads(message)
        trade = parse_binance_trade(msg)
        batch.append(trade)

        if len(batch) >= BATCH_SIZE:
            flush_batch(batch, batch_num)
            batch = []
            batch_num += 1

    def on_error(ws, error):
        print(f"WebSocket error: {error}")

    def on_close(ws, close_status, close_msg):
        print(f"WebSocket closed: {close_status} {close_msg}")
        # Flush remaining trades
        if batch:
            flush_batch(batch, batch_num)

    def on_open(ws):
        print("Connected to Binance WebSocket!")
        print("Streaming trades... (Ctrl+C to stop)\n")

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
        print(f"\nStopped. Total batches: {batch_num}")
        if batch:
            flush_batch(batch, batch_num)


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
