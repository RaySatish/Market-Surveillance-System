"""
SYNTHETIC TRADE DATA GENERATOR
================================
Generates synthetic trade data for DEVELOPMENT and TESTING only.

This is NOT used in production — real data comes from fetch_binance.py.

The synthetic data mimics the schema that Binance aggTrades produces,
with injected abuse patterns for testing the detectors:
  - ~2% wash trades  (BUY + SELL same trader, same price, same time)
  - ~2% pump & dump  (price spike up then crash down)

NOTE: Spoofing (CANCELLED events) has been removed.
  Spoofing detection has been dropped from the pipeline because Binance
  public aggTrades API never exposes CANCELLED order events.
  Keeping synthetic CANCELLED events would test a detector that doesn't exist.

Schema produced (matches fetch_binance.py output):
  trade_id, timestamp, symbol, price, quantity, side, order_id, event_type
  trader_id is included here (synthetic only) — absent in real Binance data.
"""

import csv
import uuid
import random
import os
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
NUM_TRADES = 200_000        # increase for GB-scale
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
TRADERS = [f"T{str(i).zfill(4)}" for i in range(1, 501)]
START_PRICE = {
    "BTCUSDT": 42000,
    "ETHUSDT": 2300,
    "SOLUSDT": 95
}
# Output goes to project root (one level up from ingestion/)
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "trades.csv")
START_TIME = datetime.now() - timedelta(hours=1)
# ----------------------------------------


def normal_trade(ts, symbol):
    return {
        "trade_id":   str(uuid.uuid4()),
        "timestamp":  ts.isoformat(),
        "symbol":     symbol,
        "price":      round(START_PRICE[symbol] + random.gauss(0, 5), 2),
        "quantity":   random.randint(1, 50),
        "side":       random.choice(["BUY", "SELL"]),
        "trader_id":  random.choice(TRADERS),   # synthetic only — absent in real data
        "order_id":   str(uuid.uuid4()),
        "event_type": "TRADE",
    }


def wash_trade(ts, symbol):
    """
    Wash trade: same trader, same price, same time — both BUY and SELL.
    Creates artificial volume without any real economic activity.
    """
    trader = random.choice(TRADERS)
    price  = round(START_PRICE[symbol] + random.gauss(0, 2), 2)
    qty    = random.randint(10, 30)

    return [
        {
            "trade_id":   str(uuid.uuid4()),
            "timestamp":  ts.isoformat(),
            "symbol":     symbol,
            "price":      price,
            "quantity":   qty,
            "side":       "BUY",
            "trader_id":  trader,
            "order_id":   str(uuid.uuid4()),
            "event_type": "WASH",
        },
        {
            "trade_id":   str(uuid.uuid4()),
            "timestamp":  ts.isoformat(),
            "symbol":     symbol,
            "price":      price,
            "quantity":   qty,
            "side":       "SELL",
            "trader_id":  trader,
            "order_id":   str(uuid.uuid4()),
            "event_type": "WASH",
        },
    ]


def pump_and_dump(ts, symbol):
    """
    Pump & dump: coordinated buying to spike price (PUMP),
    followed by heavy selling to crash it (DUMP).
    """
    trades     = []
    base_price = START_PRICE[symbol]

    # Pump phase — aggressive buying pushes price up
    for _ in range(20):
        trades.append({
            "trade_id":   str(uuid.uuid4()),
            "timestamp":  ts.isoformat(),
            "symbol":     symbol,
            "price":      round(base_price + random.uniform(10, 30), 2),
            "quantity":   random.randint(50, 120),
            "side":       "BUY",
            "trader_id":  random.choice(TRADERS),
            "order_id":   str(uuid.uuid4()),
            "event_type": "PUMP",
        })

    # Dump phase — heavy selling crashes the price
    for _ in range(15):
        trades.append({
            "trade_id":   str(uuid.uuid4()),
            "timestamp":  ts.isoformat(),
            "symbol":     symbol,
            "price":      round(base_price - random.uniform(5, 15), 2),
            "quantity":   random.randint(60, 150),
            "side":       "SELL",
            "trader_id":  random.choice(TRADERS),
            "order_id":   str(uuid.uuid4()),
            "event_type": "DUMP",
        })

    return trades


# ---------------- MAIN ----------------
def generate_trades():
    """
    Generate synthetic trade data with injected abuse patterns.

    Abuse injection rates:
      ~2% wash trades
      ~2% pump & dump
      ~96% normal trades
    """
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trade_id", "timestamp", "symbol",
                "price", "quantity", "side",
                "trader_id", "order_id", "event_type",
            ]
        )
        writer.writeheader()

        current_time = START_TIME

        for i in range(NUM_TRADES):
            current_time += timedelta(milliseconds=random.randint(10, 100))

            symbol = random.choice(SYMBOLS)
            r      = random.random()

            if r < 0.02:
                # Inject wash trades (~2%)
                for t in wash_trade(current_time, symbol):
                    writer.writerow(t)
            elif r < 0.04:
                # Inject pump & dump (~2%)
                for t in pump_and_dump(current_time, symbol):
                    writer.writerow(t)
            else:
                # Normal trade (~96%)
                writer.writerow(normal_trade(current_time, symbol))

    print(f"Generated {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_trades()
