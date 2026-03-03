import csv
import uuid
import random
import time
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
        "trade_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),
        "symbol": symbol,
        "price": round(START_PRICE[symbol] + random.gauss(0, 5), 2),
        "quantity": random.randint(1, 50),
        "side": random.choice(["BUY", "SELL"]),
        "trader_id": random.choice(TRADERS),
        "order_id": str(uuid.uuid4()),
        "event_type": "TRADE"
    }

def wash_trade(ts, symbol):
    trader = random.choice(TRADERS)
    price = round(START_PRICE[symbol] + random.gauss(0, 2), 2)
    qty = random.randint(10, 30)

    return [
        {
            "trade_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "symbol": symbol,
            "price": price,
            "quantity": qty,
            "side": "BUY",
            "trader_id": trader,
            "order_id": str(uuid.uuid4()),
            "event_type": "WASH"
        },
        {
            "trade_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "symbol": symbol,
            "price": price,
            "quantity": qty,
            "side": "SELL",
            "trader_id": trader,
            "order_id": str(uuid.uuid4()),
            "event_type": "WASH"
        }
    ]

def pump_and_dump(ts, symbol):
    trades = []
    base_price = START_PRICE[symbol]

    # Pump phase
    for _ in range(20):
        trades.append({
            "trade_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "symbol": symbol,
            "price": round(base_price + random.uniform(10, 30), 2),
            "quantity": random.randint(50, 120),
            "side": "BUY",
            "trader_id": random.choice(TRADERS),
            "order_id": str(uuid.uuid4()),
            "event_type": "PUMP"
        })

    # Dump phase
    for _ in range(15):
        trades.append({
            "trade_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "symbol": symbol,
            "price": round(base_price - random.uniform(5, 15), 2),
            "quantity": random.randint(60, 150),
            "side": "SELL",
            "trader_id": random.choice(TRADERS),
            "order_id": str(uuid.uuid4()),
            "event_type": "DUMP"
        })

    return trades

def spoof_orders(ts, symbol):
    trader = random.choice(TRADERS)
    orders = []

    for _ in range(10):
        orders.append({
            "trade_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "symbol": symbol,
            "price": round(START_PRICE[symbol] + random.uniform(20, 40), 2),
            "quantity": random.randint(200, 400),
            "side": "BUY",
            "trader_id": trader,
            "order_id": str(uuid.uuid4()),
            "event_type": "CANCELLED"
        })

    return orders

# ---------------- MAIN ----------------
def generate_trades():
    """Generate synthetic trade data with injected abuse patterns."""
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trade_id", "timestamp", "symbol",
                "price", "quantity", "side",
                "trader_id", "order_id", "event_type"
            ]
        )
        writer.writeheader()

        current_time = START_TIME

        for i in range(NUM_TRADES):
            current_time += timedelta(milliseconds=random.randint(10, 100))

            symbol = random.choice(SYMBOLS)

            # Inject abuse patterns occasionally
            r = random.random()

            if r < 0.02:
                for t in wash_trade(current_time, symbol):
                    writer.writerow(t)
            elif r < 0.04:
                for t in pump_and_dump(current_time, symbol):
                    writer.writerow(t)
            elif r < 0.06:
                for t in spoof_orders(current_time, symbol):
                    writer.writerow(t)
            else:
                writer.writerow(normal_trade(current_time, symbol))

    print(f"Generated {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_trades()