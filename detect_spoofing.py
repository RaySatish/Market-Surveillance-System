"""
SPOOFING DETECTION
==================
What is spoofing?
  A trader places LARGE orders they never intend to execute ("bluffing").
  These fake orders trick other traders into thinking demand exists.
  The spoofer then CANCELS the orders before they fill.

  Example: A spoofer places 10 huge BUY orders → other traders see demand
  and start buying → price goes up → spoofer sells at the higher price
  → then cancels all the fake BUY orders.

How this script detects it:
  1. Load cleaned trades from Parquet (output of ETL pipeline).
  2. For each trader, calculate:
     - Total orders placed
     - How many were CANCELLED
     - Cancellation rate = cancelled / total
     - Average size of cancelled orders vs executed orders
  3. Flag traders with:
     - High cancellation rate (>50%) AND
     - Large average cancelled order size (suggests intentional spoofing,
       not just normal order changes)

Data flow:
  generate_trades.py → trades.csv → HDFS → etl_trades.py (Spark) → HDFS Parquet → THIS SCRIPT
"""

import pandas as pd
import os
from datetime import datetime

from config import get_config, DETECTION
from hdfs_utils import read_parquet_from_hdfs


def detect_spoofing(
    input_path=None,
    output_file=None,
    cancel_rate_threshold=None,
    min_orders=None,
    size_multiplier=None
):
    """
    Detect spoofing: traders who place large orders and cancel them
    at a suspiciously high rate.
    Reads from the CLEANED Parquet produced by Spark ETL.
    """
    cfg = get_config()
    if input_path is None:
        input_path = cfg["clean_output"]
    if output_file is None:
        output_file = cfg["alerts_spoofing"]
    if cancel_rate_threshold is None:
        cancel_rate_threshold = DETECTION["spoof_cancel_rate"]
    if min_orders is None:
        min_orders = DETECTION["spoof_min_orders"]
    if size_multiplier is None:
        size_multiplier = DETECTION["spoof_size_multiplier"]

    # Ensure alerts directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ---------- STEP 1: Load cleaned Parquet from HDFS ----------
    print("Loading cleaned trades from HDFS Parquet...")
    df = read_parquet_from_hdfs(input_path)
    print(f"  Total trades loaded: {len(df):,}")

    # ---------- STEP 2: Split into executed and cancelled ----------
    cancelled = df[df["event_type"] == "CANCELLED"]
    executed = df[df["event_type"].isin(["TRADE", "WASH", "PUMP", "DUMP"])]

    print(f"  Executed orders: {len(executed):,}")
    print(f"  Cancelled orders: {len(cancelled):,}")

    # ---------- STEP 3: Per-trader statistics ----------
    alerts = []

    # Get all unique traders who have at least one cancellation
    traders_with_cancels = cancelled["trader_id"].unique()

    for trader in traders_with_cancels:
        # This trader's orders
        trader_cancelled = cancelled[cancelled["trader_id"] == trader]
        trader_executed = executed[executed["trader_id"] == trader]

        total_orders = len(trader_cancelled) + len(trader_executed)

        # Skip if too few orders (not enough data to judge)
        if total_orders < min_orders:
            continue

        # ---------- STEP 4: Calculate suspicion metrics ----------

        # Cancellation rate
        cancel_rate = len(trader_cancelled) / total_orders

        # Average order size comparison
        avg_cancel_size = trader_cancelled["quantity"].mean()
        avg_exec_size = trader_executed["quantity"].mean() if len(trader_executed) > 0 else 0

        # Size ratio: how much bigger are cancelled orders?
        if avg_exec_size > 0:
            size_ratio = avg_cancel_size / avg_exec_size
        else:
            # Trader ONLY cancels, never executes → very suspicious
            size_ratio = float("inf")

        # Symbols affected
        symbols = trader_cancelled["symbol"].unique().tolist()

        # ---------- STEP 5: Apply thresholds ----------
        if cancel_rate >= cancel_rate_threshold and size_ratio >= size_multiplier:
            # Determine severity based on how extreme the metrics are
            if cancel_rate > 0.8 and size_ratio > 5:
                severity = "CRITICAL"
            elif cancel_rate > 0.6 or size_ratio > 3:
                severity = "HIGH"
            else:
                severity = "MEDIUM"

            alerts.append({
                "alert_type": "SPOOFING",
                "trader_id": trader,
                "symbols": ", ".join(symbols),
                "total_orders": total_orders,
                "cancelled_orders": len(trader_cancelled),
                "executed_orders": len(trader_executed),
                "cancel_rate": round(cancel_rate, 4),
                "avg_cancel_size": round(avg_cancel_size, 2),
                "avg_exec_size": round(avg_exec_size, 2),
                "size_ratio": round(size_ratio, 2) if size_ratio != float("inf") else "INF",
                "severity": severity,
                "detected_at": datetime.now().isoformat()
            })

    # ---------- STEP 6: Save results ----------
    alerts_df = pd.DataFrame(alerts)
    alerts_df.to_csv(output_file, index=False)

    print(f"\n  SPOOFING ALERTS: {len(alerts_df)}")
    if not alerts_df.empty:
        print(f"  Unique traders flagged: {alerts_df['trader_id'].nunique()}")
        print(f"  Critical severity: {len(alerts_df[alerts_df['severity'] == 'CRITICAL'])}")
        print(f"  High severity:     {len(alerts_df[alerts_df['severity'] == 'HIGH'])}")
        print(f"  Medium severity:   {len(alerts_df[alerts_df['severity'] == 'MEDIUM'])}")
        print(f"\n  Sample alerts:")
        print(alerts_df.head(10).to_string(index=False))

    return alerts_df


if __name__ == "__main__":
    detect_spoofing()
