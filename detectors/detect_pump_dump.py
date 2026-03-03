"""
PUMP AND DUMP DETECTION
========================
What is a pump and dump?
  A group of traders artificially INFLATES the price of an asset (the "pump")
  by placing many large BUY orders. Once the price is high, they SELL
  aggressively (the "dump") to profit, leaving other traders with losses.

How this script detects it:
  1. Load cleaned trades from Parquet (output of ETL pipeline).
  2. Use a ROLLING TIME WINDOW (e.g., 5 minutes) per symbol.
  3. Within each window, calculate:
     - Price change (%) from start to peak
     - Volume imbalance: ratio of BUY volume vs SELL volume
     - If price spikes UP sharply AND buy volume dominates → PUMP
     - If price drops sharply AND sell volume dominates → DUMP
  4. If a PUMP is followed by a DUMP in the same symbol → flag it.

Data flow:
  generate_trades.py → trades.csv → HDFS → etl_trades.py (Spark) → HDFS Parquet → THIS SCRIPT
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from config import get_config, DETECTION
from etl.hdfs_utils import read_parquet_from_hdfs


def detect_pump_and_dump(
    input_path=None,
    output_file=None,
    window_minutes=None,
    price_spike_pct=None,
    volume_ratio_threshold=None
):
    """
    Detect pump-and-dump schemes using rolling time windows.
    Reads from the CLEANED Parquet produced by Spark ETL.
    """
    cfg = get_config()
    if input_path is None:
        input_path = cfg["clean_output"]
    if output_file is None:
        output_file = cfg["alerts_pump_dump"]
    if window_minutes is None:
        window_minutes = DETECTION["pd_window_minutes"]
    if price_spike_pct is None:
        price_spike_pct = DETECTION["pd_price_spike_pct"]
    if volume_ratio_threshold is None:
        volume_ratio_threshold = DETECTION["pd_volume_ratio"]

    # Ensure alerts directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ---------- STEP 1: Load cleaned Parquet from HDFS ----------
    print("Loading cleaned trades from HDFS Parquet...")
    df = read_parquet_from_hdfs(input_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    print(f"  Total trades loaded: {len(df):,}")

    # ---------- STEP 2: Analyze each symbol separately ----------
    alerts = []

    for symbol in df["symbol"].unique():
        sym_df = df[df["symbol"] == symbol].copy()

        # Create time-based windows
        # We use pd.Grouper to bucket trades into fixed time intervals
        sym_df.set_index("timestamp", inplace=True)

        # Resample into time windows
        windows = sym_df.resample(f"{window_minutes}min")

        prev_window_type = None  # Track if previous window was a PUMP

        for window_start, window_df in windows:
            if len(window_df) < 5:
                # Skip windows with too few trades (not meaningful)
                prev_window_type = None
                continue

            # ---------- STEP 3: Calculate metrics for this window ----------

            # Price change from first to last trade in window
            price_start = window_df["price"].iloc[0]
            price_end = window_df["price"].iloc[-1]
            price_max = window_df["price"].max()
            price_min = window_df["price"].min()

            price_change_pct = ((price_end - price_start) / price_start) * 100

            # Volume split by side
            buy_volume = window_df[window_df["side"] == "BUY"]["quantity"].sum()
            sell_volume = window_df[window_df["side"] == "SELL"]["quantity"].sum()

            # Avoid division by zero
            if sell_volume == 0:
                vol_ratio = float("inf")
            else:
                vol_ratio = buy_volume / sell_volume

            if buy_volume == 0:
                sell_ratio = float("inf")
            else:
                sell_ratio = sell_volume / buy_volume

            # ---------- STEP 4: Classify the window ----------

            # PUMP: Price went UP sharply + BUY volume dominates
            if price_change_pct > price_spike_pct and vol_ratio > volume_ratio_threshold:
                window_type = "PUMP"
                alerts.append({
                    "alert_type": "PUMP_DETECTED",
                    "symbol": symbol,
                    "window_start": str(window_start),
                    "window_end": str(window_start + timedelta(minutes=window_minutes)),
                    "price_start": round(price_start, 2),
                    "price_end": round(price_end, 2),
                    "price_change_pct": round(price_change_pct, 4),
                    "buy_volume": int(buy_volume),
                    "sell_volume": int(sell_volume),
                    "num_trades": len(window_df),
                    "severity": "HIGH",
                    "detected_at": datetime.now().isoformat()
                })

            # DUMP: Price went DOWN sharply + SELL volume dominates
            elif price_change_pct < -price_spike_pct and sell_ratio > volume_ratio_threshold:
                window_type = "DUMP"
                severity = "CRITICAL" if prev_window_type == "PUMP" else "HIGH"

                alerts.append({
                    "alert_type": "DUMP_DETECTED" if prev_window_type != "PUMP"
                                  else "PUMP_AND_DUMP_CONFIRMED",
                    "symbol": symbol,
                    "window_start": str(window_start),
                    "window_end": str(window_start + timedelta(minutes=window_minutes)),
                    "price_start": round(price_start, 2),
                    "price_end": round(price_end, 2),
                    "price_change_pct": round(price_change_pct, 4),
                    "buy_volume": int(buy_volume),
                    "sell_volume": int(sell_volume),
                    "num_trades": len(window_df),
                    "severity": severity,
                    "detected_at": datetime.now().isoformat()
                })
            else:
                window_type = "NORMAL"

            prev_window_type = window_type

    # ---------- STEP 5: Save results ----------
    alerts_df = pd.DataFrame(alerts)
    alerts_df.to_csv(output_file, index=False)

    print(f"\n  PUMP & DUMP ALERTS: {len(alerts_df)}")
    if not alerts_df.empty:
        pump_count = len(alerts_df[alerts_df["alert_type"] == "PUMP_DETECTED"])
        dump_count = len(alerts_df[alerts_df["alert_type"] == "DUMP_DETECTED"])
        confirmed = len(alerts_df[alerts_df["alert_type"] == "PUMP_AND_DUMP_CONFIRMED"])
        print(f"  Pump signals:     {pump_count}")
        print(f"  Dump signals:     {dump_count}")
        print(f"  Confirmed P&D:    {confirmed}")
        print(f"\n  Sample alerts:")
        print(alerts_df.head(10).to_string(index=False))

    return alerts_df


if __name__ == "__main__":
    detect_pump_and_dump()
