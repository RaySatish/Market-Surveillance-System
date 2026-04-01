"""
PUMP AND DUMP DETECTION
========================
What is a pump and dump?
  A group of traders artificially INFLATES the price of an asset (the "pump")
  by placing many large BUY orders. Once the price is high, they SELL
  aggressively (the "dump") to profit, leaving other traders with losses.

How this script detects it:
  1. Load cleaned trades from local Parquet (output of ETL pipeline).
  2. Use a ROLLING TIME WINDOW (e.g., 5 minutes) per symbol.
  3. Within each window, calculate:
     - Price change (%) from start to peak
     - Volume imbalance: ratio of BUY volume vs SELL volume
     - If price spikes UP sharply AND buy volume dominates → PUMP
     - If price drops sharply AND sell volume dominates → DUMP
  4. If a PUMP is followed by a DUMP in the same symbol → flag it.

Works with both synthetic data and real Binance data (price + volume fields exist in both).

Fault tolerance:
  - Local Parquet read retries automatically (via spark_utils).
  - Alert CSV is written atomically (safe_write_csv) to prevent corruption.
  - Structured logging replaces print().

Data flow:
  fetch_binance.py (or generate_trades.py) → trades.csv → etl_trades.py (Spark) → Parquet → THIS SCRIPT
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from config import get_config, DETECTION
from etl.spark_utils import read_parquet
from utils.fault_tolerance import get_logger, safe_write_csv

log = get_logger("detect_pump_dump")


def detect_pump_and_dump(input_path=None, output_file=None):
    """
    Detect pump-and-dump patterns in trade data.
    Reads from the cleaned local Parquet produced by Spark ETL.
    """
    cfg = get_config()
    if input_path is None:
        input_path = cfg["parquet_dir"]
    if output_file is None:
        output_file = cfg["alerts_pump_dump"]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ---------- STEP 1: Load the cleaned Parquet ----------
    log.info("Loading cleaned trades from Parquet…")
    df = read_parquet(input_path)
    log.info("Total trades loaded: %s", f"{len(df):,}")

    if df.empty:
        log.warning("No data loaded — skipping pump & dump detection")
        safe_write_csv(pd.DataFrame(), output_file, logger=log)
        return pd.DataFrame()

    # ---------- STEP 2: Prepare data ----------
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    window_minutes = DETECTION["pd_window_minutes"]
    price_spike_pct = DETECTION["pd_price_spike_pct"]
    volume_ratio_threshold = DETECTION["pd_volume_ratio"]

    pd_alerts = []

    # ---------- STEP 3: Rolling window analysis per symbol ----------
    for symbol, sym_df in df.groupby("symbol"):
        sym_df = sym_df.reset_index(drop=True)
        timestamps = sym_df["timestamp"].values
        prices = sym_df["price"].astype(float).values
        quantities = sym_df["quantity"].astype(float).values
        sides = sym_df["side"].values if "side" in sym_df.columns else None

        pump_windows = []
        dump_windows = []

        window_delta = timedelta(minutes=window_minutes)
        start_idx = 0

        for i in range(len(sym_df)):
            # Collect all trades within the window starting at i
            window_end = pd.Timestamp(timestamps[i]) + window_delta
            window_mask = (sym_df["timestamp"] >= sym_df["timestamp"].iloc[i]) & \
                          (sym_df["timestamp"] < window_end)
            window_df = sym_df[window_mask]

            if len(window_df) < 2:
                continue

            w_prices = window_df["price"].astype(float).values
            w_qty = window_df["quantity"].astype(float).values

            price_change_pct = ((w_prices[-1] - w_prices[0]) / w_prices[0]) * 100

            if sides is not None and "side" in window_df.columns:
                buy_vol = window_df.loc[window_df["side"] == "BUY", "quantity"].astype(float).sum()
                sell_vol = window_df.loc[window_df["side"] == "SELL", "quantity"].astype(float).sum()
            else:
                # Binance: use maker flag — m=True means seller is maker (buyer is aggressive)
                buy_vol = w_qty.sum() / 2
                sell_vol = w_qty.sum() / 2

            # Detect PUMP: price rises sharply, buy volume dominates
            if (price_change_pct >= price_spike_pct and
                    sell_vol > 0 and (buy_vol / (sell_vol + 1e-9)) >= volume_ratio_threshold):
                pump_windows.append({
                    "window_start":    window_df["timestamp"].iloc[0],
                    "window_end":      window_df["timestamp"].iloc[-1],
                    "price_change_pct": round(price_change_pct, 2),
                    "buy_volume":      buy_vol,
                    "sell_volume":     sell_vol,
                    "peak_price":      w_prices.max(),
                })

            # Detect DUMP: price drops sharply, sell volume dominates
            if (price_change_pct <= -price_spike_pct and
                    buy_vol > 0 and (sell_vol / (buy_vol + 1e-9)) >= volume_ratio_threshold):
                dump_windows.append({
                    "window_start":    window_df["timestamp"].iloc[0],
                    "window_end":      window_df["timestamp"].iloc[-1],
                    "price_change_pct": round(price_change_pct, 2),
                    "buy_volume":      buy_vol,
                    "sell_volume":     sell_vol,
                    "trough_price":    w_prices.min(),
                })

        # ---------- STEP 4: Match PUMP followed by DUMP ----------
        for pump in pump_windows:
            for dump in dump_windows:
                # DUMP must start after PUMP ends
                if dump["window_start"] > pump["window_end"]:
                    pd_alerts.append({
                        "alert_type":       "PUMP_AND_DUMP",
                        "symbol":           symbol,
                        "window_start":     pump["window_start"],
                        "pump_end":         pump["window_end"],
                        "dump_start":       dump["window_start"],
                        "window_end":       dump["window_end"],
                        "price_change_pct": pump["price_change_pct"],
                        "peak_price":       pump["peak_price"],
                        "trough_price":     dump["trough_price"],
                        "buy_volume":       pump["buy_volume"],
                        "sell_volume":      dump["sell_volume"],
                        "severity":         "CRITICAL" if abs(pump["price_change_pct"]) > 10 else "HIGH",
                        "detected_at":      datetime.now().isoformat(),
                    })
                    break  # one dump match per pump is enough

    # ---------- STEP 5: Save results ----------
    alerts_df = pd.DataFrame(pd_alerts)
    safe_write_csv(alerts_df, output_file, logger=log)

    log.info("PUMP & DUMP ALERTS: %d", len(alerts_df))
    if not alerts_df.empty:
        log.info("Symbols affected: %s", alerts_df["symbol"].unique().tolist())
        log.info("Sample alerts:\n%s", alerts_df.head(5).to_string(index=False))

    return alerts_df


if __name__ == "__main__":
    detect_pump_and_dump()
