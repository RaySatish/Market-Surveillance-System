"""
PUMP AND DUMP DETECTION
========================
What is a pump and dump?
  A group of traders artificially INFLATES the price of an asset (the "pump")
  by placing many large BUY orders. Once the price is high, they SELL
  aggressively (the "dump") to profit, leaving other traders with losses.

Reverse pattern (dump-and-pump / short squeeze):
  Traders DUMP first to drive price down, then BUY at the bottom to profit.

How this script detects it:
  1. Load cleaned trades from local Parquet (output of ETL pipeline).
  2. Resample trades into 1-minute OHLCV bars per symbol.
  3. Scan for PUMP bars (price rise >= threshold) and DUMP bars (price drop >= threshold).
  4. Match PUMP→DUMP pairs within window_minutes → classic pump & dump.
  5. Also match DUMP→PUMP pairs within window_minutes → dump & pump (reverse manipulation).

Works with both synthetic data and real Binance data.

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
from datetime import datetime

from config import get_config, DETECTION
from etl.spark_utils import read_parquet
from utils.fault_tolerance import get_logger, safe_write_csv

log = get_logger("detect_pump_dump")


def detect_pump_and_dump(input_path=None, output_file=None):
    """
    Detect pump-and-dump (and dump-and-pump) patterns in trade data.
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

    window_minutes  = DETECTION["pd_window_minutes"]
    price_spike_pct = DETECTION["pd_price_spike_pct"]

    pd_alerts = []

    # ---------- STEP 3: Per-symbol 1-min OHLCV resampling ----------
    for symbol, sym_df in df.groupby("symbol"):
        sym_df = sym_df.set_index("timestamp")

        # Build 1-min OHLCV bars
        price_bars = sym_df["price"].resample("1min").ohlc()

        if "side" in sym_df.columns:
            buy_bars  = sym_df.loc[sym_df["side"] == "BUY",  "quantity"].resample("1min").sum()
            sell_bars = sym_df.loc[sym_df["side"] == "SELL", "quantity"].resample("1min").sum()
        else:
            total_bars = sym_df["quantity"].resample("1min").sum()
            buy_bars   = total_bars / 2
            sell_bars  = total_bars / 2

        bars = price_bars.copy()
        bars["buy_vol"]  = buy_bars.reindex(bars.index, fill_value=0)
        bars["sell_vol"] = sell_bars.reindex(bars.index, fill_value=0)
        bars = bars.dropna(subset=["open", "close"])
        bars["price_chg_pct"] = ((bars["close"] - bars["open"]) / bars["open"]) * 100

        if len(bars) < 2:
            log.info("Not enough 1-min bars for %s (%d bars) — skipping", symbol, len(bars))
            continue

        log.info("%s: %d 1-min bars | price range %.2f–%.2f | max 1-min move: +%.4f%% / %.4f%%",
                 symbol, len(bars),
                 bars["open"].min(), bars["high"].max(),
                 bars["price_chg_pct"].max(), bars["price_chg_pct"].min())

        # ---------- STEP 4: Identify PUMP and DUMP bars ----------
        pump_bars = bars[bars["price_chg_pct"] >= price_spike_pct].copy()
        dump_bars = bars[bars["price_chg_pct"] <= -price_spike_pct].copy()

        log.info("%s: %d pump bars (≥+%.4f%%), %d dump bars (≤-%.4f%%)",
                 symbol, len(pump_bars), price_spike_pct, len(dump_bars), price_spike_pct)

        # ---------- STEP 5a: Match PUMP → DUMP (classic pump & dump) ----------
        matched_pumps = set()
        for pump_time, pump in pump_bars.iterrows():
            for dump_time, dump in dump_bars.iterrows():
                time_diff = (dump_time - pump_time).total_seconds() / 60
                if 0 < time_diff <= window_minutes and pump_time not in matched_pumps:
                    matched_pumps.add(pump_time)
                    severity = "CRITICAL" if abs(pump["price_chg_pct"]) > 0.3 else "HIGH"
                    pd_alerts.append({
                        "alert_type":       "PUMP_AND_DUMP",
                        "symbol":           symbol,
                        "first_bar_time":   pump_time,
                        "second_bar_time":  dump_time,
                        "first_price_chg":  round(pump["price_chg_pct"], 4),
                        "second_price_chg": round(dump["price_chg_pct"], 4),
                        "peak_price":       round(float(pump["high"]), 2),
                        "trough_price":     round(float(dump["low"]), 2),
                        "first_buy_vol":    round(float(pump["buy_vol"]), 4),
                        "second_sell_vol":  round(float(dump["sell_vol"]), 4),
                        "severity":         severity,
                        "detected_at":      datetime.now().isoformat(),
                    })
                    break

        # ---------- STEP 5b: Match DUMP → PUMP (dump & pump / reverse manipulation) ----------
        matched_dumps = set()
        for dump_time, dump in dump_bars.iterrows():
            for pump_time, pump in pump_bars.iterrows():
                time_diff = (pump_time - dump_time).total_seconds() / 60
                if 0 < time_diff <= window_minutes and dump_time not in matched_dumps:
                    matched_dumps.add(dump_time)
                    severity = "CRITICAL" if abs(dump["price_chg_pct"]) > 0.3 else "HIGH"
                    pd_alerts.append({
                        "alert_type":       "DUMP_AND_PUMP",
                        "symbol":           symbol,
                        "first_bar_time":   dump_time,
                        "second_bar_time":  pump_time,
                        "first_price_chg":  round(dump["price_chg_pct"], 4),
                        "second_price_chg": round(pump["price_chg_pct"], 4),
                        "peak_price":       round(float(pump["high"]), 2),
                        "trough_price":     round(float(dump["low"]), 2),
                        "first_buy_vol":    round(float(dump["buy_vol"]), 4),
                        "second_sell_vol":  round(float(pump["sell_vol"]), 4),
                        "severity":         severity,
                        "detected_at":      datetime.now().isoformat(),
                    })
                    break

    # ---------- STEP 6: Save results ----------
    alerts_df = pd.DataFrame(pd_alerts)
    safe_write_csv(alerts_df, output_file, logger=log)

    log.info("PUMP & DUMP ALERTS: %d", len(alerts_df))
    if not alerts_df.empty:
        log.info("Symbols affected: %s", alerts_df["symbol"].unique().tolist())
        log.info("Alert types: %s", alerts_df["alert_type"].value_counts().to_dict())
        log.info("Sample alerts:\n%s", alerts_df.head(5).to_string(index=False))

    return alerts_df


if __name__ == "__main__":
    detect_pump_and_dump()
