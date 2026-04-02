"""
WASH TRADE DETECTION
====================
What is a wash trade?
  A trader buys AND sells the SAME asset, at the SAME price, at the SAME time.
  This fakes trading volume to make a market look more active than it really is.

Detection modes:
  SYNTHETIC DATA (generate_trades.py):
    Group by (trader_id, symbol, timestamp, price) — flag groups with both BUY and SELL.

  REAL BINANCE DATA (fetch_binance.py):
    Binance public aggTrades have no real trader_id. Use statistical approach:
    Z-score on rolling volume per symbol — flag windows where volume is anomalously high.
    This detects coordinated wash trading clusters even without trader identity.

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

log = get_logger("detect_wash")


def detect_wash_trades(input_path=None, output_file=None):
    """
    Detect wash trades from local Parquet (output of ETL pipeline).

    If trader_id is available (synthetic data): group-based detection.
    If trader_id is absent/single-value (real Binance data): statistical Z-score detection.
    """
    cfg = get_config()
    if input_path is None:
        input_path = cfg["parquet_dir"]
    if output_file is None:
        output_file = cfg["alerts_wash"]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    log.info("Loading cleaned trades from Parquet…")
    df = read_parquet(input_path)
    log.info("Total trades loaded: %s", f"{len(df):,}")

    is_real_binance = (
        "trader_id" not in df.columns
        or df["trader_id"].nunique() <= 1
        or (df["trader_id"].iloc[0] == "binance_agg" if len(df) > 0 else False)
    )

    if is_real_binance:
        alerts_df = _detect_statistical(df)
    else:
        alerts_df = _detect_group_based(df)

    safe_write_csv(alerts_df, output_file, logger=log)

    log.info("WASH TRADE ALERTS: %d", len(alerts_df))
    if not alerts_df.empty:
        log.info("Sample alerts:\n%s", alerts_df.head(10).to_string(index=False))

    return alerts_df


def _detect_group_based(df: pd.DataFrame) -> pd.DataFrame:
    """
    Synthetic data mode: group by (trader_id, symbol, timestamp, price).
    Flag groups that have both BUY and SELL.
    """
    log.info("Using group-based wash detection (synthetic data mode)")
    group_cols = ["trader_id", "symbol", "timestamp", "price"]
    grouped = df.groupby(group_cols)

    wash_alerts = []
    for (trader, symbol, ts, price), group in grouped:
        sides = set(group["side"].values)
        if "BUY" in sides and "SELL" in sides:
            total_qty = group["quantity"].sum()
            wash_alerts.append({
                "alert_type":     "WASH_TRADE",
                "trader_id":      trader,
                "symbol":         symbol,
                "timestamp":      ts,
                "price":          price,
                "total_quantity": total_qty,
                "num_trades":     len(group),
                "severity":       "HIGH" if total_qty > 50 else "MEDIUM",
                "detected_at":    datetime.now().isoformat(),
            })

    return pd.DataFrame(wash_alerts)


def _detect_statistical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Real Binance data mode: Z-score on rolling trade volume per symbol.
    Resample to 1-second buckets first to avoid duplicate-index issues.
    Flags time windows where volume is anomalously high (potential coordinated wash).
    """
    log.info("Using statistical Z-score wash detection (real Binance data mode)")

    window = DETECTION["wash_rolling_window"]
    threshold = DETECTION["wash_zscore_threshold"]

    if "timestamp" not in df.columns:
        log.warning("No timestamp column — skipping statistical wash detection")
        return pd.DataFrame()

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    wash_alerts = []

    for symbol, sym_df in df.groupby("symbol"):
        sym_df = sym_df.set_index("timestamp")

        # Resample to 1-second buckets: sum quantity, take mean price
        resampled = sym_df.resample("1s").agg(
            quantity=("quantity", "sum"),
            price=("price", "mean")
        ).dropna(subset=["quantity"])

        # Rolling volume over the configured window
        rolling_vol = resampled["quantity"].rolling(window).sum()
        mean_vol = rolling_vol.mean()
        std_vol = rolling_vol.std()

        if std_vol == 0 or pd.isna(std_vol):
            log.warning("Zero std for %s — skipping", symbol)
            continue

        z_scores = (rolling_vol - mean_vol) / std_vol
        flagged = z_scores[z_scores > threshold]

        for ts, z in flagged.items():
            price_val = float(resampled.loc[ts, "price"])
            vol_val = float(rolling_vol[ts])

            wash_alerts.append({
                "alert_type":     "WASH_TRADE",
                "trader_id":      "UNKNOWN (Binance public data)",
                "symbol":         symbol,
                "timestamp":      ts,
                "price":          round(price_val, 2),
                "total_quantity": round(vol_val, 6),
                "z_score":        round(float(z), 2),
                "severity":       "CRITICAL" if float(z) > threshold * 1.5 else "HIGH",
                "detected_at":    datetime.now().isoformat(),
            })

    return pd.DataFrame(wash_alerts)


if __name__ == "__main__":
    detect_wash_trades()
