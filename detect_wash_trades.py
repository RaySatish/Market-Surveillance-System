"""
WASH TRADE DETECTION
====================
What is a wash trade?
  A trader buys AND sells the SAME asset, at the SAME price, at the SAME time.
  This fakes trading volume to make a market look more active than it really is.

How this script detects it:
  1. Load cleaned trades from Parquet (output of ETL pipeline).
  2. Group trades by (trader_id, symbol, timestamp, price)
     - If the SAME trader traded the SAME symbol at the SAME time and price,
       that's suspicious.
  3. Within each group, check if there's BOTH a BUY and a SELL.
     - A normal trader might buy twice — that's fine.
     - But buying AND selling at the exact same price/time = wash trade.
  4. Flag those trades and save alerts to a CSV.

Data flow:
  generate_trades.py → trades.csv → HDFS → etl_trades.py (Spark) → HDFS Parquet → THIS SCRIPT
"""

import pandas as pd
import os
from datetime import datetime

from config import get_config, DETECTION
from hdfs_utils import read_parquet_from_hdfs


def detect_wash_trades(input_path=None, output_file=None):
    """
    Detect wash trades: same trader, same symbol, same price, same timestamp,
    with both BUY and SELL sides present.

    Reads from the CLEANED Parquet produced by Spark ETL.
    """
    cfg = get_config()
    if input_path is None:
        input_path = cfg["clean_output"]
    if output_file is None:
        output_file = cfg["alerts_wash"]

    # Ensure alerts directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ---------- STEP 1: Load the cleaned Parquet from HDFS ----------
    # Spark reads the Parquet from HDFS, then .toPandas() brings it into memory.
    # This mirrors production: on AWS EMR, Spark reads from S3 the same way.
    print("Loading cleaned trades from HDFS Parquet...")
    df = read_parquet_from_hdfs(input_path)
    print(f"  Total trades loaded: {len(df):,}")

    # ---------- STEP 2: Group by trader + symbol + time + price ----------
    # We group on these 4 columns because a wash trade happens when the SAME
    # trader trades the SAME symbol at the SAME time for the SAME price.
    group_cols = ["trader_id", "symbol", "timestamp", "price"]
    grouped = df.groupby(group_cols)

    # ---------- STEP 3: Find groups with both BUY and SELL ----------
    wash_alerts = []

    for (trader, symbol, ts, price), group in grouped:
        sides = set(group["side"].values)

        # If this group has both BUY and SELL → wash trade
        if "BUY" in sides and "SELL" in sides:
            total_qty = group["quantity"].sum()
            wash_alerts.append({
                "alert_type": "WASH_TRADE",
                "trader_id": trader,
                "symbol": symbol,
                "timestamp": ts,
                "price": price,
                "total_quantity": total_qty,
                "num_trades": len(group),
                "severity": "HIGH" if total_qty > 50 else "MEDIUM",
                "detected_at": datetime.now().isoformat()
            })

    # ---------- STEP 4: Save results ----------
    alerts_df = pd.DataFrame(wash_alerts)
    alerts_df.to_csv(output_file, index=False)

    print(f"\n  WASH TRADE ALERTS: {len(alerts_df)}")
    if not alerts_df.empty:
        print(f"  Unique traders flagged: {alerts_df['trader_id'].nunique()}")
        print(f"  Symbols affected: {alerts_df['symbol'].unique().tolist()}")
        print(f"\n  Sample alerts:")
        print(alerts_df.head(10).to_string(index=False))

    return alerts_df


if __name__ == "__main__":
    detect_wash_trades()
