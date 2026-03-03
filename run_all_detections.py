"""
RUN FULL PIPELINE
=================
This is the master script that runs the COMPLETE pipeline:

  0. HDFS Ingestion           → uploads raw CSV to HDFS
  1. ETL (Spark)              → reads CSV from HDFS → cleans → writes Parquet to HDFS
  2. Wash Trade Detection     → reads HDFS Parquet via Spark → alerts_wash.csv
  3. Pump & Dump Detection    → reads HDFS Parquet via Spark → alerts_pump_dump.csv
  4. Spoofing Detection       → reads HDFS Parquet via Spark → alerts_spoofing.csv
  5. Combine all alerts       → all_alerts.csv (unified view)

Architecture:
  Phase 1 (local):  trades.csv → HDFS → Spark local → HDFS Parquet → detectors → alerts
  Phase 2 (AWS):    Binance API → S3 → EMR Spark → S3 Parquet → detectors → dashboard

Usage:
  python run_all_detections.py
  python run_all_detections.py --skip-etl   (skip HDFS ingestion + Spark ETL)
"""

import argparse
import pandas as pd
import os
from datetime import datetime

from config import get_config, MODE
from ingestion.ingest_to_hdfs import ingest_to_hdfs
from etl.etl_trades import run_etl
from detectors.detect_wash_trades import detect_wash_trades
from detectors.detect_pump_dump import detect_pump_and_dump
from detectors.detect_spoofing import detect_spoofing


def run_all(skip_etl=False):
    """Run ETL + all detection algorithms and combine results."""

    cfg = get_config()

    print("=" * 60)
    print("  MARKET SURVEILLANCE — FULL PIPELINE")
    print(f"  Mode:     {MODE.upper()}")
    print(f"  Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Input:    {cfg['raw_input']}")
    print(f"  Output:   {cfg['clean_output']}")
    print("=" * 60)

    # ---------- STEP 0: HDFS Ingestion + ETL (Spark) ----------
    if not skip_etl:
        print("\n" + "=" * 60)
        print("  [0/4] HDFS INGESTION — Upload CSV to HDFS")
        print("=" * 60)
        ingest_to_hdfs()

        print("\n" + "=" * 60)
        print("  [1/4] SPARK ETL PIPELINE — HDFS CSV → HDFS Parquet")
        print("=" * 60)
        run_etl()
    else:
        print("\n  Skipping HDFS ingestion + ETL (--skip-etl flag set)")

    # ---------- DETECTION 1: Wash Trades ----------
    print("\n" + "-" * 60)
    print("  [2/4] WASH TRADE DETECTION")
    print("-" * 60)
    wash_alerts = detect_wash_trades()

    # ---------- DETECTION 2: Pump & Dump ----------
    print("\n" + "-" * 60)
    print("  [3/4] PUMP & DUMP DETECTION")
    print("-" * 60)
    pd_alerts = detect_pump_and_dump()

    # ---------- DETECTION 3: Spoofing ----------
    print("\n" + "-" * 60)
    print("  [4/4] SPOOFING DETECTION")
    print("-" * 60)
    spoof_alerts = detect_spoofing()

    # ---------- COMBINE ALL ALERTS ----------
    print("\n" + "=" * 60)
    print("  COMBINING ALL ALERTS")
    print("=" * 60)

    combined_path = cfg["alerts_combined"]
    os.makedirs(os.path.dirname(combined_path), exist_ok=True)

    all_frames = []

    if not wash_alerts.empty:
        all_frames.append(wash_alerts[["alert_type", "severity", "detected_at"]].assign(
            details=wash_alerts.apply(
                lambda r: f"Trader {r['trader_id']} | {r['symbol']} | "
                          f"Price {r['price']} | Qty {r['total_quantity']}",
                axis=1
            )
        ))

    if not pd_alerts.empty:
        all_frames.append(pd_alerts[["alert_type", "severity", "detected_at"]].assign(
            details=pd_alerts.apply(
                lambda r: f"{r['symbol']} | {r['window_start']} | "
                          f"Price Δ {r['price_change_pct']}% | "
                          f"Buy Vol {r['buy_volume']} vs Sell Vol {r['sell_volume']}",
                axis=1
            )
        ))

    if not spoof_alerts.empty:
        all_frames.append(spoof_alerts[["alert_type", "severity", "detected_at"]].assign(
            details=spoof_alerts.apply(
                lambda r: f"Trader {r['trader_id']} | {r['symbols']} | "
                          f"Cancel Rate {r['cancel_rate']*100:.1f}% | "
                          f"Size Ratio {r['size_ratio']}x",
                axis=1
            )
        ))

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined.to_csv(combined_path, index=False)

        print(f"\n  Total alerts generated: {len(combined)}")
        print(f"  CRITICAL: {len(combined[combined['severity'] == 'CRITICAL'])}")
        print(f"  HIGH:     {len(combined[combined['severity'] == 'HIGH'])}")
        print(f"  MEDIUM:   {len(combined[combined['severity'] == 'MEDIUM'])}")
        print(f"\n  Saved to: {combined_path}")
    else:
        print("\n  No alerts generated. All clear!")

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)

    # Stop the shared Spark session now that all HDFS reads are done
    from pyspark.sql import SparkSession
    try:
        SparkSession.builder.getOrCreate().stop()
        print("  Spark session stopped.")
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full market surveillance pipeline")
    parser.add_argument(
        "--skip-etl", action="store_true",
        help="Skip the Spark ETL step (use existing Parquet data)"
    )
    args = parser.parse_args()

    run_all(skip_etl=args.skip_etl)
