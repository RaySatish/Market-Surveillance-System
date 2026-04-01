"""
RUN FULL PIPELINE
=================
This is the master script that runs the COMPLETE pipeline:

  0. (Optional) Fetch trades  → fetch_binance.py or generate_trades.py
  1. ETL (Spark)              → reads local CSV → cleans → writes local Parquet
  2. Wash Trade Detection     → reads local Parquet → alerts_wash.csv
  3. Pump & Dump Detection    → reads local Parquet → alerts_pump_dump.csv
  4. Combine all alerts       → all_alerts.csv (unified view)

NOTE: Spoofing detection has been removed.
  Spoofing requires CANCELLED order events. Binance public aggTrades API
  never exposes cancellations — only executed trades. Detection is not
  possible on real data, so the entire spoofing stage has been dropped.

Fault tolerance:
  - Each step is checkpointed; a resumed run skips completed stages.
  - Every step is wrapped in try/except so one failing detector doesn't
    crash the whole pipeline.
  - Alert CSVs use atomic/idempotent writes (safe_write_csv).
  - Structured logging replaces print() for full auditability.

Architecture:
  Phase 1 (local):  trades.csv → Spark local → local Parquet → detectors → alerts
  Phase 2 (AWS):    Binance API → S3 → EMR Spark → S3 Parquet → detectors → dashboard

Usage:
  python run_all_detections.py
  python run_all_detections.py --skip-etl   (skip Spark ETL, use existing Parquet)
  python run_all_detections.py --resume     (resume from last checkpoint)
"""

import argparse
import pandas as pd
import os
from datetime import datetime

from config import get_config, MODE
from etl.etl_trades import run_etl
from detectors.detect_wash_trades import detect_wash_trades
from detectors.detect_pump_dump import detect_pump_and_dump
from utils.fault_tolerance import (
    get_logger, safe_write_csv,
    save_checkpoint, load_checkpoint, clear_checkpoints,
)

log = get_logger("pipeline")


def _stage_done(stage: str, resume: bool) -> bool:
    """Return True if we should skip this stage (already checkpointed)."""
    if not resume:
        return False
    cp = load_checkpoint(stage)
    if cp:
        log.info("Checkpoint found — skipping stage '%s' (completed %s)",
                 stage, cp["completed_at"])
        return True
    return False


def run_all(skip_etl=False, resume=False):
    """Run ETL + all detection algorithms and combine results."""

    cfg = get_config()

    if not resume:
        clear_checkpoints()

    log.info("=" * 60)
    log.info("  MARKET SURVEILLANCE — FULL PIPELINE")
    log.info("  Mode:      %s", MODE.upper())
    log.info("  Run Time:  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Input CSV: %s", cfg["trades_csv"])
    log.info("  Parquet:   %s", cfg["parquet_dir"])
    log.info("  Resume:    %s", resume)
    log.info("  Detectors: Wash Trade, Pump & Dump  (Spoofing removed — not detectable on real data)")
    log.info("=" * 60)

    # ---------- STEP 1: Spark ETL ----------
    if not skip_etl:
        if not _stage_done("etl", resume):
            log.info("[1/3] SPARK ETL — local CSV → local Parquet")
            try:
                run_etl()
                save_checkpoint("etl")
            except Exception as exc:
                log.error("ETL failed: %s", exc, exc_info=True)
                raise
    else:
        log.info("Skipping Spark ETL (--skip-etl flag set)")

    # ---------- DETECTION 1: Wash Trades ----------
    wash_alerts = pd.DataFrame()
    if not _stage_done("detect_wash", resume):
        log.info("[2/3] WASH TRADE DETECTION")
        try:
            wash_alerts = detect_wash_trades()
            save_checkpoint("detect_wash", {"count": len(wash_alerts)})
        except Exception as exc:
            log.error("Wash trade detection failed: %s", exc, exc_info=True)
            # Continue — don't let one detector kill the whole pipeline
    else:
        wash_alerts = _reload_alerts(cfg["alerts_wash"])

    # ---------- DETECTION 2: Pump & Dump ----------
    pd_alerts = pd.DataFrame()
    if not _stage_done("detect_pd", resume):
        log.info("[3/3] PUMP & DUMP DETECTION")
        try:
            pd_alerts = detect_pump_and_dump()
            save_checkpoint("detect_pd", {"count": len(pd_alerts)})
        except Exception as exc:
            log.error("Pump & dump detection failed: %s", exc, exc_info=True)
    else:
        pd_alerts = _reload_alerts(cfg["alerts_pump_dump"])

    # ---------- COMBINE ALL ALERTS ----------
    log.info("COMBINING ALL ALERTS")

    combined_path = cfg["alerts_combined"]
    os.makedirs(os.path.dirname(combined_path), exist_ok=True)

    all_frames = []

    if not wash_alerts.empty:
        base_cols = ["alert_type", "severity", "detected_at"]
        available = [c for c in base_cols if c in wash_alerts.columns]
        frame = wash_alerts[available].copy()
        if "trader_id" in wash_alerts.columns and "symbol" in wash_alerts.columns:
            frame["details"] = wash_alerts.apply(
                lambda r: f"Trader {r.get('trader_id','?')} | {r.get('symbol','?')} | "
                          f"Price {r.get('price','?')} | Qty {r.get('total_quantity','?')}",
                axis=1
            )
        all_frames.append(frame)

    if not pd_alerts.empty:
        base_cols = ["alert_type", "severity", "detected_at"]
        available = [c for c in base_cols if c in pd_alerts.columns]
        frame = pd_alerts[available].copy()
        if "symbol" in pd_alerts.columns:
            frame["details"] = pd_alerts.apply(
                lambda r: f"{r.get('symbol','?')} | {r.get('window_start','?')} | "
                          f"Price Δ {r.get('price_change_pct','?')}% | "
                          f"Buy Vol {r.get('buy_volume','?')} vs Sell Vol {r.get('sell_volume','?')}",
                axis=1
            )
        all_frames.append(frame)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        safe_write_csv(combined, combined_path, logger=log)

        log.info("Total alerts generated: %d", len(combined))
        if "severity" in combined.columns:
            log.info("CRITICAL: %d  |  HIGH: %d  |  MEDIUM: %d",
                     len(combined[combined["severity"] == "CRITICAL"]),
                     len(combined[combined["severity"] == "HIGH"]),
                     len(combined[combined["severity"] == "MEDIUM"]))
        log.info("Saved to: %s", combined_path)
    else:
        log.info("No alerts generated. All clear!")

    save_checkpoint("combine")

    log.info("=" * 60)
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 60)

    # Stop the shared Spark session
    try:
        from pyspark.sql import SparkSession
        SparkSession.builder.getOrCreate().stop()
        log.info("Spark session stopped.")
    except Exception:
        pass


def _reload_alerts(path: str) -> pd.DataFrame:
    """Reload an alert CSV written in a previous run (for --resume)."""
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full market surveillance pipeline")
    parser.add_argument(
        "--skip-etl", action="store_true",
        help="Skip the Spark ETL step (use existing Parquet data)"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from last checkpoint (skip completed stages)"
    )
    args = parser.parse_args()

    run_all(skip_etl=args.skip_etl, resume=args.resume)
