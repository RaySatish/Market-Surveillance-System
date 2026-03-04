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

Fault tolerance:
  - Each step is checkpointed; a resumed run skips completed stages.
  - Every step is wrapped in try/except so one failing detector doesn't
    crash the whole pipeline.
  - Alert CSVs use atomic/idempotent writes (safe_write_csv).
  - Structured logging replaces print() for full auditability.

Architecture:
  Phase 1 (local):  trades.csv → HDFS → Spark local → HDFS Parquet → detectors → alerts
  Phase 2 (AWS):    Binance API → S3 → EMR Spark → S3 Parquet → detectors → dashboard

Usage:
  python run_all_detections.py
  python run_all_detections.py --skip-etl   (skip HDFS ingestion + Spark ETL)
  python run_all_detections.py --resume     (resume from last checkpoint)
"""

import argparse
import pandas as pd
import os
from datetime import datetime

from config import get_config, MODE, HDFS_REPLICATION_FACTOR
from ingestion.ingest_to_hdfs import ingest_to_hdfs
from etl.etl_trades import run_etl
from detectors.detect_wash_trades import detect_wash_trades
from detectors.detect_pump_dump import detect_pump_and_dump
from detectors.detect_spoofing import detect_spoofing
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
    log.info("  Mode:        %s", MODE.upper())
    log.info("  Run Time:    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Input:       %s", cfg["raw_input"])
    log.info("  Output:      %s", cfg["clean_output"])
    log.info("  Replication: %d", HDFS_REPLICATION_FACTOR)
    log.info("  Resume:      %s", resume)
    log.info("=" * 60)

    # ---------- STEP 0 + 1: HDFS Ingestion + ETL ----------
    if not skip_etl:
        # -- Ingest --
        if not _stage_done("ingest", resume):
            log.info("[0/4] HDFS INGESTION — Upload CSV to HDFS")
            try:
                ingest_to_hdfs()
                save_checkpoint("ingest")
            except Exception as exc:
                log.error("HDFS ingestion failed: %s", exc, exc_info=True)
                raise

        # -- ETL --
        if not _stage_done("etl", resume):
            log.info("[1/4] SPARK ETL PIPELINE — HDFS CSV → HDFS Parquet")
            try:
                run_etl()
                save_checkpoint("etl")
            except Exception as exc:
                log.error("ETL failed: %s", exc, exc_info=True)
                raise
    else:
        log.info("Skipping HDFS ingestion + ETL (--skip-etl flag set)")

    # ---------- DETECTION 1: Wash Trades ----------
    wash_alerts = pd.DataFrame()
    if not _stage_done("detect_wash", resume):
        log.info("[2/4] WASH TRADE DETECTION")
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
        log.info("[3/4] PUMP & DUMP DETECTION")
        try:
            pd_alerts = detect_pump_and_dump()
            save_checkpoint("detect_pd", {"count": len(pd_alerts)})
        except Exception as exc:
            log.error("Pump & dump detection failed: %s", exc, exc_info=True)
    else:
        pd_alerts = _reload_alerts(cfg["alerts_pump_dump"])

    # ---------- DETECTION 3: Spoofing ----------
    spoof_alerts = pd.DataFrame()
    if not _stage_done("detect_spoof", resume):
        log.info("[4/4] SPOOFING DETECTION")
        try:
            spoof_alerts = detect_spoofing()
            save_checkpoint("detect_spoof", {"count": len(spoof_alerts)})
        except Exception as exc:
            log.error("Spoofing detection failed: %s", exc, exc_info=True)
    else:
        spoof_alerts = _reload_alerts(cfg["alerts_spoofing"])

    # ---------- COMBINE ALL ALERTS ----------
    log.info("COMBINING ALL ALERTS")

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
        safe_write_csv(combined, combined_path, logger=log)

        log.info("Total alerts generated: %d", len(combined))
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
    from pyspark.sql import SparkSession
    try:
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
