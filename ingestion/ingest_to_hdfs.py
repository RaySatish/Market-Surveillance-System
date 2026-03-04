"""
HDFS INGESTION
==============
What this does:
  Uploads the local trades.csv file into HDFS so that Spark can read it
  from the Hadoop distributed filesystem.

Why HDFS?
  - In production (AWS EMR), data lives on HDFS / S3, not on a laptop's disk.
  - Spark's native I/O layer reads from HDFS in parallel across the cluster.
  - By using HDFS locally, we match the production architecture — the same
    Spark code works on your laptop AND on a 100-node EMR cluster.

Data flow:
  generate_trades.py → trades.csv (local disk)
       ↓  this script
  hdfs://localhost:9000/market/raw/trades.csv (HDFS)
       ↓
  etl_trades.py (Spark reads from HDFS)
       ↓
  hdfs://localhost:9000/market/clean/trades/ (Parquet on HDFS)

Fault tolerance:
  - Row-level data validation before upload; bad rows go to dead-letter queue.
  - HDFS upload is wrapped with @retry (exponential back-off).
  - Upload verified after write; failure raises an error that triggers retry.

Usage:
  python ingest_to_hdfs.py
"""

import subprocess
import os
import sys
import csv

from config import HDFS_NAMENODE, LOCAL_CSV, HDFS_REPLICATION_FACTOR
from utils.fault_tolerance import (
    get_logger, retry, validate_trade, write_dead_letter,
)

log = get_logger("ingest_hdfs")

# HDFS directory where raw data is stored
HDFS_RAW_DIR = "/market/raw"
HDFS_RAW_FILE = f"{HDFS_RAW_DIR}/trades.csv"


def _validate_csv(local_file: str) -> str:
    """
    Read the CSV, validate every row, write a clean copy, and send
    rejected rows to the dead-letter queue.

    Returns the path to the validated (clean) CSV.
    """
    clean_path = local_file + ".validated"
    total = accepted = rejected = 0

    with open(local_file, newline="") as fin, \
         open(clean_path, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            ok, reason = validate_trade(row)
            if ok:
                writer.writerow(row)
                accepted += 1
            else:
                write_dead_letter(row, reason)
                rejected += 1

    log.info("Validation complete — total: %d, accepted: %d, rejected → DLQ: %d",
             total, accepted, rejected)
    if rejected:
        log.warning("%d rows sent to dead-letter queue (see dead_letter/rejected_trades.csv)", rejected)
    return clean_path


@retry(max_retries=3, base_delay=2.0, exceptions=(subprocess.CalledProcessError, OSError))
def ingest_to_hdfs(local_file=None):
    """
    Upload a local CSV file to HDFS.

    Steps:
      1. Check that the local file exists.
      2. Validate every row; reject bad rows to dead-letter queue.
      3. Create the HDFS directory if it doesn't exist.
      4. Remove any old copy on HDFS (hdfs dfs -rm).
      5. Upload the validated file (hdfs dfs -put).
      6. Verify the upload (hdfs dfs -ls).

    Fault tolerance:
      - Retries up to 3× with exponential back-off on HDFS failures.
      - Replication factor set via -Ddfs.replication=HDFS_REPLICATION_FACTOR.
    """
    if local_file is None:
        local_file = LOCAL_CSV

    # ---------- Check local file ----------
    if not os.path.exists(local_file):
        log.error("Local file not found: %s", local_file)
        log.error("Run 'python generate_trades.py' first to create trades.csv")
        sys.exit(1)

    file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
    log.info("Local file: %s  (%.1f MB)", local_file, file_size_mb)

    # ---------- Validate rows ----------
    validated_file = _validate_csv(local_file)

    # ---------- Create HDFS directory ----------
    log.info("Creating HDFS directory: %s", HDFS_RAW_DIR)
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", HDFS_RAW_DIR],
        check=True
    )

    # ---------- Remove old file if exists ----------
    log.info("Removing old HDFS file (if any): %s", HDFS_RAW_FILE)
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-f", HDFS_RAW_FILE],
        capture_output=True  # Don't fail if file doesn't exist
    )

    # ---------- Upload with replication factor ----------
    log.info("Uploading to HDFS: %s  (replication=%d)", HDFS_RAW_FILE, HDFS_REPLICATION_FACTOR)
    subprocess.run(
        ["hdfs", "dfs",
         f"-Ddfs.replication={HDFS_REPLICATION_FACTOR}",
         "-put", validated_file, HDFS_RAW_FILE],
        check=True,
        capture_output=True,
        text=True
    )

    # Clean up validated temp file
    os.remove(validated_file)

    # ---------- Verify ----------
    log.info("Verifying HDFS upload…")
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", "-h", HDFS_RAW_FILE],
        check=True,
        capture_output=True,
        text=True
    )
    log.info("HDFS listing:\n%s", result.stdout.strip())

    log.info("INGEST COMPLETE — %s → %s%s", local_file, HDFS_NAMENODE, HDFS_RAW_FILE)
    return f"{HDFS_NAMENODE}{HDFS_RAW_FILE}"


if __name__ == "__main__":
    ingest_to_hdfs()
