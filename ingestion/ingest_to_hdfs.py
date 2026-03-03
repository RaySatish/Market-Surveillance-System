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

Usage:
  python ingest_to_hdfs.py
"""

import subprocess
import os
import sys

from config import HDFS_NAMENODE, LOCAL_CSV


# HDFS directory where raw data is stored
HDFS_RAW_DIR = "/market/raw"
HDFS_RAW_FILE = f"{HDFS_RAW_DIR}/trades.csv"


def ingest_to_hdfs(local_file=None):
    """
    Upload a local CSV file to HDFS.

    Steps:
      1. Check that the local file exists.
      2. Create the HDFS directory if it doesn't exist.
      3. Remove any old copy on HDFS (hdfs dfs -rm).
      4. Upload the file (hdfs dfs -put).
      5. Verify the upload (hdfs dfs -ls).
    """
    if local_file is None:
        local_file = LOCAL_CSV

    # ---------- Check local file ----------
    if not os.path.exists(local_file):
        print(f"ERROR: Local file not found: {local_file}")
        print("       Run 'python generate_trades.py' first to create trades.csv")
        sys.exit(1)

    file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
    print(f"Local file: {local_file}  ({file_size_mb:.1f} MB)")

    # ---------- Create HDFS directory ----------
    print(f"\nCreating HDFS directory: {HDFS_RAW_DIR}")
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", HDFS_RAW_DIR],
        check=True
    )

    # ---------- Remove old file if exists ----------
    print(f"Removing old HDFS file (if any): {HDFS_RAW_FILE}")
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-f", HDFS_RAW_FILE],
        capture_output=True  # Don't fail if file doesn't exist
    )

    # ---------- Upload ----------
    print(f"Uploading to HDFS: {HDFS_RAW_FILE}")
    result = subprocess.run(
        ["hdfs", "dfs", "-put", local_file, HDFS_RAW_FILE],
        check=True,
        capture_output=True,
        text=True
    )

    # ---------- Verify ----------
    print("\nVerifying HDFS upload:")
    subprocess.run(
        ["hdfs", "dfs", "-ls", "-h", HDFS_RAW_FILE],
        check=True
    )

    print(f"\nINGEST COMPLETE — {local_file} → {HDFS_NAMENODE}{HDFS_RAW_FILE}")
    return f"{HDFS_NAMENODE}{HDFS_RAW_FILE}"


if __name__ == "__main__":
    ingest_to_hdfs()
