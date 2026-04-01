"""
LOGGING & FAULT-TOLERANCE UTILITIES
====================================
Centralised helpers used by every module in the pipeline.

Provides:
  - get_logger()        → rotating file + console logger (replaces print())
  - retry()             → decorator with exponential back-off
  - validate_trade()    → row-level data-quality check
  - write_dead_letter() → persists rejected rows for auditing
  - safe_write_csv()    → atomic / idempotent CSV writer
  - save_checkpoint() / load_checkpoint() / clear_checkpoints()
"""

import logging
import os
import time
import functools
import csv
import shutil
import hashlib
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler

# ============================================================
#  1.  LOGGING FRAMEWORK
# ============================================================
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

_loggers: dict = {}


def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Return a named logger that writes to BOTH the console and a rotating log file.

    - Console → INFO+  (coloured level name)
    - File    → DEBUG+ (full detail, rotates at 5 MB, keeps 5 backups)
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler
    fh = RotatingFileHandler(
        os.path.join(LOG_DIR, f"{name}.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    _loggers[name] = logger
    return logger


# ============================================================
#  2.  RETRY WITH EXPONENTIAL BACK-OFF
# ============================================================
def retry(max_retries: int = 3, base_delay: float = 1.0, backoff_factor: float = 2.0,
          exceptions: tuple = (Exception,)):
    """
    Decorator: retries the wrapped function on failure.

    Parameters
    ----------
    max_retries     : total attempts = 1 + max_retries
    base_delay      : initial wait (seconds)
    backoff_factor  : multiplier applied after each failure
    exceptions      : tuple of exception classes to catch
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger("retry")
            delay = base_delay
            last_exc = None
            for attempt in range(1, max_retries + 2):  # +2 → 1-indexed includes initial
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt > max_retries:
                        logger.error(
                            "FAILED after %d attempts — %s: %s",
                            attempt, func.__name__, exc,
                        )
                        raise
                    logger.warning(
                        "Attempt %d/%d failed for %s (%s). Retrying in %.1fs…",
                        attempt, max_retries + 1, func.__name__, exc, delay,
                    )
                    time.sleep(delay)
                    delay *= backoff_factor
            raise last_exc  # should not reach here
        return wrapper
    return decorator


# ============================================================
#  3.  TRADE DATA VALIDATION
# ============================================================

# Fields that MUST be present and non-empty in every row.
# trader_id is intentionally excluded — Binance public data has no trader identity.
# Synthetic data (generate_trades.py) includes trader_id and it will be validated
# if present, but its absence is not an error.
REQUIRED_FIELDS = [
    "trade_id", "timestamp", "symbol", "price",
    "quantity", "side", "order_id", "event_type",
]

# trader_id is optional — present in synthetic data, absent in real Binance data
OPTIONAL_FIELDS = ["trader_id"]

VALID_SIDES   = {"BUY", "SELL"}
VALID_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}

# Real Binance data only produces TRADE events.
# PUMP / DUMP are injected by generate_trades.py for synthetic dev/testing only.
# WASH / CANCELLED have been removed — Binance public API never emits these,
# and spoofing detection (which needed CANCELLED) has been dropped.
VALID_EVENTS = {"TRADE", "PUMP", "DUMP", "WASH"}


def validate_trade(row: dict) -> tuple[bool, str]:
    """
    Validate a single trade row.

    Returns (True, "") on success, (False, reason) on failure.

    trader_id is optional: real Binance aggTrades have no trader identity.
    All other 8 fields are required.
    """
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in row or row[field] is None or str(row[field]).strip() == "":
            return False, f"missing_field:{field}"

    # Price must be positive
    try:
        price = float(row["price"])
        if price <= 0:
            return False, f"non_positive_price:{price}"
    except (ValueError, TypeError):
        return False, f"invalid_price:{row['price']}"

    # Quantity must be positive
    try:
        qty = float(row["quantity"])
        if qty <= 0:
            return False, f"non_positive_quantity:{qty}"
    except (ValueError, TypeError):
        return False, f"invalid_quantity:{row['quantity']}"

    # Timestamp must parse as ISO-8601
    try:
        ts = str(row["timestamp"])
        datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return False, f"invalid_timestamp:{row['timestamp']}"

    # Side
    if row["side"] not in VALID_SIDES:
        return False, f"invalid_side:{row['side']}"

    # Event type
    if row["event_type"] not in VALID_EVENTS:
        return False, f"invalid_event_type:{row['event_type']}"

    # Symbol
    if row["symbol"] not in VALID_SYMBOLS:
        return False, f"unknown_symbol:{row['symbol']}"

    return True, ""


# ============================================================
#  4.  DEAD-LETTER QUEUE
# ============================================================
DLQ_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dead_letter")
os.makedirs(DLQ_DIR, exist_ok=True)

DLQ_FILE = os.path.join(DLQ_DIR, "rejected_trades.csv")
# trader_id included in DLQ schema so synthetic-data rejections are fully captured
DLQ_FIELDNAMES = REQUIRED_FIELDS + OPTIONAL_FIELDS + ["rejection_reason", "rejected_at"]


def write_dead_letter(row: dict, reason: str):
    """
    Append a rejected trade to the dead-letter CSV for auditing.
    Thread-safe via append mode.
    """
    file_exists = os.path.exists(DLQ_FILE) and os.path.getsize(DLQ_FILE) > 0
    with open(DLQ_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DLQ_FIELDNAMES, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        row_copy = dict(row)
        row_copy["rejection_reason"] = reason
        row_copy["rejected_at"] = datetime.now().isoformat()
        writer.writerow(row_copy)


# ============================================================
#  5.  IDEMPOTENT / ATOMIC CSV WRITER
# ============================================================
def safe_write_csv(df, output_path: str, logger=None):
    """
    Write a pandas DataFrame to CSV atomically.

    1. Writes to a temp file  (.tmp)
    2. Moves the temp file to the target path (atomic on POSIX)
    3. If the same content was already written (sha256 match), skip the write.

    This prevents partial-write corruption and duplicate re-writes.
    """
    if logger is None:
        logger = get_logger("safe_write")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    tmp_path = output_path + ".tmp"
    df.to_csv(tmp_path, index=False)

    # Idempotency check — compare hashes
    new_hash = _file_hash(tmp_path)
    if os.path.exists(output_path):
        old_hash = _file_hash(output_path)
        if old_hash == new_hash:
            os.remove(tmp_path)
            logger.info("Idempotent skip — %s unchanged (sha256 match)", output_path)
            return

    shutil.move(tmp_path, output_path)  # atomic rename
    logger.info("Wrote %d rows → %s", len(df), output_path)


def _file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ============================================================
#  6.  CHECKPOINT HELPERS
# ============================================================
CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".checkpoints")
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def save_checkpoint(stage: str, data: dict = None):
    """Persist a checkpoint after a pipeline stage completes successfully."""
    cp = {
        "stage": stage,
        "completed_at": datetime.now().isoformat(),
        "data": data or {},
    }
    path = os.path.join(CHECKPOINT_DIR, f"{stage}.json")
    with open(path, "w") as f:
        json.dump(cp, f, indent=2)
    get_logger("checkpoint").info("Checkpoint saved: %s", stage)


def load_checkpoint(stage: str) -> dict | None:
    """Load a checkpoint if it exists, else return None."""
    path = os.path.join(CHECKPOINT_DIR, f"{stage}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def clear_checkpoints():
    """Remove all checkpoints (used at the start of a fresh run)."""
    for f in os.listdir(CHECKPOINT_DIR):
        os.remove(os.path.join(CHECKPOINT_DIR, f))
    get_logger("checkpoint").info("All checkpoints cleared")
