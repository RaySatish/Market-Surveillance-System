"""
LOGGING & FAULT-TOLERANCE UTILITIES
====================================
Centralised helpers used by every module in the pipeline.

Provides:
  - get_logger()        → rotating file + console logger (replaces print())
  - validate_trade()    → row-level data-quality check
  - write_dead_letter() → persists rejected rows for auditing
"""

import logging
import os
import csv
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
#  2.  TRADE DATA VALIDATION
# ============================================================

# Fields that MUST be present and non-empty in every row.
# trader_id is intentionally excluded — Binance public data has no trader identity.
REQUIRED_FIELDS = [
    "trade_id", "timestamp", "symbol", "price",
    "quantity", "side", "order_id", "event_type",
]

VALID_SIDES   = {"BUY", "SELL"}
VALID_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}

# Real Binance data only produces TRADE events.
# PUMP / DUMP / WASH are event types assigned by detectors when flagging alerts.
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
#  3.  DEAD-LETTER QUEUE
# ============================================================
DLQ_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dead_letter")
os.makedirs(DLQ_DIR, exist_ok=True)

DLQ_FILE = os.path.join(DLQ_DIR, "rejected_trades.csv")
DLQ_FIELDNAMES = REQUIRED_FIELDS + ["rejection_reason", "rejected_at"]


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
