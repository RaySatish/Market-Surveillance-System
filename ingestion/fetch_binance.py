"""
BINANCE BATCH FETCHER
=====================
Pulls recent aggTrades from Binance REST API for BTCUSDT, ETHUSDT, SOLUSDT
and writes them to trades.csv (same format the ETL pipeline expects).

Usage
-----
  python ingestion/fetch_binance.py                  # last 30 minutes (default)
  python ingestion/fetch_binance.py --minutes 60     # last 60 minutes
  python ingestion/fetch_binance.py --minutes 5      # quick test

How it works
------------
  1. For each symbol, call GET /api/v3/aggTrades with startTime/endTime.
  2. Binance returns up to 1 000 trades per call; paginate with fromId until
     the full time window is covered.
  3. Map Binance fields → our schema (no trader_id — Binance public API has none).
  4. Validate every row; bad rows go to dead_letter/rejected_trades.csv.
  5. Write the combined DataFrame atomically to trades.csv via safe_write_csv().

Field mapping (Binance aggTrades → our schema)
----------------------------------------------
  Binance field  → our field
  a              → trade_id          (aggregate trade ID)
  T              → timestamp         (ms epoch → ISO-8601 string)
  <symbol param> → symbol
  p              → price
  q              → quantity
  m              → side              (isBuyerMaker: True=BUY taker, False=SELL taker)
  a              → order_id          (reuse trade_id as proxy)
  "TRADE"        → event_type

  NOTE: trader_id is intentionally OMITTED — Binance public aggTrades API
  does not expose any trader identity. Detectors handle this gracefully:
    - detect_wash_trades.py  → falls back to statistical Z-score mode
    - detect_pump_dump.py    → never uses trader_id
    - detect_spoofing.py     → skips (no CANCELLED events either)

Fault tolerance
---------------
  - @retry on every HTTP request (3 attempts, exponential back-off)
  - validate_trade() on every row; rejects go to dead-letter queue
  - safe_write_csv() for atomic, idempotent output write
  - Structured logging via get_logger()
"""

import argparse
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

# Project root so imports work when run from any directory
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_config
from utils.fault_tolerance import (
    get_logger,
    retry,
    validate_trade,
    write_dead_letter,
    safe_write_csv,
)

log = get_logger("fetch_binance")

# ============================================================
#  CONSTANTS
# ============================================================
BASE_URL = "https://api.binance.com"
AGG_TRADES_ENDPOINT = "/api/v3/aggTrades"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
MAX_LIMIT = 1000          # Binance hard cap per request
REQUEST_PAUSE = 0.2       # seconds between paginated requests (rate-limit headroom)


# ============================================================
#  FIELD MAPPING
# ============================================================
def _map_row(raw: dict, symbol: str) -> dict:
    """
    Map a single Binance aggTrade record to our schema.

    Binance aggTrade fields used:
      a  → aggregate trade ID (int)
      T  → trade time in ms epoch (int)
      p  → price (str)
      q  → quantity (str)
      m  → isBuyerMaker (bool)
             True  → buyer is maker → seller is taker → side = BUY  (taker bought)
             False → seller is maker → buyer is taker → side = SELL (taker sold)

    trader_id is NOT included — Binance public API exposes no trader identity.
    """
    ts_ms = int(raw["T"])
    ts_iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    side = "BUY" if raw["m"] else "SELL"
    trade_id = str(raw["a"])

    return {
        "trade_id":   trade_id,
        "timestamp":  ts_iso,
        "symbol":     symbol,
        "price":      raw["p"],
        "quantity":   raw["q"],
        "side":       side,
        # trader_id intentionally absent — no identity on public Binance API
        "order_id":   trade_id,   # proxy — aggTrade ID used as order ID
        "event_type": "TRADE",
    }


# ============================================================
#  HTTP FETCH (with retry)
# ============================================================
@retry(max_retries=3, base_delay=1.0, backoff_factor=2.0, exceptions=(Exception,))
def _fetch_page(symbol: str, start_ms: int, end_ms: int, from_id: int | None = None) -> list[dict]:
    """
    Fetch one page of aggTrades from Binance.

    Uses fromId-based pagination when from_id is provided (faster than
    time-based pagination on subsequent pages).
    """
    params: dict = {
        "symbol": symbol,
        "limit":  MAX_LIMIT,
    }

    if from_id is not None:
        # Paginate by ID — faster, avoids time-window re-scanning
        params["fromId"] = from_id
    else:
        # First page — use time window
        params["startTime"] = start_ms
        params["endTime"]   = end_ms

    resp = requests.get(BASE_URL + AGG_TRADES_ENDPOINT, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ============================================================
#  FULL SYMBOL FETCH (paginated)
# ============================================================
def fetch_symbol(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    """
    Fetch ALL aggTrades for `symbol` in [start_ms, end_ms].

    Paginates using fromId so we never miss trades even if a window
    contains more than 1 000 records.

    Returns a list of mapped + validated rows (bad rows go to dead-letter).
    """
    log.info("[%s] Fetching trades from %s to %s",
             symbol,
             datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
             datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat())

    all_rows: list[dict] = []
    from_id: int | None = None
    page = 0

    while True:
        page += 1
        raw_trades = _fetch_page(symbol, start_ms, end_ms, from_id=from_id)

        if not raw_trades:
            log.info("[%s] No more trades (page %d). Done.", symbol, page)
            break

        # Filter to time window when paginating by ID (Binance may return trades
        # slightly outside the window on subsequent pages)
        if from_id is not None:
            raw_trades = [t for t in raw_trades if start_ms <= int(t["T"]) <= end_ms]

        if not raw_trades:
            log.info("[%s] All trades on page %d are outside time window. Done.", symbol, page)
            break

        page_rows = 0
        for raw in raw_trades:
            mapped = _map_row(raw, symbol)
            valid, reason = validate_trade(mapped)
            if valid:
                all_rows.append(mapped)
                page_rows += 1
            else:
                log.debug("[%s] Rejected trade %s — %s", symbol, mapped.get("trade_id"), reason)
                write_dead_letter(mapped, reason)

        log.info("[%s] Page %d: %d valid trades (cumulative: %d)",
                 symbol, page, page_rows, len(all_rows))

        # Pagination: next fromId = last trade ID + 1
        last_id = int(raw_trades[-1]["a"])
        next_from_id = last_id + 1

        # Stop if we've exhausted the time window
        last_ts = int(raw_trades[-1]["T"])
        if last_ts >= end_ms:
            log.info("[%s] Reached end of time window at trade ID %d.", symbol, last_id)
            break

        # Stop if Binance returned fewer than MAX_LIMIT (no more pages)
        if len(raw_trades) < MAX_LIMIT:
            log.info("[%s] Received %d < %d trades — last page reached.",
                     symbol, len(raw_trades), MAX_LIMIT)
            break

        from_id = next_from_id
        time.sleep(REQUEST_PAUSE)   # be polite to Binance rate limits

    log.info("[%s] Total valid trades fetched: %d", symbol, len(all_rows))
    return all_rows


# ============================================================
#  MAIN
# ============================================================
def fetch_all(minutes: int = 30) -> pd.DataFrame:
    """
    Fetch the last `minutes` of aggTrades for all symbols.

    Returns a combined DataFrame and writes it to trades.csv.
    """
    cfg = get_config()
    output_path = cfg["trades_csv"]

    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - (minutes * 60 * 1000)

    log.info("=" * 60)
    log.info("Binance batch fetch — last %d minutes", minutes)
    log.info("Window: %s → %s",
             datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
             datetime.fromtimestamp(now_ms   / 1000, tz=timezone.utc).isoformat())
    log.info("Symbols: %s", SYMBOLS)
    log.info("Output:  %s", output_path)
    log.info("=" * 60)

    all_rows: list[dict] = []

    for symbol in SYMBOLS:
        try:
            rows = fetch_symbol(symbol, start_ms, now_ms)
            all_rows.extend(rows)
        except Exception as exc:
            # One symbol failing should NOT abort the whole fetch
            log.error("[%s] Failed to fetch — %s. Continuing with other symbols.", symbol, exc)

    if not all_rows:
        log.warning("No trades fetched from any symbol. trades.csv will be empty.")
        df = pd.DataFrame(columns=[
            "trade_id", "timestamp", "symbol", "price",
            "quantity", "side", "order_id", "event_type",
        ])
        safe_write_csv(df, output_path, logger=log)
        return df

    df = pd.DataFrame(all_rows)

    # Sort by timestamp ascending (ETL expects chronological order)
    df = df.sort_values("timestamp").reset_index(drop=True)

    log.info("-" * 60)
    log.info("TOTAL TRADES FETCHED: %d", len(df))
    log.info("Symbol breakdown:\n%s", df["symbol"].value_counts().to_string())
    log.info("Time range: %s → %s", df["timestamp"].min(), df["timestamp"].max())
    log.info("-" * 60)

    safe_write_csv(df, output_path, logger=log)
    return df


# ============================================================
#  CLI ENTRY POINT
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch recent Binance aggTrades and write to trades.csv"
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="How many minutes of history to pull (default: 30)",
    )
    args = parser.parse_args()

    df = fetch_all(minutes=args.minutes)
    print(f"\n✅ Done — {len(df):,} trades written to trades.csv")
