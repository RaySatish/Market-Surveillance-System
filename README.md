# Market Surveillance for Trade Abuse Detection

A **big-data analytics pipeline** that detects manipulative trading patterns in real financial market data. Pulls live trade data from the **Binance REST API**, processes it through **Apache Spark**, detects **wash trading** and **pump & dump** schemes, and surfaces results through an interactive **Streamlit dashboard**.

Runs fully on your local machine. Designed to scale to **AWS EMR + S3** with a single config change.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app.streamlit.app)

---

## Table of Contents

- [What It Detects](#what-it-detects)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Fault Tolerance](#fault-tolerance)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Streaming Mode (Phase 2)](#streaming-mode-phase-2)
- [Dashboard](#dashboard)
- [Detection Algorithms](#detection-algorithms)
- [Configuration](#configuration)
- [Future Roadmap](#future-roadmap)

---

## What It Detects

| Abuse Type | What It Is | How It's Detected |
|---|---|---|
| **Wash Trading** | Artificially inflating volume by trading with oneself or colluding parties | Rolling Z-score on trade volume per symbol; windows with statistically anomalous volume are flagged |
| **Pump & Dump** | Coordinated price inflation followed by a rapid sell-off | Consecutive time windows per symbol; flags PUMP (price spike + volume surge) followed by DUMP (price crash) |

---

## Architecture

### Phase 1 — Batch Pipeline

```
Binance REST API (fetch_binance.py)
        │
        ▼
   trades.csv  ◄── or generate_trades.py (synthetic, for dev/testing)
        │
        ▼
  Spark ETL (etl_trades.py)
  - Type casting & cleaning
  - Adds trade_value column
  - Writes Parquet partitioned by symbol
        │
        ▼
┌───────────────────┐    ┌──────────────────────┐
│ detect_wash_trades│    │ detect_pump_dump.py   │
│ (Z-score volume)  │    │ (price+volume windows)│
└────────┬──────────┘    └──────────┬────────────┘
         │                          │
         ▼                          ▼
   alerts_wash.csv         alerts_pump_dump.csv
              │            │
              ▼            ▼
          all_alerts.csv (combined)
                  │
                  ▼
          dashboard.py (Streamlit)
```

### Phase 2 — Streaming Pipeline (Kafka + Spark Structured Streaming)

```
Binance WebSocket (kafka_producer.py)
        │
        ▼
  Kafka topic: market-trades
  (Docker KRaft — single broker, no Zookeeper)
        │
        ▼
┌───────────────────────────┐    ┌──────────────────────────────┐
│ spark_streaming_wash.py   │    │ spark_streaming_pump_dump.py │
│ (micro-batch Z-score)     │    │ (micro-batch price windows)  │
└────────────┬──────────────┘    └───────────────┬──────────────┘
             │                                   │
             ▼                                   ▼
  streaming_wash_alerts.csv       streaming_pump_dump_alerts.csv
                    │                   │
                    ▼                   ▼
          stream_alerts_dashboard.py (Streamlit, auto-refresh)
```

**Two deployment modes** — controlled by `MODE` in `config.py`:
- **`"local"` (Phase 1 & 2, now):** Spark `local[*]`, local filesystem paths
- **`"aws"` (Phase 3):** EMR + S3 — same code, just change `MODE`

---

## Project Structure

```
Market-Surveillance-for-Trade-Abuse-Detection/
│
├── config.py                   # Central config: all paths, thresholds, MODE switch
├── run_all_detections.py       # Orchestrator: ETL → wash → pump&dump
├── dashboard.py                # Batch Streamlit dashboard (no Spark needed)
├── docker-compose.yml          # KRaft Kafka broker (Phase 2)
├── requirements.txt
│
├── ingestion/
│   ├── fetch_binance.py        # Pulls real data from Binance aggTrades REST API
│   ├── generate_trades.py      # Generates synthetic data (dev/testing only)
│   ├── ingest_to_hdfs.py       # Legacy HDFS ingestion (unused)
│   └── stream_binance.py       # Legacy WebSocket stub (unused)
│
├── etl/
│   ├── etl_trades.py           # Spark ETL: CSV → cleaned Parquet
│   ├── spark_utils.py          # SparkSession factory + Parquet reader
│   └── hdfs_utils.py           # Legacy HDFS utilities (unused)
│
├── detectors/
│   ├── detect_wash_trades.py   # Batch: Z-score volume anomaly detection
│   └── detect_pump_dump.py     # Batch: price spike + dump pattern detection
│
├── streaming/                  # Phase 2: Kafka + Spark Structured Streaming
│   ├── __init__.py
│   ├── kafka_producer.py       # Binance WebSocket → Kafka topic
│   ├── spark_streaming_wash.py         # Structured Streaming wash detector
│   ├── spark_streaming_pump_dump.py    # Structured Streaming P&D detector
│   ├── run_streaming_pipeline.py       # Orchestrator: start producer + consumers
│   └── stream_alerts_dashboard.py      # Streaming Streamlit dashboard (auto-refresh)
│
├── utils/
│   └── fault_tolerance.py      # Logging, retry, validation, safe writes, checkpoints
│
├── alerts/                     # Batch alert CSVs (committed — dashboard works without pipeline)
│   ├── alerts_wash.csv
│   ├── alerts_pump_dump.csv
│   ├── all_alerts.csv
│   ├── streaming_wash_alerts.csv       # Streaming alert output (Phase 2)
│   └── streaming_pump_dump_alerts.csv  # Streaming alert output (Phase 2)
│
├── models/                     # ML model artefacts (future)
├── tests/                      # Unit tests
├── data/                       # Parquet output (gitignored)
├── logs/                       # Rotating log files (gitignored)
├── dead_letter/                # Rejected/invalid rows (gitignored)
└── .checkpoints/               # Pipeline resume checkpoints (gitignored)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Data Source** | Binance REST API (`/api/v3/aggTrades`) + WebSocket (streaming) |
| **Big Data Processing** | Apache Spark (PySpark) — batch ETL + Structured Streaming |
| **Message Broker** | Apache Kafka (Docker KRaft — no Zookeeper) |
| **Storage** | Local filesystem (Phase 1 & 2) → AWS S3 (Phase 3) |
| **Detection Logic** | Pandas (batch) + Spark Structured Streaming (streaming) |
| **Dashboard** | Streamlit (batch: `dashboard.py`; streaming: `stream_alerts_dashboard.py`) |
| **Containerisation** | Docker Compose (Kafka broker) |
| **Cloud (Phase 3)** | AWS EMR + S3 |
| **Language** | Python 3.10+ |

---

## Fault Tolerance

Every pipeline stage follows the same pattern (implemented in `utils/fault_tolerance.py`):

| Mechanism | What It Does |
|---|---|
| **Retry + exponential backoff** | Wraps all Spark and I/O operations; retries on transient failures |
| **Row-level validation** | Checks all required fields and valid values; rejects bad rows to `dead_letter/` |
| **Atomic writes** | Writes to temp file → SHA-256 idempotency check → rename (no partial outputs) |
| **Checkpoints** | JSON checkpoints in `.checkpoints/` after each stage — supports `--resume` |
| **Isolated detector failures** | Each detector in `run_all_detections.py` is wrapped in try/except; one failure doesn't crash the pipeline |

---

## Getting Started

### Prerequisites

- Python 3.10+
- Apache Spark / PySpark installed separately (not in `requirements.txt`)
- Java 8 or 11 (required by Spark)
- Docker Desktop (required for Phase 2 Kafka)

### Install dependencies

```bash
git clone https://github.com/your-username/Market-Surveillance-for-Trade-Abuse-Detection.git
cd Market-Surveillance-for-Trade-Abuse-Detection
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install pyspark               # install separately
```

### Get trade data

**Option A — Real Binance data (recommended):**
```bash
python ingestion/fetch_binance.py
```
Fetches recent `aggTrades` for `BTCUSDT`, `ETHUSDT`, `SOLUSDT` and writes `trades.csv`.

**Option B — Synthetic data (dev/testing):**
```bash
python ingestion/generate_trades.py
```
Generates ~300K synthetic records with injected wash and pump&dump patterns. Note: synthetic data includes a `trader_id` column that real Binance data does not have.

---

## Running the Pipeline

### Batch Pipeline (Phase 1)

```bash
# Full run: ETL → wash detection → pump&dump detection
python run_all_detections.py

# Skip ETL (reuse existing Parquet), re-run detectors only
python run_all_detections.py --skip-etl

# Resume from last checkpoint (after a crash mid-run)
python run_all_detections.py --resume
```

#### Run individual detectors

```bash
python detectors/detect_wash_trades.py
python detectors/detect_pump_dump.py
```

---

## Streaming Mode (Phase 2)

Phase 2 adds near-real-time detection via Kafka and Spark Structured Streaming. Trades stream from Binance WebSocket → Kafka → Spark micro-batches → streaming alert CSVs → live Streamlit dashboard.

### Start Kafka

```bash
docker compose up -d
```

Starts a single KRaft Kafka broker (no Zookeeper) on `localhost:9092`. The `market-trades` topic is auto-created on first message.

### Run the streaming pipeline

```bash
# Full streaming run: producer + both detectors
python streaming/run_streaming_pipeline.py

# Test mode: inject synthetic trades instead of live WebSocket
python streaming/run_streaming_pipeline.py --test

# Producer only (no detectors)
python streaming/run_streaming_pipeline.py --producer-only

# Consumers only (detectors only, no producer)
python streaming/run_streaming_pipeline.py --consumers-only
```

### Streaming dashboard

```bash
streamlit run streaming/stream_alerts_dashboard.py
```

The streaming dashboard auto-refreshes every 5–60 seconds (configurable via sidebar). It reads from `alerts/streaming_wash_alerts.csv` and `alerts/streaming_pump_dump_alerts.csv` — no Spark dependency.

### Stop Kafka

```bash
docker compose down
```

---

## Dashboard

### Batch Dashboard

```bash
streamlit run dashboard.py
```

Reads directly from `trades.csv` and `alerts/*.csv` — **no Spark or HDFS required**. The `alerts/` directory is committed to the repo so the dashboard works immediately without running the pipeline.

**Sections:**
- **Alert Overview** — total alerts, breakdown by type and severity, data freshness window
- **Alert Timeline** — alerts per 1-minute bin over time
- **Wash Trade Alerts** — severity distribution, Z-score by symbol, raw alert table
- **Pump & Dump Alerts** — severity distribution, pump vs dump scatter, raw alert table
- **Volume Analysis** — buy vs sell volume by symbol + volume over time
- **Symbol Risk Summary** — weighted risk score per symbol (CRITICAL×3 + HIGH×2 + MEDIUM×1)

### Streaming Dashboard

```bash
streamlit run streaming/stream_alerts_dashboard.py
```

**Sections:**
- **Pipeline Status** — whether streaming alert files are present (live indicator)
- **Overview Metrics** — total alerts, wash count, P&D count, critical count, symbols flagged
- **Alert Timeline** — alerts per 1-minute bin, color-coded by type
- **Wash Trade Alerts** — severity pie, avg Z-score by symbol, raw table
- **Pump & Dump Alerts** — severity pie, pump% vs dump% scatter, raw table
- **Symbol Risk Summary** — per-symbol weighted risk score

---

## Detection Algorithms

### Wash Trading (Z-Score Volume Anomaly)

Since Binance's public API does not expose `trader_id`, classical wash trade detection (same buyer + seller) is not possible on real data. Instead, the detector uses **statistical volume anomaly detection**:

1. Compute a rolling baseline of trade volume per symbol (default: 5-minute window)
2. Calculate the Z-score of each window's volume against the baseline
3. Flag any window where Z-score > 3.0 as a potential wash trade cluster
4. Severity: `CRITICAL` (Z > 5), `HIGH` (Z > 4), `MEDIUM` (Z > 3)

When `trader_id` is present (synthetic/dev data), group-based detection (same trader, same symbol, both BUY and SELL in the same window) is also applied.

### Pump & Dump

1. Divide the timeline into rolling windows (default: 5 minutes) per symbol
2. **PUMP detection:** price change > 5% AND volume ratio > 3× baseline in the same window
3. **DUMP detection:** price reversal (negative change) following a confirmed PUMP window
4. Confirmed P&D: a PUMP window directly followed by a DUMP window
5. Severity: `CRITICAL` (price change > 15%), `HIGH` (> 10%), `MEDIUM` (> 5%)

---

## Configuration

All thresholds and paths live in `config.py` — never hardcoded in detector files.

```python
# Switch between local and AWS
MODE = "local"   # change to "aws" for EMR + S3

# Detection thresholds
DETECTION = {
    "wash_zscore_threshold": 3.0,    # Z-score cutoff for wash trade flagging
    "wash_rolling_window":   "5min", # rolling window for volume baseline

    "pd_window_minutes":  5,         # time window for pump&dump detection
    "pd_price_spike_pct": 5,         # minimum % price change to flag as PUMP
    "pd_volume_ratio":    3.0,       # volume must be 3× baseline to confirm PUMP
}
```

---

## Future Roadmap

### Phase 3 — AWS Cloud Scale
- Change `MODE = "aws"` in `config.py`
- Binance REST/WebSocket → S3
- Spark on EMR; alerts written back to S3
- Streamlit Cloud reads from S3 directly

### Future Detectors (requires private data feeds)
- **Spoofing** — needs private order-book access (cancelled orders), not available via public Binance API
- **Layering** — similar requirement to spoofing
- **Front-running** — requires microsecond-level order flow data

---

## License

MIT License — see [LICENSE](LICENSE)
