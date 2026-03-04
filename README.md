# Market Surveillance for Trade Abuse Detection

## About

Market Surveillance for Trade Abuse Detection is a **big-data analytics pipeline** built to identify manipulative trading patterns in financial markets. It processes **200K+ trade records** using **Apache Spark** and **Hadoop HDFS**, detects three major abuse types — **wash trading**, **pump & dump**, and **spoofing** — and surfaces results through an interactive **Streamlit dashboard** with real-time charts, severity-ranked alerts, and trader risk scores.

Built as a production-ready prototype that runs locally and is designed to scale to **AWS EMR** with a single config change.

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
- [Dashboard](#dashboard)
- [Detection Algorithms](#detection-algorithms)
- [Configuration](#configuration)
- [Future Roadmap (AWS Phase 2)](#future-roadmap-aws-phase-2)

---

## What It Detects

| Abuse Type       | What It Is                                                                      | How We Detect It                                                              |
| ---------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| **Wash Trading** | Same trader buys AND sells the same asset at the same price/time to fake volume | Group by `(trader, symbol, price, time)` → check for both BUY + SELL sides    |
| **Pump & Dump**  | Artificially inflate price with large buys, then dump at the peak               | Rolling 5-min windows → detect price spikes ≥5% with volume imbalance ≥3:1    |
| **Spoofing**     | Place large fake orders to manipulate perception, then cancel before execution  | Per-trader cancellation rate >50% AND cancelled order sizes >2× executed size |

---

## Architecture

```
┌─────────────────────┐
│  generate_trades.py  │   Synthetic trade data (200K+ records)
│  stream_binance.py   │   Live Binance WebSocket (Phase 2)
└────────┬────────────┘
         │  trades.csv
         ▼
┌─────────────────────┐
│  ingest_to_hdfs.py   │   Validate rows → Upload to HDFS (replication=3)
│                      │   Bad rows → dead_letter/rejected_trades.csv
└────────┬────────────┘
         │  hdfs://localhost:9000/market/raw/
         ▼
┌─────────────────────┐
│  etl_trades.py       │   Spark ETL: CSV → clean → Parquet
│  (PySpark)           │   Atomic staging write + rename
└────────┬────────────┘
         │  hdfs://localhost:9000/market/clean/trades/ (Parquet)
         ▼
┌─────────────────────────────────────────────┐
│  detect_wash_trades.py                       │
│  detect_pump_dump.py     Detection Layer     │
│  detect_spoofing.py      (Pandas + Spark)    │
│  ─── retry + idempotent atomic CSV writes ── │
└────────┬────────────────────────────────────┘
         │  alerts/*.csv
         ▼
┌─────────────────────┐
│  dashboard.py        │   Streamlit interactive dashboard
│  (Streamlit + Plotly)│   Deployed on Streamlit Cloud
└─────────────────────┘

         ┌──────────────────────────────────┐
         │  utils/fault_tolerance.py         │  Cross-cutting concerns:
         │  ── logging (rotating files)      │  retry, validation, DLQ,
         │  ── retry with exp. backoff       │  checkpointing, idempotent
         │  ── data validation + DLQ         │  writes
         │  ── checkpoint / resume           │
         └──────────────────────────────────┘
```

---

## Project Structure

```
├── README.md
├── requirements.txt
├── config.py                          # Central configuration (paths, thresholds, mode)
├── run_all_detections.py              # Master script — runs full pipeline end-to-end
├── dashboard.py                       # Streamlit dashboard (deployed on Streamlit Cloud)
├── trades.csv                         # Raw synthetic trade data (~200K+ rows)
│
├── ingestion/                         # Data generation & ingestion layer
│   ├── __init__.py
│   ├── generate_trades.py             #   Generate synthetic trades with injected abuse
│   ├── ingest_to_hdfs.py              #   Validate + upload raw CSV to HDFS (replication=3)
│   └── stream_binance.py              #   Binance WebSocket live ingestion (auto-reconnect)
│
├── etl/                               # Extract–Transform–Load layer
│   ├── __init__.py
│   ├── etl_trades.py                  #   Spark ETL: CSV → clean → Parquet (atomic writes)
│   └── hdfs_utils.py                  #   HDFS/Spark helpers (retry-wrapped)
│
├── detectors/                         # Abuse detection algorithms
│   ├── __init__.py
│   ├── detect_wash_trades.py          #   Wash trade detection
│   ├── detect_pump_dump.py            #   Pump & dump detection
│   └── detect_spoofing.py             #   Spoofing detection
│
├── utils/                             # Shared fault-tolerance utilities
│   ├── __init__.py
│   └── fault_tolerance.py             #   Logging, retry, validation, DLQ, checkpoints
│
├── alerts/                            # Detection output (generated CSVs)
│   ├── alerts_wash.csv
│   ├── alerts_pump_dump.csv
│   ├── alerts_spoofing.csv
│   └── all_alerts.csv
│
├── logs/                              # Rotating log files (auto-generated, .gitignored)
├── dead_letter/                       # Rejected rows for auditing (.gitignored)
├── .checkpoints/                      # Pipeline resume state (.gitignored)
│
└── data/                              # Cleaned Parquet output (.gitignored)
    └── clean/trades/                  #   Partitioned by symbol
```

---

## Tech Stack

| Layer               | Technology             | Why                                                                         |
| ------------------- | ---------------------- | --------------------------------------------------------------------------- |
| **Data Generation** | Python `csv`, `random` | Synthetic trades with realistic abuse patterns injected                     |
| **Storage**         | Hadoop HDFS            | Distributed filesystem — mirrors production architecture                    |
| **ETL**             | Apache Spark (PySpark) | Distributes processing across cores; scales from laptop to 100-node cluster |
| **Data Format**     | Parquet (columnar)     | 5–10× compression vs CSV; 10–100× faster column scans                       |
| **Detection**       | Pandas + NumPy         | Full-dataset analysis for pattern matching                                  |
| **Dashboard**       | Streamlit + Plotly     | Interactive web UI with real-time charts                                    |
| **Deployment**      | Streamlit Cloud        | Free hosting, auto-deploys from GitHub                                      |

---

## Fault Tolerance

The pipeline is built with production-grade resilience:

| Mechanism                          | Description                                                                                                 | Where                                    |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| **Retry with Exponential Backoff** | Failed operations retry up to 3× with increasing delays (1s → 2s → 4s)                                      | HDFS ingestion, Spark ETL, Parquet reads |
| **HDFS Replication Factor = 3**    | Every HDFS block is stored on 3 datanodes for redundancy                                                    | `config.py` → `HDFS_REPLICATION_FACTOR`  |
| **Data Validation**                | Every trade row is validated (positive price, valid side/symbol/timestamp) before HDFS upload               | `ingest_to_hdfs.py`, `stream_binance.py` |
| **Dead Letter Queue**              | Invalid/rejected rows are persisted to `dead_letter/rejected_trades.csv` for auditing                       | `utils/fault_tolerance.py`               |
| **Atomic/Idempotent CSV Writes**   | Alert CSVs are written to a `.tmp` file first, then atomically renamed; SHA-256 dedup skips unchanged files | All detectors                            |
| **Atomic Parquet Writes**          | Spark writes to a staging directory, then renames to the final path — prevents partial-write corruption     | `etl_trades.py`                          |
| **Pipeline Checkpointing**         | Each stage saves a checkpoint; use `--resume` to skip completed stages after a crash                        | `run_all_detections.py`                  |
| **Detector Isolation**             | One failing detector doesn't crash the whole pipeline — errors are logged and execution continues           | `run_all_detections.py`                  |
| **WebSocket Auto-Reconnect**       | Binance stream reconnects up to 10× with exponential backoff on disconnect                                  | `stream_binance.py`                      |
| **Structured Logging**             | Rotating file + console logger (5 MB, 5 backups) replaces all `print()` calls                               | `utils/fault_tolerance.py` → `logs/`     |

## Getting Started

### Prerequisites

- **Python 3.9+**
- **Java 11+** (required by Spark)
- **Apache Hadoop** (HDFS) — for the full pipeline
- **Apache Spark** (PySpark) — for the ETL layer

### Installation

```bash
# Clone the repo
git clone https://github.com/your-username/Market-Surveillance-for-Trade-Abuse-Detection.git
cd Market-Surveillance-for-Trade-Abuse-Detection

# Install Python dependencies
pip install -r requirements.txt
```

### Verify Spark & Hadoop

```bash
java -version          # Should show Java 11+
spark-submit --version # Should show Spark 3.x
hdfs version           # Should show Hadoop 3.x
```

---

## Running the Pipeline

### Option 1: Full Pipeline (Recommended)

```bash
# 1. Start HDFS
start-dfs.sh

# 2. Generate synthetic trade data
python generate_trades.py

# 3. Run the complete pipeline (ingest → ETL → all detectors)
python run_all_detections.py
```

### Option 2: Step by Step

```bash
# Generate trades
python -m ingestion.generate_trades    # → trades.csv

# Upload to HDFS
python -m ingestion.ingest_to_hdfs     # → hdfs:///market/raw/trades.csv

# Run Spark ETL
python -m etl.etl_trades               # → hdfs:///market/clean/trades/ (Parquet)

# Run individual detectors
python -m detectors.detect_wash_trades  # → alerts/alerts_wash.csv
python -m detectors.detect_pump_dump    # → alerts/alerts_pump_dump.csv
python -m detectors.detect_spoofing     # → alerts/alerts_spoofing.csv
```

### Option 3: Skip ETL (if Parquet already exists)

```bash
python run_all_detections.py --skip-etl
```

### Option 4: Resume After a Crash

```bash
# If the pipeline failed mid-run, resume from the last completed stage
python run_all_detections.py --resume
```

---

## Dashboard

### Run Locally

```bash
streamlit run dashboard.py
```

Opens at **http://localhost:8501**

### Deploy on Streamlit Cloud

1. Push your repo to GitHub (including `trades.csv` and `alerts/`)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select your repo → set main file to `dashboard.py`
4. Click **Deploy**

### Dashboard Sections

| Section                    | What It Shows                                                       |
| -------------------------- | ------------------------------------------------------------------- |
| **Overview**               | Total trades, alert counts, key metrics                             |
| **Alert Severity**         | Pie chart by type, bar chart by severity (CRITICAL / HIGH / MEDIUM) |
| **Price Charts**           | Price over time with abuse events overlaid as colored markers       |
| **Volume Analysis**        | Volume by event type + volume over time (5-min buckets)             |
| **Trader Risk Scoreboard** | Top 20 riskiest traders ranked by weighted risk score               |
| **Raw Alert Tables**       | Filterable tables for each alert type                               |

---

## Detection Algorithms

### Wash Trade Detection

```
Group trades by (trader_id, symbol, timestamp, price)
   → If same trader has BOTH BUY and SELL in the group → WASH TRADE
   → Severity: MEDIUM (all wash trades)
```

### Pump & Dump Detection

```
For each symbol, use a rolling 5-minute window:
   1. Calculate price change (%) from start to peak
   2. Calculate buy/sell volume ratio
   3. If price_change ≥ 5% AND buy_volume / sell_volume ≥ 3.0 → PUMP
   4. If price drops after pump AND sell volume dominates → DUMP
   → Severity: CRITICAL (pump+dump pair), HIGH (pump only)
```

### Spoofing Detection

```
For each trader:
   1. Count total orders and cancelled orders
   2. cancellation_rate = cancelled / total
   3. Compare avg size of cancelled vs executed orders
   4. If cancel_rate > 50% AND avg_cancelled_size > 2× avg_executed → SPOOFER
   → Severity: HIGH (high cancel rate), CRITICAL (extreme size mismatch)
```

---

## Configuration

All settings are centralized in `config.py`:

### Detection Thresholds

| Parameter                 | Default | Description                                          |
| ------------------------- | ------- | ---------------------------------------------------- |
| `wash_min_group_size`     | 2       | Minimum trades in a group to flag as wash            |
| `pd_window_minutes`       | 5       | Rolling window size for pump & dump detection        |
| `pd_price_spike_pct`      | 5       | Minimum price spike (5%) to flag as pump             |
| `pd_volume_ratio`         | 3.0     | Minimum buy/sell volume ratio for pump signal        |
| `spoof_cancel_rate`       | 0.5     | Cancellation rate threshold (50%)                    |
| `spoof_min_orders`        | 3       | Minimum orders before evaluating a trader            |
| `spoof_size_multiplier`   | 2.0     | How much larger cancelled orders must be vs executed |
| `HDFS_REPLICATION_FACTOR` | 3       | Number of HDFS block replicas for fault tolerance    |

### Data Paths

| Setting      | Value                                         |
| ------------ | --------------------------------------------- |
| Raw input    | `hdfs://localhost:9000/market/raw/trades.csv` |
| Clean output | `hdfs://localhost:9000/market/clean/trades`   |
| Alert CSVs   | `alerts/` directory (local)                   |

---

## Future Roadmap (AWS Phase 2)

The pipeline is **designed to scale** to production on AWS with minimal code changes:

| Component       | Local (Phase 1)                       | AWS (Phase 2)                     |
| --------------- | ------------------------------------- | --------------------------------- |
| **Data Source** | `generate_trades.py` (synthetic)      | Binance WebSocket API (real-time) |
| **Storage**     | HDFS on localhost                     | Amazon S3 via EMRFS               |
| **Compute**     | Spark `local[*]`                      | Spark on AWS EMR (YARN cluster)   |
| **Streaming**   | `stream_binance.py --test`            | Kafka / Kinesis → Spark Streaming |
| **Dashboard**   | Streamlit Cloud                       | Streamlit Cloud / EC2             |
| **Alerting**    | CSV files                             | SNS / CloudWatch / PagerDuty      |
| **Scheduling**  | Manual `python run_all_detections.py` | Apache Airflow / Step Functions   |

To switch: change `MODE = "aws"` in `config.py` and configure your S3 bucket path.

---

## Sample Output

**Wash Trade Alert:**

```
alert_type: WASH_TRADE
trader_id: T0001
symbol: BTCUSDT
price: 42002.24
total_quantity: 48
severity: MEDIUM
```

**Pump & Dump Alert:**

```
alert_type: PUMP_AND_DUMP
symbol: ETHUSDT
price_change_pct: 7.2%
volume_ratio: 4.5
severity: CRITICAL
```

**Spoofing Alert:**

```
alert_type: SPOOFING
trader_id: T0310
cancel_rate: 0.73
avg_cancel_size: 142
severity: HIGH
```

---

## License

This project is for **educational and research purposes**.

---

_Built with Apache Spark, Hadoop HDFS, Streamlit, and Python._
