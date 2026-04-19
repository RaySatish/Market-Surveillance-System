#!/usr/bin/env bash
# ============================================================
# reproduce.sh — One-command reproduction script
# ============================================================
# Market Surveillance for Trade Abuse Detection
#
# Prerequisites:
#   - Python 3.10+
#   - Java 11+ (for PySpark)
#   - Docker Desktop (running)
#
# Usage:
#   chmod +x reproduce.sh
#   ./reproduce.sh              # full pipeline (live Binance data + dashboard)
#   ./reproduce.sh --test       # streaming with synthetic data (no API needed)
#   ./reproduce.sh --stop       # stop all services and clean up
#
# The script will:
#   1. Create a virtual environment and install dependencies
#   2. Start Docker services (Kafka + PostgreSQL)
#   3. Initialize the database schema
#   4. Run the streaming pipeline (Ctrl+C to stop)
#   5. Launch the Streamlit dashboard
# ============================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1"; exit 1; }
info() { echo -e "${BLUE}[→]${NC} $1"; }

MODE="${1:-}"

echo ""
echo "=============================================="
echo "  Market Surveillance — Reproduction Script"
echo "=============================================="
echo ""

# ── Handle --stop flag ──────────────────────────────────────
if [[ "$MODE" == "--stop" ]]; then
    warn "Stopping all services..."
    # Kill any running pipeline/dashboard processes
    pkill -f "run_streaming_pipeline" 2>/dev/null || true
    pkill -f "streamlit run" 2>/dev/null || true
    docker compose down 2>/dev/null || true
    log "All services stopped."
    exit 0
fi

# ── Step 1: Check prerequisites ──────────────────────────────

info "Checking prerequisites..."

# Python
if ! command -v python3 &>/dev/null; then
    err "Python 3 not found. Install Python 3.10+ first."
fi
PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
log "Python $PYTHON_VER found"

# Java (needed for PySpark)
if ! command -v java &>/dev/null; then
    warn "Java not found. PySpark requires Java 11+."
    warn "Install with: brew install openjdk@11  (macOS) or apt install openjdk-11-jdk (Linux)"
fi

# Docker
if ! command -v docker &>/dev/null; then
    err "Docker not found. Install Docker Desktop first."
fi
if ! docker info &>/dev/null 2>&1; then
    err "Docker daemon not running. Start Docker Desktop first."
fi
log "Docker is running"

# ── Step 2: Virtual environment + dependencies ───────────────

info "Setting up Python virtual environment..."

if [[ ! -d ".venv" ]]; then
    python3 -m venv .venv
    log "Created .venv"
else
    log ".venv already exists"
fi

source .venv/bin/activate
log "Activated .venv ($(python3 --version))"

info "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
log "All dependencies installed"

# ── Step 3: Start Docker services ────────────────────────────

echo ""
info "Starting Kafka + PostgreSQL via Docker Compose..."
docker compose up -d

# Wait for PostgreSQL
info "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U surveillance &>/dev/null 2>&1; then
        log "PostgreSQL is ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        err "PostgreSQL failed to start after 30 seconds"
    fi
    sleep 1
done

# Wait for Kafka
info "Waiting for Kafka to be ready..."
for i in $(seq 1 30); do
    if docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null 2>&1; then
        log "Kafka is ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        warn "Kafka may not be fully ready yet, continuing..."
    fi
    sleep 1
done

# ── Step 4: Initialize database ──────────────────────────────

info "Initializing database schema..."
python3 streaming/db.py --init
log "Database schema initialized (alert + sensitivity tables)"

# ── Step 5: Run streaming pipeline ───────────────────────────

PIPELINE_ARGS="--mode phase3"
if [[ "$MODE" == "--test" ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --test"
    info "Using synthetic test data (no Binance API needed)"
else
    PIPELINE_ARGS="$PIPELINE_ARGS --live"
    info "Using live Binance WebSocket data"
fi

echo ""
log "Starting streaming pipeline..."
info "Pipeline: Kafka Producer → Spark Detectors → Alert Consumer → PostgreSQL"
info "Press Ctrl+C to stop everything."
echo ""

# Start pipeline in background
python3 streaming/run_streaming_pipeline.py $PIPELINE_ARGS &
PIPELINE_PID=$!

# Give pipeline time to start producing data
info "Waiting 15 seconds for initial data to flow..."
sleep 15

# ── Step 6: Launch dashboard ─────────────────────────────────

log "Launching Streamlit dashboard..."
echo ""
echo "=============================================="
echo "  Dashboard: http://localhost:8501"
echo "  Press Ctrl+C to stop pipeline + dashboard"
echo "=============================================="
echo ""

# Trap Ctrl+C to clean up
cleanup() {
    echo ""
    warn "Shutting down..."
    kill $PIPELINE_PID 2>/dev/null || true
    wait $PIPELINE_PID 2>/dev/null || true
    info "Stopping Docker services..."
    docker compose down
    log "All services stopped."
}
trap cleanup INT TERM

streamlit run streaming/stream_alerts_dashboard.py

# If streamlit exits normally, clean up
cleanup
