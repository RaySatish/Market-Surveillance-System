"""
MARKET SURVEILLANCE DASHBOARD
==============================
A Streamlit web dashboard that visualizes:
  1. Alert summary (counts by type and severity)
  2. Trade volume over time
  3. Price charts with abuse events highlighted
  4. Trader risk scoreboard

Usage:
  streamlit run dashboard.py

Reads from:
  - trades.csv  (raw trade data)
  - alerts/     (CSVs — output of detection pipeline)

Deployment:
  - Works on Streamlit Cloud (no Spark/HDFS needed)
  - Commit trades.csv and alerts/ to your repo
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# ---------- paths (no Spark / HDFS needed) ----------
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
TRADES_CSV     = os.path.join(BASE_DIR, "trades.csv")
ALERTS_DIR     = os.path.join(BASE_DIR, "alerts")
ALERTS_WASH    = os.path.join(ALERTS_DIR, "alerts_wash.csv")
ALERTS_PD      = os.path.join(ALERTS_DIR, "alerts_pump_dump.csv")
ALERTS_SPOOF   = os.path.join(ALERTS_DIR, "alerts_spoofing.csv")
ALERTS_ALL     = os.path.join(ALERTS_DIR, "all_alerts.csv")

# ========== PAGE CONFIG ==========
st.set_page_config(
    page_title="Market Surveillance Dashboard",
    page_icon="🔍",
    layout="wide"
)

st.title("Market Surveillance — Trade Abuse Detection")
st.markdown("Real-time monitoring of wash trades, pump & dump schemes, and spoofing activity.")


# ========== LOAD DATA ==========
@st.cache_data
def load_trades():
    """Load trade data from local CSV (works on Streamlit Cloud)."""
    if os.path.exists(TRADES_CSV):
        df = pd.read_csv(TRADES_CSV)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    return pd.DataFrame()


@st.cache_data
def load_alerts(filename):
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            return pd.read_csv(filename)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    return pd.DataFrame()


# Load everything
trades = load_trades()
wash_alerts = load_alerts(ALERTS_WASH)
pd_alerts = load_alerts(ALERTS_PD)
spoof_alerts = load_alerts(ALERTS_SPOOF)
all_alerts = load_alerts(ALERTS_ALL)

# Check if detections have been run
if all_alerts.empty and wash_alerts.empty and pd_alerts.empty and spoof_alerts.empty:
    st.warning("No alert files found. Run `python run_all_detections.py` first, then refresh.")
    st.stop()


# ========== SECTION 1: KEY METRICS ==========
# Display big numbers at the top using Streamlit "metric" cards
st.header("Overview")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Trades", f"{len(trades):,}")
with col2:
    st.metric("Wash Trade Alerts", len(wash_alerts) if not wash_alerts.empty else 0)
with col3:
    st.metric("Pump & Dump Alerts", len(pd_alerts) if not pd_alerts.empty else 0)
with col4:
    st.metric("Spoofing Alerts", len(spoof_alerts) if not spoof_alerts.empty else 0)
with col5:
    total_alerts = len(all_alerts) if not all_alerts.empty else 0
    st.metric("Total Alerts", total_alerts)


# ========== SECTION 2: ALERT SEVERITY BREAKDOWN ==========
st.header("Alert Severity Distribution")

if not all_alerts.empty:
    col_left, col_right = st.columns(2)

    with col_left:
        # Pie chart: alerts by type
        type_counts = all_alerts["alert_type"].value_counts().reset_index()
        type_counts.columns = ["Alert Type", "Count"]
        fig_type = px.pie(
            type_counts, names="Alert Type", values="Count",
            title="Alerts by Type",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        st.plotly_chart(fig_type, use_container_width=True)

    with col_right:
        # Bar chart: alerts by severity
        sev_counts = all_alerts["severity"].value_counts().reset_index()
        sev_counts.columns = ["Severity", "Count"]
        color_map = {"CRITICAL": "#FF4444", "HIGH": "#FF8800", "MEDIUM": "#FFCC00"}
        fig_sev = px.bar(
            sev_counts, x="Severity", y="Count",
            title="Alerts by Severity",
            color="Severity",
            color_discrete_map=color_map
        )
        st.plotly_chart(fig_sev, use_container_width=True)


# ========== SECTION 3: PRICE CHART WITH ABUSE MARKERS ==========
st.header("Price Charts with Abuse Events")

# Let the user pick which symbol to view
symbol = st.selectbox("Select Symbol", trades["symbol"].unique())
sym_trades = trades[trades["symbol"] == symbol].copy()

# Create a price chart using Plotly
fig_price = go.Figure()

# Normal trades as a line
normal = sym_trades[sym_trades["event_type"] == "TRADE"]
fig_price.add_trace(go.Scatter(
    x=normal["timestamp"], y=normal["price"],
    mode="lines", name="Normal Price",
    line=dict(color="#4A90D9", width=1)
))

# Overlay abuse events as colored markers
abuse_types = {
    "WASH":      {"color": "orange",  "symbol": "diamond",      "size": 8},
    "PUMP":      {"color": "red",     "symbol": "triangle-up",  "size": 10},
    "DUMP":      {"color": "purple",  "symbol": "triangle-down","size": 10},
    "CANCELLED": {"color": "gray",    "symbol": "x",            "size": 7},
}

for etype, style in abuse_types.items():
    abuse_df = sym_trades[sym_trades["event_type"] == etype]
    if not abuse_df.empty:
        fig_price.add_trace(go.Scatter(
            x=abuse_df["timestamp"], y=abuse_df["price"],
            mode="markers", name=etype,
            marker=dict(
                color=style["color"],
                symbol=style["symbol"],
                size=style["size"]
            )
        ))

fig_price.update_layout(
    title=f"{symbol} — Price with Abuse Events",
    xaxis_title="Time",
    yaxis_title="Price",
    height=500
)
st.plotly_chart(fig_price, use_container_width=True)


# ========== SECTION 4: VOLUME ANALYSIS ==========
st.header("Volume Analysis")

col_v1, col_v2 = st.columns(2)

with col_v1:
    # Volume by event type (how much volume is "suspicious"?)
    vol_by_type = trades.groupby("event_type")["quantity"].sum().reset_index()
    vol_by_type.columns = ["Event Type", "Total Volume"]
    fig_vol = px.bar(
        vol_by_type, x="Event Type", y="Total Volume",
        title="Trading Volume by Event Type",
        color="Event Type",
        color_discrete_map={
            "TRADE": "#4A90D9",
            "WASH": "#FFA500",
            "PUMP": "#FF4444",
            "DUMP": "#8B00FF",
            "CANCELLED": "#888888"
        }
    )
    st.plotly_chart(fig_vol, use_container_width=True)

with col_v2:
    # Volume over time (5-minute buckets)
    vol_time = trades.set_index("timestamp").resample("5min")["quantity"].sum().reset_index()
    vol_time.columns = ["Time", "Volume"]
    fig_vol_time = px.area(
        vol_time, x="Time", y="Volume",
        title="Trading Volume Over Time (5-min buckets)"
    )
    st.plotly_chart(fig_vol_time, use_container_width=True)


# ========== SECTION 5: TRADER RISK SCOREBOARD ==========
st.header("Trader Risk Scoreboard")

# Build a risk profile per trader
trader_stats = trades.groupby("trader_id").agg(
    total_trades=("trade_id", "count"),
    total_volume=("quantity", "sum"),
    wash_count=("event_type", lambda x: (x == "WASH").sum()),
    pump_count=("event_type", lambda x: (x == "PUMP").sum()),
    dump_count=("event_type", lambda x: (x == "DUMP").sum()),
    cancel_count=("event_type", lambda x: (x == "CANCELLED").sum()),
).reset_index()

# Calculate a simple risk score (higher = more suspicious)
# Weighted sum: wash trades are bad, pump/dump is worse, cancellations add up
trader_stats["risk_score"] = (
    trader_stats["wash_count"] * 3 +
    trader_stats["pump_count"] * 5 +
    trader_stats["dump_count"] * 5 +
    trader_stats["cancel_count"] * 2
)

# Sort by risk score descending and show top offenders
top_risk = trader_stats.sort_values("risk_score", ascending=False).head(20)

if top_risk["risk_score"].max() > 0:
    fig_risk = px.bar(
        top_risk, x="trader_id", y="risk_score",
        title="Top 20 Riskiest Traders",
        color="risk_score",
        color_continuous_scale="Reds"
    )
    st.plotly_chart(fig_risk, use_container_width=True)

    # Show the full table
    st.dataframe(top_risk, use_container_width=True)
else:
    st.info("No risky traders detected.")


# ========== SECTION 6: RAW ALERT TABLES ==========
st.header("Raw Alert Data")

tab1, tab2, tab3 = st.tabs(["Wash Trades", "Pump & Dump", "Spoofing"])

with tab1:
    if not wash_alerts.empty:
        st.dataframe(wash_alerts, use_container_width=True)
    else:
        st.info("No wash trade alerts.")

with tab2:
    if not pd_alerts.empty:
        st.dataframe(pd_alerts, use_container_width=True)
    else:
        st.info("No pump & dump alerts.")

with tab3:
    if not spoof_alerts.empty:
        st.dataframe(spoof_alerts, use_container_width=True)
    else:
        st.info("No spoofing alerts.")


# ========== FOOTER ==========
st.markdown("---")
st.markdown(
    f"*Dashboard generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
    f"| {len(trades):,} trades analyzed*"
)
