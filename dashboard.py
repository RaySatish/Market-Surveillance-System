"""
MARKET SURVEILLANCE DASHBOARD
==============================
A Streamlit web dashboard that visualizes:
  1. Alert summary (counts by type and severity)
  2. Price charts with alert event overlays (from alert CSVs)
  3. Buy vs Sell volume analysis per symbol
  4. Symbol-level risk summary (replaces trader scoreboard)
  5. Raw alert tables with real-data-aware column display

Usage:
  streamlit run dashboard.py

Reads from:
  - trades.csv  (raw trade data — from fetch_binance.py)
  - alerts/     (CSVs — output of detection pipeline)

Detectors supported:
  - Wash Trade Detection   (Z-score on rolling volume per symbol)
  - Pump & Dump Detection  (rolling time-window price spike + volume imbalance)

NOTE: Real Binance aggTrades data has NO trader_id.
  The Trader Risk Scoreboard has been replaced with a Symbol Risk Summary.
  All event_type values in trades.csv are "TRADE" — abuse events are
  identified via the alert CSVs, not the trades themselves.

Deployment:
  - Works on Streamlit Cloud (no Spark needed)
  - Commit trades.csv and alerts/ to your repo
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# ---------- paths (no Spark needed) ----------
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
TRADES_CSV  = os.path.join(BASE_DIR, "trades.csv")
ALERTS_DIR  = os.path.join(BASE_DIR, "alerts")
ALERTS_WASH = os.path.join(ALERTS_DIR, "alerts_wash.csv")
ALERTS_PD   = os.path.join(ALERTS_DIR, "alerts_pump_dump.csv")
ALERTS_ALL  = os.path.join(ALERTS_DIR, "all_alerts.csv")

# ========== PAGE CONFIG ==========
st.set_page_config(
    page_title="Market Surveillance Dashboard",
    page_icon="🔍",
    layout="wide"
)

st.title("Market Surveillance — Trade Abuse Detection")
st.markdown(
    "Real-time monitoring of **wash trades** and **pump & dump** schemes. "
    "Data sourced from Binance public aggTrades API (BTCUSDT · ETHUSDT · SOLUSDT)."
)


# ========== LOAD DATA ==========
@st.cache_data
def load_trades():
    """Load trade data from local CSV."""
    if os.path.exists(TRADES_CSV):
        df = pd.read_csv(TRADES_CSV)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    return pd.DataFrame()


@st.cache_data
def load_alerts(filename):
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            df = pd.read_csv(filename)
            return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    return pd.DataFrame()


# Load everything
trades      = load_trades()
wash_alerts = load_alerts(ALERTS_WASH)
pd_alerts   = load_alerts(ALERTS_PD)
all_alerts  = load_alerts(ALERTS_ALL)

# Check if detections have been run
if all_alerts.empty and wash_alerts.empty and pd_alerts.empty:
    st.warning("No alert files found. Run `python run_all_detections.py` first, then refresh.")
    st.stop()

# Parse timestamps in alert files
if not wash_alerts.empty and "timestamp" in wash_alerts.columns:
    wash_alerts["timestamp"] = pd.to_datetime(wash_alerts["timestamp"])

if not pd_alerts.empty:
    for col in ["first_bar_time", "second_bar_time"]:
        if col in pd_alerts.columns:
            pd_alerts[col] = pd.to_datetime(pd_alerts[col])

if not all_alerts.empty and "detected_at" in all_alerts.columns:
    all_alerts["detected_at"] = pd.to_datetime(all_alerts["detected_at"])


# ========== DATA FRESHNESS ==========
if not trades.empty:
    data_start = trades["timestamp"].min()
    data_end   = trades["timestamp"].max()
    data_span  = data_end - data_start
    st.caption(
        f"📡 **Data window:** {data_start.strftime('%Y-%m-%d %H:%M')} → "
        f"{data_end.strftime('%Y-%m-%d %H:%M')} UTC  "
        f"({int(data_span.total_seconds() // 60)} min of trades)"
    )


# ========== SECTION 1: KEY METRICS ==========
st.header("Overview")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Trades", f"{len(trades):,}")
with col2:
    n_wash = len(wash_alerts) if not wash_alerts.empty else 0
    st.metric("Wash Trade Alerts", n_wash)
with col3:
    n_pd = len(pd_alerts) if not pd_alerts.empty else 0
    st.metric("Pump & Dump Alerts", n_pd)
with col4:
    total_alerts = len(all_alerts) if not all_alerts.empty else 0
    st.metric("Total Alerts", total_alerts)
with col5:
    if not all_alerts.empty and "severity" in all_alerts.columns:
        n_critical = (all_alerts["severity"] == "CRITICAL").sum()
        st.metric("🔴 Critical Alerts", int(n_critical))
    else:
        st.metric("🔴 Critical Alerts", 0)


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
        sev_order = ["CRITICAL", "HIGH", "MEDIUM"]
        sev_counts = all_alerts["severity"].value_counts().reindex(sev_order, fill_value=0).reset_index()
        sev_counts.columns = ["Severity", "Count"]
        color_map = {"CRITICAL": "#FF4444", "HIGH": "#FF8800", "MEDIUM": "#FFCC00"}
        fig_sev = px.bar(
            sev_counts, x="Severity", y="Count",
            title="Alerts by Severity",
            color="Severity",
            color_discrete_map=color_map
        )
        st.plotly_chart(fig_sev, use_container_width=True)


# ========== SECTION 3: PRICE CHART WITH ALERT OVERLAYS ==========
st.header("Price Charts with Abuse Event Overlays")

st.caption(
    "Abuse events are overlaid from the alert files (not from trades.csv). "
    "Real Binance data has `event_type = TRADE` for all rows — abuse is identified by the detectors."
)

if trades.empty:
    st.info("No trade data loaded.")
else:
    symbol = st.selectbox("Select Symbol", sorted(trades["symbol"].unique()))
    sym_trades = trades[trades["symbol"] == symbol].copy()

    # Resample to 1-second OHLC for a cleaner price line
    sym_trades = sym_trades.sort_values("timestamp")

    fig_price = go.Figure()

    # Price line (all trades are TRADE type in real data)
    fig_price.add_trace(go.Scatter(
        x=sym_trades["timestamp"],
        y=sym_trades["price"],
        mode="lines",
        name="Price",
        line=dict(color="#4A90D9", width=1),
        opacity=0.8
    ))

    # Overlay wash trade alerts for this symbol
    if not wash_alerts.empty and "symbol" in wash_alerts.columns and "timestamp" in wash_alerts.columns:
        sym_wash = wash_alerts[wash_alerts["symbol"] == symbol]
        if not sym_wash.empty:
            fig_price.add_trace(go.Scatter(
                x=sym_wash["timestamp"],
                y=sym_wash["price"],
                mode="markers",
                name="Wash Trade Alert",
                marker=dict(color="orange", symbol="diamond", size=9),
                hovertemplate=(
                    "<b>WASH TRADE</b><br>"
                    "Time: %{x}<br>"
                    "Price: %{y}<br>"
                    "Z-Score: " + sym_wash["z_score"].astype(str) + "<br>"
                    "Severity: " + sym_wash["severity"] + "<extra></extra>"
                ) if "z_score" in sym_wash.columns else None
            ))

    # Overlay pump & dump alerts for this symbol
    if not pd_alerts.empty and "symbol" in pd_alerts.columns:
        sym_pd = pd_alerts[pd_alerts["symbol"] == symbol]

        pump_rows = sym_pd[sym_pd["alert_type"].isin(["PUMP_AND_DUMP"])] if "alert_type" in sym_pd.columns else pd.DataFrame()
        dump_rows = sym_pd[sym_pd["alert_type"].isin(["DUMP_AND_PUMP"])] if "alert_type" in sym_pd.columns else pd.DataFrame()

        if not pump_rows.empty and "first_bar_time" in pump_rows.columns and "peak_price" in pump_rows.columns:
            fig_price.add_trace(go.Scatter(
                x=pump_rows["first_bar_time"],
                y=pump_rows["peak_price"],
                mode="markers",
                name="Pump & Dump",
                marker=dict(color="red", symbol="triangle-up", size=12),
                hovertemplate=(
                    "<b>PUMP & DUMP</b><br>"
                    "Time: %{x}<br>"
                    "Peak Price: %{y}<br>"
                    "Price Chg: " + pump_rows["first_price_chg"].map("{:+.3f}%".format) + "<extra></extra>"
                ) if "first_price_chg" in pump_rows.columns else None
            ))

        if not dump_rows.empty and "first_bar_time" in dump_rows.columns and "trough_price" in dump_rows.columns:
            fig_price.add_trace(go.Scatter(
                x=dump_rows["first_bar_time"],
                y=dump_rows["trough_price"],
                mode="markers",
                name="Dump & Pump",
                marker=dict(color="purple", symbol="triangle-down", size=12),
                hovertemplate=(
                    "<b>DUMP & PUMP</b><br>"
                    "Time: %{x}<br>"
                    "Trough Price: %{y}<br>"
                    "Price Chg: " + dump_rows["first_price_chg"].map("{:+.3f}%".format) + "<extra></extra>"
                ) if "first_price_chg" in dump_rows.columns else None
            ))

    fig_price.update_layout(
        title=f"{symbol} — Price with Detected Abuse Events",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_price, use_container_width=True)


# ========== SECTION 4: VOLUME ANALYSIS ==========
st.header("Volume Analysis")

if not trades.empty:
    col_v1, col_v2 = st.columns(2)

    with col_v1:
        # BUY vs SELL volume breakdown per symbol (meaningful for real data)
        if "side" in trades.columns:
            vol_by_side = trades.groupby(["symbol", "side"])["quantity"].sum().reset_index()
            vol_by_side.columns = ["Symbol", "Side", "Total Volume"]
            fig_vol_side = px.bar(
                vol_by_side, x="Symbol", y="Total Volume", color="Side",
                title="Buy vs Sell Volume by Symbol",
                barmode="group",
                color_discrete_map={"BUY": "#2ECC71", "SELL": "#E74C3C"}
            )
            st.plotly_chart(fig_vol_side, use_container_width=True)
        else:
            st.info("No 'side' column found in trades data.")

    with col_v2:
        # Volume over time (all symbols combined, 1-min buckets)
        vol_time = (
            trades.set_index("timestamp")
            .resample("1min")["quantity"]
            .sum()
            .reset_index()
        )
        vol_time.columns = ["Time", "Volume"]
        fig_vol_time = px.area(
            vol_time, x="Time", y="Volume",
            title="Total Trading Volume Over Time (1-min buckets)"
        )
        st.plotly_chart(fig_vol_time, use_container_width=True)

    # Per-symbol volume over time (stacked)
    st.subheader("Per-Symbol Volume Over Time")
    vol_sym_time = (
        trades.set_index("timestamp")
        .groupby("symbol")
        .resample("1min")["quantity"]
        .sum()
        .reset_index()
    )
    vol_sym_time.columns = ["Symbol", "Time", "Volume"]
    fig_vol_sym = px.line(
        vol_sym_time, x="Time", y="Volume", color="Symbol",
        title="Volume per Symbol (1-min buckets)",
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    st.plotly_chart(fig_vol_sym, use_container_width=True)


# ========== SECTION 5: SYMBOL RISK SUMMARY ==========
# Replaces trader scoreboard — real Binance data has no trader_id
st.header("Symbol Risk Summary")

st.caption(
    "Since Binance public API does not expose trader identity, "
    "risk is summarised at the **symbol level** based on alert counts and severity."
)

if not all_alerts.empty:
    # Parse symbol from the 'details' field in all_alerts.csv
    # Format: "Trader UNKNOWN | BTCUSDT | Price X | Qty Y"  (wash)
    # or use alerts_wash / alerts_pd directly for cleaner data

    # Build symbol risk from wash_alerts + pd_alerts
    rows = []

    if not wash_alerts.empty and "symbol" in wash_alerts.columns:
        for sym, grp in wash_alerts.groupby("symbol"):
            rows.append({
                "Symbol": sym,
                "Wash Alerts": len(grp),
                "P&D Alerts": 0,
                "Critical": int((grp["severity"] == "CRITICAL").sum()) if "severity" in grp.columns else 0,
                "High":     int((grp["severity"] == "HIGH").sum())     if "severity" in grp.columns else 0,
                "Medium":   int((grp["severity"] == "MEDIUM").sum())   if "severity" in grp.columns else 0,
            })

    if not pd_alerts.empty and "symbol" in pd_alerts.columns:
        for sym, grp in pd_alerts.groupby("symbol"):
            # Find existing row for this symbol or create new
            existing = next((r for r in rows if r["Symbol"] == sym), None)
            if existing:
                existing["P&D Alerts"] += len(grp)
                if "severity" in grp.columns:
                    existing["Critical"] += int((grp["severity"] == "CRITICAL").sum())
                    existing["High"]     += int((grp["severity"] == "HIGH").sum())
                    existing["Medium"]   += int((grp["severity"] == "MEDIUM").sum())
            else:
                rows.append({
                    "Symbol": sym,
                    "Wash Alerts": 0,
                    "P&D Alerts": len(grp),
                    "Critical": int((grp["severity"] == "CRITICAL").sum()) if "severity" in grp.columns else 0,
                    "High":     int((grp["severity"] == "HIGH").sum())     if "severity" in grp.columns else 0,
                    "Medium":   int((grp["severity"] == "MEDIUM").sum())   if "severity" in grp.columns else 0,
                })

    if rows:
        risk_df = pd.DataFrame(rows)
        risk_df["Total Alerts"] = risk_df["Wash Alerts"] + risk_df["P&D Alerts"]
        risk_df["Risk Score"] = (
            risk_df["Critical"] * 5 +
            risk_df["High"]     * 3 +
            risk_df["Medium"]   * 1
        )
        risk_df = risk_df.sort_values("Risk Score", ascending=False)

        col_r1, col_r2 = st.columns(2)

        with col_r1:
            fig_risk = px.bar(
                risk_df, x="Symbol", y="Risk Score",
                title="Symbol Risk Score",
                color="Risk Score",
                color_continuous_scale="Reds",
                text="Risk Score"
            )
            fig_risk.update_traces(textposition="outside")
            st.plotly_chart(fig_risk, use_container_width=True)

        with col_r2:
            fig_alert_breakdown = px.bar(
                risk_df.melt(id_vars="Symbol", value_vars=["Wash Alerts", "P&D Alerts"],
                             var_name="Alert Type", value_name="Count"),
                x="Symbol", y="Count", color="Alert Type",
                title="Alert Breakdown by Symbol",
                barmode="stack",
                color_discrete_map={"Wash Alerts": "#FFA500", "P&D Alerts": "#E74C3C"}
            )
            st.plotly_chart(fig_alert_breakdown, use_container_width=True)

        st.dataframe(
            risk_df[["Symbol", "Wash Alerts", "P&D Alerts", "Total Alerts", "Critical", "High", "Medium", "Risk Score"]],
            use_container_width=True
        )
    else:
        st.info("No symbol risk data available.")
else:
    st.info("No alerts found to build symbol risk summary.")


# ========== SECTION 6: ALERT TIMELINE ==========
st.header("Alert Timeline")

if not all_alerts.empty and "detected_at" in all_alerts.columns and "alert_type" in all_alerts.columns:
    timeline_df = all_alerts.copy()
    timeline_df["detected_at"] = pd.to_datetime(timeline_df["detected_at"])
    timeline_df = timeline_df.sort_values("detected_at")

    # Bin by minute for timeline chart
    timeline_df["minute"] = timeline_df["detected_at"].dt.floor("1min")
    timeline_binned = timeline_df.groupby(["minute", "alert_type"]).size().reset_index(name="count")

    fig_timeline = px.bar(
        timeline_binned, x="minute", y="count", color="alert_type",
        title="Alerts Detected Over Time (1-min bins)",
        labels={"minute": "Time", "count": "Alert Count", "alert_type": "Alert Type"},
        color_discrete_map={"WASH_TRADE": "#FFA500", "PUMP_AND_DUMP": "#E74C3C", "DUMP_AND_PUMP": "#8B00FF"},
        barmode="stack"
    )
    st.plotly_chart(fig_timeline, use_container_width=True)


# ========== SECTION 7: RAW ALERT TABLES ==========
st.header("Raw Alert Data")

tab1, tab2 = st.tabs(["🟠 Wash Trades", "🔴 Pump & Dump"])

with tab1:
    if not wash_alerts.empty:
        # Drop the trader_id column — it's always "UNKNOWN (Binance public data)" for real data
        display_wash = wash_alerts.drop(columns=["trader_id"], errors="ignore")
        st.dataframe(display_wash, use_container_width=True)
    else:
        st.info("No wash trade alerts.")

with tab2:
    if not pd_alerts.empty:
        # Format percentage columns for readability
        display_pd = pd_alerts.copy()
        for col in ["first_price_chg", "second_price_chg"]:
            if col in display_pd.columns:
                display_pd[col] = display_pd[col].map("{:+.4f}%".format)
        st.dataframe(display_pd, use_container_width=True)
    else:
        st.info("No pump & dump alerts.")


# ========== FOOTER ==========
st.markdown("---")
st.markdown(
    f"*Dashboard generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
    f"| {len(trades):,} trades analyzed | "
    f"Data: Binance aggTrades (BTCUSDT · ETHUSDT · SOLUSDT) | "
    f"Detectors: Wash Trade (Z-score) · Pump & Dump (rolling window)*"
)
