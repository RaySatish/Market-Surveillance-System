"""
STREAMING ALERTS DASHBOARD
============================
Phase 2: Lightweight Streamlit dashboard that auto-refreshes from streaming
alert files written by spark_streaming_wash.py and spark_streaming_pump_dump.py.

This dashboard is intentionally Spark-free — it reads only the CSV files
that the streaming detectors append to every 30 seconds.

Alert files read:
  alerts/streaming_wash_alerts.csv      — from spark_streaming_wash.py
  alerts/streaming_pump_dump_alerts.csv — from spark_streaming_pump_dump.py

Wash alert columns:
  alert_type, window_start, window_end, symbol,
  window_volume, avg_price, trade_count, z_score, severity, detected_at

P&D alert columns:
  alert_type, symbol, pump_window_start, dump_window_start,
  pump_price_chg, dump_price_chg, peak_price, pump_buy_vol,
  dump_sell_vol, severity, detected_at

Usage:
  # Start the full streaming pipeline first:
  docker compose up -d
  python streaming/run_streaming_pipeline.py --test

  # In a separate terminal:
  streamlit run streaming/stream_alerts_dashboard.py
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ============================================================
#  PATHS
# ============================================================
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WASH_ALERTS_PATH = os.path.join(_ROOT, "alerts", "streaming_wash_alerts.csv")
PD_ALERTS_PATH   = os.path.join(_ROOT, "alerts", "streaming_pump_dump_alerts.csv")

# ============================================================
#  PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Streaming Surveillance Dashboard",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
#  SIDEBAR — Controls
# ============================================================
st.sidebar.title("📡 Streaming Dashboard")
st.sidebar.markdown("---")

refresh_interval = st.sidebar.slider(
    "Auto-refresh interval (seconds)",
    min_value=5,
    max_value=60,
    value=15,
    step=5,
)

lookback_minutes = st.sidebar.slider(
    "Show alerts from last N minutes",
    min_value=5,
    max_value=120,
    value=30,
    step=5,
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Alert Files**")
st.sidebar.markdown(f"`alerts/streaming_wash_alerts.csv`")
st.sidebar.markdown(f"`alerts/streaming_pump_dump_alerts.csv`")
st.sidebar.markdown("---")
st.sidebar.markdown(
    "ℹ️ Start the streaming pipeline before running this dashboard:\n\n"
    "```\ndocker compose up -d\npython streaming/run_streaming_pipeline.py --test\n```"
)

# ============================================================
#  DATA LOADERS
# ============================================================

def load_wash_alerts() -> pd.DataFrame:
    """Load streaming wash trade alerts CSV."""
    if not os.path.exists(WASH_ALERTS_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(WASH_ALERTS_PATH)
        if df.empty:
            return df
        df["detected_at"] = pd.to_datetime(df["detected_at"], errors="coerce")
        df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
        df["window_end"]   = pd.to_datetime(df["window_end"],   errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def load_pd_alerts() -> pd.DataFrame:
    """Load streaming pump & dump alerts CSV."""
    if not os.path.exists(PD_ALERTS_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(PD_ALERTS_PATH)
        if df.empty:
            return df
        df["detected_at"]       = pd.to_datetime(df["detected_at"],       errors="coerce")
        df["pump_window_start"] = pd.to_datetime(df["pump_window_start"], errors="coerce")
        df["dump_window_start"] = pd.to_datetime(df["dump_window_start"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def filter_by_lookback(df: pd.DataFrame, col: str, minutes: int) -> pd.DataFrame:
    """Keep only rows where `col` is within the last `minutes` minutes."""
    if df.empty or col not in df.columns:
        return df
    cutoff = pd.Timestamp.now(tz=None) - pd.Timedelta(minutes=minutes)
    # Strip tz if present so comparison works
    series = df[col]
    if series.dt.tz is not None:
        series = series.dt.tz_localize(None)
    return df[series >= cutoff]


# ============================================================
#  SEVERITY COLOUR MAP
# ============================================================
SEVERITY_COLORS = {
    "CRITICAL": "#e74c3c",
    "HIGH":     "#f39c12",
    "MEDIUM":   "#3498db",
}


# ============================================================
#  MAIN DASHBOARD
# ============================================================

def render():
    # ── Header ──────────────────────────────────────────────
    st.title("📡 Real-Time Market Surveillance")
    st.caption(
        f"Auto-refreshes every **{refresh_interval}s** · "
        f"Showing last **{lookback_minutes} min** · "
        f"Last loaded: **{datetime.now().strftime('%H:%M:%S')}**"
    )

    # ── Load data ───────────────────────────────────────────
    wash_df = load_wash_alerts()
    pd_df   = load_pd_alerts()

    wash_recent = filter_by_lookback(wash_df, "detected_at", lookback_minutes)
    pd_recent   = filter_by_lookback(pd_df,   "detected_at", lookback_minutes)

    # ── Pipeline status banner ───────────────────────────────
    wash_file_ok = os.path.exists(WASH_ALERTS_PATH)
    pd_file_ok   = os.path.exists(PD_ALERTS_PATH)

    if not wash_file_ok and not pd_file_ok:
        st.warning(
            "⚠️ No streaming alert files found yet. "
            "Start the streaming pipeline first:\n\n"
            "```\ndocker compose up -d\npython streaming/run_streaming_pipeline.py --test\n```"
        )
    else:
        col_w, col_p = st.columns(2)
        col_w.success("✅ Wash alerts file present") if wash_file_ok else col_w.error("❌ Wash alerts file missing")
        col_p.success("✅ P&D alerts file present")  if pd_file_ok   else col_p.error("❌ P&D alerts file missing")

    st.markdown("---")

    # ── Section 1: Overview metrics ──────────────────────────
    st.subheader("📊 Alert Overview")

    total_wash     = len(wash_recent)
    total_pd       = len(pd_recent)
    total_alerts   = total_wash + total_pd
    critical_count = 0
    if not wash_recent.empty and "severity" in wash_recent.columns:
        critical_count += (wash_recent["severity"] == "CRITICAL").sum()
    if not pd_recent.empty and "severity" in pd_recent.columns:
        critical_count += (pd_recent["severity"] == "CRITICAL").sum()

    # Symbols with at least one alert
    symbols_hit = set()
    if not wash_recent.empty and "symbol" in wash_recent.columns:
        symbols_hit.update(wash_recent["symbol"].unique())
    if not pd_recent.empty and "symbol" in pd_recent.columns:
        symbols_hit.update(pd_recent["symbol"].unique())

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Alerts", total_alerts)
    m2.metric("Wash Trade Alerts", total_wash)
    m3.metric("Pump & Dump Alerts", total_pd)
    m4.metric("🔴 Critical", int(critical_count))
    m5.metric("Symbols Flagged", len(symbols_hit))

    st.markdown("---")

    # ── Section 2: Alert timeline ────────────────────────────
    st.subheader("⏱️ Alert Timeline (1-minute bins)")

    all_recent_frames = []
    if not wash_recent.empty and "detected_at" in wash_recent.columns:
        tmp = wash_recent[["detected_at", "severity"]].copy()
        tmp["alert_type"] = "Wash Trade"
        all_recent_frames.append(tmp)
    if not pd_recent.empty and "detected_at" in pd_recent.columns:
        tmp = pd_recent[["detected_at", "severity"]].copy()
        tmp["alert_type"] = "Pump & Dump"
        all_recent_frames.append(tmp)

    if all_recent_frames:
        combined = pd.concat(all_recent_frames, ignore_index=True)
        combined["minute"] = combined["detected_at"].dt.floor("1min")
        timeline = (
            combined.groupby(["minute", "alert_type"])
            .size()
            .reset_index(name="count")
        )
        fig_timeline = px.bar(
            timeline,
            x="minute", y="count",
            color="alert_type",
            color_discrete_map={"Wash Trade": "#3498db", "Pump & Dump": "#e74c3c"},
            labels={"minute": "Time", "count": "Alerts", "alert_type": "Type"},
            title="Alerts per Minute",
        )
        fig_timeline.update_layout(
            xaxis_title="Time",
            yaxis_title="Alert Count",
            legend_title="Alert Type",
            height=300,
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No alerts in the selected time window yet.")

    st.markdown("---")

    # ── Section 3: Wash Trade Alerts ────────────────────────
    st.subheader("🔁 Wash Trade Alerts")

    if wash_recent.empty:
        st.info("No wash trade alerts in the last {} minutes.".format(lookback_minutes))
    else:
        # Severity breakdown pie
        col_pie, col_table = st.columns([1, 2])

        with col_pie:
            sev_counts = wash_recent["severity"].value_counts().reset_index()
            sev_counts.columns = ["Severity", "Count"]
            fig_sev = px.pie(
                sev_counts,
                names="Severity",
                values="Count",
                color="Severity",
                color_discrete_map=SEVERITY_COLORS,
                title="Wash Alert Severity",
                hole=0.4,
            )
            fig_sev.update_layout(height=300)
            st.plotly_chart(fig_sev, use_container_width=True)

        with col_table:
            # Z-score bar chart per symbol
            if "z_score" in wash_recent.columns and "symbol" in wash_recent.columns:
                avg_z = (
                    wash_recent.groupby("symbol")["z_score"]
                    .mean()
                    .reset_index()
                    .rename(columns={"z_score": "avg_z_score"})
                )
                fig_z = px.bar(
                    avg_z,
                    x="symbol", y="avg_z_score",
                    color="symbol",
                    title="Average Z-Score by Symbol",
                    labels={"avg_z_score": "Avg Z-Score", "symbol": "Symbol"},
                )
                fig_z.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig_z, use_container_width=True)

        # Raw table
        st.markdown("**Recent Wash Alerts**")
        display_cols = [c for c in [
            "detected_at", "symbol", "severity", "z_score",
            "window_volume", "avg_price", "trade_count",
            "window_start", "window_end",
        ] if c in wash_recent.columns]
        st.dataframe(
            wash_recent[display_cols].sort_values("detected_at", ascending=False).head(50),
            use_container_width=True,
        )

    st.markdown("---")

    # ── Section 4: Pump & Dump Alerts ───────────────────────
    st.subheader("📈📉 Pump & Dump Alerts")

    if pd_recent.empty:
        st.info("No pump & dump alerts in the last {} minutes.".format(lookback_minutes))
    else:
        col_pie2, col_chart = st.columns([1, 2])

        with col_pie2:
            sev_counts2 = pd_recent["severity"].value_counts().reset_index()
            sev_counts2.columns = ["Severity", "Count"]
            fig_sev2 = px.pie(
                sev_counts2,
                names="Severity",
                values="Count",
                color="Severity",
                color_discrete_map=SEVERITY_COLORS,
                title="P&D Alert Severity",
                hole=0.4,
            )
            fig_sev2.update_layout(height=300)
            st.plotly_chart(fig_sev2, use_container_width=True)

        with col_chart:
            # Pump vs Dump price change scatter
            if all(c in pd_recent.columns for c in ["pump_price_chg", "dump_price_chg", "symbol"]):
                fig_scatter = px.scatter(
                    pd_recent,
                    x="pump_price_chg",
                    y="dump_price_chg",
                    color="symbol",
                    size_max=15,
                    hover_data=["detected_at", "severity", "peak_price"],
                    title="Pump % vs Dump % per Alert",
                    labels={
                        "pump_price_chg": "Pump Price Change (%)",
                        "dump_price_chg": "Dump Price Change (%)",
                    },
                )
                fig_scatter.update_layout(height=300)
                st.plotly_chart(fig_scatter, use_container_width=True)

        # Raw table
        st.markdown("**Recent P&D Alerts**")
        display_cols2 = [c for c in [
            "detected_at", "symbol", "severity",
            "pump_price_chg", "dump_price_chg", "peak_price",
            "pump_buy_vol", "dump_sell_vol",
            "pump_window_start", "dump_window_start",
        ] if c in pd_recent.columns]
        # Format % columns
        show_df = pd_recent[display_cols2].sort_values("detected_at", ascending=False).head(50).copy()
        for col in ["pump_price_chg", "dump_price_chg"]:
            if col in show_df.columns:
                show_df[col] = show_df[col].apply(lambda v: f"{v:.4f}%" if pd.notna(v) else "")
        st.dataframe(show_df, use_container_width=True)

    st.markdown("---")

    # ── Section 5: Symbol Risk Summary ──────────────────────
    st.subheader("🏆 Symbol Risk Summary")

    risk_rows = []
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        w_sym = wash_recent[wash_recent["symbol"] == sym] if not wash_recent.empty and "symbol" in wash_recent.columns else pd.DataFrame()
        p_sym = pd_recent[pd_recent["symbol"] == sym]    if not pd_recent.empty  and "symbol" in pd_recent.columns  else pd.DataFrame()

        wash_count    = len(w_sym)
        pd_count      = len(p_sym)
        critical_cnt  = 0
        if not w_sym.empty and "severity" in w_sym.columns:
            critical_cnt += (w_sym["severity"] == "CRITICAL").sum()
        if not p_sym.empty and "severity" in p_sym.columns:
            critical_cnt += (p_sym["severity"] == "CRITICAL").sum()

        # Weighted risk score: CRITICAL=3, HIGH=2, MEDIUM=1
        score = 0
        for df_sym in [w_sym, p_sym]:
            if not df_sym.empty and "severity" in df_sym.columns:
                score += (df_sym["severity"] == "CRITICAL").sum() * 3
                score += (df_sym["severity"] == "HIGH").sum() * 2
                score += (df_sym["severity"] == "MEDIUM").sum() * 1

        risk_rows.append({
            "Symbol": sym,
            "Wash Alerts": wash_count,
            "P&D Alerts": pd_count,
            "Critical": int(critical_cnt),
            "Risk Score": int(score),
        })

    risk_df = pd.DataFrame(risk_rows).sort_values("Risk Score", ascending=False)

    col_risk_table, col_risk_bar = st.columns([1, 2])
    with col_risk_table:
        st.dataframe(risk_df, use_container_width=True, hide_index=True)
    with col_risk_bar:
        fig_risk = px.bar(
            risk_df,
            x="Symbol", y="Risk Score",
            color="Symbol",
            title="Risk Score by Symbol",
            labels={"Risk Score": "Weighted Risk Score"},
        )
        fig_risk.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_risk, use_container_width=True)

    st.markdown("---")

    # ── Auto-refresh countdown ───────────────────────────────
    st.caption(f"⏳ Next refresh in {refresh_interval} seconds...")
    time.sleep(refresh_interval)
    st.rerun()


# ============================================================
#  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    render()
