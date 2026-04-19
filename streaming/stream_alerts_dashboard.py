"""
STREAMING ALERTS DASHBOARD
===========================
Streamlit dashboard for real-time market surveillance alerts.

Queries PostgreSQL live with auto-refresh.

Usage:
  streamlit run streaming/stream_alerts_dashboard.py

All timestamps are stored in UTC in PostgreSQL but displayed in IST (UTC+5:30).
"""

import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ── Project root path fix ──────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from config import get_config

# ── Timezone: display all times in IST ──────────────────────────
try:
    from zoneinfo import ZoneInfo          # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")

cfg = get_config()

# ── Determine data source mode ─────────────────────────────────────

# Only show alerts for valid trading symbols (filter out test/debug data)
VALID_SYMBOLS = set(cfg.get("binance_symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT"]))

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

# Show current mode
mode_label = "PostgreSQL (live)"
st.sidebar.info(f"**Mode:** {mode_label}")

refresh_interval = st.sidebar.slider(
    "Auto-refresh interval (seconds)",
    min_value=5,
    max_value=60,
    value=5,
    step=5,
)

_all_time = st.sidebar.checkbox("Show all alerts (no time filter)", value=False)
lookback_minutes = st.sidebar.slider(
    "Show alerts from last N minutes",
    min_value=5,
    max_value=1440,
    value=120,
    step=5,
    disabled=_all_time,
)
if _all_time:
    lookback_minutes = 999999  # effectively all time

#  sidebar filters
symbol_filter = None
severity_filter = None
st.sidebar.markdown("---")
st.sidebar.markdown("**Filters (PostgreSQL)**")
symbol_filter = st.sidebar.selectbox(
    "Symbol",
    ["All", "BTCUSDT", "ETHUSDT", "SOLUSDT"],
    index=0,
)
if symbol_filter == "All":
    symbol_filter = None

severity_filter = st.sidebar.selectbox(
    "Severity",
    ["All", "CRITICAL", "HIGH", "MEDIUM"],
    index=0,
)
if severity_filter == "All":
    severity_filter = None

st.sidebar.markdown("---")

st.sidebar.markdown("**Data Source:** PostgreSQL")
st.sidebar.markdown(f"`{cfg.get('pg_host', 'localhost')}:{cfg.get('pg_port', 5432)}/{cfg.get('pg_database', 'surveillance')}`")

st.sidebar.markdown("---")
st.sidebar.markdown(
    "ℹ️ Start the streaming pipeline before running this dashboard:\n\n"
    "```\ndocker compose up -d\npython streaming/run_streaming_pipeline.py --test\n```"
)

# ============================================================
# ============================================================

def load_wash_alerts_db() -> pd.DataFrame:
    """ Query wash alerts from PostgreSQL."""
    try:
        from streaming.db import query_alerts
        rows = query_alerts(
            alert_type="wash",
            symbol=symbol_filter,
            severity=severity_filter,
            limit=2000,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        for col in ["detected_at", "window_start", "window_end"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"PostgreSQL query failed: {e}")
        return pd.DataFrame()

def load_pd_alerts_db() -> pd.DataFrame:
    """ Query P&D alerts by matching PUMP→DUMP pairs from sensitivity data.
    
    Uses the pd_sensitivity table which evaluates every 1-min OHLCV bar against
    multiple threshold combinations. Matches PUMP bars followed by DUMP bars on the
    same symbol within 5 minutes — the signature of a pump-and-dump pattern.
    """
    try:
        from streaming.db import get_connection
        import psycopg2.extras as _extras
        conn = get_connection()
        cur = conn.cursor(cursor_factory=_extras.RealDictCursor)
        
        # Find PUMP→DUMP pairs at the most sensitive threshold (0.0005 / 0.5)
        # This gives the best coverage while still requiring both phases
        cur.execute("""
            WITH pumps AS (
                SELECT symbol, window_start, window_end, price_change_pct, volume_ratio, detected_at
                FROM pd_sensitivity
                WHERE flagged = true AND phase = 'PUMP'
                  AND price_threshold = 0.0005 AND vol_threshold = 0.5
            ),
            dumps AS (
                SELECT symbol, window_start, window_end, price_change_pct, volume_ratio, detected_at
                FROM pd_sensitivity
                WHERE flagged = true AND phase = 'DUMP'
                  AND price_threshold = 0.0005 AND vol_threshold = 0.5
            ),
            pairs AS (
                SELECT DISTINCT ON (p.symbol, p.window_start)
                    p.symbol,
                    p.window_start AS pump_start,
                    p.window_end   AS pump_end,
                    d.window_start AS dump_start,
                    d.window_end   AS dump_end,
                    p.price_change_pct AS pump_price_chg_pct,
                    d.price_change_pct AS dump_price_chg_pct,
                    p.volume_ratio     AS pump_vol_ratio,
                    d.volume_ratio     AS dump_vol_ratio,
                    GREATEST(p.detected_at, d.detected_at) AS detected_at
                FROM pumps p
                JOIN dumps d ON p.symbol = d.symbol
                    AND d.window_start > p.window_start
                    AND d.window_start <= p.window_start + INTERVAL '5 minutes'
                ORDER BY p.symbol, p.window_start, d.window_start
            )
            SELECT * FROM pairs ORDER BY pump_start DESC LIMIT 500
        """)
        rows = cur.fetchall()
        conn.close()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        # Add standard columns for compatibility
        df["window_start"] = pd.to_datetime(df["pump_start"])
        df["window_end"]   = pd.to_datetime(df["dump_end"])
        df["detected_at"]  = pd.to_datetime(df["detected_at"])
        df["phase"]        = "PUMP→DUMP"
        df["alert_type"]   = "PUMP_DUMP"
        
        # Severity based on pump magnitude
        def _severity(row):
            pct = abs(row["pump_price_chg_pct"])
            vol = row.get("pump_vol_ratio", 0)
            if pct > 0.15 or vol > 3.0:
                return "CRITICAL"
            elif pct > 0.08 or vol > 1.5:
                return "HIGH"
            return "MEDIUM"
        df["severity"] = df.apply(_severity, axis=1)
        
        # Apply filters
        if symbol_filter:
            df = df[df["symbol"] == symbol_filter]
        if severity_filter:
            df = df[df["severity"] == severity_filter]
        
        return df
    except Exception as e:
        st.error(f"PostgreSQL P&D query failed: {e}")
        return pd.DataFrame()

# ============================================================
#  UNIFIED DATA LOADERS
# ============================================================

def load_wash_alerts() -> pd.DataFrame:
    """Load wash alerts from appropriate source based on MODE."""
    df = load_wash_alerts_db()
    if not df.empty and "symbol" in df.columns:
        df = df[df["symbol"].isin(VALID_SYMBOLS)]
    return _to_ist(df)

def load_pd_alerts() -> pd.DataFrame:
    """Load P&D alerts from appropriate source based on MODE."""
    df = load_pd_alerts_db()
    if not df.empty and "symbol" in df.columns:
        df = df[df["symbol"].isin(VALID_SYMBOLS)]
    return _to_ist(df)

def filter_by_lookback(df: pd.DataFrame, col: str, minutes: int) -> pd.DataFrame:
    """Keep only rows where `col` is within the last `minutes` minutes."""
    if df.empty or col not in df.columns:
        return df
    cutoff = pd.Timestamp.now(tz=IST).tz_localize(None) - pd.Timedelta(minutes=minutes)
    # Strip tz if present so comparison works
    series = df[col]
    if series.dt.tz is not None:
        series = series.dt.tz_localize(None)
    return df[series >= cutoff]

# ============================================================
#  UTC → IST CONVERSION
# ============================================================

# Only detected_at is stored as UTC in PostgreSQL.
# Window timestamps (window_start, window_end, pump_start, dump_start, etc.)
# are already in IST because Spark uses the JVM default timezone (Asia/Kolkata).
_UTC_COLS = ["detected_at"]

def _to_ist(df: pd.DataFrame) -> pd.DataFrame:
    """Convert UTC columns (detected_at) to IST for display.
    
    Window timestamps are left as-is — Spark writes them in the JVM's
    local timezone (IST on this host), so no conversion is needed.
    """
    if df.empty:
        return df
    for col in _UTC_COLS:
        if col not in df.columns:
            continue
        s = df[col]
        if s.dt.tz is None:
            s = s.dt.tz_localize("UTC")
        df[col] = s.dt.tz_convert(IST).dt.tz_localize(None)  # naive IST
    return df

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
    # ── Header ──────────────────────────────────────────────────
    st.title("📡 Real-Time Market Surveillance")
    st.caption(
        f"Auto-refreshes every **{refresh_interval}s** · "
        f"Showing **all alerts** · " if _all_time else f"Showing last **{lookback_minutes} min** · "
        f"Mode: **{mode_label}** · "
        f"Last loaded: **{datetime.now(IST).strftime('%H:%M:%S IST')}**"
    )

    # ── Load data ─────────────────────────────────────────────────
    wash_df = load_wash_alerts()
    pd_df   = load_pd_alerts()

    if _all_time:
        wash_recent = wash_df
        pd_recent   = pd_df
    else:
        wash_recent = filter_by_lookback(wash_df, "detected_at", lookback_minutes)
        pd_recent   = filter_by_lookback(pd_df, "window_start" if "window_start" in pd_df.columns else "detected_at", lookback_minutes)

    # ── Debug: data counts (sidebar) ─────────────────────────
    with st.sidebar.expander("Debug: Data Counts"):
        st.write(f"Wash raw: {len(wash_df)}, filtered: {len(wash_recent)}")
        st.write(f"P&D raw: {len(pd_df)}, filtered: {len(pd_recent)}")
        if not pd_df.empty and "detected_at" in pd_df.columns:
            st.write(f"P&D date range: {pd_df['detected_at'].min()} to {pd_df['detected_at'].max()}")

    # ── Pipeline status banner ───────────────────────────────────
    #  check PostgreSQL connectivity
    try:
        from streaming.db import get_connection
        conn = get_connection()
        conn.close()
        st.success("✅ Connected to PostgreSQL")
    except Exception as e:
        st.error(f"❌ PostgreSQL connection failed: {e}")

    st.markdown("---")

    # ── Section 1: Overview metrics ──────────────────────────────
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

    # ── Section 2: Alert timeline ────────────────────────────────
    st.subheader("⏱️ Alert Timeline (1-minute bins)")

    all_recent_frames = []
    if not wash_recent.empty and "detected_at" in wash_recent.columns:
        tmp = wash_recent[["detected_at", "severity"]].copy()
        tmp["alert_type"] = "Wash Trade"
        all_recent_frames.append(tmp)
    if not pd_recent.empty:
        # Use window_start (= pump_start) for timeline placement — shows when the event actually happened
        time_col = "window_start" if "window_start" in pd_recent.columns else "detected_at"
        if time_col in pd_recent.columns and "severity" in pd_recent.columns:
            tmp = pd_recent[[time_col, "severity"]].copy()
            tmp = tmp.rename(columns={time_col: "detected_at"})
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

    # ── Section 3: Wash Trade Alerts ─────────────────────────────
    st.subheader("🔁 Wash Trade Alerts")

    if wash_recent.empty:
        st.info("No wash trade alerts found." if _all_time else "No wash trade alerts in the last {} minutes.".format(lookback_minutes))
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
            z_col = "z_score" if "z_score" in wash_recent.columns else None
            if not z_col and "zscore" in wash_recent.columns:
                z_col = "zscore"
            if z_col and "symbol" in wash_recent.columns:
                avg_z = (
                    wash_recent.groupby("symbol")[z_col]
                    .mean()
                    .reset_index()
                    .rename(columns={z_col: "avg_z_score"})
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
            "detected_at", "symbol", "severity", "z_score", "zscore",
            "total_volume", "mean_volume", "trade_count",
            "window_start", "window_end",
        ] if c in wash_recent.columns]
        st.dataframe(
            wash_recent[display_cols].sort_values("detected_at", ascending=False).head(50),
            use_container_width=True,
        )

    st.markdown("---")

    # ── Section 4: Pump & Dump Alerts ────────────────────────────
    st.subheader("📈📉 Pump & Dump Alerts")
    st.caption("Matched PUMP→DUMP pairs: price spike followed by reversal on the same symbol within 5 minutes.")

    if pd_recent.empty:
        st.info("No pump & dump patterns detected." if _all_time else "No pump & dump patterns in the last {} minutes.".format(lookback_minutes))
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
            if "pump_price_chg_pct" in pd_recent.columns and "dump_price_chg_pct" in pd_recent.columns:
                fig_scatter = px.scatter(
                    pd_recent,
                    x="pump_price_chg_pct",
                    y="dump_price_chg_pct",
                    color="symbol",
                    size_max=15,
                    hover_data=["detected_at", "severity"],
                    title="Pump % vs Dump % per Matched Pair",
                    labels={
                        "pump_price_chg_pct": "Pump Price Change (%)",
                        "dump_price_chg_pct": "Dump Price Change (%)",
                    },
                )
                fig_scatter.update_layout(height=300)
                st.plotly_chart(fig_scatter, use_container_width=True)

        # Raw table
        st.markdown("**Detected P&D Patterns**")
        display_cols2 = [c for c in [
            "detected_at", "symbol", "severity",
            "pump_price_chg_pct", "dump_price_chg_pct",
            "pump_vol_ratio", "dump_vol_ratio",
            "pump_start", "dump_start",
        ] if c in pd_recent.columns]
        show_df = pd_recent[display_cols2].sort_values("detected_at", ascending=False).head(50).copy()
        # Rename columns for display
        col_rename = {
            "pump_price_chg_pct": "Pump %", "dump_price_chg_pct": "Dump %",
            "pump_vol_ratio": "Pump Vol×", "dump_vol_ratio": "Dump Vol×",
            "pump_start": "Pump Time", "dump_start": "Dump Time",
        }
        show_df = show_df.rename(columns={k: v for k, v in col_rename.items() if k in show_df.columns})
        for col in ["Pump %", "Dump %"]:
            if col in show_df.columns:
                show_df[col] = show_df[col].apply(lambda v: f"{v:.4f}%" if pd.notna(v) else "")
        for col in ["Pump Vol×", "Dump Vol×"]:
            if col in show_df.columns:
                show_df[col] = show_df[col].apply(lambda v: f"{v:.2f}×" if pd.notna(v) else "")
        st.dataframe(show_df, use_container_width=True)

    st.markdown("---")

    # ── Section 5: Symbol Risk Summary ───────────────────────────
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

    # ── Auto-refresh countdown ───────────────────────────────────
    st.caption(f"⏳ Next refresh in {refresh_interval} seconds...")
    time.sleep(refresh_interval)
    st.rerun()

# ============================================================
#  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    render()
