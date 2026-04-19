"""
streaming/db.py — PostgreSQL connection and schema management
=========================================================================

Provides:
  - get_connection()           → psycopg2 connection from config
  - init_schema()              → CREATE TABLE IF NOT EXISTS + indexes
  - insert_wash_alert(dict)    → UPSERT with ON CONFLICT DO NOTHING
  - insert_pump_dump_alert(dict) → UPSERT with ON CONFLICT DO NOTHING
  - query_alerts(...)          → Parameterized SELECT for dashboard

Usage:
  python streaming/db.py --init    # Create tables + indexes
  python streaming/db.py --test    # Insert a test row and query it back

All writes are idempotent (ON CONFLICT DO NOTHING) to support
at-least-once delivery from Kafka alert consumer.
"""

import sys
import os
import argparse
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from psycopg2 import sql as psycopg2_sql

# Add project root to path so we can import config and utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_config
from utils.fault_tolerance import get_logger

logger = get_logger("db")


# ============================================================
#  Connection
# ============================================================

def get_connection():
    """
    Returns a psycopg2 connection using config values.
    Caller is responsible for closing the connection.
    """
    cfg = get_config()

    try:
        conn = psycopg2.connect(
            host=cfg["pg_host"],
            port=cfg["pg_port"],
            dbname=cfg["pg_database"],
            user=cfg["pg_user"],
            password=cfg["pg_password"],
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        # Common local dev failure:
        #   role "<pg_user>" does not exist
        # This happens when the Docker volume was created previously with a
        # different POSTGRES_USER, so the new POSTGRES_USER env is ignored.
        role = cfg.get("pg_user")
        if role and f'role "{role}" does not exist' in str(e):
            admin_user = cfg.get("pg_admin_user", "postgres")
            admin_password = cfg.get("pg_admin_password", cfg.get("pg_password"))
            logger.warning(
                f'PostgreSQL role "{role}" missing; falling back to admin user '
                f'"{admin_user}" for DB connections.'
            )
            conn = psycopg2.connect(
                host=cfg["pg_host"],
                port=cfg["pg_port"],
                dbname=cfg["pg_database"],
                user=admin_user,
                password=admin_password,
            )
            conn.autocommit = False
            return conn
        raise


# ============================================================
#  Schema
# ============================================================

_SCHEMA_SQL = """
-- ---- Wash alerts ----
CREATE TABLE IF NOT EXISTS wash_alerts (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    trade_count     INTEGER,
    total_volume    DOUBLE PRECISION,
    mean_volume     DOUBLE PRECISION,
    std_volume      DOUBLE PRECISION,
    z_score         DOUBLE PRECISION,
    severity        VARCHAR(10) NOT NULL,
    alert_type      VARCHAR(20) DEFAULT 'WASH_TRADE',
    detected_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(window_start, window_end, symbol)
);

-- ---- Pump & dump alerts ----
CREATE TABLE IF NOT EXISTS pump_dump_alerts (
    id                  SERIAL PRIMARY KEY,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    phase               VARCHAR(10) NOT NULL,
    price_change_pct    DOUBLE PRECISION,
    volume_ratio        DOUBLE PRECISION,
    severity            VARCHAR(10) NOT NULL,
    alert_type          VARCHAR(20) DEFAULT 'PUMP_DUMP',
    detected_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE(window_start, symbol, phase)
);

-- ---- Indexes for dashboard query performance ----
CREATE INDEX IF NOT EXISTS idx_wash_detected_at
    ON wash_alerts (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_wash_symbol_detected
    ON wash_alerts (symbol, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pd_detected_at
    ON pump_dump_alerts (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pd_symbol_detected
    ON pump_dump_alerts (symbol, detected_at DESC);

-- ---- Threshold sensitivity sweep (paper Table 3) ----
CREATE TABLE IF NOT EXISTS wash_sensitivity (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    trade_count     INTEGER,
    total_volume    DOUBLE PRECISION,
    z_score         DOUBLE PRECISION,
    threshold       DOUBLE PRECISION NOT NULL,
    flagged         BOOLEAN NOT NULL,
    severity        VARCHAR(10),
    detected_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(window_start, window_end, symbol, threshold)
);

CREATE INDEX IF NOT EXISTS idx_sensitivity_threshold
    ON wash_sensitivity (threshold, flagged);

-- ---- P&D Threshold sensitivity sweep (paper Table 4) ----
CREATE TABLE IF NOT EXISTS pd_sensitivity (
    id                  SERIAL PRIMARY KEY,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    price_change_pct    DOUBLE PRECISION,
    volume_ratio        DOUBLE PRECISION,
    price_threshold     DOUBLE PRECISION NOT NULL,
    vol_threshold       DOUBLE PRECISION NOT NULL,
    phase               VARCHAR(10),
    flagged             BOOLEAN DEFAULT FALSE,
    severity            VARCHAR(10),
    detected_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE(window_start, symbol, price_threshold, vol_threshold, phase)
);

CREATE INDEX IF NOT EXISTS idx_pd_sensitivity_thresh
    ON pd_sensitivity (price_threshold, vol_threshold, flagged);

"""


def init_schema(conn=None):
    """
    Create tables and indexes if they don't exist.
    If no connection is provided, opens and closes one internally.
    """
    cfg = get_config()

    # If the caller provided a connection, assume it already points at the
    # target database and just create tables.
    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            conn.commit()
            logger.info("Database schema initialized successfully")
            return
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to initialize schema: {e}")
            raise

    # Otherwise, do a more defensive setup:
    # 1) connect to an admin DB (configured admin user, else OS user)
    # 2) create missing target role + database (if needed)
    # 3) connect to the target DB as `cfg["pg_user"]` and run schema SQL
    admin_conn = None
    try:
        admin_conn = _get_admin_connection()
        _ensure_role_and_database_exist(admin_conn)

        # Now connect to target DB with the configured pg_user.
        target_conn = get_connection()
        try:
            with target_conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            target_conn.commit()
            logger.info("Database schema initialized successfully")
        finally:
            target_conn.close()
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
        raise
    finally:
        if admin_conn is not None:
            admin_conn.close()


def _get_admin_connection():
    """
    Returns a psycopg2 connection suitable for creating roles/databases.
    Tries:
      1) configured pg_admin_user
      1.5) configured pg_user (often the superuser in Docker setups)
      2) fallback to connecting as the current OS user (peer/local auth)
    """
    cfg = get_config()
    host = cfg["pg_host"]
    port = cfg["pg_port"]
    admin_db = cfg.get("pg_admin_database", "postgres")

    admin_user = cfg.get("pg_admin_user", "postgres")
    admin_password = cfg.get("pg_admin_password", cfg.get("pg_password"))

    # 1) Configured admin user.
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=admin_db,
            user=admin_user,
            password=admin_password,
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        # If the admin role doesn't exist (common when Docker volume
        # initialization happened with a different POSTGRES_USER), fall back.
        logger.warning(
            f'Admin role "{admin_user}" not usable for DB init ({e}); '
            f"falling back to OS user connection."
        )

    # 1.5) Try configured pg_user as the admin connection.
    # In this repo's Docker Compose, POSTGRES_USER is typically `surveillance`,
    # meaning `pg_user` is the effective superuser even if `pg_admin_user`
    # (default `postgres`) doesn't exist.
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=admin_db,
            user=cfg["pg_user"],
            password=cfg["pg_password"],
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError:
        # Fall through to OS user.
        pass

    # 2) OS user (no user/password specified).
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=admin_db,
    )
    conn.autocommit = False
    return conn


def _ensure_role_and_database_exist(conn):
    """
    Ensure `cfg["pg_user"]` and `cfg["pg_database"]` exist.
    Uses `conn` that is connected to an admin DB.
    """
    cfg = get_config()
    role = cfg["pg_user"]
    role_password = cfg["pg_password"]
    dbname = cfg["pg_database"]
    # Role creation can happen in a normal transaction.
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %(role)s", {"role": role})
        role_exists = cur.fetchone() is not None
        if not role_exists:
            cur.execute(
                psycopg2_sql.SQL("CREATE ROLE {role} WITH LOGIN PASSWORD %s").format(
                    role=psycopg2_sql.Identifier(role)
                ),
                (role_password,),
            )
            logger.info(f'Created missing PostgreSQL role "{role}".')

    # Database creation must run outside a transaction block.
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %(db)s", {"db": dbname})
        db_exists = cur.fetchone() is not None

    if not db_exists:
        old_autocommit = conn.autocommit
        try:
            # CREATE DATABASE cannot run inside a transaction block. If we
            # previously executed statements with autocommit=False, ensure
            # the transaction is ended before toggling autocommit.
            if not old_autocommit:
                conn.commit()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    psycopg2_sql.SQL("CREATE DATABASE {db} OWNER {role}").format(
                        db=psycopg2_sql.Identifier(dbname),
                        role=psycopg2_sql.Identifier(role),
                    )
                )
                logger.info(f'Created missing PostgreSQL database "{dbname}".')

                # Ensure the role can connect.
                cur.execute(
                    psycopg2_sql.SQL("GRANT CONNECT ON DATABASE {db} TO {role}").format(
                        db=psycopg2_sql.Identifier(dbname),
                        role=psycopg2_sql.Identifier(role),
                    )
                )
        finally:
            conn.autocommit = old_autocommit
    else:
        # DB exists already; grants can be handled transactionally.
        with conn.cursor() as cur:
            cur.execute(
                psycopg2_sql.SQL("GRANT CONNECT ON DATABASE {db} TO {role}").format(
                    db=psycopg2_sql.Identifier(dbname),
                    role=psycopg2_sql.Identifier(role),
                )
            )

    if not conn.autocommit:
        conn.commit()


# ============================================================
#  Insert (UPSERT — idempotent)
# ============================================================

def insert_wash_alert(alert_dict, conn=None):
    """
    Insert a wash trade alert. ON CONFLICT DO NOTHING for deduplication.

    Expected keys in alert_dict:
        window_start, window_end, symbol, trade_count, total_volume,
        mean_volume, std_volume, z_score (or zscore), severity,
        alert_type (optional, defaults to WASH_TRADE),
        detected_at (optional, defaults to NOW())
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    # Normalize key: accept both 'zscore' and 'z_score'
    z_score = alert_dict.get("z_score", alert_dict.get("zscore"))

    sql = """
        INSERT INTO wash_alerts
            (window_start, window_end, symbol, trade_count, total_volume,
             mean_volume, std_volume, z_score, severity, alert_type, detected_at)
        VALUES
            (%(window_start)s, %(window_end)s, %(symbol)s, %(trade_count)s,
             %(total_volume)s, %(mean_volume)s, %(std_volume)s, %(z_score)s,
             %(severity)s, %(alert_type)s, %(detected_at)s)
        ON CONFLICT (window_start, window_end, symbol) DO NOTHING
    """

    params = {
        "window_start":  alert_dict["window_start"],
        "window_end":    alert_dict["window_end"],
        "symbol":        alert_dict["symbol"],
        "trade_count":   alert_dict.get("trade_count"),
        "total_volume":  alert_dict.get("total_volume"),
        "mean_volume":   alert_dict.get("mean_volume"),
        "std_volume":    alert_dict.get("std_volume"),
        "z_score":       z_score,
        "severity":      alert_dict["severity"],
        "alert_type":    alert_dict.get("alert_type", "WASH_TRADE"),
        "detected_at":   alert_dict.get("detected_at", datetime.now(timezone.utc)),
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.debug(f"Inserted wash alert: {alert_dict.get('symbol')} "
                     f"[{alert_dict.get('window_start')}]")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert wash alert: {e}")
        raise
    finally:
        if close_conn:
            conn.close()


def insert_pump_dump_alert(alert_dict, conn=None):
    """
    Insert a pump & dump alert. ON CONFLICT DO NOTHING for deduplication.

    Expected keys in alert_dict:
        window_start, window_end, symbol, phase, price_change_pct,
        volume_ratio, severity,
        alert_type (optional, defaults to PUMP_DUMP),
        detected_at (optional, defaults to NOW())
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    sql = """
        INSERT INTO pump_dump_alerts
            (window_start, window_end, symbol, phase, price_change_pct,
             volume_ratio, severity, alert_type, detected_at)
        VALUES
            (%(window_start)s, %(window_end)s, %(symbol)s, %(phase)s,
             %(price_change_pct)s, %(volume_ratio)s, %(severity)s,
             %(alert_type)s, %(detected_at)s)
        ON CONFLICT (window_start, symbol, phase) DO NOTHING
    """

    params = {
        "window_start":    alert_dict["window_start"],
        "window_end":      alert_dict["window_end"],
        "symbol":          alert_dict["symbol"],
        "phase":           alert_dict["phase"],
        "price_change_pct": alert_dict.get("price_change_pct"),
        "volume_ratio":    alert_dict.get("volume_ratio"),
        "severity":        alert_dict["severity"],
        "alert_type":      alert_dict.get("alert_type", "PUMP_DUMP"),
        "detected_at":     alert_dict.get("detected_at", datetime.now(timezone.utc)),
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.debug(f"Inserted P&D alert: {alert_dict.get('symbol')} "
                     f"{alert_dict.get('phase')} [{alert_dict.get('window_start')}]")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert P&D alert: {e}")
        raise
    finally:
        if close_conn:
            conn.close()




def insert_wash_sensitivity(row_dict, conn=None):
    """
    Insert a threshold sensitivity evaluation row.
    Used by the streaming wash detector during the sensitivity sweep.
    ON CONFLICT DO UPDATE so each batch overwrites with the latest z-score.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    sql = """
        INSERT INTO wash_sensitivity
            (window_start, window_end, symbol, trade_count, total_volume,
             z_score, threshold, flagged, severity, detected_at)
        VALUES
            (%(window_start)s, %(window_end)s, %(symbol)s, %(trade_count)s,
             %(total_volume)s, %(z_score)s, %(threshold)s, %(flagged)s,
             %(severity)s, %(detected_at)s)
        ON CONFLICT (window_start, window_end, symbol, threshold) DO UPDATE SET
            z_score = EXCLUDED.z_score,
            trade_count = EXCLUDED.trade_count,
            total_volume = EXCLUDED.total_volume,
            flagged = EXCLUDED.flagged,
            severity = EXCLUDED.severity,
            detected_at = EXCLUDED.detected_at
    """

    params = {
        "window_start":  row_dict["window_start"],
        "window_end":    row_dict["window_end"],
        "symbol":        row_dict["symbol"],
        "trade_count":   row_dict.get("trade_count"),
        "total_volume":  row_dict.get("total_volume"),
        "z_score":       row_dict.get("z_score"),
        "threshold":     row_dict["threshold"],
        "flagged":       row_dict["flagged"],
        "severity":      row_dict.get("severity"),
        "detected_at":   row_dict.get("detected_at", datetime.now(timezone.utc)),
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.debug(f"Inserted sensitivity row: {row_dict.get('symbol')} "
                     f"threshold={row_dict.get('threshold')} flagged={row_dict.get('flagged')}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert sensitivity row: {e}")
    finally:
        if close_conn:
            conn.close()


# ============================================================
#  Query (for dashboard)
# ============================================================

def query_alerts(alert_type="all", symbol=None, severity=None,
                 limit=1000, conn=None):
    """
    Query alerts from PostgreSQL for the dashboard.

    Args:
        alert_type: "wash", "pump_dump", or "all"
        symbol:     Optional filter (e.g., "BTCUSDT")
        severity:   Optional filter (e.g., "CRITICAL")
        limit:      Max rows to return (default 1000)
        conn:       Optional existing connection

    Returns:
        List of dicts (one per alert row)
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    results = []

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if alert_type in ("wash", "all"):
                sql = _build_query("wash_alerts", symbol, severity, limit)
                params = _build_params(symbol, severity, limit)
                cur.execute(sql, params)
                results.extend([dict(row) for row in cur.fetchall()])

            if alert_type in ("pump_dump", "all"):
                sql = _build_query("pump_dump_alerts", symbol, severity, limit)
                params = _build_params(symbol, severity, limit)
                cur.execute(sql, params)
                results.extend([dict(row) for row in cur.fetchall()])

    except Exception as e:
        logger.error(f"Failed to query alerts: {e}")
        raise
    finally:
        if close_conn:
            conn.close()

    return results


def _build_query(table, symbol, severity, limit):
    """Build a parameterized SELECT query with optional filters."""
    sql = f"SELECT * FROM {table} WHERE 1=1"
    if symbol:
        sql += " AND symbol = %(symbol)s"
    if severity:
        sql += " AND severity = %(severity)s"
    sql += " ORDER BY detected_at DESC LIMIT %(limit)s"
    return sql


def _build_params(symbol, severity, limit):
    """Build parameter dict for the query."""
    params = {"limit": limit}
    if symbol:
        params["symbol"] = symbol
    if severity:
        params["severity"] = severity
    return params


# ============================================================
#  CLI entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Database schema management")
    parser.add_argument("--init", action="store_true",
                        help="Create tables and indexes")
    parser.add_argument("--test", action="store_true",
                        help="Insert a test row and query it back")
    args = parser.parse_args()

    if args.init:
        print("Initializing database schema...")
        init_schema()
        print("✓ Schema initialized successfully")

    if args.test:
        print("Inserting test wash alert...")
        now = datetime.now(timezone.utc)
        test_alert = {
            "window_start": now,
            "window_end":   now,
            "symbol":       "BTCUSDT",
            "trade_count":  42,
            "total_volume":  1234.56,
            "mean_volume":   100.0,
            "std_volume":    50.0,
            "z_score":       3.5,
            "severity":     "HIGH",
            "alert_type":   "WASH_TRADE",
            "detected_at":  now,
        }
        insert_wash_alert(test_alert)
        print("✓ Test alert inserted")

        print("Querying alerts...")
        alerts = query_alerts(alert_type="wash", limit=5)
        for a in alerts:
            print(f"  {a['symbol']} | {a['severity']} | z={a['z_score']} | {a['detected_at']}")
        print(f"✓ Found {len(alerts)} wash alert(s)")

    if not args.init and not args.test:
        parser.print_help()


if __name__ == "__main__":
    main()


# ── Pump & Dump Sensitivity Table ──────────────────────────────────────────
_PD_SENSITIVITY_TABLE = """
CREATE TABLE IF NOT EXISTS pd_sensitivity (
    id                  SERIAL PRIMARY KEY,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    price_change_pct    DOUBLE PRECISION,
    volume_ratio        DOUBLE PRECISION,
    price_threshold     DOUBLE PRECISION NOT NULL,
    vol_threshold       DOUBLE PRECISION NOT NULL,
    phase               VARCHAR(10),
    flagged             BOOLEAN DEFAULT FALSE,
    severity            VARCHAR(10),
    detected_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE(window_start, symbol, price_threshold, vol_threshold, phase)
);
"""

def init_pd_sensitivity_schema(conn=None):
    """Create pd_sensitivity table if it doesn't exist."""
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(_PD_SENSITIVITY_TABLE)
        conn.commit()
    finally:
        if close:
            conn.close()


def insert_pd_sensitivity(row: dict, conn=None):
    """Insert a P&D sensitivity sweep row (UPSERT)."""
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pd_sensitivity
                    (window_start, window_end, symbol, price_change_pct, volume_ratio,
                     price_threshold, vol_threshold, phase, flagged, severity, detected_at)
                VALUES (%(window_start)s, %(window_end)s, %(symbol)s, %(price_change_pct)s,
                        %(volume_ratio)s, %(price_threshold)s, %(vol_threshold)s,
                        %(phase)s, %(flagged)s, %(severity)s, %(detected_at)s)
                ON CONFLICT (window_start, symbol, price_threshold, vol_threshold, phase)
                DO UPDATE SET
                    price_change_pct = EXCLUDED.price_change_pct,
                    volume_ratio     = EXCLUDED.volume_ratio,
                    flagged          = EXCLUDED.flagged,
                    severity         = EXCLUDED.severity,
                    detected_at      = EXCLUDED.detected_at
            """, row)
        conn.commit()
    finally:
        if close:
            conn.close()
