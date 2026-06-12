"""
Lakebase (PostgreSQL) connection pool with OAuth token refresh and retry.
Handles InterfaceError + OperationalError from stale tokens. Converts
Decimal/datetime to JSON-safe types.

Usage:
    from backend.core import run_pg_query, write_pg, _init_pg_pool
    rows = run_pg_query("SELECT * FROM my_table WHERE id = %s", (42,))
    result = write_pg("INSERT INTO notes (text) VALUES (%s) RETURNING *", ("hello",))
"""

import os
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from databricks.sdk import WorkspaceClient

log = logging.getLogger("lakebase")

w = WorkspaceClient()

_pg_pool: Optional[ThreadedConnectionPool] = None


def _get_pg_token() -> str:
    """Get OAuth token from Databricks SDK for Lakebase auth."""
    hf = w.config._header_factory
    if callable(hf):
        r = hf()
        return r.get("Authorization", "").removeprefix("Bearer ") if isinstance(r, dict) else ""
    return ""


def _init_pg_pool(force: bool = False):
    """Initialize or reinitialize the Lakebase connection pool.

    Skips initialization if PGHOST is not set (database resource not yet
    injected by Databricks Apps). This allows the app to serve Delta Lake
    pages even when Lakebase is not configured.
    """
    global _pg_pool
    if _pg_pool and not force:
        return
    if _pg_pool:
        try:
            _pg_pool.closeall()
        except Exception:
            pass
    host = os.getenv("PGHOST", "")
    if not host:
        log.warning(
            "PGHOST not set — Lakebase pool skipped. "
            "Ensure the database resource is in app.yaml AND the app has been "
            "redeployed AFTER the resource was added. Databricks Apps only inject "
            "PGHOST/PGPORT/PGDATABASE/PGUSER at deploy time."
        )
        _pg_pool = None
        return
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "")
    user = os.getenv("PGUSER", "")
    ssl = os.getenv("PGSSLMODE", "require")
    token = _get_pg_token()
    _pg_pool = ThreadedConnectionPool(
        1, 5, host=host, port=port, dbname=db,
        user=user, password=token, sslmode=ssl,
    )
    log.info("Lakebase pool initialised host=%s db=%s", host, db)


def _get_pg_conn():
    """Get a connection from the pool, reinitializing on stale token."""
    _init_pg_pool()
    if _pg_pool is None:
        raise psycopg2.OperationalError(
            "Lakebase pool not initialized — PGHOST is not set. "
            "Add a database resource to app.yaml and redeploy."
        )
    try:
        conn = _pg_pool.getconn()
        conn.cursor().execute("SELECT 1")
        return conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.pool.PoolError):
        log.warning("Lakebase connection stale — reinitialising pool with fresh token")
        _init_pg_pool(force=True)
        if _pg_pool is None:
            raise psycopg2.OperationalError("Lakebase pool reinit failed — PGHOST not set")
        return _pg_pool.getconn()


def _put_pg_conn(conn, close: bool = False):
    """Return a connection to the pool."""
    if _pg_pool:
        try:
            _pg_pool.putconn(conn, close=close)
        except Exception:
            pass


def _pg_rows(cur) -> list[dict]:
    """Convert cursor results to list of dicts with JSON-safe types."""
    cols = [d[0] for d in cur.description]
    rows = []
    for r in cur.fetchall():
        d = {}
        for i, v in enumerate(r):
            if isinstance(v, Decimal):
                d[cols[i]] = float(v)
            elif isinstance(v, (datetime, date)):
                d[cols[i]] = v.isoformat()
            else:
                d[cols[i]] = v
        rows.append(d)
    return rows


def run_pg_query(sql: str, params=None) -> list[dict]:
    """Execute a read query against Lakebase. Retries on stale connection."""
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _pg_rows(cur)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        _put_pg_conn(conn, close=True)
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _pg_rows(cur)
    finally:
        _put_pg_conn(conn)


def write_pg(sql: str, params=None) -> Optional[dict]:
    """Execute a write query (INSERT/UPDATE/DELETE). Returns RETURNING row or affected count."""
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            if cur.description:
                return _pg_rows(cur)[0] if cur.rowcount else None
            return {"affected": cur.rowcount}
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        _put_pg_conn(conn, close=True)
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            if cur.description:
                return _pg_rows(cur)[0] if cur.rowcount else None
            return {"affected": cur.rowcount}
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_pg_conn(conn)
