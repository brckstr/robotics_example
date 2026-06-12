"""
Lakebase MCP Server — exposes PostgreSQL CRUD as MCP tools for Databricks MAS.
Deployed as a Databricks App. Uses PGHOST/PGPORT/PGDATABASE/PGUSER env vars
with OAuth token from Databricks SDK for authentication.

Routes:
  /mcp/                  — MCP endpoint (default database from env)
  /db/{database}/mcp/    — MCP endpoint scoped to a specific database
  /                      — Health/status UI
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from mcp.server.fastmcp import FastMCP
from psycopg2.pool import ThreadedConnectionPool
from starlette.routing import Mount

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("lakebase-mcp")

# ── connection pools (one per database) ──────────────────────────────────────

_pools: dict[str, ThreadedConnectionPool] = {}


def _get_token() -> str:
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        hf = w.config._header_factory
        if callable(hf):
            r = hf()
            return r.get("Authorization", "").removeprefix("Bearer ") if isinstance(r, dict) else ""
    except Exception as e:
        log.warning("Token fetch failed: %s", e)
    return ""


def _get_pool(database: Optional[str] = None) -> ThreadedConnectionPool:
    db = database or os.getenv("PGDATABASE", "")
    if not db:
        raise RuntimeError("No database specified and PGDATABASE is not set")
    if db not in _pools:
        host = os.getenv("PGHOST", "")
        if not host:
            raise RuntimeError("PGHOST not set — Lakebase resource not injected")
        port = int(os.getenv("PGPORT", "5432"))
        user = os.getenv("PGUSER", "")
        ssl = os.getenv("PGSSLMODE", "require")
        token = _get_token()
        _pools[db] = ThreadedConnectionPool(1, 5, host=host, port=port, dbname=db,
                                             user=user, password=token, sslmode=ssl)
        log.info("Pool created for database=%s host=%s", db, host)
    return _pools[db]


def _get_conn(database: Optional[str] = None):
    pool = _get_pool(database)
    try:
        conn = pool.getconn()
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        log.warning("Stale connection — reinitializing pool for db=%s", database)
        db = database or os.getenv("PGDATABASE", "")
        _pools.pop(db, None)
        pool = _get_pool(database)
        return pool.getconn()


def _put_conn(conn, database: Optional[str] = None, close: bool = False):
    db = database or os.getenv("PGDATABASE", "")
    pool = _pools.get(db)
    if pool:
        try:
            pool.putconn(conn, close=close)
        except Exception:
            pass


def _serialize_rows(cur) -> list[dict]:
    if cur.description is None:
        return []
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


def _ensure_dict(val: Any) -> dict:
    """Coerce JSON string or dict to dict (MAS serializes args as JSON strings)."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return val or {}


def _ensure_list(val: Any) -> list:
    """Coerce JSON string or list to list."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return val or []


# ── MCP server factory ────────────────────────────────────────────────────────

def make_mcp_server(database: Optional[str] = None) -> FastMCP:
    """Create an MCP server instance scoped to a specific database."""
    db_label = database or os.getenv("PGDATABASE", "default")
    mcp = FastMCP(f"lakebase-{db_label}", stateless_http=True)

    # ── READ tools ────────────────────────────────────────────────────────────

    @mcp.tool()
    def list_tables() -> list[str]:
        """List all tables in the public schema."""
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
                )
                return [r[0] for r in cur.fetchall()]
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def describe_table(table_name: str) -> list[dict]:
        """Get column names and types for a table."""
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT column_name, data_type, is_nullable, column_default
                       FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = %s
                       ORDER BY ordinal_position""",
                    (table_name,),
                )
                return _serialize_rows(cur)
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def read_query(sql: str, limit: int = 100) -> list[dict]:
        """Execute a SELECT query and return results as a list of dicts."""
        if not sql.strip().upper().startswith("SELECT"):
            raise ValueError("read_query only accepts SELECT statements")
        safe_sql = sql.rstrip(";")
        if limit and "LIMIT" not in safe_sql.upper():
            safe_sql = f"{safe_sql} LIMIT {limit}"
        conn = _get_conn(database)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(safe_sql)
                rows = [dict(r) for r in cur.fetchall()]
                return [{k: (float(v) if isinstance(v, Decimal)
                              else v.isoformat() if isinstance(v, (datetime, date))
                              else v)
                         for k, v in row.items()} for row in rows]
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def get_connection_info() -> dict:
        """Return current database connection info (no credentials)."""
        return {
            "host": os.getenv("PGHOST", "not set"),
            "port": os.getenv("PGPORT", "5432"),
            "database": database or os.getenv("PGDATABASE", "not set"),
            "user": os.getenv("PGUSER", "not set"),
        }

    @mcp.tool()
    def list_schemas() -> list[str]:
        """List all schemas in the database."""
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
                )
                return [r[0] for r in cur.fetchall()]
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def list_slow_queries() -> list[dict]:
        """List currently running queries longer than 1 second."""
        conn = _get_conn(database)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT pid, now() - pg_stat_activity.query_start AS duration,
                              query, state
                       FROM pg_stat_activity
                       WHERE (now() - pg_stat_activity.query_start) > interval '1 seconds'
                         AND state != 'idle'
                       ORDER BY duration DESC LIMIT 20"""
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            _put_conn(conn, database)

    # ── WRITE tools ───────────────────────────────────────────────────────────

    @mcp.tool()
    def insert_record(table: str, data: Any) -> dict:
        """Insert a single record into a table. Returns the inserted row.
        data: dict of column -> value pairs."""
        record = _ensure_dict(data)
        if not record:
            raise ValueError("data must be a non-empty dict")
        cols = list(record.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        values = [record[c] for c in cols]
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) RETURNING *"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                conn.commit()
                return _serialize_rows(cur)[0] if cur.rowcount else {}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def update_records(table: str, where: str, data: Any) -> dict:
        """Update records in a table. where: SQL WHERE clause (e.g. 'id = 5').
        data: dict of column -> new value pairs. Returns affected count."""
        updates = _ensure_dict(data)
        if not updates:
            raise ValueError("data must be a non-empty dict")
        set_parts = ", ".join([f"{k} = %s" for k in updates.keys()])
        values = list(updates.values())
        sql = f"UPDATE {table} SET {set_parts} WHERE {where}"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                conn.commit()
                return {"affected": cur.rowcount}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def delete_records(table: str, where: str) -> dict:
        """Delete records matching a WHERE clause. Returns affected count."""
        sql = f"DELETE FROM {table} WHERE {where}"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return {"affected": cur.rowcount}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def batch_insert(table: str, records: Any) -> dict:
        """Insert multiple records into a table. Returns inserted count.
        records: list of dicts with the same keys."""
        rows = _ensure_list(records)
        if not rows:
            raise ValueError("records must be a non-empty list")
        cols = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        values_list = [[r[c] for c in cols] for r in rows]
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, values_list)
                conn.commit()
                return {"inserted": len(values_list)}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    # ── SQL tools ─────────────────────────────────────────────────────────────

    @mcp.tool()
    def execute_sql(sql: str) -> dict:
        """Execute any SQL statement. Returns rows for SELECT, affected count for writes."""
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                if cur.description:
                    rows = _serialize_rows(cur)
                    conn.commit()
                    return {"rows": rows, "count": len(rows)}
                else:
                    conn.commit()
                    return {"affected": cur.rowcount}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def execute_transaction(statements: Any) -> dict:
        """Execute multiple SQL statements as a transaction.
        statements: list of SQL strings."""
        stmts = _ensure_list(statements)
        if not stmts:
            raise ValueError("statements must be a non-empty list")
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                for stmt in stmts:
                    cur.execute(stmt)
            conn.commit()
            return {"executed": len(stmts), "status": "committed"}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def explain_query(sql: str) -> list[str]:
        """Return the query plan for a SQL statement."""
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN {sql}")
                return [r[0] for r in cur.fetchall()]
        finally:
            _put_conn(conn, database)

    # ── DDL tools ─────────────────────────────────────────────────────────────

    @mcp.tool()
    def create_table(table_name: str, columns: Any) -> dict:
        """Create a new table. columns: list of 'name TYPE [constraints]' strings."""
        col_defs = _ensure_list(columns)
        if not col_defs:
            raise ValueError("columns must be a non-empty list")
        col_sql = ", ".join(col_defs)
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_sql})"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return {"created": table_name}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def drop_table(table_name: str, cascade: bool = False) -> dict:
        """Drop a table. Set cascade=True to drop dependent objects."""
        suffix = " CASCADE" if cascade else ""
        sql = f"DROP TABLE IF EXISTS {table_name}{suffix}"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return {"dropped": table_name}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    @mcp.tool()
    def alter_table(table_name: str, alteration: str) -> dict:
        """Alter a table. alteration: e.g. 'ADD COLUMN notes TEXT' or 'DROP COLUMN old_col'."""
        sql = f"ALTER TABLE {table_name} {alteration}"
        conn = _get_conn(database)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return {"altered": table_name}
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn, database)

    return mcp


# ── FastAPI app with multi-database routing ───────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Lakebase MCP Server starting. PGHOST=%s PGDATABASE=%s",
             os.getenv("PGHOST", "unset"), os.getenv("PGDATABASE", "unset"))
    yield
    for pool in _pools.values():
        try:
            pool.closeall()
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)

# Default /mcp/ route (uses PGDATABASE env var)
_default_mcp = make_mcp_server()
app.mount("/mcp", _default_mcp.streamable_http_app())

# Per-database /db/{database}/mcp/ route
_db_mcps: dict[str, FastMCP] = {}


@app.api_route("/db/{database}/mcp/{path:path}", methods=["GET", "POST", "DELETE"])
async def db_mcp_route(database: str, path: str, request: Request):
    """Route MCP requests to the correct per-database MCP server."""
    if database not in _db_mcps:
        _db_mcps[database] = make_mcp_server(database)
    db_app = _db_mcps[database].streamable_http_app()
    scope = dict(request.scope)
    scope["path"] = f"/mcp/{path}" if path else "/mcp/"
    scope["root_path"] = f"/db/{database}"
    return await db_app(scope, request._receive, request._send)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Simple status page."""
    pools_info = [
        f"<li><strong>{db}</strong>: pool open</li>"
        for db in _pools
    ]
    pools_html = "<ul>" + "".join(pools_info) + "</ul>" if pools_info else "<p>No active pools</p>"
    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>Lakebase MCP Server</title>
    <style>body{{font-family:monospace;max-width:800px;margin:40px auto;padding:20px}}</style>
    </head>
    <body>
    <h1>Lakebase MCP Server</h1>
    <p><strong>Status:</strong> Running</p>
    <p><strong>PGHOST:</strong> {os.getenv("PGHOST","not set")}</p>
    <p><strong>PGDATABASE:</strong> {os.getenv("PGDATABASE","not set")}</p>
    <h2>Active Connection Pools</h2>
    {pools_html}
    <h2>Endpoints</h2>
    <ul>
      <li><code>POST /mcp/</code> — MCP endpoint (default database)</li>
      <li><code>POST /db/{{database}}/mcp/</code> — MCP endpoint for specific database</li>
    </ul>
    <h2>Tools (16)</h2>
    <p>READ: list_tables, describe_table, read_query, list_schemas, get_connection_info, list_slow_queries</p>
    <p>WRITE: insert_record, update_records, delete_records, batch_insert</p>
    <p>SQL: execute_sql, execute_transaction, explain_query</p>
    <p>DDL: create_table, drop_table, alter_table</p>
    </body>
    </html>
    """
