"""
Lakehouse query execution via Databricks Statement Execution API.
Handles ResultManifest compatibility across SDK versions.

Usage:
    from backend.core import run_query
    rows = run_query("SELECT * FROM my_table LIMIT 10")
"""

import os
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.getenv("CATALOG", "")
SCHEMA = os.getenv("SCHEMA", "")

w = WorkspaceClient()


def run_query(sql: str) -> list[dict]:
    """Execute SQL via Statement Execution API and return list of dicts."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        statement=sql,
        wait_timeout="50s",
    )
    if not resp.result or not resp.result.data_array:
        return []

    manifest = resp.manifest
    # ResultManifest compat: newer SDK uses manifest.schema.columns
    columns = getattr(manifest, "columns", None) or getattr(manifest.schema, "columns", [])
    col_names = [c.name for c in columns]
    col_types = [c.type_text.upper() if c.type_text else "STRING" for c in columns]

    rows = []
    for row in resp.result.data_array:
        d = {}
        for i, val in enumerate(row):
            if val is None:
                d[col_names[i]] = None
            elif "INT" in col_types[i]:
                d[col_names[i]] = int(val)
            elif col_types[i] in ("DOUBLE", "FLOAT", "DECIMAL"):
                d[col_names[i]] = float(val)
            elif col_types[i] == "BOOLEAN":
                d[col_names[i]] = val.lower() in ("true", "1")
            else:
                d[col_names[i]] = val
        rows.append(d)
    return rows
