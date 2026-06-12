# Deployment Guide (Full Reference)

Complete step-by-step deployment with all commands and troubleshooting. Referenced from CLAUDE.md.

---

## Prerequisites

- Databricks CLI authenticated: `databricks auth login <url> --profile=<name>`
- FEVM workspace provisioned (or existing workspace)
- Unity Catalog access with catalog/schema creation rights

## Phase A: Delta Lake Data (do this FIRST)

**CRITICAL: These notebooks MUST run BEFORE the app deployment. If you deploy the app first, users will see an empty dashboard with no data.**

### Step 1: Create catalog/schema
Run `notebooks/01_setup_schema.sql` in the Databricks notebook UI. This creates:
- Schema in the auto-provisioned catalog
- Tables will be created by the data generation notebook

### Step 2: Generate Delta Lake data
Run `notebooks/02_generate_data.py` — this creates the tables the app reads from. Verify:
```sql
SHOW TABLES IN <catalog>.<schema>
```

### Step 3: Verify tables
All domain tables should appear with row counts > 0.

---

## Phase B: Lakebase

### Step 4: Create Lakebase instance
Instance names use **HYPHENS** not underscores (Gotcha #5). `--capacity` is **required** (Gotcha #30).
```bash
databricks database create-database-instance <instance-name> --capacity CU_1 --profile=<profile>
# Takes ~6 minutes. Poll until state is AVAILABLE (not RUNNING — Gotcha #31):
databricks database get-database-instance <instance-name> --profile=<profile> -o json | jq '.state'
```

### Step 5: Create database
```bash
databricks psql <instance> --profile=<profile> -- -c "CREATE DATABASE <db_name>;"
```

### Step 6: Apply schemas
```bash
# Core schema (notes, agent_actions, workflows) — required
databricks psql <instance> --profile=<profile> -- -d <db> -f lakebase/core_schema.sql

# Domain schema — your tables
databricks psql <instance> --profile=<profile> -- -d <db> -f lakebase/domain_schema.sql
```

**Do NOT grant SP access yet** — the SP role doesn't exist until the app's database resource is registered and redeployed (Gotcha #33). Grants happen after Step 10/14.

### Step 7: Seed Lakebase
**Recommended: Seed via local CLI** (not serverless notebooks — Gotcha #32). Serverless runtimes run as ephemeral `spark-*` users that have no Lakebase role.
```bash
# Option A: Direct SQL file
databricks psql <instance> --profile=<profile> -- -d <database> -f /tmp/seed.sql

# Option B: Local Python script using CLI credentials
databricks database generate-database-credential \
  --json '{"instance_names": ["<instance>"], "request_id": "seed"}' \
  --profile=<profile>
# Then connect with psycopg2: user=<your-email>, password=<token>
```

**Fallback (notebook):** If you must use a notebook, add `%pip install --upgrade databricks-sdk` as the first cell, restart Python, and set `PG_USER` to your email (not empty string). See Gotcha #22.

---

## Phase C: AI Layer

### Step 8: Create Genie Space
Multi-step process — `table_identifiers` is silently ignored (Gotcha #10):

```bash
# Create blank space
databricks api post /api/2.0/genie/spaces --json '{
  "serialized_space": "{\"version\": 2}",
  "warehouse_id": "<warehouse-id>"
}' --profile=<profile>

# PATCH title/description
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "title": "...", "description": "..."
}' --profile=<profile>

# PATCH tables via serialized_space (sorted alphabetically)
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "serialized_space": "{\"version\":2,\"data_sources\":{\"tables\":[{\"identifier\":\"catalog.schema.table1\"},{\"identifier\":\"catalog.schema.table2\"}]}}"
}' --profile=<profile>

# PATCH instructions
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "instructions": "..."
}' --profile=<profile>

# Verify
databricks api get /api/2.0/genie/spaces/<space_id>?include_serialized_space=true --profile=<profile>
```

### Step 9: Grant Genie permissions
```bash
# Endpoint: /permissions/genie/{id} — NOT /permissions/genie/spaces/{id} (Gotcha #11)
databricks api patch /api/2.0/permissions/genie/<space_id> --json '{
  "access_control_list": [{"group_name": "users", "permission_level": "CAN_RUN"}]
}' --profile=<profile>
```

### Step 10: Deploy Lakebase MCP Server
If this is your first demo, deploy the shared MCP server (see `docs/API_PATTERNS.md` > Lakebase MCP Server Deployment). If already deployed, just add the new database and create a UC HTTP connection.

### Step 11: Create MAS
Agent types use kebab-case (Gotcha #12). After creation, discover the tile ID (Gotcha #24):

```bash
databricks api get /api/2.0/serving-endpoints --profile=<profile> | \
  jq '.endpoints[] | select(.name | startswith("mas-")) | .tile_endpoint_metadata.tile_id'
```

---

## Phase D: App Deployment (do this LAST)

### Step 12: Fill app.yaml
Set warehouse ID, catalog, schema, MAS tile ID (first 8 chars), Lakebase instance/db.

### Step 13: Deploy app
```bash
databricks apps create <app-name> --profile=<profile>
databricks sync ./app /Workspace/Users/<you>/demos/<name>/app --profile=<profile> --watch=false
databricks apps deploy <app-name> --source-code-path /Workspace/Users/<you>/demos/<name>/app --profile=<profile>
```

### Step 14: Register resources via API
**CRITICAL: `app.yaml` resources are NOT automatically registered** (Gotcha #8).
```bash
databricks apps update <app-name> --json '{
  "resources": [
    {"name": "sql-warehouse", "sql_warehouse": {"id": "<warehouse-id>", "permission": "CAN_USE"}},
    {"name": "mas-endpoint", "serving_endpoint": {"name": "mas-<tile-8-chars>-endpoint", "permission": "CAN_QUERY"}},
    {"name": "database", "database": {"instance_name": "<instance>", "database_name": "<db>", "permission": "CAN_CONNECT_AND_CREATE"}}
  ]
}' --profile=<profile>
```

### Step 15: Redeploy after resource registration
```bash
databricks apps deploy <app-name> --source-code-path <path> --profile=<profile>
```
Databricks Apps only inject `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER` at deploy time (Gotcha #23).

### Step 16: Grant permissions explicitly
Resource registration does NOT reliably grant permissions (Gotcha #25):
```bash
# MAS CAN_QUERY
databricks api patch /api/2.0/permissions/serving-endpoints/<mas-endpoint-name> \
  --json '{"access_control_list":[{"service_principal_name":"<app-sp>","permission_level":"CAN_QUERY"}]}' \
  --profile=<profile>

# SQL warehouse CAN_USE (if not already granted)
# Catalog/schema USE_CATALOG, USE_SCHEMA, SELECT to app SP
```

### Step 17: Verify
Visit the app URL in browser (OAuth login required). Check:
- Dashboard shows data
- `GET /api/health` returns `{"status": "healthy"}`
- All three checks pass (SDK, SQL warehouse, Lakebase)

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Health shows `lakebase: error` | Resources not registered via API, or not redeployed after registration | Run `databricks apps update` with all resources, then redeploy |
| Health shows `sql_warehouse: error` | Warehouse resource not registered or SP lacks CAN_USE | Register via API + grant CAN_USE to app SP |
| Dashboard shows zeros / empty | Delta Lake tables don't exist | Run `notebooks/02_generate_data.py` first |
| Dashboard loads but Lakebase pages empty | Lakebase schemas not applied or SP lacks table grants | Apply `core_schema.sql` + `domain_schema.sql`, grant SP access |
| Chat returns 403 | MAS endpoint resource registered but SP lacks CAN_QUERY | Grant CAN_QUERY explicitly on the serving endpoint (Gotcha #25) |
| 401 / empty `{}` from curl | Normal — Databricks Apps require browser OAuth | Open the app URL in a browser instead |
| Agent Workflows page shows zeros | Lakebase `core_schema.sql` not applied | Apply core schema, verify `workflows` + `agent_actions` tables exist |
| Genie Space has no tables | Used `table_identifiers` instead of `serialized_space` | Re-PATCH with `serialized_space` format (Gotcha #10) |
| MAS creation fails with bad agent type | Used snake_case instead of kebab-case | Use `genie-space`, `external-mcp-server`, etc. (Gotcha #12) |
| UC HTTP connection fails with "Missing cloud file system scheme" | `host` field missing `https://` | Add `https://` prefix to host (Gotcha #18) |
