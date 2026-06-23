# Vibe Demo Accelerator

Reusable scaffold for building customer demos on Databricks. Clone this repo, tell vibe what you want, and have a deployed demo in 2-4 hours.

**Architecture:** FastAPI backend + single-file HTML/JS frontend + Databricks Apps deployment.
**Stack:** Delta Lake (analytics) + Lakebase/PostgreSQL (OLTP) + Lakebase MCP Server (agent writes) + MAS Agent Bricks (AI chat) + Genie Space (NL queries).
**Compute:** All serverless — no classic clusters needed.

## CRITICAL: First-Time Setup

**When a user clones this scaffold and opens it in vibe, you MUST ask them for ALL of the following before generating any code:**

**Infrastructure (required first):**
1. **Demo Name** — What is this demo called? (e.g., "Apex Steel Predictive Maintenance")
2. **Customer** — Real or fictional customer name?
3. **Databricks Workspace URL** — e.g., `https://fe-sandbox-serverless-my-demo.cloud.databricks.com`
4. **Databricks CLI Profile** — e.g., `my-demo` (see FEVM Setup below)
5. **Catalog Name** — e.g., `serverless_my_demo_catalog` (FEVM auto-creates this)
6. **Schema Name** — e.g., `my_demo_schema` (you'll create this)
7. **SQL Warehouse ID** — From the workspace SQL Warehouses page
8. **Domain / Industry** — What industry is this demo for?
9. **Key Use Cases** — 2-4 use cases the demo should showcase

**UI Preferences (ask before building frontend):**
10. **Layout style** — Sidebar nav, top nav bar, or dashboard-first?
11. **Color scheme** — Dark industrial, clean medical, corporate blue, or custom brand colors?
12. **Dashboard content** — What should the main landing page show? (KPIs, charts, AI input, morning briefing, etc.)
13. **Additional pages** — What domain pages beyond AI Chat and Agent Workflows? (e.g., inventory, shipments, risk scores)

Fill in the Project Identity section below immediately after gathering this info. Then generate all code with these values — never leave TODOs in generated files.

## FEVM Workspace Setup

Most SAs use FE Vending Machine (FEVM) workspaces. These are serverless-only Databricks environments with auto-provisioned Unity Catalog.

### Creating a workspace
```bash
# Use vibe's built-in FEVM skill:
/databricks-fe-vm-workspace-deployment
# Choose: Serverless Deployment (Template 3, AWS, ~5-10 min)
# Name: use hyphens, e.g., my-customer-demo
# Lifetime: 30 days max
```

### Setting up the CLI profile
```bash
# After workspace is created, authenticate:
databricks auth login https://fe-sandbox-serverless-<name>.cloud.databricks.com --profile=<name>

# Verify:
databricks current-user me --profile=<name>
```

### FEVM naming conventions
- **Workspace URL:** `https://fe-sandbox-serverless-<name>.cloud.databricks.com`
- **Auto-created catalog:** `serverless_<name_with_underscores>_catalog` (hyphens become underscores)
- **Compute:** 100% serverless — SQL warehouses are pre-provisioned, no cluster management needed
- **Lifetime:** Up to 30 days, then must be recreated

## Project Identity

- **Demo Name:** Physical AI Fleet Health Console
- **Customer:** Amazon Robotics
- **Workspace URL:** https://fevm-amz-robotics-9868sm.cloud.databricks.com
- **CLI Profile:** DEFAULT
- **Catalog:** amz_robotics_9868sm_catalog
- **Schema:** fleet_health
- **SQL Warehouse ID:** 6cd0f5c0ba7fd445
- **Lakebase Instance:** amz-robotics-db (fresh — created in Phase 8B)
- **Lakebase Database:** amz_robotics
- **Lakebase MCP App Name:** lakebase-mcp-server (shared — first deploy on this workspace)
- **MAS Tile ID:** TODO (first 8 chars of tile_id — see Gotcha #24 for discovery)
- **Genie Space ID:** TODO (set in Phase 8C)
- **KA Tile ID:** TODO (set in Phase 8C — Knowledge Assistant over service_manual_chunks)
- **App Name:** fleet-health-console
- **Wow Moment:** Incidents page — persona switcher (RME Tech ↔ RME Lead) on a single incident card

## Architecture — 3 Layers

### Layer 1: CORE (never modify)
`app/backend/core/` — Battle-tested wiring extracted from production demos.
- `lakehouse.py` — `run_query()` for Delta Lake via Statement Execution API
- `lakebase.py` — PostgreSQL pool with OAuth token refresh and retry
- `streaming.py` — MAS SSE streaming proxy with action card detection
- `health.py` — 3-check health endpoint (SDK, SQL warehouse, Lakebase)
- `helpers.py` — `_safe()` input validation, `_extract_agent_response()` parser

`lakebase-mcp-server/` — Standalone Lakebase MCP server deployed as a separate Databricks App.
- `mcp_server.py` — 16 MCP tools (CRUD, DDL, SQL, transactions) + web UI
- `app.yaml` — Template pointing to Lakebase instance
- Deployed separately, wired to MAS as an `external-mcp-server` sub-agent for writes

### Layer 2: SKELETON (fill placeholders)
- `app/app.yaml` — Deployment config with TODO placeholders
- `app/backend/main.py` — App assembly, imports core, mounts routes
- `lakebase/core_schema.sql` — 3 required tables (notes, agent_actions, workflows)
- `lakebase/domain_schema.sql` — Your domain-specific tables
- `notebooks/` — Schema setup, data generation, Lakebase seeding
- `agent_bricks/` — MAS and KA config templates
- `genie_spaces/` — Genie Space config template

### Layer 3: CUSTOMER (vibe generates)
- Domain-specific routes in `main.py`
- Frontend pages and visualizations
- Data model and generation scripts
- Agent prompts and tool descriptions
- Talk track and demo flow

### Lakebase MCP Server (agent writes — shared across demos)

The Lakebase MCP Server is a **single shared Databricks App** that exposes Lakebase CRUD operations as MCP tools. Deploy it once and reuse it across multiple demos via URL-based database routing.

**How it works:**
1. Deploy `lakebase-mcp-server/` as a Databricks App (e.g., `lakebase-mcp-server`) — **one-time setup**
2. For each demo, create a UC HTTP connection pointing to `https://<mcp-app-url>/db/<database_name>/mcp/`
3. Add the MCP connection as a sub-agent in the demo's MAS config
4. MAS now has 16 tools scoped to that demo's database

**Multi-database routing:** The server uses `/db/{database}/mcp/` URL routing with per-database connection pools. Multiple demos can use the same MCP server concurrently without interference. The `/mcp/` endpoint (no database prefix) still works for backward compatibility using the default database.

**Adding a new demo's database to the shared server:**
1. Register the database as a resource: `databricks apps update lakebase-mcp-server --json '{"resources": [... all databases ...]}'`
2. Redeploy to grant the SP access
3. Grant table access via `databricks psql`
4. Create a UC HTTP connection with `base_path=/db/<new_database>/mcp/`

**16 MCP Tools available to MAS:**
- READ: `list_tables`, `describe_table`, `read_query`, `list_schemas`, `get_connection_info`, `list_slow_queries`
- WRITE: `insert_record`, `update_records`, `delete_records`, `batch_insert`
- SQL: `execute_sql`, `execute_transaction`, `explain_query`
- DDL: `create_table`, `drop_table`, `alter_table`

See `lakebase-mcp-server/CLAUDE.md` for full deployment and multi-database setup instructions.

## Core Module Reference

### `run_query(sql: str) -> list[dict]`
Execute SQL against Delta Lake via the Statement Execution API. Returns typed dicts (INT, DOUBLE, BOOLEAN auto-converted). Handles ResultManifest compatibility across SDK versions.

### `run_pg_query(sql: str, params=None) -> list[dict]`
Execute a read query against Lakebase. Auto-retries on stale token (InterfaceError/OperationalError). Returns dicts with Decimal→float and datetime→isoformat conversion.

### `write_pg(sql: str, params=None) -> Optional[dict]`
Execute INSERT/UPDATE/DELETE against Lakebase. Use `RETURNING *` to get the created/updated row back. Returns `{"affected": N}` for non-RETURNING queries.

### `_safe(val: str) -> str`
Whitelist regex validator for values injected into SQL strings. Raises HTTP 400 on injection attempts. Use for filter params in WHERE clauses.

### `_extract_agent_response(data: dict) -> str`
Parse MAS Agent Bricks response format. Handles `output[].content[].text`, plain string output, and legacy chat completions format.

### `stream_mas_chat(message, chat_history, action_card_tables) -> AsyncGenerator`
Async generator yielding SSE events from MAS. Configure `action_card_tables` to auto-detect entities created during chat.

### `health_router` (FastAPI APIRouter)
Include with `app.include_router(health_router)`. Exposes `GET /api/health` returning `{status: "healthy"|"degraded", checks: {sdk, sql_warehouse, lakebase}}`.

## Backend Patterns

### All blocking I/O must use asyncio.to_thread()
```python
# CORRECT
rows = await asyncio.to_thread(run_query, "SELECT * FROM my_table")
# WRONG — blocks the event loop
rows = run_query("SELECT * FROM my_table")
```

### Parallel queries with asyncio.gather()
```python
q_metrics, q_alerts, q_recent = await asyncio.gather(
    asyncio.to_thread(run_query, "SELECT COUNT(*) as total FROM items"),
    asyncio.to_thread(run_pg_query, "SELECT * FROM alerts WHERE status = 'open'"),
    asyncio.to_thread(run_pg_query, "SELECT * FROM activity ORDER BY created_at DESC LIMIT 10"),
)
```

### Use _safe() for all user-provided filter values
```python
@app.get("/api/items")
async def get_items(status: Optional[str] = None):
    where = []
    if status:
        where.append(f"status = '{_safe(status)}'")
    clause = "WHERE " + " AND ".join(where) if where else ""
    return await asyncio.to_thread(run_query, f"SELECT * FROM items {clause}")
```

### Use parameterized queries (%s) for Lakebase
```python
# CORRECT — parameterized
rows = run_pg_query("SELECT * FROM notes WHERE entity_id = %s", (entity_id,))
# WRONG — string interpolation
rows = run_pg_query(f"SELECT * FROM notes WHERE entity_id = '{entity_id}'")
```

### Pydantic models for request bodies
```python
from pydantic import BaseModel
class ItemCreate(BaseModel):
    name: str
    priority: str = "medium"
    description: str
    assigned_to: Optional[str] = None

@app.post("/api/items")
async def create_item(body: ItemCreate):
    return await asyncio.to_thread(
        write_pg,
        "INSERT INTO items (name, priority, description, assigned_to) VALUES (%s, %s, %s, %s) RETURNING *",
        (body.name, body.priority, body.description, body.assigned_to),
    )
```

## Frontend Patterns

### SSE Streaming Protocol
The chat UI handles these SSE event types (all implemented in `sendChat()`):
- `thinking` — Reasoning text from intermediate rounds (render as collapsible block above answer)
- `delta` — Text chunk from the final answer (stream into the answer div)
- `tool_call` — Sub-agent invoked (show as step indicator)
- `agent_switch` — MAS switched sub-agents (update step)
- `sub_result` — Data returned from sub-agent (show as completed step)
- `action_card` — Entity created/referenced (render interactive card)
- `suggested_actions` — Follow-up prompts (render as clickable buttons)
- `error` — Error message (display in red)
- `session_expired` — OBO token expired; frontend should auto-reload (see Gotcha #29)
- `[DONE]` — Stream complete

**Phase tracking:** The backend buffers text per MAS round. Non-final rounds (with pending MCP approvals) emit text as `thinking`. The final round emits text as `delta`. The frontend renders `thinking` in a collapsible reasoning block and `delta` as the clean answer. `renderThinkingBlock()` splits reasoning into steps at action-phrase boundaries.

### Action Cards
Configure `ACTION_CARD_TABLES` in `main.py` to auto-detect entities created during chat. Each card gets approve/dismiss buttons that PATCH the entity status via your API.

### CSS Variables for Theming
Override these in `:root` to rebrand. Variables use semantic names (`--primary`, `--accent`) not color names:
```css
:root {
  --primary: #1a2332;     /* Main dark — sidebar, headers */
  --accent: #f59e0b;      /* CTA / highlight color */
  --green: #10b981;       /* Success */
  --red: #ef4444;         /* Error/critical */
  --blue: #3b82f6;        /* Info */
}
```

### Adding New Pages
1. Add nav link in sidebar: `<a href="#" data-page="mypage">...</a>`
2. Add page div: `<div id="page-mypage" class="page">...</div>`
3. Add to PAGES array and PAGE_TITLES map in JS
4. Add `loadMypage()` function and call it from `navigate()`
5. Use `fetchApi()` to load data and render into the page div

### formatAgentName() Mapping
Customize this function to map MAS tool names to display labels:
```javascript
function formatAgentName(name) {
  const shortName = name.includes('__') ? name.split('__').pop() : name;
  const map = {
    'my_data_space': 'Data Query',
    'my_knowledge_base': 'Knowledge Base',
    'my_calculator': 'Calculator Tool',
    'mcp-lakebase-connection': 'Database (write)',
  };
  return map[shortName] || map[name] || shortName.replace(/[-_]/g, ' ');
}
```

## Frontend Generation Flow

**IMPORTANT: Before building any frontend pages, ask the user these questions:**

1. **Layout style** — Do you want a sidebar nav (default), top nav bar, or a dashboard-first layout?
2. **Color scheme** — What colors match the customer's brand? Options: dark industrial (navy/orange), clean medical (white/teal), corporate blue (navy/blue), or provide custom hex colors.
3. **Dashboard content** — What should the main dashboard show? KPI cards, charts, tables, a morning briefing, or a command-center style with AI input?
4. **Additional pages** — Beyond AI Chat and Agent Workflows (included), what domain pages are needed? (e.g., inventory, shipments, patients, risk scores)

**The template includes two starter pages that are already functional:**
- **AI Chat** — Full SSE streaming with sub-agent step indicators, action cards, follow-up suggestions. Do NOT rebuild this — customize the suggested prompts, welcome card text, and `formatAgentName()` mapping.
- **Agent Workflows** — Workflow cards with severity, status filters, centered modal with two-column layout (details + animated agent flow diagram), inline AI analysis. Backend endpoints `GET /api/agent-overview` and `PATCH /api/workflows/{id}` are built into `main.py`. Do NOT rebuild this — customize `DOMAIN_AGENTS`, `WORKFLOW_AGENTS`, and `TYPE_LABELS`.

**The template layout (sidebar + topbar) is a minimal placeholder.** Replace it entirely based on the user's layout preference. The JS navigation system (`PAGES`, `PAGE_TITLES`, `navigate()`) supports any layout — just update the nav links and add page divs.

**When generating new pages:**
- Use the CSS component toolkit (`.kpi-row`, `.card`, `.grid-2`, `.badge-*`, `.btn-*`, `.filter-bar`, `table`, `.pill-*`) — these are layout-agnostic
- Use `fetchApi()` for all data loading, `showSkeleton()` for loading states, `animateKPIs()` for entrance animations
- Use `askAI(prompt)` to bridge any page to the chat (e.g., "Ask AI about this item")
- Follow the Adding New Pages pattern above

## Data Model Convention

| Layer | Technology | Purpose | Tables |
|-------|-----------|---------|--------|
| **Analytics** | Delta Lake | Historical data, aggregations, ML features | Domain-specific (e.g., assets, sensors, orders) |
| **OLTP** | Lakebase (PostgreSQL) | Real-time operations, agent writes | Core (notes, agent_actions, workflows) + domain-specific |
| **AI** | Genie Space | Natural language queries | Points at Delta Lake tables |
| **AI** | MAS Agent Bricks | Multi-agent orchestration | Orchestrates Genie, KA, UC functions, Lakebase |

## Deployment Sequence

**CRITICAL: Data notebooks (steps 1-6) MUST run BEFORE the app deployment (step 12). If you deploy the app first, users will see an empty dashboard with no data. The app queries Delta Lake tables that don't exist until the notebooks create them.**

### Phase A: Delta Lake Data (do this FIRST)
1. **Create catalog/schema** — Run `notebooks/01_setup_schema.sql`
2. **Generate Delta Lake data** — Run `notebooks/02_generate_data.py` — **This creates the tables the app reads from**
3. **Verify tables** — `SHOW TABLES IN <catalog>.<schema>` should list your domain tables

### Phase B: Lakebase
4. **Create Lakebase instance** — `databricks database create-database-instance <name> --capacity CU_1 --profile=<profile>`. Instance name uses HYPHENS not underscores. Takes ~6 min. Poll until state is `AVAILABLE` (NOT `RUNNING` — see Gotcha #33):
```bash
databricks database get-database-instance <name> --profile=<profile> -o json | jq '.state'
```
5. **Create database** — `databricks psql <instance> --profile=<profile> -- -c "CREATE DATABASE <db_name>;"`
6. **Apply schemas** — `databricks psql <instance> --profile=<profile> -- -d <db> -f lakebase/core_schema.sql` then `domain_schema.sql`. **Do NOT grant SP access yet** — the SP role doesn't exist until the app's database resource is registered and redeployed (see Gotcha #35). Grants happen after Step 14/15.
7. **Seed Lakebase** — **Recommended: Seed via local CLI** (not serverless notebooks — see Gotcha #34). Serverless runtimes run as ephemeral `spark-*` users that have no Lakebase role. Use `databricks psql <instance> --profile=<profile> -- -d <db> -f /tmp/seed.sql` or a local Python script with `generate-database-credential` (CLI requires `request_id` in the JSON payload — see Gotcha #36).

### Phase C: AI Layer
8. **Create Genie Space** — POST with `serialized_space` to create a blank space, then PATCH title/description, then PATCH tables via `serialized_space` with sorted dotted identifiers (NOT `table_identifiers` — that field is silently ignored). Verify with `?include_serialized_space=true`. See Gotcha #10 for full commands.
9. **Grant Genie permissions** — `CAN_RUN` to app SP and users via `PATCH /api/2.0/permissions/genie/<space_id>` (note: just `genie`, NOT `genie/spaces` -- see Gotcha #11)
10. **Deploy Lakebase MCP Server** — Deploy `lakebase-mcp-server/` as a separate app, create UC HTTP connection (see Lakebase MCP section)
11. **Create MAS** — POST to `/api/2.0/multi-agent-supervisors` with agent config. Agent types use **kebab-case**: `genie-space` (with `genie_space.id`), `external-mcp-server` (with `external_mcp_server.connection_name`), `knowledge-assistant`, `unity-catalog-function`. **IMPORTANT:** POST fails with `external-mcp-server` agents — create with simpler agents first (POST), then add MCP agents via PATCH (see Gotcha #34). After creation, discover the full tile UUID via serving endpoints (see Gotcha #24).

### Phase D: App Deployment (do this LAST)
12. **Fill app.yaml** — Set warehouse ID, catalog, schema, MAS tile ID (first 8 chars), Lakebase instance/db
13. **Deploy app** — `databricks apps deploy <name> --source-code-path <path> --profile=<profile>`
14. **Register resources via API** — **CRITICAL: `app.yaml` resources are NOT automatically registered.** You MUST register them via the API, then redeploy:
```bash
databricks apps update <app-name> --json '{
  "resources": [
    {"name": "sql-warehouse", "sql_warehouse": {"id": "<warehouse-id>", "permission": "CAN_USE"}},
    {"name": "mas-endpoint", "serving_endpoint": {"name": "mas-<tile-8-chars>-endpoint", "permission": "CAN_QUERY"}},
    {"name": "database", "database": {"instance_name": "<instance>", "database_name": "<db>", "permission": "CAN_CONNECT_AND_CREATE"}}
  ]
}' --profile=<profile>
```
15. **Redeploy after resource registration** — `databricks apps deploy <name> --source-code-path <path> --profile=<profile>` — Databricks Apps only inject `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER` env vars at deploy time. If you skip this redeploy, Lakebase connections will fail.
16. **Grant permissions explicitly** — Resource registration does NOT reliably grant permissions. Grant each one explicitly:
    - SQL warehouse: `CAN_USE` to app SP
    - Catalog/schema: `USE_CATALOG`, `USE_SCHEMA`, `SELECT` to app SP
    - MAS serving endpoint: `CAN_QUERY` to app SP (see Gotcha #25 — without this, chat returns 403)
17. **Verify** — Visit the app URL in your browser (OAuth login required). Dashboard should show data. `GET /api/health` should return `{"status": "healthy"}` with all three checks passing (SDK, SQL warehouse, Lakebase). If any check fails, see the troubleshooting table below.

**Troubleshooting after deploy:**

| Symptom | Cause | Fix |
|---------|-------|-----|
| Health shows `lakebase: error` | Resources not registered via API, or not redeployed after registration | Run `databricks apps update` with all resources, then redeploy |
| Health shows `sql_warehouse: error` | Warehouse resource not registered or SP lacks CAN_USE | Register via API + grant CAN_USE to app SP |
| Dashboard shows zeros / empty | Delta Lake tables don't exist | Run `notebooks/02_generate_data.py` first |
| Dashboard loads but Lakebase pages empty | Lakebase schemas not applied or SP lacks table grants | Apply `core_schema.sql` + `domain_schema.sql`, grant SP access |
| Chat returns 403 | MAS endpoint resource registered but SP lacks CAN_QUERY | Grant CAN_QUERY explicitly on the serving endpoint (Gotcha #25) — no redeploy needed |
| Chat returns 403 (intermittent, works after page refresh) | OBO user token expired (~12h lifetime) | Implement `session_expired` auto-refresh pattern (Gotcha #29) |
| 401 / empty `{}` from curl | Normal — Databricks Apps require browser OAuth | Open the app URL in a browser instead |

## Known Gotchas

### 1. Lakebase InterfaceError + OperationalError
The Lakebase OAuth token expires periodically. The core pool catches BOTH `psycopg2.InterfaceError` AND `psycopg2.OperationalError` and reinitializes with a fresh token. This is already handled in `core/lakebase.py` — do NOT modify.

### 2. Agent Brick endpoint naming
The MAS serving endpoint name is: `mas-{first_8_chars_of_tile_id}-endpoint`. Use the short 8-char prefix as `MAS_TILE_ID` in `app.yaml`, NOT the full UUID.

### 3. MAS PATCH requires full agents array
When updating MAS instructions via PATCH `/api/2.0/multi-agent-supervisors/{tile_id}`, you must include `name` AND the complete `agents` array, even if you're only changing instructions. **IMPORTANT:** The `{tile_id}` in the API path must be the **full UUID**, not the 8-char prefix used in the endpoint name. Agent types use **kebab-case**: `genie-space`, `external-mcp-server`, `knowledge-assistant`, `unity-catalog-function`.

### 4. MCP create_or_update_mas doesn't support all agent types
The Databricks MCP tool for MAS doesn't support `unity-catalog-function` or `external-mcp-server` agent types. Use the REST API directly for MAS configs that include these. Note: agent types use kebab-case (`external-mcp-server`, not `mcp_connection`).

### 5. Lakebase instance uses hyphens
Instance names use hyphens (e.g., `my-demo-db`), NOT underscores. The Lakebase API will reject names with underscores.

### 6. Notebook auth: use generate_database_credential()
In serverless notebooks, `w.config._header_factory` tokens are NOT valid for Lakebase PG connections. Use:
```python
cred = w.database.generate_database_credential(instance_names=["my-instance"])
password = cred.token
```
`_header_factory` only works inside Databricks Apps where PGHOST/PGUSER are injected by the app resource system.

### 7. ResultManifest SDK compatibility
Newer SDK versions use `manifest.schema.columns` instead of `manifest.columns`. The core module handles this:
```python
columns = getattr(manifest, "columns", None) or getattr(manifest.schema, "columns", [])
```

### 8. app.yaml resources are NOT auto-registered — you MUST use the API
**This is the #1 cause of "app deployed but nothing works."** The `app.yaml` `resources:` section is declarative documentation only — it does NOT register resources with the Databricks Apps platform. You MUST register resources via `databricks apps update --json '{"resources": [...]}'` AND then redeploy. Without this:
- `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER` env vars are never injected → Lakebase connection fails
- The app SP has no access to the SQL warehouse → Delta Lake queries fail
- The app SP has no access to the MAS endpoint → chat returns 403

**The fix is always:** register resources via API → redeploy → verify with `/api/health`.

### 9. `databricks apps update` replaces all resources
PATCH/update to app resources replaces the entire resources array. Always include ALL resources in the update, not just the new one.

### 10. Genie Space creation — tables MUST use serialized_space format
The Genie Space API is full of silent-failure traps. Both POST and PATCH silently ignore `table_identifiers` — tables only work via the `serialized_space` JSON string field with dotted three-part identifiers, sorted alphabetically.

```bash
# Step 1: Create a blank Genie Space
databricks api post /api/2.0/genie/spaces --json '{
  "serialized_space": "{\"version\": 2}",
  "warehouse_id": "<warehouse-id>"
}' --profile=<profile>
# Returns: {"space_id": "abc123..."}

# Step 2: PATCH title and description (these fields work directly)
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "title": "My Demo Data Space",
  "description": "Query data about ..."
}' --profile=<profile>

# Step 3: PATCH tables via serialized_space (ONLY way that works)
# Tables must be dotted 3-part names, SORTED ALPHABETICALLY
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "serialized_space": "{\"version\":2,\"data_sources\":{\"tables\":[{\"identifier\":\"my_catalog.my_schema.table1\"},{\"identifier\":\"my_catalog.my_schema.table2\"}]}}"
}' --profile=<profile>

# Step 4: PATCH instructions (optional but recommended)
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "instructions": "You are a data assistant for ... Use these terms: ..."
}' --profile=<profile>

# Step 5: VERIFY tables are actually attached
databricks api get /api/2.0/genie/spaces/<space_id>?include_serialized_space=true --profile=<profile>
# Parse the serialized_space JSON string → data_sources.tables should list your tables
# WARNING: The table_identifiers field in the GET response is ALWAYS EMPTY — only check serialized_space
```

**Silent failure traps:**
- `table_identifiers` in POST body → silently ignored, space created with zero tables
- `table_identifiers` in PATCH body → returns 200 but tables are NOT attached
- Unsorted tables in `serialized_space` → returns 400: "data_sources.tables must be sorted by identifier"
- `table_identifiers` in GET response → always empty even if tables exist; parse `serialized_space` instead

### 11. Grant CAN_RUN on Genie Space
The app SP needs CAN_RUN permission on the Genie Space. Also grant to the `account users` group for demo users.

**IMPORTANT:** The permissions endpoint is `/api/2.0/permissions/genie/{space_id}` -- note it is just `genie`, NOT `genie/spaces`.
```bash
# CORRECT endpoint:
databricks api patch /api/2.0/permissions/genie/<space_id> --json '{
  "access_control_list": [
    {"group_name": "users", "permission_level": "CAN_RUN"}
  ]
}' --profile=<profile>

# WRONG (returns 404):
# databricks api patch /api/2.0/permissions/genie/spaces/<space_id> ...
```

### 12. MAS agent types use kebab-case
All MAS agent types use **kebab-case**, not snake_case. The correct formats are:
```json
{"agent_type": "genie-space", "genie_space": {"id": "..."}}
{"agent_type": "knowledge-assistant", "knowledge_assistant": {"knowledge_assistant_id": "..."}}
{"agent_type": "unity-catalog-function", "unity_catalog_function": {"uc_path": {"catalog": "...", "schema": "...", "name": "..."}}}
{"agent_type": "external-mcp-server", "external_mcp_server": {"connection_name": "..."}}
```
**Common mistake:** Using `databricks_genie` instead of `genie-space`, or `mcp_connection.mcp_connection_id` instead of `external_mcp_server.connection_name`.

### 13. Empty app = data notebooks not run
If the app dashboard shows empty/zero metrics, the Delta Lake tables don't exist yet. You MUST run `02_generate_data.py` BEFORE deploying the app. The app queries tables that this notebook creates.

### 14. App health returns {} or 401
The Databricks Apps proxy requires OAuth authentication. `curl` from the terminal gets a 401 (`{}`). You must visit the app URL in a browser to trigger OAuth login and see the real app. The health endpoint works correctly when accessed through the browser.

### 15. Lakebase MCP trailing slash
The MCP endpoint is at `/mcp/` (with trailing slash). When creating the UC HTTP connection, set `base_path=/mcp/`. Without the trailing slash, Starlette redirects to `localhost:8000/mcp/` which breaks behind the Databricks App proxy.

### 16. MAS sends JSON strings for MCP tool params
MAS agents serialize nested objects as JSON strings instead of native objects. The Lakebase MCP server handles this via `_ensure_dict()` and `_ensure_list()` coercion, but if you build custom MCP tools, you must handle both formats.

### 17. CAN_USE on Lakebase MCP app for MAS
MAS External MCP Server goes through a Databricks MCP proxy that authenticates as a service principal. You must grant `CAN_USE` to the `users` group on the Lakebase MCP app, otherwise the proxy gets 401.

### 18. OAuth M2M for UC HTTP Connection
For the UC HTTP connection to the Lakebase MCP server, use Databricks OAuth M2M (not PAT). SP OAuth secrets can be created via the workspace API. Connection fields: `host`, `port=443`, `base_path=/mcp/`, `client_id`, `client_secret`, `oauth_scope=all-apis`.

**Creating SP OAuth secrets:**
```bash
# 1. Find the SP's numeric ID (NOT the application/client UUID)
databricks service-principals list --profile=<profile> -o json | \
  jq '.[] | select(.applicationId == "<client-uuid>") | .id'

# 2. Create a secret using the numeric ID
databricks api post /api/2.0/accounts/servicePrincipals/<numeric-id>/credentials/secrets \
  --profile=<profile>
# Returns: {"secret": "dose...", "id": "...", ...}
```

**CRITICAL: The `host` field MUST include the `https://` scheme.** Without it, you get a "Missing cloud file system scheme" error.
```
# CORRECT:
host = "https://my-app.aws.databricksapps.com"

# WRONG (causes "Missing cloud file system scheme" error):
host = "my-app.aws.databricksapps.com"
```

### 19. Agent Workflows page requires Lakebase tables
The Agent Workflows page fetches data from `GET /api/agent-overview` (built into `main.py`), which queries the Lakebase `workflows` and `agent_actions` tables (from `core_schema.sql`). The approve/dismiss buttons call `PATCH /api/workflows/{id}` (also built-in). If Lakebase is not set up, the endpoint returns empty data gracefully. **You MUST create the Lakebase instance, database, and apply `core_schema.sql` BEFORE deploying the app** for the KPIs and workflow cards to show meaningful data.

### 20. Frontend has no dashboard — vibe must build it
The scaffold template only includes two starter pages (AI Chat + Agent Workflows). There is NO dashboard page by default. Vibe must generate the dashboard, layout, and domain pages based on the user's preferences. See the "Frontend Generation Flow" section for what to ask before building.

### 21. Statement Execution API only supports single statements
The Databricks Statement Execution API (`POST /api/2.0/sql/statements`) executes a **single** SQL statement per request. Sending multiple statements separated by `;` fails with a parse error. The notebook UI splits on `-- COMMAND ----------` markers and sends each cell individually, so multi-statement `.sql` files work fine in the notebook UI but fail when executed via API or CLI. When automating notebook execution via API, send each statement as a separate API call.

### 22. Serverless notebook SDK missing `w.database` — upgrade first
The serverless notebook runtime ships an older `databricks-sdk` that does NOT include the `w.database` module. Calling `w.database.generate_database_credential()` throws `AttributeError: 'WorkspaceClient' object has no attribute 'database'`. **Fix:** Add `%pip install --upgrade databricks-sdk` as the first cell and restart the Python interpreter (`dbutils.library.restartPython()`). Alternatively, skip the notebook entirely and seed Lakebase via local CLI: `databricks psql <instance> --profile=<profile> -- -d <database> -f /tmp/seed.sql`.

### 23. PGHOST not set after resource PATCH — must redeploy
If you add a `database` resource via `databricks api patch /api/2.0/apps/<name>` AFTER deploying, the app crashes with `psycopg2.OperationalError: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432"`. Databricks Apps only inject `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER` env vars **at deploy time**. A resource PATCH alone does NOT inject them. **Fix: Always redeploy after adding or changing resources.** The core `lakebase.py` guards against this — if `PGHOST` is empty, the pool is skipped and a clear warning is logged instead of crashing.

### 24. MAS tile ID discovery — no list endpoint
There is **NO list endpoint** for Multi-Agent Supervisors. To find the MAS tile ID after creation:
1. `GET /api/2.0/serving-endpoints` — list all serving endpoints
2. Find the one named `mas-{8chars}-endpoint`
3. Extract `tile_endpoint_metadata.tile_id` from the endpoint object — this is the **full UUID**
4. Use the **full UUID** for GET/PATCH: `/api/2.0/multi-agent-supervisors/{full-uuid}`

**Common mistake:** Using the 8-char prefix as the tile ID in API calls. The 8-char prefix is only for the endpoint name. The REST API requires the full UUID.

```bash
# Example: discover the MAS tile ID
databricks api get /api/2.0/serving-endpoints --profile=<profile> | \
  jq '.endpoints[] | select(.name | startswith("mas-")) | .tile_endpoint_metadata.tile_id'
```

### 25. MAS serving endpoint CAN_QUERY must be granted explicitly
Registering the `mas-endpoint` resource via `databricks apps update` with `"permission": "CAN_QUERY"` declares the intent but does NOT reliably grant the permission to the app SP. The chat endpoint returns 403 even though the resource is registered. **Fix:** Grant CAN_QUERY explicitly on the serving endpoint itself. This does NOT require a redeploy — permissions take effect immediately.

**IMPORTANT:** The permissions API requires the endpoint's **UUID**, not its display name. Using the name returns `'mas-xxx-endpoint' is not a valid Inference Endpoint ID`.
```bash
# Step 1: Get the endpoint UUID
databricks api get /api/2.0/serving-endpoints --profile=<profile> | \
  jq '.endpoints[] | select(.name == "mas-<tile-8-chars>-endpoint") | .id'

# Step 2: Grant using UUID
databricks api patch /api/2.0/permissions/serving-endpoints/<endpoint-uuid> \
  --json '{"access_control_list":[{"service_principal_name":"<app-sp-client-id>","permission_level":"CAN_QUERY"}]}' \
  --profile=<profile>
```

### 26. MCP tool calls require auto-approval in streaming
When MAS calls an External MCP Server tool, it emits an `mcp_approval_request` event and pauses — it will NOT execute the tool until the caller sends back an approval. Without handling this, the MAS tells the user "I don't see the tool available" even though it's configured.

The `core/streaming.py` module handles this automatically: it detects `mcp_approval_request` items, auto-approves them by sending a follow-up request with `mcp_approval_response`, and continues streaming. The approval payload format:
```json
{
  "type": "mcp_approval_response",
  "id": "approval-1-<request_id>",
  "approval_request_id": "<id from mcp_approval_request>",
  "approve": true
}
```
The follow-up request must include the original conversation + **ALL output items** from the current round (message, function_call, function_call_output, mcp_approval_request) + the approval response(s). **Do NOT filter output items** — omitting function_call/function_call_output breaks the MAS conversation context and causes "unexpected position" errors. Up to 10 approval rounds are supported per chat turn (safety limit).

### 27. MAS instructions must reference actual MCP tool names
MAS instructions that say "Use the Lakebase MCP tool to INSERT..." will confuse the model — it looks for a tool literally called "Lakebase MCP tool" and fails. The actual tool names are `insert_record`, `execute_sql`, `read_query`, `update_records`, etc. Always reference specific tool names in MAS instructions and include a tool reference section listing available tools.

### 28. MAS max_turns must be increased for MCP workflows
The default MAS step/turn limit is too low for multi-tool workflows that include MCP. The model exhausts its budget on analysis tools (Genie, UC functions) before reaching MCP tools, then reports "MCP tools not accessible." **Fix:** Pass `"max_turns": 15` in the MAS invocation payload. The `core/streaming.py` module does this automatically. Without this, MCP tools will never be called in complex workflows.

### 29. MCP tools require user identity (OBO), not app SP
MAS External MCP Server tools ONLY work when called with a **user's OAuth token**, not the app's Service Principal token. With an SP token, MCP tools appear as `function_call` with "Tool not found" instead of `mcp_approval_request`. The Databricks App proxy forwards the user's token as `x-forwarded-access-token`. **Setup:**
1. Configure the app to forward user scopes:
```bash
databricks api patch /api/2.0/apps/<app-name> --json '{"user_api_scopes": ["serving.serving-endpoints", "sql"]}' --profile=<profile>
```
2. Read the token in the backend: `request.headers.get("x-forwarded-access-token", "")`
3. Use it for MAS calls: `Authorization: Bearer <user_token>`
4. Users must **re-authenticate** (incognito window) after scope changes take effect.

The `core/streaming.py` module supports both modes via the `user_token` parameter. Without a user token, it falls back to SP auth with a warning.

### 30. MCP approval accumulation — ALL items from ALL rounds
Each MCP approval round must include ALL output items from ALL previous rounds in the follow-up request. Without this, the model loses context between rounds and skips write operations (e.g., queries data but never inserts). The `core/streaming.py` module uses `all_accumulated` list that persists across rounds. **Never rebuild `input_messages` from only the current round's items.**

### 31. MCP user-approval mode — frontend approval UI
The `core/streaming.py` supports two modes via `mcp_auto_approve` parameter:
- **Auto-approve** (`True`, default): Backend auto-approves all MCP tool calls. Simple, seamless.
- **User approval** (`False`): Backend pauses, sends `mcp_approval` SSE event to frontend with tool name/args. Frontend shows approval UI. User clicks Approve/Deny. Frontend sends `{"approve_mcp": true/false}` back. Backend resumes with full accumulated context.

For user-approval mode, the chat endpoint must handle two request types:
1. New message: `{"message": "...", "history": [...]}`
2. Approval continuation: `{"approve_mcp": true/false}` (no message field)

### 32. Lakebase CLI uses compound command names
The Lakebase CLI subcommands use longer compound names: `create-database-instance` (not `create-instance`), `get-database-instance` (not `get-instance`), `generate-database-credential`. Run `databricks database --help` to confirm exact subcommand names.

### 33. Lakebase instance state is AVAILABLE, not RUNNING
When polling a Lakebase instance after creation, check for state `AVAILABLE` (not `RUNNING`). The instance transitions from `STARTING` → `AVAILABLE`. Polling for `RUNNING` will time out indefinitely while the instance is already functional. `--capacity` is also **required** — valid values: `CU_1`, `CU_2`, `CU_4`, `CU_8`. Use `CU_1` for demos.

### 34. Serverless notebooks can't authenticate to Lakebase
Serverless notebook runtimes run as ephemeral `spark-*` service accounts (e.g., `spark-3da802a0-03e1-4171-...`). These users have NO Lakebase role and `psycopg2` connections fail with `FATAL: role "spark-..." does not exist`. Even if `generate_database_credential()` produces a valid token, the connection user must be set to a real identity (email), not empty string. **Fix:** Seed Lakebase via local CLI (`databricks psql` or local Python script with `generate-database-credential`) instead of serverless notebooks.

### 35. SP role in Lakebase requires app resource registration + redeploy
Service principal roles are only created in a Lakebase instance AFTER the SP's app has a database resource registered AND has been redeployed. A brand-new instance has no SP roles. Trying to `GRANT ALL ... TO "<sp-client-id>"` before this returns `role "..." does not exist`. **Correct order:** (1) create instance + database, (2) apply schemas, (3) register database as app resource (`databricks apps update`), (4) redeploy app, (5) THEN grant to the SP.

### 36. `generate-database-credential` CLI requires `request_id`
The CLI's `generate-database-credential` command requires `request_id` in the JSON body, even though the Python SDK's `w.database.generate_database_credential()` doesn't:
```bash
# CORRECT:
databricks database generate-database-credential --json '{"instance_names": ["<instance>"], "request_id": "seed"}' --profile=<profile>

# WRONG (returns "Field request_id must be defined"):
databricks database generate-database-credential --json '{"instance_names": ["<instance>"]}' --profile=<profile>
```

### 37. MAS POST fails with external-mcp-server agents
Creating a MAS with `external-mcp-server` agents in the initial POST payload returns `Unknown agent type: Empty`. **Fix:** Create the MAS with simpler agents first (genie-space, knowledge-assistant) via POST, then add MCP agents via PATCH. The PATCH must include the `name` field and the **full agents array** (all existing + new agents).

### 38. `agent_actions` status values — use `executed`, not `completed`
The `core_schema.sql` CHECK constraint on `agent_actions.status` only allows: `pending`, `executed`, `dismissed`, `failed`. Using `completed` (a natural-sounding synonym) causes INSERT to fail. Always verify enum values against the table's CHECK constraints when seeding data.

### 39. f-string backslash syntax crashes Python 3.11
Python 3.11 (Databricks Apps runtime) does NOT allow `\n`, `\\n`, `\'` inside the `{...}` expression part of f-strings. The app crashes with `SyntaxError` visible only in `/logz`. **Fix:** Extract the inner string to a variable first:
```python
# WRONG: yield f"data: {json.dumps({'text': f'Summary\\n{ctx}'})}\n\n"
_text = "Summary\n" + ctx
yield f"data: {json.dumps({'text': _text})}\n\n"
```

### 40. Databricks SQL INTERVAL syntax requires quoted numbers
Use `INTERVAL '7' DAY` (quoted number), not `INTERVAL 7 DAY`. The unquoted form may silently return wrong results. **Note:** PostgreSQL (Lakebase) uses different syntax: `INTERVAL '7 days'` (number + unit together inside quotes). Don't mix them up.

### 41. asyncio.gather without return_exceptions kills all queries
`asyncio.gather()` without `return_exceptions=True` propagates the first exception, causing all parallel queries to fail. If one Lakebase query fails, the entire gather fails — returning zeros for Delta Lake queries that would have succeeded. **Fix:** Always use `return_exceptions=True` when mixing Delta Lake and Lakebase queries, and check each result with `isinstance(result, Exception)`.

### 42. Lakebase instance workspace limit (~10)
FEVM workspaces limit ~10 Lakebase instances. **Workaround:** Reuse an existing instance by creating a new database: `databricks psql <instance> -- -c "CREATE DATABASE <new_db>;"`. Update all config files that reference the instance name.

### 43. App crash logs only visible via browser /logz
`databricks apps deploy` gives no useful error on crash. Logs are only at `https://<app-url>/logz` (browser OAuth required). **Workaround:** Use `python3 -c "import py_compile; py_compile.compile('backend/main.py', doraise=True)"` or Chrome DevTools MCP to view `/logz`.

### 44. UC HTTP connection requires token_endpoint field
Without `token_endpoint` in the connection options, the UC API defaults to bearer token auth and rejects OAuth M2M. **Fix:** Include `"token_endpoint": "https://<workspace-url>/oidc/v1/token"` and `"is_mcp_connection": "true"` in the connection options.

### 45. `databricks apps deploy` clears resources array
Deploying resets `resources: []`. Any resources registered before the deploy are lost. **Fix:** Always use the 3-step cycle: (1) deploy code, (2) register resources via `databricks apps update`, (3) redeploy to inject `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER`. Verify with `databricks apps get ... | jq '.resources'`.

### 46. asyncio.gather resilience in ALL Lakebase-touching endpoints
Health check passes (`SELECT 1`) but individual endpoints fail because they query Lakebase tables without error handling. **Root causes:** `/api/agent-overview` has 7 queries without `return_exceptions=True`; `/api/architecture` inner gather mixes Delta+Lakebase without it; `/api/exceptions` has no try/except; dashboard `Promise.all` lacks `.catch()`. **Fix:** Every `asyncio.gather` with `run_pg_query` needs `return_exceptions=True`. Every frontend `Promise.all` needs `.catch()` on each call.

## Lakebase MCP Server Deployment

The scaffold includes a **shared** Lakebase MCP server at `lakebase-mcp-server/`. Deploy it once and reuse across all demos via URL-based database routing (`/db/{database}/mcp/`).

### First-Time Setup (deploy once)
```bash
# 1. Create the app (shared name — not per-demo)
databricks apps create lakebase-mcp-server --profile=<profile>

# 2. Update lakebase-mcp-server/app/app.yaml with your instance and first database

# 3. Sync and deploy
databricks sync ./lakebase-mcp-server/app /Workspace/Users/<you>/lakebase-mcp-server/app --profile=<profile> --watch=false
databricks apps deploy lakebase-mcp-server --source-code-path /Workspace/Users/<you>/lakebase-mcp-server/app --profile=<profile>

# 4. Grant CAN_USE to users group (required for MAS proxy)
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<profile>

# 5. Grant table access to app SP for the first database
databricks psql <instance> --profile=<profile> -- -d <database> -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"<mcp-app-sp-client-id>\";
"
```

### Adding a New Demo's Database (subsequent demos)
```bash
# 1. Register the new database as a resource (include ALL existing databases too)
databricks apps update lakebase-mcp-server --json '{
  "resources": [
    {"name": "database", "database": {"instance_name": "<instance>", "database_name": "<existing_db>", "permission": "CAN_CONNECT_AND_CREATE"}},
    {"name": "database-2", "database": {"instance_name": "<instance>", "database_name": "<new_demo_db>", "permission": "CAN_CONNECT_AND_CREATE"}}
  ]
}' --profile=<profile>

# 2. Redeploy to grant SP access to the new database
databricks apps deploy lakebase-mcp-server --source-code-path /Workspace/Users/<you>/lakebase-mcp-server/app --profile=<profile>

# 3. Grant table access in the new database
databricks psql <instance> --profile=<profile> -- -d <new_demo_db> -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"<mcp-app-sp-client-id>\";
"
```

### Create UC HTTP Connection (per demo)
Create a Unity Catalog connection for each demo pointing to the shared MCP server:
- **Type:** HTTP
- **URL:** `https://<mcp-app-url>/db/<database_name>/mcp/` (database name in the path)
- **Auth:** Databricks OAuth M2M
- **Connection fields:** `host` (**must include `https://`** -- see Gotcha #18), `port=443`, `base_path=/db/<database_name>/mcp/`, `client_id`, `client_secret`, `oauth_scope=all-apis`

For backward compatibility, `base_path=/mcp/` still works (uses the default database from `app.yaml`).

### Wire to MAS
In `agent_bricks/mas_config.json`, add (note: `agent_type` uses kebab-case):
```json
{
  "agent_type": "external-mcp-server",
  "external_mcp_server": {
    "connection_name": "<uc-http-connection-name>"
  },
  "name": "mcp-lakebase-connection",
  "description": "Write operational data to Lakebase PostgreSQL tables. Use for creating work orders, updating statuses, managing alerts, and other CRUD operations."
}
```

## What to Customize vs Keep

### DO NOT MODIFY
- `app/backend/core/` — All 5 core modules
- `app/requirements.txt` — Pinned dependencies
- `lakebase/core_schema.sql` — Required tables
- `lakebase-mcp-server/app/mcp_server.py` — MCP server code (works with any Lakebase database)

### CUSTOMIZE (fill placeholders)
- `app/app.yaml` — Replace all TODO values
- `app/backend/main.py` — Add domain routes after the marked section
- `lakebase/domain_schema.sql` — Add domain-specific tables
- `lakebase-mcp-server/app/app.yaml` — Set instance_name and database_name
- `notebooks/01_setup_schema.sql` — Set catalog/schema names
- `notebooks/02_generate_data.py` — Define domain constants, generate tables
- `notebooks/03_seed_lakebase.py` — Seed domain Lakebase tables
- `agent_bricks/mas_config.json` — Configure MAS agents (include Lakebase MCP connection)
- `genie_spaces/config.json` — Configure Genie Space tables
- `.mcp.json` — Set Databricks CLI profile

### BUILD FROM SCRATCH (vibe generates — ask user preferences first)
- **App layout and navigation** — sidebar, top nav, or dashboard-first (ask user)
- **Color scheme and branding** — update CSS variables in `:root` (ask user)
- **Dashboard page** — KPIs, charts, tables, briefing (ask user what metrics matter)
- **Domain-specific pages** — inventory, shipments, patients, etc. (ask user)
- Domain-specific API endpoints
- Agent prompts and tool descriptions
- Data model and constants for data generation
- Talk track and demo narrative

## Example Prompts for Vibe

### Example 1: Predictive Maintenance (dark industrial theme)
> Build me a predictive maintenance demo for Apex Steel. 5 factories, 200+ CNC machines, IoT sensors streaming vibration/temperature data. Use cases: anomaly detection, work order automation, spare parts optimization. I want a dark industrial theme (navy/orange), sidebar nav, and a dashboard with asset health KPIs and a morning briefing.

### Example 2: Healthcare Operations (clean medical theme)
> Build me a hospital operations demo for Baptist Health. Patient flow across 3 facilities — ER, OR scheduling, bed management. Key metrics: wait times, bed utilization, surgical throughput. I want a clean medical look (white/teal), top nav bar layout, and a dashboard showing real-time bed occupancy and patient queue.

### Example 3: Financial Risk (corporate theme)
> Build me a credit risk monitoring demo for First National Bank. Portfolio of 50K loans, real-time risk scoring, regulatory reporting. I want a corporate blue theme, dashboard-first layout with portfolio breakdown charts, and a risk score heatmap page. Use the scaffold's workflow management for alert handling.

## Reference

See `examples/supply_chain_routes.py` for a complete reference implementation showing all route patterns (KPI metrics, paginated lists, detail views, CRUD, workflows, filters).
