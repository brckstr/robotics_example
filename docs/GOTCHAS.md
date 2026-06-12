# Known Gotchas (Complete Reference)

All documented gotchas with full code examples. Referenced from CLAUDE.md.

---

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
- `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER` env vars are never injected -> Lakebase connection fails
- The app SP has no access to the SQL warehouse -> Delta Lake queries fail
- The app SP has no access to the MAS endpoint -> chat returns 403

**The fix is always:** register resources via API -> redeploy -> verify with `/api/health`.

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
# Parse the serialized_space JSON string -> data_sources.tables should list your tables
# WARNING: The table_identifiers field in the GET response is ALWAYS EMPTY — only check serialized_space
```

**Silent failure traps:**
- `table_identifiers` in POST body -> silently ignored, space created with zero tables
- `table_identifiers` in PATCH body -> returns 200 but tables are NOT attached
- Unsorted tables in `serialized_space` -> returns 400: "data_sources.tables must be sorted by identifier"
- `table_identifiers` in GET response -> always empty even if tables exist; parse `serialized_space` instead

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
**Common mistakes:**
- Using `databricks_genie` instead of `genie-space`
- Using `mcp_connection.mcp_connection_id` instead of `external_mcp_server.connection_name` — the field name changed and the old format silently fails with "Unknown agent type: Empty"

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
For the UC HTTP connection to the Lakebase MCP server, use Databricks OAuth M2M (not PAT). SP OAuth secrets can be created via the workspace API: `POST /api/2.0/accounts/servicePrincipals/{numeric_sp_id}/credentials/secrets` (use the numeric SP ID from `databricks service-principals list`, NOT the application/client UUID). Connection fields: `host`, `port=443`, `base_path=/mcp/`, `client_id`, `client_secret`, `oauth_scope=all-apis`.

**CRITICAL: The `host` field MUST include the `https://` scheme.** Without it, you get a "Missing cloud file system scheme" error.
```
# CORRECT:
host = "https://my-app.aws.databricksapps.com"

# WRONG (causes "Missing cloud file system scheme" error):
host = "my-app.aws.databricksapps.com"
```

### 19. Agent Workflows page requires Lakebase tables
The Agent Workflows page fetches data from `/api/agent-overview`, which queries the Lakebase `workflows` and `agent_actions` tables (from `core_schema.sql`). If Lakebase is not set up, the page shows zeros or errors. **You MUST create the Lakebase instance, database, and apply `core_schema.sql` BEFORE deploying the app.** The frontend `loadAgentPage()` function calls the `/api/agent-overview` endpoint — if you leave it with placeholder/hardcoded values, the KPIs will be misleading.

### 20. Frontend has no dashboard — vibe must build it
The scaffold template only includes two starter pages (AI Chat + Agent Workflows). There is NO dashboard page by default. Vibe must generate the dashboard, layout, and domain pages based on the user's preferences. See the "Frontend Generation Flow" section in CLAUDE.md for what to ask before building.

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

**IMPORTANT:** The permissions API requires the endpoint's **UUID**, not its name. The name works for the management API (`/serving-endpoints/{name}`) but NOT for permissions.
```bash
# Step 1: Get the endpoint UUID
databricks api get /api/2.0/serving-endpoints --profile=<profile> | \
  jq '.endpoints[] | select(.name == "mas-<tile-8-chars>-endpoint") | .id'

# Step 2: Grant using the UUID
databricks api patch /api/2.0/permissions/serving-endpoints/<endpoint-uuid> \
  --json '{"access_control_list":[{"service_principal_name":"<app-sp-client-id>","permission_level":"CAN_QUERY"}]}' \
  --profile=<profile>
```

### 26. Workflow approval only updates status — no side-effects
The scaffold's default `PATCH /api/workflows/{id}` only sets `status = 'approved'` and `completed_at = NOW()`. **No domain actions are executed** — linked entities are not updated, no communications are sent, no notes or agent_actions are recorded. The demo looks broken: user approves an escalation but nothing changes.

**Fix:** Wire side-effects in your `update_workflow()` endpoint per `workflow_type`. After the status PATCH returns the row, dispatch type-specific writes:

```python
@app.patch("/api/workflows/{workflow_id}")
async def update_workflow(workflow_id: int, body: WorkflowUpdate):
    row = await asyncio.to_thread(write_pg, "UPDATE workflows SET ... RETURNING *", ...)

    actions_taken = []
    if row and body.status == "approved":
        actions_taken = await _execute_workflow_actions(row)
    elif row and body.status == "dismissed":
        actions_taken = await _record_dismiss(row)

    if row:
        row["actions_taken"] = actions_taken
    return row
```

**What to wire per workflow type on approve:**

| Concern | Example |
|---------|---------|
| Entity update | Change status, reassign team, set resolution on the linked entity |
| Communication | Insert a draft/approved communication record |
| Agent action record | Insert into `agent_actions` with `status='executed'` |
| Note | Insert into `notes` documenting what happened |

### 27. `.mcp.json` profile must be set — Databricks MCP tools are invisible without it
The scaffold ships `.mcp.json` with `"DATABRICKS_CONFIG_PROFILE": "TODO"`. If left as `TODO`, the Databricks MCP server never starts and tools like `create_or_update_ka`, `create_or_update_mas`, and `create_genie_space` are completely invisible — they don't show up in `ToolSearch` and can't be called. This means KA creation, Genie Space management, and MAS wiring must all be done via raw REST APIs or the Databricks UI, which is slower and error-prone.

**Detection:** If `ToolSearch` for "databricks" returns no results, the MCP server isn't running.

**Fix:** The `/new-demo` wizard updates `.mcp.json` automatically in Phase 2.3 when the CLI profile is collected. If you're not using the wizard, manually update `.mcp.json`:
```json
{
  "mcpServers": {
    "databricks": {
      "command": "~/ai-dev-kit/databricks-mcp-server/.venv/bin/python",
      "args": ["~/ai-dev-kit/databricks-mcp-server/run_server.py"],
      "env": {"DATABRICKS_CONFIG_PROFILE": "<your-profile>"}
    }
  }
}
```
Then **restart Claude Code** — MCP servers only load at session start.

**Fallback:** If the MCP server isn't available (e.g., `ai-dev-kit` not installed), you can call the Python library directly:
```python
import os
os.environ['DATABRICKS_CONFIG_PROFILE'] = '<profile>'
from databricks_tools_core.agent_bricks import AgentBricksManager
manager = AgentBricksManager()
manager.ka_create_or_update(name=..., knowledge_sources=[...])
```

**On dismiss:** Always record an `agent_actions` entry with `status='dismissed'` and a note, but do NOT execute domain side-effects.

**Checklist for each demo:**
- Identify all `workflow_type` values
- Define what "approve" means in domain terms for each type
- Implement `_execute_workflow_actions(wf_row)` with a dispatch per type
- Implement `_record_dismiss(wf_row)` for the audit trail
- Use `asyncio.gather()` for parallel writes within each type
- Return `actions_taken` list so frontend can show descriptive flash messages
- Update frontend `approveWorkflow()`/`dismissWorkflow()` to read `actions_taken` from the response
- Look up the linked entity using `entity_id` from the workflow row
- Parse `reasoning_chain` JSONB for agent findings to include in notes

**Reference:** `examples/supply_chain_routes.py` — workflow approval pattern

### 29. OBO (on-behalf-of-user) token expires — app must auto-refresh

The Databricks Apps proxy passes the user's OAuth token via `x-forwarded-access-token`. This token expires after ~12 hours. When it does, MAS endpoint calls return 403 but the rest of the app (pages, data) still works because those use the app SP token. Users see "Error: 403 Forbidden" in the AI chat with no indication of what went wrong.

**The backend cannot refresh the token** — the OAuth session is between the user's browser and the Databricks Apps proxy. Only a browser-side page reload triggers a fresh OAuth flow.

**Fix (backend):** Before `raise_for_status()` on the MAS stream, check for 401/403 when using a user token. Stream a `session_expired` event and return:
```python
async with client.stream("POST", url, json=payload, headers=...) as resp:
    if resp.status_code in (401, 403) and user_token:
        log.warning("MAS %d with user token — OBO session expired", resp.status_code)
        yield f"data: {json.dumps({'type': 'session_expired'})}\n\n"
        yield "data: [DONE]\n\n"
        return
    resp.raise_for_status()
```

**Fix (frontend):** Handle `session_expired` in every SSE stream parser. Show a brief toast and auto-reload:
```javascript
function handleSessionExpired() {
  const toast = document.createElement('div');
  toast.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:99999;'
    + 'background:#1e3a5f;color:#fff;text-align:center;padding:14px;font-size:15px;';
  toast.textContent = 'Session expired — refreshing automatically…';
  document.body.appendChild(toast);
  setTimeout(() => window.location.reload(), 1200);
}

// In every SSE parser, add before the 'error' handler:
else if (evt.type === 'session_expired') { handleSessionExpired(); return; }
```

**Why not fall back to the SP token?** MAS with MCP tools requires the user's identity for on-behalf-of-user authorization. Using the SP token would break MCP tool routing and audit trails. The auto-refresh is seamless (~1-2 second reload) and gives the user a fresh token without manual intervention.

### 30. Lakebase CLI commands use compound names and require `--capacity`
The Lakebase CLI subcommands use longer compound names, not short forms:
```bash
# CORRECT:
databricks database create-database-instance <name> --capacity CU_1

# WRONG:
databricks database create-instance <name>
```
`--capacity` is **required** (not optional). Valid values: `CU_1`, `CU_2`, `CU_4`, `CU_8`. For demos, `CU_1` is always sufficient. Other compound names: `get-database-instance`, `list-database-instances`, `generate-database-credential`.

### 31. Lakebase instance ready state is AVAILABLE, not RUNNING
When polling a Lakebase instance after creation, check for state `AVAILABLE` (not `RUNNING`). The instance takes ~6 minutes to provision. Polling for `RUNNING` will time out.
```bash
# Poll until AVAILABLE
databricks database get-database-instance <name> --profile=<profile> -o json | \
  jq '.state'
# Returns: "STARTING" -> "AVAILABLE"
```

### 32. Serverless notebooks can't authenticate to Lakebase as PG user
Serverless notebook runtimes run as ephemeral `spark-*` service accounts that have no Lakebase role. Even with `generate_database_credential()`, setting `PG_USER = ""` causes psycopg2 to connect as the OS user (`spark-3da802a0-...`), which fails with `FATAL: role "spark-..." does not exist`.

**Fix:** Use the CLI fallback for Lakebase seeding instead of notebooks:
```bash
# Option A: Direct psql
databricks psql <instance> --profile=<profile> -- -d <database> -f seed.sql

# Option B: Local Python script that gets credentials via CLI
databricks database generate-database-credential \
  --json '{"instance_names": ["<instance>"], "request_id": "seed"}' \
  --profile=<profile>
# Then connect with user=<your-email> and password=<token>
```

**Note:** The CLI's `generate-database-credential` requires `request_id` in the JSON body, unlike the Python SDK which doesn't.

### 33. SP roles in Lakebase only exist after app resource registration + redeploy
You cannot GRANT permissions to a service principal in a Lakebase instance until the SP's app has the database registered as a resource AND has been redeployed. A brand-new instance has no SP roles.

**Correct order:**
1. Create instance + database + apply schemas
2. Register database as app resource (`databricks apps update`)
3. Redeploy the app (`databricks apps deploy`) — this creates the SP role
4. GRANT table access to the SP (`databricks psql ... -c "GRANT ALL ..."`)

Attempting GRANTs before step 3 fails with: `role "<sp-client-id>" does not exist`.

### 34. MAS POST fails with external-mcp-server — use POST then PATCH
Creating a MAS with `external-mcp-server` agents in the initial POST body returns `Unknown agent type: Empty`. The POST endpoint has issues with MCP agents in the creation payload.

**Workaround:** Create the MAS with simpler agents (genie-space, knowledge-assistant) via POST, then add external-mcp-server and unity-catalog-function agents via PATCH:
```bash
# Step 1: POST with genie-space only
databricks api post /api/2.0/multi-agent-supervisors --json @config_without_mcp.json

# Step 2: PATCH to add MCP agent (include ALL agents + name)
databricks api patch /api/2.0/multi-agent-supervisors/<full-uuid> --json @config_with_mcp.json
```

### 35. `agent_actions` status values: use `executed`, not `completed`
The `agent_actions` table in `core_schema.sql` has a CHECK constraint allowing only: `pending`, `executed`, `dismissed`, `failed`. Code generation often produces `"completed"` which violates the constraint. Always use `"executed"` for successful agent actions.

---

### 39. f-string backslash syntax crashes Python 3.11 (Databricks Apps runtime)

**Symptom:** App deploys but immediately crashes with `SyntaxError: f-string expression part cannot include a backslash`. The error only appears in `/logz` (browser-only, requires OAuth). The CLI deploy output just says "app crashed unexpectedly".

**Cause:** Python 3.11 (used by Databricks Apps runtime) does NOT allow backslash escape sequences (`\n`, `\\n`, `\'`) inside the `{}` expression portion of an f-string. This restriction was lifted in Python 3.12, but Databricks Apps still runs 3.11.

**Example of broken code:**
```python
# WRONG — crashes on Python 3.11
yield f"data: {json.dumps({'type': 'delta', 'text': f'**Summary**\\n{context}'})}\n\n"
```

**Fix:** Extract the inner string into a variable first:
```python
# CORRECT — works on Python 3.11+
_text = "**Summary**\n" + context
yield f"data: {json.dumps({'type': 'delta', 'text': _text})}\n\n"
```

**Prevention:** When generating code, never use `\n`, `\\n`, `\'`, or any backslash inside `{...}` in an f-string. Always extract to a variable first. This applies to ALL Python code that runs on Databricks Apps.

---

### 40. Databricks SQL INTERVAL syntax requires quoted numbers

**Symptom:** Delta Lake queries with date/time intervals return empty results or errors.

**Cause:** Databricks SQL requires the number in INTERVAL expressions to be quoted: `INTERVAL '7' DAY`, not `INTERVAL 7 DAY`. The unquoted form may silently return wrong results or fail depending on context.

**Example:**
```python
# WRONG — may fail or return wrong results
f"WHERE reading_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY"

# CORRECT
f"WHERE reading_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '7' DAY"
```

**Note:** PostgreSQL (Lakebase) uses a different syntax: `INTERVAL '7 days'` (number and unit together inside quotes). Don't mix them up:
```python
# Delta Lake (Databricks SQL):
f"INTERVAL '7' DAY"

# Lakebase (PostgreSQL):
"INTERVAL '7 days'"
```

---

### 41. asyncio.gather without return_exceptions kills all queries on one failure

**Symptom:** Dashboard metrics endpoint returns all zeros even though Delta Lake tables have data. Only one of the parallel queries (e.g., the Lakebase query) is failing, but it takes down all 6 queries.

**Cause:** `asyncio.gather()` without `return_exceptions=True` propagates the first exception immediately, causing the entire gather to fail. If one query (e.g., Lakebase `run_pg_query`) fails, the exception handler catches it and returns all zeros — even for queries that would have succeeded.

**Fix:** Always use `return_exceptions=True` when mixing Delta Lake and Lakebase queries in `asyncio.gather`:
```python
results = await asyncio.gather(
    asyncio.to_thread(run_query, "SELECT ..."),      # Delta Lake
    asyncio.to_thread(run_pg_query, "SELECT ..."),   # Lakebase — may fail independently
    asyncio.to_thread(run_query, "SELECT ..."),      # Delta Lake
    return_exceptions=True,  # <-- CRITICAL
)

def _val(result):
    if isinstance(result, Exception):
        log.warning("Sub-query failed: %s", result)
        return 0
    rows = result or [{}]
    v = rows[0].get("val") if rows else None
    return v if v is not None else 0
```

**When to use this:** Any endpoint that runs multiple parallel queries where you want partial results rather than all-or-nothing failure. Dashboard metrics, briefing context, and field detail endpoints are prime candidates.

---

### 42. Lakebase instance workspace limit (~10 instances)

**Symptom:** `databricks database create-database-instance` fails with a "workspace limit" error.

**Cause:** FEVM (serverless sandbox) workspaces have a limit of approximately 10 Lakebase instances. Once hit, you must reuse an existing instance or delete unused ones.

**Workaround:** Reuse an existing instance by creating a new database within it:
```bash
# List existing instances
databricks database list-database-instances --profile=<profile>

# Create a new database on an existing instance
databricks psql <existing-instance> --profile=<profile> -- -c "CREATE DATABASE <new_db>;"

# Then apply schemas to the new database
databricks psql <existing-instance> --profile=<profile> -- -d <new_db> -f lakebase/core_schema.sql
```

**Important:** When reusing an instance, update ALL config files that reference the instance name:
- `app/app.yaml` (resources section)
- `CLAUDE.md` (Project Identity)
- `notebooks/03_seed_lakebase.py` (instance name constant)
- `demo-config.yaml` (if applicable)

---

### 43. App crash logs only visible via browser /logz endpoint

**Symptom:** `databricks apps deploy` returns "app crashed unexpectedly. Please check /logz for more details" but there's no CLI command to view the logs.

**Cause:** Databricks Apps does not expose application logs via the CLI or REST API. The only way to see the actual Python traceback is:
1. Navigate to `https://<app-url>/logz` in a browser (requires OAuth login)
2. Or use Chrome DevTools MCP to navigate to the logz endpoint

**Workaround for debugging:** If you can't access `/logz`:
1. Test the import chain locally: `python3 -c "import ast; ast.parse(open('backend/main.py').read()); print('OK')"`
2. Check for Python 3.11 compatibility issues (f-string backslashes, match/case, etc.)
3. Compile-check all core modules: `python3 -c "import py_compile; py_compile.compile('backend/main.py', doraise=True)"`
4. Compare with a working app's structure on the same workspace

---

### 44. UC HTTP connection creation requires token_endpoint field

**Symptom:** `databricks api post /api/2.0/unity-catalog/connections` with OAuth M2M fields returns "must include bearer_token" error.

**Cause:** The UC connections API requires the `token_endpoint` field to be explicitly set for OAuth M2M connections. Without it, the API defaults to bearer token auth.

**Fix:** Include `token_endpoint` pointing to the workspace OIDC endpoint:
```json
{
  "name": "my-mcp-connection",
  "connection_type": "HTTP",
  "options": {
    "host": "https://my-app.aws.databricksapps.com",
    "port": "443",
    "base_path": "/db/my_database/mcp/",
    "client_id": "<sp-client-id>",
    "client_secret": "<sp-secret>",
    "oauth_scope": "all-apis",
    "token_endpoint": "https://<workspace-url>/oidc/v1/token",
    "is_mcp_connection": "true"
  }
}
```

The `token_endpoint` format is always: `https://<workspace-url>/oidc/v1/token`

---

### 45. `databricks apps deploy` clears the resources array

**Symptom:** After deploying the app, `PGHOST` is not set and Lakebase health check fails — even though you previously registered resources via `databricks apps update`. Checking `databricks apps get` shows `resources: []`.

**Cause:** `databricks apps deploy` resets the resources array to empty. Any resources registered via `databricks apps update` before the deploy are lost.

**Fix:** Always register resources AFTER the deploy, then redeploy again:
```bash
# Step 1: Deploy the app code
databricks apps deploy <name> --source-code-path <path> --profile=<profile>

# Step 2: Register resources (AFTER deploy)
databricks apps update <name> --json '{
  "resources": [
    {"name": "sql-warehouse", "sql_warehouse": {"id": "<id>", "permission": "CAN_USE"}},
    {"name": "mas-endpoint", "serving_endpoint": {"name": "mas-<tile>-endpoint", "permission": "CAN_QUERY"}},
    {"name": "database", "database": {"instance_name": "<instance>", "database_name": "<db>", "permission": "CAN_CONNECT_AND_CREATE"}}
  ]
}' --profile=<profile>

# Step 3: Redeploy to inject PGHOST/PGPORT/PGDATABASE/PGUSER
databricks apps deploy <name> --source-code-path <path> --profile=<profile>

# Step 4: Verify resources survived
databricks apps get <name> --profile=<profile> -o json | jq '.resources'
```

**CRITICAL:** The resources MUST be present at deploy time for `PGHOST` to be injected. If resources are cleared by the deploy, you need the full 3-step cycle: deploy -> register -> redeploy.

---

### 46. asyncio.gather resilience needed in ALL Lakebase-touching endpoints

**Symptom:** Workflows page, Architecture page, or any page that calls Lakebase "fails to load" or returns 500 errors — even though `/api/health` shows `lakebase: ok`.

**Cause:** The health check only runs `SELECT 1` (no table access). Individual endpoints that query Lakebase tables (workflows, agent_actions, alerts, exceptions) will fail if:
- The app SP lacks table permissions (Gotcha #33)
- The connection pool has stale connections after permission changes
- One Lakebase query in an `asyncio.gather` fails, taking down all parallel queries

**Root causes discovered:**
1. `/api/agent-overview` — 7 Lakebase queries in `asyncio.gather` without `return_exceptions=True`
2. `/api/architecture` — inner `asyncio.gather` mixes Delta + Lakebase without `return_exceptions=True` AND no try/except wrapper -> returns raw 500
3. `/api/exceptions` — single Lakebase query with no try/except -> returns raw 500
4. Dashboard `Promise.all` — calls `/api/agent-overview` without `.catch()` -> one failing endpoint breaks entire dashboard

**Fix pattern for ALL endpoints with mixed queries:**
```python
# Backend: Always use return_exceptions=True
results = await asyncio.gather(
    asyncio.to_thread(run_query, "..."),      # Delta
    asyncio.to_thread(run_pg_query, "..."),   # Lakebase
    return_exceptions=True,
)
def _safe(r, default=None):
    if isinstance(r, Exception):
        log.warning("Query failed: %s", r)
        return default if default is not None else []
    return r

# Frontend: Always add .catch() in Promise.all
const [metrics, overview] = await Promise.all([
    fetchApi('/api/metrics').catch(() => ({})),
    fetchApi('/api/agent-overview').catch(() => ({kpis: {}, workflows: []})),
]);
```

**Prevention:** When generating template code, EVERY `asyncio.gather` that includes `run_pg_query` calls MUST have `return_exceptions=True`. EVERY `Promise.all` in the frontend MUST have `.catch()` on each call.
