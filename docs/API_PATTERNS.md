# API Patterns Reference

Correct API formats for Genie Spaces, MAS, UC HTTP Connections, and Lakebase MCP. Referenced from CLAUDE.md.

---

## Genie Space API

### Create (3-step process)

```bash
# Step 1: Create blank space
databricks api post /api/2.0/genie/spaces --json '{
  "serialized_space": "{\"version\": 2}",
  "warehouse_id": "<warehouse-id>"
}' --profile=<profile>
# Returns: {"space_id": "abc123..."}

# Step 2: PATCH title + description
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "title": "My Demo Data Space",
  "description": "Query data about ..."
}' --profile=<profile>

# Step 3: PATCH tables via serialized_space (ONLY way that works)
# Tables: dotted 3-part names, SORTED ALPHABETICALLY
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "serialized_space": "{\"version\":2,\"data_sources\":{\"tables\":[{\"identifier\":\"catalog.schema.table1\"},{\"identifier\":\"catalog.schema.table2\"}]}}"
}' --profile=<profile>

# Step 4 (optional): PATCH instructions
databricks api patch /api/2.0/genie/spaces/<space_id> --json '{
  "instructions": "You are a data assistant for ..."
}' --profile=<profile>
```

### Verify tables are attached

```bash
databricks api get /api/2.0/genie/spaces/<space_id>?include_serialized_space=true --profile=<profile>
# Parse serialized_space JSON -> data_sources.tables
# WARNING: table_identifiers in GET is ALWAYS EMPTY
```

### Grant permissions

```bash
# Endpoint: /permissions/genie/{id} — NOT /permissions/genie/spaces/{id}
databricks api patch /api/2.0/permissions/genie/<space_id> --json '{
  "access_control_list": [
    {"group_name": "users", "permission_level": "CAN_RUN"}
  ]
}' --profile=<profile>
```

---

## MAS (Multi-Agent Supervisor) API

### Agent type formats (kebab-case)

```json
{"agent_type": "genie-space", "genie_space": {"id": "<space_id>"}}
{"agent_type": "knowledge-assistant", "knowledge_assistant": {"knowledge_assistant_id": "<ka_id>"}}
{"agent_type": "unity-catalog-function", "unity_catalog_function": {"uc_path": {"catalog": "...", "schema": "...", "name": "..."}}}
{"agent_type": "external-mcp-server", "external_mcp_server": {"connection_name": "<connection_name>"}}
```

### Create MAS

**IMPORTANT:** The POST endpoint fails with `external-mcp-server` agents in the initial payload (Gotcha #34). Create with simpler agents first, then PATCH to add MCP agents.

```bash
# Step 1: POST with genie-space only (no MCP agents)
databricks api post /api/2.0/multi-agent-supervisors --json '{
  "name": "my-demo-supervisor",
  "description": "Multi-agent supervisor for ...",
  "instructions": "You are an AI assistant...",
  "agents": [
    {
      "agent_type": "genie-space",
      "genie_space": {"id": "<genie_space_id>"},
      "name": "data-analyst",
      "description": "Query analytics data about ..."
    }
  ]
}' --profile=<profile>
# Returns tile_id in response

# Step 2: PATCH to add MCP agent (include ALL agents + name)
databricks api patch /api/2.0/multi-agent-supervisors/<full-uuid-tile-id> --json '{
  "name": "my-demo-supervisor",
  "agents": [
    {
      "agent_type": "genie-space",
      "genie_space": {"id": "<genie_space_id>"},
      "name": "data-analyst",
      "description": "Query analytics data about ..."
    },
    {
      "agent_type": "external-mcp-server",
      "external_mcp_server": {"connection_name": "<connection_name>"},
      "name": "mcp-lakebase-connection",
      "description": "Write operational data to Lakebase ..."
    }
  ]
}' --profile=<profile>
```

### Discover tile ID (no list endpoint)

```bash
databricks api get /api/2.0/serving-endpoints --profile=<profile> | \
  jq '.endpoints[] | select(.name | startswith("mas-")) | {name: .name, tile_id: .tile_endpoint_metadata.tile_id}'
```

### Update MAS (PATCH requires full agents array)

```bash
# Use the FULL UUID tile_id, NOT the 8-char prefix
databricks api patch /api/2.0/multi-agent-supervisors/<full-uuid-tile-id> --json '{
  "name": "my-demo-supervisor",
  "agents": [... full array ...]
}' --profile=<profile>
```

---

## UC HTTP Connection (for Lakebase MCP)

Create in Databricks UI: **Catalog > External Connections > Create Connection**

| Field | Value |
|-------|-------|
| Type | HTTP |
| Host | `https://<mcp-app-url>` (**must include `https://`**) |
| Port | `443` |
| Base path | `/db/<database_name>/mcp/` (per-demo database routing) |
| Auth type | Databricks OAuth M2M |
| Client ID | Service principal application ID |
| Client secret | SP OAuth secret (create at **Account Console > App Connections**) |
| OAuth scope | `all-apis` |

**Key rules:**
- Host MUST include `https://` scheme (Gotcha #18)
- Base path MUST have trailing slash on `/mcp/` (Gotcha #15)

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

---

## Lakebase MCP Server Deployment

### First-time setup

```bash
# 1. Create the app
databricks apps create lakebase-mcp-server --profile=<profile>

# 2. Update lakebase-mcp-server/app/app.yaml with instance + first database

# 3. Sync and deploy
databricks sync ./lakebase-mcp-server/app /Workspace/Users/<you>/lakebase-mcp-server/app \
  --profile=<profile> --watch=false
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<you>/lakebase-mcp-server/app --profile=<profile>

# 4. Grant CAN_USE to users group (required for MAS proxy)
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<profile>

# 5. Grant table access to app SP
databricks psql <instance> --profile=<profile> -- -d <database> -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"<mcp-app-sp-client-id>\";
"
```

### Adding a new demo's database

```bash
# 1. Register new database (include ALL existing databases)
databricks apps update lakebase-mcp-server --json '{
  "resources": [
    {"name": "database", "database": {"instance_name": "<inst>", "database_name": "<existing_db>", "permission": "CAN_CONNECT_AND_CREATE"}},
    {"name": "database-2", "database": {"instance_name": "<inst>", "database_name": "<new_db>", "permission": "CAN_CONNECT_AND_CREATE"}}
  ]
}' --profile=<profile>

# 2. Redeploy
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<you>/lakebase-mcp-server/app --profile=<profile>

# 3. Grant table access in new database
databricks psql <instance> --profile=<profile> -- -d <new_db> -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<mcp-app-sp-client-id>\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"<mcp-app-sp-client-id>\";
"
```

---

## App Resource Registration

```bash
# Register resources via API (app.yaml alone does NOT register them)
databricks apps update <app-name> --json '{
  "resources": [
    {"name": "sql-warehouse", "sql_warehouse": {"id": "<warehouse-id>", "permission": "CAN_USE"}},
    {"name": "mas-endpoint", "serving_endpoint": {"name": "mas-<tile-8-chars>-endpoint", "permission": "CAN_QUERY"}},
    {"name": "database", "database": {"instance_name": "<instance>", "database_name": "<db>", "permission": "CAN_CONNECT_AND_CREATE"}}
  ]
}' --profile=<profile>

# MUST redeploy after registration for PGHOST injection
databricks apps deploy <app-name> --source-code-path <path> --profile=<profile>

# Grant permissions explicitly (resource registration doesn't reliably grant them)
# SQL warehouse CAN_USE, catalog USE_CATALOG/USE_SCHEMA/SELECT, MAS CAN_QUERY

# IMPORTANT: Permissions API uses the endpoint UUID, not its name (Gotcha #25)
# Step 1: Get the UUID
databricks api get /api/2.0/serving-endpoints --profile=<profile> | \
  jq '.endpoints[] | select(.name == "mas-<tile-8-chars>-endpoint") | .id'

# Step 2: Grant using UUID
databricks api patch /api/2.0/permissions/serving-endpoints/<endpoint-uuid> \
  --json '{"access_control_list":[{"service_principal_name":"<app-sp>","permission_level":"CAN_QUERY"}]}' \
  --profile=<profile>
```
