"""
Demo App — FastAPI assembly file.
Imports core modules, sets up lifespan, mounts health + chat + frontend.

Add your domain-specific routes below the marked section.
"""

import asyncio
import json
import logging
import os
import re
import time as _time
import uuid as _uuid
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import httpx
import yaml
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse

from databricks.sdk import WorkspaceClient

from backend.core.lakebase import _init_pg_pool
from backend.core.health import health_router
from backend.core.streaming import stream_mas_chat, _sse_keepalive, get_mcp_pending, clear_mcp_pending
from backend.core import run_query, run_pg_query, write_pg, _safe, _get_mas_auth

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# ─── Chat history (in-memory, per-process) ────────────────────────────────
_chat_history: list[dict] = []

# ─── Chat Persistence (Lakebase-backed session & message history) ─────────
# Requires chat_sessions and chat_messages tables (see lakebase/core_schema.sql).
# Falls back gracefully if tables don't exist yet.

_chat_session_id: str | None = None


def _ensure_chat_session() -> str:
    global _chat_session_id
    if _chat_session_id:
        return _chat_session_id
    try:
        rows = run_pg_query("SELECT session_id FROM chat_sessions ORDER BY updated_at DESC LIMIT 1")
        if rows:
            _chat_session_id = rows[0]["session_id"]
            return _chat_session_id
    except Exception:
        pass
    return _new_chat_session()


def _new_chat_session() -> str:
    global _chat_session_id
    sid = f"chat-{date.today().isoformat()}-{_uuid.uuid4().hex[:6]}"
    try:
        write_pg("INSERT INTO chat_sessions (session_id) VALUES (%s) ON CONFLICT DO NOTHING", (sid,))
    except Exception as e:
        log.warning("Could not create chat session: %s", e)
    _chat_session_id = sid
    return sid


def _save_chat_message(role: str, content: str):
    sid = _ensure_chat_session()
    try:
        write_pg(
            "INSERT INTO chat_messages (session_id, role, content) VALUES (%s, %s, %s)",
            (sid, role, content),
        )
        if role == "user":
            title = content[:100] + ("..." if len(content) > 100 else "")
            write_pg(
                "UPDATE chat_sessions SET updated_at = NOW(), title = COALESCE(NULLIF(title, 'New conversation'), %s) WHERE session_id = %s",
                (title, sid),
            )
        else:
            write_pg("UPDATE chat_sessions SET updated_at = NOW() WHERE session_id = %s", (sid,))
    except Exception as e:
        log.warning("Could not save chat message: %s", e)


def _load_chat_history() -> list[dict]:
    sid = _ensure_chat_session()
    try:
        rows = run_pg_query(
            "SELECT role, content FROM chat_messages WHERE session_id = %s ORDER BY created_at ASC",
            (sid,),
        )
        return [{"role": r["role"], "content": r["content"]} for r in rows]
    except Exception:
        return []


def _clear_chat_history():
    global _chat_session_id
    if _chat_session_id:
        try:
            write_pg("DELETE FROM chat_sessions WHERE session_id = %s", (_chat_session_id,))
        except Exception:
            pass
    _chat_session_id = None

# ─── Action card config for your domain ───────────────────────────────────
# Override this list to detect entities created by the MAS agent during chat.
# Each entry maps a Lakebase table to an action card in the chat UI.
#
ACTION_CARD_TABLES: list[dict] = [
    {
        "table": "work_orders",
        "card_type": "work_order",
        "id_col": "work_order_id",
        "title_template": "Work Order {wo_number}",
        "actions": ["approve", "dismiss"],
        "detail_cols": {
            "robot": "robot_id", "family": "family", "fc_site": "fc_site",
            "severity": "severity", "status": "status",
        },
    },
    {
        "table": "agent_memory",
        "card_type": "agent_memory",
        "id_col": "memory_id",
        "title_template": "Memory: {robot_id}",
        "actions": ["approve", "dismiss"],
        "detail_cols": {
            "robot": "robot_id", "family": "family", "signal": "signal",
            "outcome": "outcome", "model_version": "model_version",
        },
    },
]


# ─── Lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(application: FastAPI):
    try:
        _init_pg_pool()
    except Exception as e:
        log.warning("Lakebase pool init deferred: %s", e)
    # Hydrate in-memory chat history from Lakebase
    try:
        saved = _load_chat_history()
        if saved:
            _chat_history.extend(saved)
            log.info("Loaded %d messages from Lakebase chat history", len(saved))
    except Exception as e:
        log.warning("Could not load chat history: %s", e)
    yield


app = FastAPI(title="Demo App", lifespan=lifespan)

# ─── Health endpoint ──────────────────────────────────────────────────────
app.include_router(health_router)


# ─── Chat endpoint (MAS streaming) ───────────────────────────────────────

@app.post("/api/chat")
async def chat(request: Request, body: dict):
    """Streaming SSE endpoint for MAS chat with MCP approval flow.

    Normal message:   {"message": "Create a PO...", "auto_approve_mcp": true}
    MCP approval:     {"approve_mcp": true}  or  {"approve_mcp": false}
    """
    approve_mcp = body.get("approve_mcp", None)
    auto_approve_mcp = body.get("auto_approve_mcp", False)
    message = body.get("message", "").strip()
    context = body.get("context", "").strip()

    # Extract OBO token for MAS calls (required for MCP tools, auto-refreshes on expiry)
    user_token = request.headers.get("x-forwarded-access-token", "")

    # Determine starting state
    mcp_state = get_mcp_pending()
    if approve_mcp is not None and mcp_state:
        # Continuing from MCP approval — build input from saved state
        log.info("MCP APPROVAL received: approve=%s", approve_mcp)
        all_accumulated = mcp_state["accumulated"]
        tools_called = mcp_state["tools_called"]
        lakebase_called = mcp_state["lakebase_called"]
        approval_round = mcp_state["round"]

        for req in mcp_state["pending"]:
            all_accumulated.append({
                "type": "mcp_approval_response",
                "id": f"approval-{approval_round}-{req.get('id', '')}",
                "approval_request_id": req.get("id", ""),
                "approve": bool(approve_mcp),
            })
        start_messages = list(_chat_history[-10:]) + all_accumulated
        clear_mcp_pending()
    elif approve_mcp is not None and not mcp_state:
        # Stale approval — no pending state
        async def stale():
            yield f"data: {json.dumps({'type': 'error', 'text': 'No pending MCP approval.'})}\n\n"
            yield "data: [DONE]\n\n"
        return StreamingResponse(stale(), media_type="text/event-stream")
    else:
        # New user message
        if not message:
            raise HTTPException(400, "Empty message")
        full_message = f"Context: {context}\n\nQuestion: {message}" if context else message
        _chat_history.append({"role": "user", "content": full_message})
        try:
            _save_chat_message("user", full_message)
        except Exception:
            pass
        start_messages = list(_chat_history[-10:])
        all_accumulated = []
        tools_called = set()
        lakebase_called = False
        approval_round = 0

    async def event_stream():
        final_text = ""
        async for chunk in stream_mas_chat(
            message, _chat_history, ACTION_CARD_TABLES,
            user_token=user_token,
            auto_approve_mcp=auto_approve_mcp,
            start_messages=start_messages,
            initial_accumulated=all_accumulated,
            initial_tools_called=tools_called,
            initial_lakebase_called=lakebase_called,
            initial_approval_round=approval_round,
        ):
            yield chunk
            # Track final text for chat history
            if chunk.startswith("data: ") and chunk.strip() != "data: [DONE]":
                try:
                    evt = json.loads(chunk[6:])
                    if evt.get("type") == "delta":
                        final_text += evt.get("text", "")
                except (json.JSONDecodeError, KeyError):
                    pass
        # Save final response to chat history
        if final_text:
            _chat_history.append({"role": "assistant", "content": final_text})
            try:
                _save_chat_message("assistant", final_text)
            except Exception:
                pass

    return StreamingResponse(
        _sse_keepalive(event_stream()),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ─── Chat History & Clear Endpoints ──────────────────────────────────────

@app.get("/api/chat/history")
async def get_chat_history():
    """Return all messages from the current chat session."""
    messages = await asyncio.to_thread(_load_chat_history)
    return {"session_id": _chat_session_id or "", "messages": messages}


@app.post("/api/chat/clear")
async def clear_chat():
    """Clear chat history (both in-memory and Lakebase)."""
    _chat_history.clear()
    await asyncio.to_thread(_clear_chat_history)
    return {"status": "cleared"}


# ═══════════════════════════════════════════════════════════════════════════
# Architecture (dynamic, auto-discovers MAS sub-agents)
# ═══════════════════════════════════════════════════════════════════════════

CATALOG = os.getenv("CATALOG", "")
SCHEMA = os.getenv("SCHEMA", "")
MAS_TILE_ID = os.getenv("MAS_TILE_ID", "")

w = WorkspaceClient()


def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _load_demo_config() -> dict:
    """Load demo-config.yaml — checks app/ first (deployed), then project root (local dev).

    Path resolution:
      main.py -> .parent (backend/) -> .parent (app/) -> demo-config.yaml   [deployed]
      main.py -> .parent (backend/) -> .parent (app/) -> .. (root/) -> demo-config.yaml  [local dev]
    """
    app_dir = Path(__file__).resolve().parent.parent
    for candidate in [app_dir / "demo-config.yaml", app_dir.parent / "demo-config.yaml"]:
        try:
            with open(candidate) as f:
                cfg = yaml.safe_load(f) or {}
                if cfg:
                    log.info("Loaded demo-config.yaml from %s", candidate)
                    return cfg
        except FileNotFoundError:
            continue
        except Exception as e:
            log.warning("Error reading %s: %s", candidate, e)
    log.warning("No demo-config.yaml found — agent discovery will rely on MAS API or mas_config.json")
    return {}


_DEMO_CONFIG: dict | None = None


def _get_demo_config() -> dict:
    global _DEMO_CONFIG
    if _DEMO_CONFIG is None:
        _DEMO_CONFIG = _load_demo_config()
    return _DEMO_CONFIG


def _agents_from_demo_config() -> list[dict]:
    """Convert ai_layer.sub_agents from demo-config.yaml into MAS agent format."""
    cfg = _get_demo_config()
    ai = cfg.get("ai_layer", {})
    sub_agents = ai.get("sub_agents", [])
    infra = cfg.get("infrastructure", {})
    catalog = infra.get("catalog", CATALOG)
    schema = infra.get("schema", SCHEMA)
    genie_space_id = os.getenv("GENIE_SPACE_ID", "")

    # Map demo-config.yaml types to MAS API kebab-case agent types.
    # MAS API requires kebab-case: genie-space, external-mcp-server, etc.
    TYPE_CONVERT = {
        "genie_space": "genie-space",
        "lakebase_mcp": "external-mcp-server",
        "knowledge_assistant": "knowledge-assistant",
        "unity_catalog_function": "unity-catalog-function",
    }

    agents = []
    for sa in sub_agents:
        sa_type = sa.get("type", "")
        agent_type = TYPE_CONVERT.get(sa_type, sa_type)
        name = sa.get("name", sa_type.replace("_", " ").title())
        desc = sa.get("description", "")

        agent: dict = {"agent_type": agent_type, "name": name, "description": desc}

        if agent_type == "genie-space":
            sa_genie_id = sa.get("genie_space_id", "") or genie_space_id
            agent["genie_space"] = {"id": sa_genie_id}
            if not agent.get("name") or agent["name"] == sa_type:
                agent["name"] = sa.get("name", "analytics_genie")
        elif agent_type == "external-mcp-server":
            agent["external_mcp_server"] = {"connection_name": sa.get("connection_name", sa.get("mcp_connection_id", ""))}
            if not agent.get("name") or agent["name"] == sa_type:
                agent["name"] = "lakebase_mcp"
        elif agent_type == "knowledge-assistant":
            ka_tile = os.getenv("KA_TILE_ID", "")
            agent["knowledge_assistant"] = {"knowledge_assistant_id": ka_tile} if ka_tile else {}
            if not agent.get("name") or agent["name"] == sa_type:
                agent["name"] = "knowledge_assistant"
        elif agent_type == "unity-catalog-function":
            agent["unity_catalog_function"] = {
                "uc_path": {"catalog": catalog, "schema": schema, "name": sa.get("function_name", "custom_function")}
            }
            if not agent.get("name") or agent["name"] == sa_type:
                agent["name"] = "uc_functions"

        agents.append(agent)

    return agents


def _read_mas_config_from_disk() -> list[dict]:
    """Read agents[] from app/agent_bricks/mas_config.json as fallback.

    Path: main.py -> .parent (backend/) -> .parent (app/) -> agent_bricks/mas_config.json
    This works both locally and on Databricks since app/ is the synced directory.
    """
    try:
        config_path = Path(__file__).resolve().parent.parent / "agent_bricks" / "mas_config.json"
        with open(config_path) as f:
            data = json.load(f)
        agents = data.get("agents", [])
        # Strip $comment/$note keys but keep agents that have real keys
        return [
            {k: v for k, v in a.items() if not k.startswith("$")}
            for a in agents
            if not all(k.startswith("$") for k in a.keys())
        ]
    except Exception as e:
        log.warning("Could not read MAS config from disk: %s", e)
        return []


async def _fetch_mas_agents() -> list[dict]:
    """Fetch MAS agents: live API first, then demo-config, then mas_config.json fallback."""
    tile = MAS_TILE_ID
    if tile and tile != "TODO":
        try:
            ep = await asyncio.to_thread(w.serving_endpoints.get, f"mas-{tile}-endpoint")
            full_uuid = ep.tile_endpoint_metadata.tile_id
            host, auth = await asyncio.to_thread(_get_mas_auth)
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{host}/api/2.0/multi-agent-supervisors/{full_uuid}",
                    headers={"Authorization": auth},
                )
                resp.raise_for_status()
                agents = resp.json().get("agents", [])
                if agents:
                    return agents
        except Exception as e:
            log.warning("Live MAS API failed (%s) — trying demo-config fallback", e)

    agents = _agents_from_demo_config()
    if agents:
        log.info("Using demo-config.yaml for architecture (%d agents)", len(agents))
        return agents

    agents = _read_mas_config_from_disk()
    if agents:
        log.info("Using mas_config.json for architecture (%d agents)", len(agents))
        return agents

    log.warning("No agent definitions found — architecture will show data + app nodes only")
    return []


def _build_data_nodes(
    catalog: str, schema: str, workspace_url: str,
    delta_tables: list[dict], delta_row_counts: dict,
    lakebase_tables: list[str], lakebase_row_counts: dict,
) -> list[dict]:
    """Two data-source nodes: Delta Lake (analytics) and Lakebase (operational)."""
    nodes = []
    dt_items = [{"text": f"{len(delta_tables)} tables", "status": "success"}]
    for t in delta_tables[:4]:
        name = t.get("name", "")
        cnt = delta_row_counts.get(name, "")
        label = f"{name}  ({cnt:,} rows)" if isinstance(cnt, int) else name
        dt_items.append({"text": label, "status": "info"})
    total_delta_rows = sum(v for v in delta_row_counts.values() if isinstance(v, int))
    nodes.append({
        "key": "lakehouse", "name": "Lakehouse", "type": "lakehouse-node",
        "status": "online", "color": "#3b82f6", "_layout_column": "data-sources",
        "type_badge": "LAKEHOUSE",
        "description": f"Unity Catalog tables in {catalog}.{schema} — analytical data powering Genie queries, UC Functions, and dashboards.",
        "display_items": dt_items,
        "actions": [],
        "details": {"tables": [t.get("name", "") for t in delta_tables], "row_counts": delta_row_counts},
        "stats": [{"label": "tables", "value": len(delta_tables)}, {"label": "rows", "value": total_delta_rows}],
        "workspace_url": f"{workspace_url}/explore/data/{catalog}/{schema}" if workspace_url else "",
    })
    lb_items = [{"text": f"{len(lakebase_tables)} operational tables", "status": "success"}]
    for t in lakebase_tables[:4]:
        cnt = lakebase_row_counts.get(t, "")
        label = f"{t}  ({cnt:,} rows)" if isinstance(cnt, int) else t
        lb_items.append({"text": label, "status": "info"})
    total_lb_rows = sum(v for v in lakebase_row_counts.values() if isinstance(v, int))
    nodes.append({
        "key": "lakebase", "name": "Lakebase (Operational)", "type": "lakebase",
        "status": "online", "color": "#f59e0b", "_layout_column": "data-sources",
        "type_badge": "LAKEBASE",
        "description": "Postgres-compatible operational database for real-time state management, workflows, and agent actions.",
        "display_items": lb_items,
        "actions": [],
        "details": {"tables": lakebase_tables, "row_counts": lakebase_row_counts},
        "stats": [{"label": "tables", "value": len(lakebase_tables)}, {"label": "rows", "value": total_lb_rows}],
        "workspace_url": f"{workspace_url}/sql/databases" if workspace_url else "",
    })
    return nodes


def _build_app_node(
    workspace_url: str, demo_name: str, active_cases: int, pending_wf: int,
    lakebase_tables: list[str],
) -> dict:
    app_url = os.getenv("DATABRICKS_APP_URL", "")
    items = [
        {"text": "FastAPI + Vanilla JS", "status": "success"},
    ]
    if active_cases:
        items.append({"text": f"{active_cases} active records", "status": "info"})
    if pending_wf:
        items.append({"text": f"{pending_wf} pending workflows", "status": "info"})
    return {
        "key": "app", "name": demo_name or "Demo Application", "type": "app",
        "status": "online", "color": "#E4002B", "_layout_column": "application",
        "type_badge": "APPLICATION",
        "description": "Databricks App serving the dashboard, AI chat, agent workflows, and domain pages.",
        "display_items": items,
        "actions": [],
        "details": {"app_url": app_url},
        "stats": [{"label": "pending", "value": pending_wf}],
        "workspace_url": f"{workspace_url}/apps" if workspace_url else app_url,
    }


def _build_agent_nodes(
    agents: list[dict], workspace_url: str,
    delta_tables: list[dict], delta_row_counts: dict,
    lakebase_tables: list[str], lakebase_row_counts: dict,
    genie_space_id_env: str = "",
) -> list[dict]:
    """Map MAS agents to architecture nodes dynamically."""
    nodes = []
    uc_functions = []

    TYPE_MAP = {
        "genie-space": "genie", "databricks_genie": "genie",
        "knowledge-assistant": "ka", "knowledge_assistant": "ka",
        "external-mcp-server": "mcp", "mcp_connection": "mcp",
        "unity-catalog-function": "uc", "unity_catalog_function": "uc",
        "serving-endpoint": "se", "serving_endpoint": "se",
    }
    COLOR_MAP = {"genie": "#3b82f6", "ka": "#8b5cf6", "mcp": "#f59e0b", "uc": "#06b6d4", "se": "#ec4899"}
    BADGE_MAP = {"genie": "GENIE SPACE", "ka": "KNOWLEDGE ASSISTANT", "mcp": "MCP SERVER", "uc": "UC FUNCTIONS", "se": "SERVING ENDPOINT"}
    TYPE_NAME_MAP = {"genie": "genie-space", "ka": "serving-endpoint", "mcp": "mcp-server", "uc": "infrastructure", "se": "serving-endpoint"}

    for agent in agents:
        atype = agent.get("agent_type", "")
        category = TYPE_MAP.get(atype, "unknown")
        name = agent.get("name", "agent")
        desc = agent.get("description", "")
        slug = _slugify(name)

        if category == "uc":
            uc_path = agent.get("unity_catalog_function", {}).get("uc_path", {})
            fn_name = uc_path.get("name", name) if uc_path else name
            uc_functions.append({"name": fn_name, "description": desc})
            continue

        if category == "se":
            se_name = agent.get("serving_endpoint", {}).get("name", "")
            if "ka-" in se_name or "knowledge" in name.lower():
                category = "ka"

        node = {
            "key": f"{category}_{slug}",
            "name": name.replace("_", " ").replace("-", " ").title(),
            "type": TYPE_NAME_MAP.get(category, "mcp-server"),
            "status": "online", "color": COLOR_MAP.get(category, "#9ca3af"),
            "_layout_column": "agents",
            "type_badge": BADGE_MAP.get(category, "AGENT"),
            "description": desc, "display_items": [], "actions": [], "details": {}, "stats": [],
            "workspace_url": "",
        }

        if category == "genie":
            node["_feeds_from_delta"] = True
            genie_id = agent.get("genie_space", agent.get("databricks_genie", {})).get("id", agent.get("databricks_genie", {}).get("genie_space_id", ""))
            genie_id = genie_id or genie_space_id_env
            connected_tables = [t.get("name", "") for t in delta_tables]
            items = [{"text": f"{len(delta_tables)} Delta tables connected", "status": "success"}]
            for t in delta_tables[:3]:
                tname = t.get("name", "")
                cnt = delta_row_counts.get(tname, "")
                label = f"{tname}  ({cnt:,} rows)" if isinstance(cnt, int) else tname
                items.append({"text": label, "status": "info"})
            node["display_items"] = items
            node["details"] = {"genie_space_id": genie_id, "tables": connected_tables}
            node["stats"] = [{"label": "tables", "value": len(delta_tables)}]
            node["workspace_url"] = f"{workspace_url}/genie/rooms/{genie_id}" if workspace_url and genie_id else ""
        elif category == "ka":
            node["_feeds_from_delta"] = True
            ka_id = ""
            se_cfg = agent.get("serving_endpoint", {})
            if se_cfg:
                ka_id = se_cfg.get("name", "").replace("ka-", "").replace("-endpoint", "")
            node["display_items"] = [
                {"text": "Knowledge retrieval", "status": "success"},
                {"text": "Searches KB articles + documentation", "status": "info"},
            ]
            node["workspace_url"] = f"{workspace_url}/ml/bricks/ka/configure/{ka_id}" if workspace_url and ka_id else ""
        elif category == "mcp":
            node["_feeds_from_lakebase"] = True
            node["display_items"] = [
                {"text": "16 MCP tools available", "status": "success"},
                {"text": f"{len(lakebase_tables)} operational tables", "status": "info"},
                {"text": "READ, WRITE, SQL, DDL", "status": "info"},
            ]
            node["details"] = {"tables": lakebase_tables}
            node["stats"] = [{"label": "MCP tools", "value": 16}, {"label": "tables", "value": len(lakebase_tables)}]

        nodes.append(node)

    if uc_functions:
        fn_names = [f["name"] for f in uc_functions]
        fn_items = []
        for f in uc_functions[:8]:
            label = f["name"]
            if f.get("description"):
                label += f" — {f['description'][:40]}"
            fn_items.append({"text": label, "status": "info"})
        if len(uc_functions) > 8:
            fn_items.append({"text": f"+ {len(uc_functions) - 8} more functions", "status": "info"})
        nodes.append({
            "key": "uc_functions", "name": "UC Functions", "type": "infrastructure",
            "status": "online", "color": "#06b6d4", "_layout_column": "agents",
            "_feeds_from_delta": True,
            "type_badge": "UC FUNCTIONS",
            "description": f"{len(uc_functions)} Unity Catalog function(s) for custom computations.",
            "display_items": fn_items,
            "actions": [], "details": {"functions": fn_names},
            "stats": [{"label": "functions", "value": len(uc_functions)}],
            "workspace_url": f"{workspace_url}/explore/data/{CATALOG}/{SCHEMA}" if workspace_url else "",
        })

    return nodes


def _build_mas_node(agents: list[dict], mas_tile: str, workspace_url: str, status: str, pending_wf: int = 0) -> dict:
    items = [
        {"text": f"{len(agents)} sub-agents connected", "status": "success"},
    ]
    for a in agents:
        aname = a.get("name", "").replace("_", " ").title()
        if aname:
            items.append({"text": aname, "status": "info"})
    if mas_tile:
        items.append({"text": f"Endpoint: mas-{mas_tile}-endpoint", "status": "info"})
    if pending_wf:
        items.append({"text": f"{pending_wf} pending workflows", "status": "info"})
    return {
        "key": "mas", "name": "Multi-Agent Supervisor", "type": "mas",
        "status": status, "color": "#E4002B", "_layout_column": "orchestration",
        "type_badge": "MAS",
        "description": f"Orchestrates {len(agents)} specialized AI agents. Routes user queries to the best agent, manages context, and coordinates multi-step workflows.",
        "display_items": items,
        "actions": [],
        "details": {
            "tile_id": mas_tile,
            "endpoint": f"mas-{mas_tile}-endpoint",
            "sub_agent_names": [a.get("name", "").replace("_", " ").title() for a in agents],
        } if mas_tile else {"sub_agent_names": [a.get("name", "").replace("_", " ").title() for a in agents]},
        "stats": [{"label": "sub-agents", "value": len(agents)}],
        "workspace_url": f"{workspace_url}/ml/bricks/mas/configure/{mas_tile}" if workspace_url and mas_tile else "",
    }


def _compute_edges(nodes: list[dict]) -> list[dict]:
    edges = []
    node_keys = {n["key"] for n in nodes}
    has_mas = "mas" in node_keys
    has_app = "app" in node_keys

    for n in nodes:
        if has_mas and n.get("_layout_column") == "agents":
            edges.append({"source": n["key"], "target": "mas", "color": "#9ca3af"})
        if n.get("_feeds_from_delta") and "lakehouse" in node_keys:
            edges.append({"source": "lakehouse", "target": n["key"], "color": "#3b82f6"})
        if n.get("_feeds_from_lakebase") and "lakebase" in node_keys:
            edges.append({"source": "lakebase", "target": n["key"], "color": "#f59e0b"})

    if has_mas and has_app:
        edges.append({"source": "mas", "target": "app", "color": "#E4002B"})
    if "lakebase" in node_keys and has_app:
        edges.append({"source": "lakebase", "target": "app", "color": "#f59e0b"})

    return edges


def _compute_layout(nodes: list[dict]) -> None:
    AGENT_GAP = 140
    COL_X = {"data-sources": 100, "agents": 450, "orchestration": 930, "application": 1250}

    data_nodes = [n for n in nodes if n.get("_layout_column") == "data-sources"]
    agent_nodes = [n for n in nodes if n.get("_layout_column") == "agents"]
    orch_nodes = [n for n in nodes if n.get("_layout_column") == "orchestration"]
    app_nodes = [n for n in nodes if n.get("_layout_column") == "application"]

    agent_nodes.sort(key=lambda n: (1 if n.get("_feeds_from_lakebase") and not n.get("_feeds_from_delta") else 0))

    agent_start_y = 30
    for i, n in enumerate(agent_nodes):
        n["position"] = {"x": COL_X["agents"], "y": agent_start_y + i * AGENT_GAP}
    agent_bottom = agent_start_y + max(0, len(agent_nodes) - 1) * AGENT_GAP

    lakehouse = [n for n in data_nodes if n["key"] == "lakehouse"]
    lakebase = [n for n in data_nodes if n["key"] == "lakebase"]

    if lakehouse:
        lakehouse[0]["position"] = {"x": COL_X["data-sources"], "y": agent_start_y}
    if lakebase:
        lakebase[0]["position"] = {"x": COL_X["data-sources"], "y": agent_bottom + AGENT_GAP}

    for i, n in enumerate(orch_nodes):
        n["position"] = {"x": COL_X["orchestration"], "y": agent_start_y + i * AGENT_GAP}

    agent_center_y = agent_start_y + (agent_bottom - agent_start_y) / 2
    for i, n in enumerate(app_nodes):
        n["position"] = {"x": COL_X["application"], "y": max(agent_center_y - 20, agent_start_y) + i * AGENT_GAP}

    for n in nodes:
        n.pop("_layout_column", None)
        n.pop("_feeds_from_delta", None)
        n.pop("_feeds_from_lakebase", None)


@app.get("/api/architecture")
async def get_architecture():
    """Return live architecture metadata as flat nodes[] + edges[] for the DAG canvas."""
    catalog = CATALOG
    schema = SCHEMA
    demo_name = os.getenv("DEMO_NAME", "")
    demo_customer = os.getenv("DEMO_CUSTOMER", "")
    genie_space_id = os.getenv("GENIE_SPACE_ID", "")
    _cfg = _get_demo_config()
    workspace_url = _cfg.get("infrastructure", {}).get("workspace_url", "").rstrip("/")
    if not workspace_url:
        workspace_url = os.getenv("DATABRICKS_HOST", "").rstrip("/")

    async def _empty():
        return []

    (q_tables, q_lb_tables, q_health, q_cases, q_wf, q_lb_counts), agents = await asyncio.gather(
        asyncio.gather(
            asyncio.to_thread(run_query, f"SHOW TABLES IN {catalog}.{schema}") if catalog and schema else _empty(),
            asyncio.to_thread(run_pg_query, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"),
            asyncio.to_thread(run_query, "SELECT 1 as ok"),
            _empty(),  # placeholder — override with domain active-record count if needed
            asyncio.to_thread(run_pg_query, "SELECT count(*) as cnt FROM workflows WHERE status = 'pending_approval'"),
            asyncio.to_thread(run_pg_query, "SELECT relname, n_live_tup FROM pg_stat_user_tables"),
            return_exceptions=True,  # Gotcha #41/#46: don't let one Lakebase failure kill all queries
        ),
        _fetch_mas_agents(),
    )

    # Safely extract results — any sub-query may be an Exception (Gotcha #46)
    def _arch_safe(result, default=None):
        if isinstance(result, Exception):
            log.warning("Architecture sub-query failed: %s", result)
            return default if default is not None else []
        return result if result is not None else (default if default is not None else [])

    delta_tables = [{"name": t.get("tableName") or t.get("table_name", "")} for t in _arch_safe(q_tables, [])]
    lakebase_tables = [t["tablename"] for t in _arch_safe(q_lb_tables, [])]
    infra_status = "online" if not isinstance(q_health, Exception) and q_health else "error"
    active_cases = 0
    try:
        active_cases = (_arch_safe(q_cases, [{}]) or [{}])[0].get("cnt", 0)
    except Exception:
        pass
    pending_wf = (_arch_safe(q_wf, [{}]) or [{}])[0].get("cnt", 0)

    lakebase_row_counts = {}
    for row in _arch_safe(q_lb_counts, []):
        lakebase_row_counts[row.get("relname", "")] = int(row.get("n_live_tup", 0))

    delta_row_counts = {}
    if delta_tables and catalog and schema:
        async def _count_delta(tname):
            try:
                r = await asyncio.to_thread(run_query, f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.{tname}")
                return tname, int((r or [{}])[0].get("cnt", 0))
            except Exception:
                return tname, "?"
        results = await asyncio.gather(*[_count_delta(t["name"]) for t in delta_tables])
        delta_row_counts = dict(results)

    # ── Rich metadata: table schemas, genie details, function signatures ──
    delta_schemas = {}
    lakebase_schemas = {}

    async def _describe_delta(tname):
        try:
            r = await asyncio.to_thread(run_query, f"DESCRIBE TABLE {catalog}.{schema}.{tname}")
            cols = [{"col": row.get("col_name", ""), "type": row.get("data_type", "")} for row in (r or []) if row.get("col_name") and not row.get("col_name", "").startswith("#")]
            return tname, cols
        except Exception:
            return tname, []

    async def _describe_lakebase_all():
        try:
            r = await asyncio.to_thread(
                run_pg_query,
                "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position",
            )
            schemas = {}
            for row in (r or []):
                tbl = row.get("table_name", "")
                if tbl not in schemas:
                    schemas[tbl] = []
                schemas[tbl].append({"col": row.get("column_name", ""), "type": row.get("data_type", "")})
            return schemas
        except Exception:
            return {}

    async def _describe_genie(genie_id):
        if not genie_id:
            return {}
        try:
            host_val = workspace_url or os.getenv("DATABRICKS_HOST", "").rstrip("/")
            token = os.getenv("DATABRICKS_TOKEN", "")
            if not token:
                _h, _a = await asyncio.to_thread(_get_mas_auth)
                auth_header = _a
            else:
                auth_header = f"Bearer {token}"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{host_val}/api/2.0/data-rooms/{genie_id}",
                    headers={"Authorization": auth_header},
                )
                resp.raise_for_status()
                d = resp.json()
                return {
                    "name": d.get("display_name", d.get("name", "")),
                    "description": d.get("description", ""),
                    "table_count": len(d.get("table_identifiers", [])),
                    "tables": [t.get("table_identifier", "") for t in d.get("table_identifiers", [])[:10]],
                    "instructions_snippet": (d.get("curated_questions") or [{}])[0].get("question", "") if d.get("curated_questions") else "",
                }
        except Exception as e:
            log.warning("Genie describe failed for %s: %s", genie_id, e)
            return {}

    async def _describe_ka_endpoint(endpoint_name):
        if not endpoint_name:
            return {}
        try:
            ep = await asyncio.to_thread(w.serving_endpoints.get, endpoint_name)
            state = "READY" if ep.state and ep.state.ready == "READY" else "NOT_READY"
            return {"endpoint_name": endpoint_name, "state": state}
        except Exception as e:
            log.warning("KA endpoint describe failed for %s: %s", endpoint_name, e)
            return {}

    async def _describe_ka(ka_id):
        if not ka_id:
            return {}
        try:
            host_val = os.getenv("DATABRICKS_HOST", "").rstrip("/")
            auth_header = _get_mas_auth()[1]  # reuse auth helper
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{host_val}/api/2.0/knowledge-assistants/{ka_id}",
                    headers={"Authorization": auth_header},
                )
                resp.raise_for_status()
                d = resp.json()
                return {
                    "name": d.get("display_name", d.get("name", "")),
                    "description": d.get("description", ""),
                    "index_count": len(d.get("vector_search_indexes", d.get("indexes", []))),
                    "indexes": [idx.get("name", idx.get("index_name", "")) for idx in d.get("vector_search_indexes", d.get("indexes", []))[:5]],
                    "endpoint_name": d.get("endpoint_name", d.get("serving_endpoint_name", "")),
                }
        except Exception as e:
            log.warning("KA describe failed for %s: %s", ka_id, e)
            return {}

    async def _describe_uc_fn(fn_catalog, fn_schema, fn_name):
        try:
            r = await asyncio.to_thread(run_query, f"DESCRIBE FUNCTION {fn_catalog}.{fn_schema}.{fn_name}")
            info = {}
            for row in (r or []):
                key = row.get("info_name", row.get("function_desc", ""))
                val = row.get("info_value", row.get("function_desc", ""))
                if key:
                    info[key.lower().replace(" ", "_")] = val
            return fn_name, info
        except Exception:
            return fn_name, {}

    describe_tasks = []
    for t in delta_tables:
        describe_tasks.append(_describe_delta(t["name"]))
    describe_tasks.append(_describe_lakebase_all())

    genie_ids = []
    ka_endpoints = []
    ka_ids = []
    uc_fn_specs = []
    for ag in agents:
        atype = ag.get("agent_type", "")
        if atype in ("databricks_genie", "genie-space"):
            gid = ag.get("databricks_genie", ag.get("genie_space", {})).get("genie_space_id", ag.get("genie_space", ag.get("databricks_genie", {})).get("id", ""))
            gid = gid or genie_space_id
            if gid:
                genie_ids.append(gid)
                describe_tasks.append(_describe_genie(gid))
        elif atype in ("serving_endpoint", "knowledge_assistant"):
            ep_name = ag.get("serving_endpoint", {}).get("name", "")
            if ep_name and "ka-" in ep_name:
                ka_endpoints.append(ep_name)
                describe_tasks.append(_describe_ka_endpoint(ep_name))
            # Also try to get full KA details via KA API
            ka_id = ag.get("knowledge_assistant", {}).get("ka_id", "")
            if not ka_id and ep_name:
                ka_id = ep_name.replace("ka-", "").replace("-endpoint", "")
            if ka_id:
                ka_ids.append(ka_id)
                describe_tasks.append(_describe_ka(ka_id))
        elif atype == "unity_catalog_function":
            uc_path = ag.get("unity_catalog_function", {}).get("uc_path", {})
            if uc_path:
                fn_c = uc_path.get("catalog", catalog)
                fn_s = uc_path.get("schema", schema)
                fn_n = uc_path.get("name", "")
                if fn_n:
                    uc_fn_specs.append(fn_n)
                    describe_tasks.append(_describe_uc_fn(fn_c, fn_s, fn_n))

    describe_results = await asyncio.gather(*describe_tasks, return_exceptions=True)

    idx = 0
    for t in delta_tables:
        r = describe_results[idx]
        if not isinstance(r, Exception):
            name, cols = r
            delta_schemas[name] = cols
        idx += 1
    r = describe_results[idx]
    if not isinstance(r, Exception):
        lakebase_schemas = r
    idx += 1
    genie_details = {}
    for gid in genie_ids:
        r = describe_results[idx]
        if not isinstance(r, Exception):
            genie_details[gid] = r
        idx += 1
    ka_details = {}
    for ep in ka_endpoints:
        r = describe_results[idx]
        if not isinstance(r, Exception):
            ka_details[ep] = r
        idx += 1
    # KA full details (from KA API)
    ka_full_details = {}
    for kid in ka_ids:
        r = describe_results[idx]
        if not isinstance(r, Exception):
            ka_full_details[kid] = r
        idx += 1
    uc_fn_details = {}
    for fn in uc_fn_specs:
        r = describe_results[idx]
        if not isinstance(r, Exception):
            fname, info = r
            uc_fn_details[fname] = info
        idx += 1

    # Build nodes
    data_nodes = _build_data_nodes(
        catalog, schema, workspace_url,
        delta_tables, delta_row_counts,
        lakebase_tables, lakebase_row_counts,
    )
    for n in data_nodes:
        if n["key"] == "lakehouse":
            n["details"]["table_schemas"] = delta_schemas
        elif n["key"] == "lakebase":
            n["details"]["table_schemas"] = lakebase_schemas

    agent_nodes = _build_agent_nodes(
        agents, workspace_url,
        delta_tables, delta_row_counts,
        lakebase_tables, lakebase_row_counts,
        genie_space_id_env=genie_space_id,
    )
    for n in agent_nodes:
        if n["type"] == "genie-space":
            gid = n.get("details", {}).get("genie_space_id", "")
            if gid and gid in genie_details:
                gd = genie_details[gid]
                n["details"]["genie_name"] = gd.get("name", "")
                n["details"]["genie_description"] = gd.get("description", "")
                n["details"]["genie_table_count"] = gd.get("table_count", 0)
                n["details"]["genie_tables"] = gd.get("tables", [])
                n["details"]["curated_question"] = gd.get("instructions_snippet", "")
                if gd.get("name"):
                    n["name"] = gd["name"]
        elif n.get("type") == "serving-endpoint" and n.get("type_badge") in ("KNOWLEDGE ASSISTANT", "SERVING ENDPOINT"):
            for ep, kd in ka_details.items():
                if ep in str(n.get("details", {})) or ep in str(n.get("workspace_url", "")):
                    n["details"]["endpoint_state"] = kd.get("state", "")
                    if kd.get("state") == "READY":
                        n["display_items"].append({"text": "Endpoint READY", "status": "success"})
                    break
            # Enrich with full KA API details
            for kid, kfull in ka_full_details.items():
                if kid in str(n.get("workspace_url", "")) or kid in str(n.get("details", {})):
                    if kfull.get("name"):
                        n["name"] = kfull["name"]
                    if kfull.get("description"):
                        n["details"]["ka_description"] = kfull["description"]
                    if kfull.get("indexes"):
                        n["details"]["ka_indexes"] = kfull["indexes"]
                        n["details"]["ka_index_count"] = kfull.get("index_count", 0)
                    if kfull.get("endpoint_name"):
                        n["details"]["ka_endpoint"] = kfull["endpoint_name"]
                    break
        elif n.get("key") == "uc_functions":
            n["details"]["function_details"] = uc_fn_details

    all_nodes = data_nodes + agent_nodes

    if agents:
        mas_node = _build_mas_node(agents, MAS_TILE_ID, workspace_url, infra_status, pending_wf)
        cfg = _get_demo_config()
        mas_persona = cfg.get("ai_layer", {}).get("mas_persona", "")
        if mas_persona:
            mas_node["details"]["instructions"] = mas_persona
        all_nodes.append(mas_node)

    all_nodes.append(_build_app_node(workspace_url, demo_name, active_cases, pending_wf, lakebase_tables))

    edges = _compute_edges(all_nodes)
    _compute_layout(all_nodes)

    return {
        "workspace_url": workspace_url,
        "demo_name": demo_name,
        "demo_customer": demo_customer,
        "column_labels": [
            {"key": "data-sources", "label": "DATA SOURCES", "x": 100},
            {"key": "agents", "label": "AI AGENTS", "x": 450},
            {"key": "orchestration", "label": "ORCHESTRATION", "x": 930},
            {"key": "application", "label": "APPLICATION", "x": 1250},
        ],
        "nodes": all_nodes,
        "edges": edges,
        "live_stats": {
            "active_cases": active_cases,
            "pending_workflows": pending_wf,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# Architecture: Table Data Explorer
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/architecture/table-data")
async def get_architecture_table_data(
    table: str = Query(..., min_length=1, max_length=128),
    source: str = Query(..., pattern=r"^(delta|lakebase)$"),
    limit: int = Query(20, ge=1, le=100),
):
    """Return schema + sample rows for a single table (used by the data explorer)."""
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
        raise HTTPException(400, "Invalid table name")

    catalog = CATALOG
    schema = SCHEMA

    if source == "delta":
        known = await asyncio.to_thread(
            run_query, f"SHOW TABLES IN {catalog}.{schema}"
        )
        known_names = {t.get("tableName") or t.get("table_name", "") for t in (known or [])}
        if table not in known_names:
            raise HTTPException(404, f"Table '{table}' not found in {catalog}.{schema}")

        fqn = f"{catalog}.{schema}.{table}"
        q_rows, q_desc, q_count = await asyncio.gather(
            asyncio.to_thread(run_query, f"SELECT * FROM {fqn} LIMIT {limit}"),
            asyncio.to_thread(run_query, f"DESCRIBE TABLE {fqn}"),
            asyncio.to_thread(run_query, f"SELECT COUNT(*) as cnt FROM {fqn}"),
        )
        columns = [
            {"col": r.get("col_name", ""), "type": r.get("data_type", "")}
            for r in (q_desc or [])
            if r.get("col_name") and not r.get("col_name", "").startswith("#")
        ]
        return {
            "table": table,
            "source": "delta",
            "columns": columns,
            "rows": q_rows or [],
            "row_count": int((q_count or [{}])[0].get("cnt", 0)),
        }

    else:  # lakebase
        known = await asyncio.to_thread(
            run_pg_query,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
        )
        known_names = {t["tablename"] for t in (known or [])}
        if table not in known_names:
            raise HTTPException(404, f"Table '{table}' not found in Lakebase")

        q_rows, q_schema, q_count = await asyncio.gather(
            asyncio.to_thread(
                run_pg_query,
                f"SELECT * FROM {table} LIMIT %s",
                (limit,),
            ),
            asyncio.to_thread(
                run_pg_query,
                """SELECT c.column_name, c.data_type, c.is_nullable, c.column_default,
                          CASE WHEN kcu.column_name IS NOT NULL THEN 'PK' ELSE NULL END as key
                   FROM information_schema.columns c
                   LEFT JOIN information_schema.key_column_usage kcu
                     ON kcu.table_name = c.table_name AND kcu.column_name = c.column_name
                     AND kcu.table_schema = c.table_schema
                   WHERE c.table_schema = 'public' AND c.table_name = %s
                   ORDER BY c.ordinal_position""",
                (table,),
            ),
            asyncio.to_thread(
                run_pg_query,
                "SELECT n_live_tup as cnt FROM pg_stat_user_tables WHERE relname = %s",
                (table,),
            ),
        )
        columns = [
            {
                "col": r.get("column_name", ""),
                "type": r.get("data_type", ""),
                "nullable": r.get("is_nullable", "YES") == "YES",
                "default": r.get("column_default"),
                "key": r.get("key"),
            }
            for r in (q_schema or [])
        ]
        return {
            "table": table,
            "source": "lakebase",
            "columns": columns,
            "rows": q_rows or [],
            "row_count": int((q_count or [{}])[0].get("cnt", 0)),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Agent Workflows — powers the Agent Workflows page
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/agent-overview")
async def get_agent_overview():
    """Return KPIs, workflows, and recent agent actions for the Agent page."""

    def _cnt(result):
        """Safely extract count from a query result, handling exceptions (Gotcha #41)."""
        if isinstance(result, Exception):
            log.warning("Agent overview sub-query failed: %s", result)
            return 0
        return (result or [{}])[0].get("cnt", 0)

    def _rows(result, default=None):
        """Safely extract rows from a query result, handling exceptions (Gotcha #41)."""
        if isinstance(result, Exception):
            log.warning("Agent overview sub-query failed: %s", result)
            return default if default is not None else []
        return result or (default if default is not None else [])

    try:
        q_pending, q_in_progress, q_completed_7d, q_actions_24h, q_workflows, q_actions, q_open_exceptions = await asyncio.gather(
            asyncio.to_thread(run_pg_query, "SELECT count(*) as cnt FROM workflows WHERE status = 'pending_approval'"),
            asyncio.to_thread(run_pg_query, "SELECT count(*) as cnt FROM workflows WHERE status = 'in_progress'"),
            asyncio.to_thread(run_pg_query, "SELECT count(*) as cnt FROM workflows WHERE status = 'approved' AND completed_at >= NOW() - INTERVAL '7 days'"),
            asyncio.to_thread(run_pg_query, "SELECT count(*) as cnt FROM agent_actions WHERE created_at >= NOW() - INTERVAL '24 hours'"),
            asyncio.to_thread(run_pg_query, "SELECT * FROM workflows ORDER BY created_at DESC LIMIT 50"),
            asyncio.to_thread(run_pg_query, "SELECT * FROM agent_actions ORDER BY created_at DESC LIMIT 20"),
            asyncio.to_thread(run_pg_query, "SELECT count(*) as cnt FROM exceptions WHERE status = 'open'"),
            return_exceptions=True,  # Gotcha #41: don't let one failure kill all queries
        )
        workflows = [_enrich_workflow(wf) for wf in _rows(q_workflows)]
        return {
            "kpis": {
                "pending_approval": _cnt(q_pending),
                "in_progress": _cnt(q_in_progress),
                "completed_7d": _cnt(q_completed_7d),
                "agent_actions_24h": _cnt(q_actions_24h),
                "open_exceptions": _cnt(q_open_exceptions),
            },
            "workflows": workflows,
            "agent_actions_recent": _rows(q_actions),
        }
    except Exception as e:
        log.warning("Agent overview query failed (Lakebase tables may not exist): %s", e)
        return {"kpis": {}, "workflows": [], "agent_actions_recent": []}


@app.get("/api/workflows/{workflow_id}")
async def get_workflow(workflow_id: int):
    """Return a single workflow by ID (used by the workflow detail modal)."""
    rows = await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM workflows WHERE workflow_id = %s",
        (workflow_id,),
    )
    if not rows:
        raise HTTPException(404, f"Workflow {workflow_id} not found")
    return rows[0]


@app.patch("/api/workflows/{workflow_id}")
async def update_workflow(workflow_id: int, body: dict):
    """Update a workflow's status (approve/dismiss). Wires Gotcha #26 side-effects."""
    new_status = body.get("status", "")
    if new_status not in ("approved", "dismissed"):
        raise HTTPException(400, "Status must be 'approved' or 'dismissed'")
    completed = "NOW()" if new_status in ("approved", "dismissed") else "NULL"
    result = await asyncio.to_thread(
        write_pg,
        f"UPDATE workflows SET status = %s, completed_at = {completed} WHERE workflow_id = %s RETURNING *",
        (new_status, workflow_id),
    )
    if not result:
        raise HTTPException(404, f"Workflow {workflow_id} not found")

    actions_taken: list[str] = []
    try:
        if new_status == "approved":
            actions_taken = await _execute_workflow_actions(result)
        else:
            actions_taken = await _record_dismiss(result)
    except Exception as e:
        log.warning("Workflow side-effects failed (workflow_id=%s): %s", workflow_id, e)
        actions_taken = [f"side-effects-error: {e}"]

    result["actions_taken"] = actions_taken
    return result


async def _execute_workflow_actions(wf: dict) -> list[str]:
    """Dispatch side-effects per workflow_type (Gotcha #26).

    Fleet-health workflow types:
      - work_order_create: creates a work_orders row + audit + note
      - anomaly_escalate:  marks an exception + audit + note
      - memory_persist:    inserts an agent_memory row + audit + note
    """
    wf_type = wf.get("workflow_type", "")
    entity_id = wf.get("entity_id") or ""
    severity = wf.get("severity") or "medium"
    summary = wf.get("summary") or ""
    actions: list[str] = []

    if wf_type == "work_order_create":
        # Open a real work order for the robot
        wo_num = f"WO-AUTO-{wf.get('workflow_id'):06d}"
        family_guess = entity_id.split("-")[0].title() if "-" in entity_id else "Sparrow"
        fam_map = {"Spa": "Sparrow", "Her": "Hercules", "Pro": "Proteus", "Seq": "Sequoia"}
        family = fam_map.get(family_guess[:3], "Sparrow")
        fc_site = entity_id.split("-")[1] if entity_id.count("-") >= 2 else ""
        await asyncio.to_thread(
            write_pg,
            """INSERT INTO work_orders
               (wo_number, robot_id, family, fc_site, severity, priority, title,
                root_cause, remediation_steps, status, created_by)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (wo_number) DO NOTHING""",
            (
                wo_num, entity_id, family, fc_site, severity,
                "urgent" if severity == "critical" else "high",
                f"Agent-recommended remediation — {entity_id}",
                summary, "See AI Chat recommendation for full step list.",
                "open", "agent",
            ),
        )
        actions.append(f"Created work order {wo_num}")

    elif wf_type == "anomaly_escalate":
        await asyncio.to_thread(
            write_pg,
            """INSERT INTO exceptions
               (entity_type, entity_id, exception_type, severity, description, status)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            ("robot", entity_id, "anomaly_cluster", severity,
             summary[:1800] or f"Escalated anomaly cluster on {entity_id}", "escalated"),
        )
        actions.append(f"Escalated anomaly cluster for {entity_id}")

    elif wf_type == "memory_persist":
        family_guess = entity_id.split("-")[0].title() if "-" in entity_id else "Sparrow"
        fam_map = {"Spa": "Sparrow", "Her": "Hercules", "Pro": "Proteus", "Seq": "Sequoia"}
        family = fam_map.get(family_guess[:3], "Sparrow")
        await asyncio.to_thread(
            write_pg,
            """INSERT INTO agent_memory
               (robot_id, family, signal, persona, diagnostic, remediation,
                outcome, model_version, confidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (entity_id, family, "multi_signal", "rme_tech",
             summary, "Persisted from agent reasoning chain.",
             "resolved", "sparrow-detect-v5", 0.85),
        )
        actions.append(f"Persisted memory for {entity_id}")

    # Always audit + note
    await asyncio.gather(
        asyncio.to_thread(
            write_pg,
            """INSERT INTO agent_actions
               (action_type, severity, entity_type, entity_id, description,
                action_taken, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (wf_type, severity, "robot", entity_id,
             summary[:500], "; ".join(actions) or "approved", "executed"),
        ),
        asyncio.to_thread(
            write_pg,
            """INSERT INTO notes (entity_type, entity_id, note_text, author)
               VALUES (%s,%s,%s,%s)""",
            ("robot", entity_id, f"Workflow approved: {wf_type} — {summary[:200]}", "agent"),
        ),
    )
    actions.append("Audit + note recorded")
    return actions


async def _record_dismiss(wf: dict) -> list[str]:
    """Record dismissal for the audit trail without firing domain side-effects."""
    wf_type = wf.get("workflow_type", "")
    severity = wf.get("severity") or "medium"
    entity_id = wf.get("entity_id") or ""
    summary = wf.get("summary") or ""
    await asyncio.gather(
        asyncio.to_thread(
            write_pg,
            """INSERT INTO agent_actions
               (action_type, severity, entity_type, entity_id, description,
                action_taken, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (wf_type, severity, "robot", entity_id,
             summary[:500], "Operator dismissed", "dismissed"),
        ),
        asyncio.to_thread(
            write_pg,
            """INSERT INTO notes (entity_type, entity_id, note_text, author)
               VALUES (%s,%s,%s,%s)""",
            ("robot", entity_id, f"Workflow dismissed: {wf_type}", "agent"),
        ),
    )
    return ["Operator dismissed — audit + note recorded"]


# ═══════════════════════════════════════════════════════════════════════════
# Exception Management (Nice-to-Have #13) — generic alerts/exceptions CRUD
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/exceptions")
async def list_exceptions(
    status: str = Query(None),
    severity: str = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """List exceptions with optional status/severity filters."""
    clauses, params = [], []
    if status:
        clauses.append("status = %s")
        params.append(_safe(status))
    if severity:
        clauses.append("severity = %s")
        params.append(_safe(severity))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    try:
        rows = await asyncio.to_thread(
            run_pg_query,
            f"SELECT * FROM exceptions{where} ORDER BY created_at DESC LIMIT %s",
            tuple(params),
        )
        return rows or []
    except Exception as e:
        # Gotcha #46: Lakebase table may not exist or SP may lack permissions
        log.warning("Exceptions query failed (table may not exist): %s", e)
        return []


@app.post("/api/exceptions")
async def create_exception(body: dict):
    """Create a new exception/alert."""
    required = ["entity_type", "entity_id", "exception_type", "description"]
    for key in required:
        if not body.get(key):
            raise HTTPException(400, f"Missing required field: {key}")
    result = await asyncio.to_thread(
        write_pg,
        """INSERT INTO exceptions (entity_type, entity_id, exception_type, severity, description, assigned_to)
           VALUES (%s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (
            _safe(body["entity_type"]),
            _safe(body["entity_id"]),
            _safe(body["exception_type"]),
            _safe(body.get("severity", "medium")),
            body["description"][:2000],
            body.get("assigned_to", "")[:100] or None,
        ),
    )
    return result


@app.patch("/api/exceptions/{exception_id}")
async def update_exception(exception_id: int, body: dict):
    """Update exception status (acknowledge / resolve / escalate / cancel)."""
    new_status = body.get("status", "")
    if new_status not in ("acknowledged", "resolved", "escalated", "cancelled"):
        raise HTTPException(400, "Status must be acknowledged, resolved, escalated, or cancelled")
    resolution = body.get("resolution", "")
    resolved_at = "NOW()" if new_status in ("resolved", "cancelled") else "NULL"
    result = await asyncio.to_thread(
        write_pg,
        f"""UPDATE exceptions
            SET status = %s, resolution = %s, resolved_at = {resolved_at}
            WHERE exception_id = %s
            RETURNING *""",
        (new_status, resolution[:2000] if resolution else None, exception_id),
    )
    if not result:
        raise HTTPException(404, f"Exception {exception_id} not found")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Morning Briefing — AI-generated summary of current state
# ═══════════════════════════════════════════════════════════════════════════

_briefing_cache: dict = {}
_BRIEFING_TTL = 300  # 5 minutes


def _build_briefing_context() -> str:
    """Gather operational data for the briefing prompt."""
    parts = []
    try:
        exceptions = run_pg_query(
            "SELECT severity, count(*) as cnt FROM exceptions WHERE status = 'open' GROUP BY severity"
        )
        if exceptions:
            parts.append("Open exceptions: " + ", ".join(f"{r['severity']}: {r['cnt']}" for r in exceptions))
    except Exception:
        pass
    try:
        workflows = run_pg_query(
            "SELECT status, count(*) as cnt FROM workflows GROUP BY status"
        )
        if workflows:
            parts.append("Workflows: " + ", ".join(f"{r['status']}: {r['cnt']}" for r in workflows))
    except Exception:
        pass
    try:
        actions = run_pg_query(
            "SELECT count(*) as cnt FROM agent_actions WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )
        if actions:
            parts.append(f"Agent actions (24h): {actions[0].get('cnt', 0)}")
    except Exception:
        pass
    # Fleet-specific signals for the briefing
    try:
        crit = run_query(
            f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.anomalies "
            f"WHERE status = 'open' AND severity = 'critical'"
        )
        if crit:
            parts.append(f"Critical open anomalies: {crit[0].get('cnt', 0)}")
    except Exception:
        pass
    try:
        wo = run_pg_query(
            "SELECT count(*) as cnt FROM work_orders WHERE status IN ('open','in_progress')"
        )
        if wo:
            parts.append(f"Active work orders: {wo[0].get('cnt', 0)}")
    except Exception:
        pass
    try:
        fam = run_query(
            f"SELECT family, COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.anomalies "
            f"WHERE status = 'open' GROUP BY family ORDER BY cnt DESC LIMIT 4"
        )
        if fam:
            parts.append("Open anomalies by family: " + ", ".join(f"{r['family']}: {r['cnt']}" for r in fam))
    except Exception:
        pass
    return "\n".join(parts) if parts else "No operational data available."


@app.get("/api/briefing")
async def get_briefing():
    """Non-streaming briefing — cached with 5-min TTL."""
    now = _time.time()
    if _briefing_cache.get("data") and now - _briefing_cache.get("ts", 0) < _BRIEFING_TTL:
        return _briefing_cache["data"]

    context = await asyncio.to_thread(_build_briefing_context)

    # Try MAS for a rich briefing
    briefing_text = ""
    try:
        host, auth = await asyncio.to_thread(_get_mas_auth)
        tile = MAS_TILE_ID
        if tile and tile != "TODO":
            ep = await asyncio.to_thread(w.serving_endpoints.get, f"mas-{tile}-endpoint")
            full_uuid = ep.tile_endpoint_metadata.tile_id
            # TODO(vibe): Customize the briefing prompt for your domain
            prompt = f"Generate a brief morning operational briefing (3-5 bullet points). Focus on items needing attention. Data:\n{context}"
            payload = {
                "messages": [{"role": "user", "content": prompt}],
                "multi_agent_supervisor_id": full_uuid,
            }
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{host}/api/2.0/serving-endpoints/mas-{tile}-endpoint/invocations",
                    headers={"Authorization": auth, "Content-Type": "application/json"},
                    json=payload,
                )
                resp.raise_for_status()
                data = resp.json()
                # Extract text from MAS response
                choices = data.get("choices", [])
                if choices:
                    briefing_text = choices[0].get("message", {}).get("content", "")
    except Exception as e:
        log.warning("MAS briefing failed, using raw data: %s", e)

    if not briefing_text:
        briefing_text = f"**Operational Summary**\n{context}"

    result = {"briefing": briefing_text, "raw_data": context, "generated_at": now}
    _briefing_cache["data"] = result
    _briefing_cache["ts"] = now
    return result


@app.get("/api/briefing/stream")
async def stream_briefing():
    """SSE streaming version of the briefing."""
    context = await asyncio.to_thread(_build_briefing_context)

    async def _stream():
        tile = MAS_TILE_ID
        if not tile or tile == "TODO":
            # Gotcha #39: Never use backslashes inside f-string {…} — crashes Python 3.11
            _fallback_text = "**Operational Summary**\n" + context
            yield f"data: {json.dumps({'type': 'delta', 'text': _fallback_text})}\n\n"
            yield "data: [DONE]\n\n"
            return
        try:
            host, auth = await asyncio.to_thread(_get_mas_auth)
            ep = await asyncio.to_thread(w.serving_endpoints.get, f"mas-{tile}-endpoint")
            full_uuid = ep.tile_endpoint_metadata.tile_id
            prompt = f"Generate a brief morning operational briefing (3-5 bullet points). Focus on items needing attention. Data:\n{context}"
            payload = {
                "messages": [{"role": "user", "content": prompt}],
                "multi_agent_supervisor_id": full_uuid,
                "stream": True,
            }
            async with httpx.AsyncClient(timeout=120) as client:
                async with client.stream(
                    "POST",
                    f"{host}/api/2.0/serving-endpoints/mas-{tile}-endpoint/invocations",
                    headers={"Authorization": auth, "Content-Type": "application/json"},
                    json=payload,
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.startswith("data: "):
                            raw = line[6:]
                            if raw == "[DONE]":
                                break
                            try:
                                evt = json.loads(raw)
                                choices = evt.get("choices", [])
                                if choices:
                                    delta = choices[0].get("delta", {}).get("content", "")
                                    if delta:
                                        yield f"data: {json.dumps({'type': 'delta', 'text': delta})}\n\n"
                            except json.JSONDecodeError:
                                pass
        except Exception as e:
            log.warning("Streaming briefing failed: %s", e)
            # Gotcha #39: Never use backslashes inside f-string {…} — crashes Python 3.11
            _err_fallback = "**Operational Summary**\n" + context
            yield f"data: {json.dumps({'type': 'delta', 'text': _err_fallback})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


# ═══════════════════════════════════════════════════════════════════════════
# Workflow Enrichment (Nice-to-Have #14) — reasoning chains, agent flows
# ═══════════════════════════════════════════════════════════════════════════

def _enrich_workflow(wf: dict) -> dict:
    """Enrich a workflow with headline, enriched_summary, reasoning_chain, and agent_flow.

    This is a SKELETON function — domain-specific labels and templates are marked
    with TODO(vibe) comments for vibe to fill during demo creation.
    """
    wf = dict(wf)  # don't mutate original

    # ── Headline ──
    if not wf.get("headline"):
        TYPE_HEADLINES = {
            "work_order_create": f"Open Work Order: {wf.get('entity_id', 'unknown')}",
            "anomaly_escalate":  f"Escalate Anomaly Cluster: {wf.get('entity_id', 'unknown')}",
            "memory_persist":    f"Persist Fleet Memory: {wf.get('entity_id', 'unknown')}",
        }
        wtype = wf.get("workflow_type", "workflow")
        entity = f"{wf.get('entity_type', '')} {wf.get('entity_id', '')}".strip()
        wf["headline"] = TYPE_HEADLINES.get(
            wtype,
            f"{wtype.replace('_', ' ').title()}: {entity}" if entity else wtype.replace("_", " ").title(),
        )

    # ── Enriched Summary ──
    if not wf.get("enriched_summary"):
        summary = wf.get("summary", "")
        # TODO(vibe): Add domain-specific narrative generation
        wf["enriched_summary"] = summary

    # ── Reasoning Chain ──
    chain = wf.get("reasoning_chain")
    if isinstance(chain, str):
        try:
            chain = json.loads(chain)
        except (json.JSONDecodeError, TypeError):
            chain = []
    if not chain or not isinstance(chain, list):
        # Build a template chain from workflow metadata
        chain = [
            {"step": 1, "tool": "monitor", "label": "Trigger detected", "output": wf.get("trigger_source", "monitor"), "status": "completed"},
            {"step": 2, "tool": "analyze", "label": "Analyzing situation", "output": wf.get("summary", "")[:100], "status": "completed"},
        ]
        if wf.get("entity_type"):
            chain.append({"step": 3, "tool": "action", "label": f"Action on {wf.get('entity_type', '')}", "output": wf.get("entity_id", ""), "status": "completed"})
    wf["reasoning_chain"] = chain

    # ── Agent Flow ──
    if not wf.get("agent_flow"):
        # TODO(vibe): Customize agent flow data for your domain
        wf["agent_flow"] = None  # Frontend falls back to buildDomainAgentFlow()

    return wf


# ═══════════════════════════════════════════════════════════════════════════
# Physical AI Fleet Health Console — Domain Routes
# ═══════════════════════════════════════════════════════════════════════════

from pydantic import BaseModel
from typing import Optional


# ─── Dashboard endpoints (KPIs + fleet map + activity) ────────────────────

@app.get("/api/fleet/metrics")
async def get_fleet_metrics():
    """6 exec KPIs for the dashboard."""
    return await _get_fleet_kpis()


async def _get_fleet_kpis() -> dict:
    q_total_robots, q_operational, q_mtbf, q_mttr, q_open_anom, q_auto_rate, q_downtime = await asyncio.gather(
        asyncio.to_thread(run_query, f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.robots"),
        asyncio.to_thread(run_query, f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.robots WHERE status = 'operational'"),
        asyncio.to_thread(
            run_query,
            f"""SELECT family,
                   ROUND(AVG(hours_operated) / GREATEST(1, COUNT(*)), 1) AS mtbf_hours
                FROM {CATALOG}.{SCHEMA}.robots
                LEFT JOIN (
                    SELECT robot_id, COUNT(*) as failures
                    FROM {CATALOG}.{SCHEMA}.anomalies
                    WHERE severity IN ('high','critical')
                    GROUP BY robot_id
                ) f USING (robot_id)
                GROUP BY family"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT ROUND(AVG(downtime_minutes), 1) as avg_mttr
                FROM {CATALOG}.{SCHEMA}.anomalies
                WHERE status = 'resolved' AND downtime_minutes > 0
                  AND detected_at >= CURRENT_DATE - INTERVAL '30' DAY"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT severity, COUNT(*) as cnt
                FROM {CATALOG}.{SCHEMA}.anomalies
                WHERE status IN ('open','investigating')
                GROUP BY severity"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT
                  ROUND(100.0 * SUM(CASE WHEN auto_resolved THEN 1 ELSE 0 END)
                              / GREATEST(1, COUNT(*)), 1) AS auto_pct
                FROM {CATALOG}.{SCHEMA}.anomalies
                WHERE status = 'resolved'
                  AND detected_at >= CURRENT_DATE - INTERVAL '30' DAY"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT
                  SUM(downtime_minutes) AS total_minutes_avoided
                FROM {CATALOG}.{SCHEMA}.anomalies
                WHERE auto_resolved = true
                  AND detected_at >= CURRENT_DATE - INTERVAL '30' DAY"""
        ),
        return_exceptions=True,
    )

    def _v(r, key="cnt", default=0):
        if isinstance(r, Exception):
            log.warning("Fleet KPI sub-query failed: %s", r)
            return default
        return (r or [{}])[0].get(key, default)

    total = _v(q_total_robots) or 1
    operational = _v(q_operational)
    availability_pct = round(100.0 * operational / total, 1)

    mtbf_rows = q_mtbf if not isinstance(q_mtbf, Exception) else []
    mtbf_avg = 0
    if mtbf_rows:
        vals = [r.get("mtbf_hours") for r in mtbf_rows if r.get("mtbf_hours") is not None]
        if vals:
            mtbf_avg = round(sum(vals) / len(vals), 0)

    sev_rows = q_open_anom if not isinstance(q_open_anom, Exception) else []
    open_by_sev = {r["severity"]: int(r["cnt"]) for r in sev_rows}
    open_total = sum(open_by_sev.values())
    open_critical = open_by_sev.get("critical", 0)

    # Downtime-avoided $: 1 minute of avoided downtime ~ $150 (defensible demo number)
    minutes_avoided = _v(q_downtime, "total_minutes_avoided", 0) or 0
    downtime_dollars = int(minutes_avoided) * 150

    return {
        "availability_pct": availability_pct,
        "mtbf_hours": int(mtbf_avg),
        "mttr_minutes": int(_v(q_mttr, "avg_mttr", 0) or 0),
        "open_anomalies": open_total,
        "open_critical": open_critical,
        "open_by_severity": open_by_sev,
        "autonomous_resolution_pct": float(_v(q_auto_rate, "auto_pct", 0.0) or 0.0),
        "downtime_avoided_dollars": downtime_dollars,
        "total_robots": int(total),
        "operational_robots": int(operational),
    }


@app.get("/api/fleet/site-health")
async def get_site_health():
    """FC-site fleet map data — severity breakdown by site."""
    rows = await asyncio.to_thread(
        run_query,
        f"""SELECT
              r.fc_site,
              MAX(r.region) AS region,
              MAX(r.fc_type) AS fc_type,
              COUNT(DISTINCT r.robot_id) AS robot_count,
              SUM(CASE WHEN r.status = 'operational' THEN 1 ELSE 0 END) AS operational,
              SUM(CASE WHEN a.severity = 'critical' AND a.status IN ('open','investigating') THEN 1 ELSE 0 END) AS critical_open,
              SUM(CASE WHEN a.severity = 'high'     AND a.status IN ('open','investigating') THEN 1 ELSE 0 END) AS high_open,
              SUM(CASE WHEN a.severity = 'medium'   AND a.status IN ('open','investigating') THEN 1 ELSE 0 END) AS medium_open
            FROM {CATALOG}.{SCHEMA}.robots r
            LEFT JOIN {CATALOG}.{SCHEMA}.anomalies a ON a.robot_id = r.robot_id
            GROUP BY r.fc_site
            ORDER BY r.fc_site"""
    )
    out = []
    for r in (rows or []):
        crit = int(r.get("critical_open") or 0)
        high = int(r.get("high_open") or 0)
        if crit > 0:
            health = "critical"
        elif high > 2:
            health = "warning"
        else:
            health = "healthy"
        r["health"] = health
        out.append(r)
    return out


@app.get("/api/fleet/recent-activity")
async def get_recent_activity():
    """Unified feed: recent agent_actions + agent_memory entries."""
    actions, memory = await asyncio.gather(
        asyncio.to_thread(
            run_pg_query,
            """SELECT 'action' as kind, action_id as id, action_type as title,
                      description, status, severity, entity_id, created_at
                 FROM agent_actions
                 ORDER BY created_at DESC LIMIT 10"""
        ),
        asyncio.to_thread(
            run_pg_query,
            """SELECT 'memory' as kind, memory_id as id, family || ' / ' || signal as title,
                      diagnostic as description, outcome as status, NULL as severity,
                      robot_id as entity_id, created_at
                 FROM agent_memory
                 ORDER BY created_at DESC LIMIT 10"""
        ),
        return_exceptions=True,
    )

    def _safe_r(r):
        return r if not isinstance(r, Exception) else []
    feed = (_safe_r(actions) or []) + (_safe_r(memory) or [])
    feed.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    return feed[:15]


# ─── Fleet page (robots) ──────────────────────────────────────────────────

@app.get("/api/fleet/robots")
async def get_robots(
    family: Optional[str] = Query(None),
    fc_site: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
):
    """Robots list with filters."""
    where = []
    if family:
        where.append(f"family = '{_safe(family)}'")
    if fc_site:
        where.append(f"fc_site = '{_safe(fc_site)}'")
    if status:
        where.append(f"status = '{_safe(status)}'")
    if search:
        s = _safe(search).lower()
        where.append(f"LOWER(robot_id) LIKE '%{s}%'")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = await asyncio.to_thread(
        run_query,
        f"""SELECT robot_id, family, kind, fc_site, region, fc_type,
                   firmware_version, detect_model, status,
                   install_date, hours_operated
            FROM {CATALOG}.{SCHEMA}.robots {clause}
            ORDER BY fc_site, family, robot_id
            LIMIT {limit}"""
    )
    return rows or []


@app.get("/api/fleet/robots/{robot_id}")
async def get_robot_detail(robot_id: str):
    """Robot detail with telemetry sparkline, recent anomalies, work order history."""
    safe_id = _safe(robot_id)
    q_robot, q_telemetry, q_anomalies, q_work_orders = await asyncio.gather(
        asyncio.to_thread(
            run_query,
            f"""SELECT * FROM {CATALOG}.{SCHEMA}.robots
                WHERE robot_id = '{safe_id}' LIMIT 1"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT signal, reading_date, reading_value, health_score
                FROM {CATALOG}.{SCHEMA}.telemetry
                WHERE robot_id = '{safe_id}'
                  AND reading_date >= CURRENT_DATE - INTERVAL '30' DAY
                ORDER BY reading_date"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT anomaly_id, signal, severity, detected_at, status,
                       detect_model_version, confidence, downtime_minutes, auto_resolved
                FROM {CATALOG}.{SCHEMA}.anomalies
                WHERE robot_id = '{safe_id}'
                ORDER BY detected_at DESC LIMIT 20"""
        ),
        asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM work_orders WHERE robot_id = %s ORDER BY created_at DESC LIMIT 20",
            (robot_id,),
        ),
        return_exceptions=True,
    )

    def _safe_r(r, default=None):
        return r if not isinstance(r, Exception) else (default if default is not None else [])

    robot_rows = _safe_r(q_robot, [])
    if not robot_rows:
        raise HTTPException(404, f"Robot {robot_id} not found")
    return {
        "robot": robot_rows[0],
        "telemetry": _safe_r(q_telemetry, []),
        "anomalies": _safe_r(q_anomalies, []),
        "work_orders": _safe_r(q_work_orders, []),
    }


# ─── Incidents page (anomalies + persona switcher) ────────────────────────

@app.get("/api/incidents")
async def get_incidents(
    severity: Optional[str] = Query(None),
    family: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    fc_site: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=300),
):
    """Anomalies list."""
    where = []
    if severity:
        where.append(f"severity = '{_safe(severity)}'")
    if family:
        where.append(f"family = '{_safe(family)}'")
    if status:
        where.append(f"status = '{_safe(status)}'")
    if fc_site:
        where.append(f"fc_site = '{_safe(fc_site)}'")
    if not status:
        where.append("status IN ('open','investigating')")
    clause = "WHERE " + " AND ".join(where) if where else ""
    rows = await asyncio.to_thread(
        run_query,
        f"""SELECT anomaly_id, robot_id, family, fc_site, signal, severity,
                   detected_at, status, detect_model_version, confidence,
                   downtime_minutes, auto_resolved
            FROM {CATALOG}.{SCHEMA}.anomalies {clause}
            ORDER BY
              CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                            WHEN 'medium'   THEN 2 ELSE 3 END,
              detected_at DESC
            LIMIT {limit}"""
    )
    return rows or []


@app.get("/api/incidents/{anomaly_id}")
async def get_incident(anomaly_id: str):
    """Single incident with related telemetry + manual citation."""
    safe_id = _safe(anomaly_id)
    q_inc, q_recent_signal = await asyncio.gather(
        asyncio.to_thread(
            run_query,
            f"""SELECT * FROM {CATALOG}.{SCHEMA}.anomalies
                WHERE anomaly_id = '{safe_id}' LIMIT 1"""
        ),
        asyncio.to_thread(
            run_query,
            f"""SELECT signal, reading_date, reading_value, health_score
                FROM {CATALOG}.{SCHEMA}.telemetry
                WHERE robot_id IN (
                    SELECT robot_id FROM {CATALOG}.{SCHEMA}.anomalies
                    WHERE anomaly_id = '{safe_id}'
                )
                AND signal IN (
                    SELECT signal FROM {CATALOG}.{SCHEMA}.anomalies
                    WHERE anomaly_id = '{safe_id}'
                )
                ORDER BY reading_date DESC LIMIT 14"""
        ),
        return_exceptions=True,
    )
    inc = (q_inc if not isinstance(q_inc, Exception) else [])
    if not inc:
        raise HTTPException(404, f"Incident {anomaly_id} not found")
    incident = inc[0]
    # Pull the matching manual chunk for "governed RAG over manuals" surfacing
    chunk = await asyncio.to_thread(
        run_query,
        f"""SELECT chunk_id, manual_section, page, chunk_text, source_uri, source_doc
            FROM {CATALOG}.{SCHEMA}.service_manual_chunks
            WHERE family = '{_safe(incident.get('family',''))}'
              AND signal_tag = '{_safe(incident.get('signal',''))}'
            LIMIT 1""",
    )
    return {
        "incident": incident,
        "telemetry_window": q_recent_signal if not isinstance(q_recent_signal, Exception) else [],
        "manual_citation": (chunk or [None])[0] if chunk else None,
    }


@app.get("/api/incidents/{anomaly_id}/persona")
async def get_incident_persona_view(anomaly_id: str):
    """THE WOW MOMENT — returns BOTH the RME Tech and RME Lead view in one payload."""
    detail = await get_incident(anomaly_id)
    incident = detail["incident"]
    citation = detail.get("manual_citation") or {}
    family = incident.get("family", "")
    signal = incident.get("signal", "")
    severity = incident.get("severity", "medium")

    # Try to pull a matching agent_memory row for the lead view (recurring-case learning)
    try:
        memory_rows = await asyncio.to_thread(
            run_pg_query,
            """SELECT * FROM agent_memory
               WHERE family = %s AND signal = %s
               ORDER BY created_at DESC LIMIT 1""",
            (family, signal),
        )
    except Exception:
        memory_rows = []
    prior_memory = memory_rows[0] if memory_rows else None

    tech_view = {
        "header": f"Step-by-step — {family} / {signal.replace('_', ' ')}",
        "steps": [
            {"n": 1, "text": citation.get("chunk_text", "Consult service manual for remediation procedure.")},
            {"n": 2, "text": "Replace consumable / lubricate per spec listed above."},
            {"n": 3, "text": "Recalibrate via RME console and confirm telemetry returns to baseline."},
        ],
        "parts": ["AR-SPR-VAC-005 (gripper seal)", "AR-SPR-FLT-002 (inline filter)"]
            if "vacuum" in signal else ["See manual for P/N"],
        "manual": {
            "section": citation.get("manual_section", ""),
            "page": citation.get("page"),
            "source_uri": citation.get("source_uri", ""),
            "source_doc": citation.get("source_doc", ""),
        },
        "estimated_time_minutes": 25,
        "torque_spec_nm": 12.0 if "joint" in signal else None,
    }

    _prior_model_note = None
    if prior_memory:
        _prior_model_note = (
            "This pattern was previously diagnosed under "
            + str(prior_memory.get("model_version"))
            + " — institutional memory carried forward."
        )

    lead_view = {
        "header": f"Root cause + SLA exposure — {family} fleet, {signal.replace('_', ' ')}",
        "summary": (
            f"{severity.title()} {signal.replace('_', ' ')} anomaly on {family} at {incident.get('fc_site', '')}. "
            f"Confidence {incident.get('confidence', 0):.2f}. Detected by {incident.get('detect_model_version', '')}."
        ),
        "sla_risk": {
            "critical": "high" if severity == "critical" else "medium",
            "projected_minutes_downtime": int(incident.get("downtime_minutes") or 0) or 60,
            "redeploy_recommended": severity in ("high", "critical"),
        },
        "trend_signal": signal,
        "prior_memory": prior_memory,
        "redeploy": {
            "from_site": incident.get("fc_site", ""),
            "rationale": "Pull spare from adjacent FC if PM window exceeds 4h.",
        } if severity in ("high", "critical") else None,
        "model_version_note": _prior_model_note,
    }

    return {
        "incident": incident,
        "rme_tech": tech_view,
        "rme_lead": lead_view,
        "manual_citation": detail.get("manual_citation"),
    }


# ─── Work Orders page ─────────────────────────────────────────────────────

class WorkOrderUpdate(BaseModel):
    status: Optional[str] = None
    technician: Optional[str] = None


@app.get("/api/work-orders")
async def get_work_orders(
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    technician: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=300),
):
    clauses, params = [], []
    if status:
        clauses.append("status = %s"); params.append(_safe(status))
    if severity:
        clauses.append("severity = %s"); params.append(_safe(severity))
    if technician:
        clauses.append("technician = %s"); params.append(_safe(technician))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    try:
        rows = await asyncio.to_thread(
            run_pg_query,
            f"SELECT * FROM work_orders{where} ORDER BY created_at DESC LIMIT %s",
            tuple(params),
        )
        return rows or []
    except Exception as e:
        log.warning("Work orders query failed: %s", e)
        return []


@app.patch("/api/work-orders/{work_order_id}")
async def update_work_order(work_order_id: int, body: WorkOrderUpdate):
    sets, params = [], []
    if body.status:
        if body.status not in ("open", "in_progress", "awaiting_parts", "completed", "cancelled"):
            raise HTTPException(400, "Invalid status")
        sets.append("status = %s"); params.append(body.status)
        if body.status == "completed":
            sets.append("resolved_at = NOW()")
    if body.technician is not None:
        sets.append("technician = %s"); params.append(body.technician[:100])
    if not sets:
        raise HTTPException(400, "No updates provided")
    params.append(work_order_id)
    result = await asyncio.to_thread(
        write_pg,
        f"UPDATE work_orders SET {', '.join(sets)} WHERE work_order_id = %s RETURNING *",
        tuple(params),
    )
    if not result:
        raise HTTPException(404, f"Work order {work_order_id} not found")
    return result


# ─── Memory page (agent_memory — the differentiator) ──────────────────────

@app.get("/api/memory")
async def get_memory(
    family: Optional[str] = Query(None),
    model_version: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=300),
):
    clauses, params = [], []
    if family:
        clauses.append("family = %s"); params.append(_safe(family))
    if model_version:
        clauses.append("model_version = %s"); params.append(_safe(model_version))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    try:
        rows = await asyncio.to_thread(
            run_pg_query,
            f"SELECT * FROM agent_memory{where} ORDER BY created_at DESC LIMIT %s",
            tuple(params),
        )
        return rows or []
    except Exception as e:
        log.warning("Memory query failed: %s", e)
        return []


@app.get("/api/memory/versions")
async def get_memory_versions():
    """Group memory rows by model_version — shows the v4 → v5 carry-forward story."""
    try:
        rows = await asyncio.to_thread(
            run_pg_query,
            """SELECT model_version, family, COUNT(*) as cnt,
                      MIN(created_at) as first_seen, MAX(created_at) as last_seen
                 FROM agent_memory
                 GROUP BY model_version, family
                 ORDER BY model_version, family""",
        )
        return rows or []
    except Exception as e:
        log.warning("Memory versions query failed: %s", e)
        return []


# ─── Service Manuals page ─────────────────────────────────────────────────

@app.get("/api/manuals")
async def get_manuals(
    family: Optional[str] = Query(None),
    section: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=300),
):
    where = []
    if family:
        where.append(f"family = '{_safe(family)}'")
    if section:
        where.append(f"manual_section = '{_safe(section)}'")
    if search:
        where.append(f"LOWER(chunk_text) LIKE '%{_safe(search).lower()}%'")
    clause = "WHERE " + " AND ".join(where) if where else ""
    rows = await asyncio.to_thread(
        run_query,
        f"""SELECT chunk_id, family, manual_section, page, signal_tag,
                   chunk_text, source_uri, source_doc
            FROM {CATALOG}.{SCHEMA}.service_manual_chunks {clause}
            ORDER BY family, page
            LIMIT {limit}""",
    )
    return rows or []


@app.get("/api/manuals/{chunk_id}")
async def get_manual_chunk(chunk_id: str):
    safe_id = _safe(chunk_id)
    rows = await asyncio.to_thread(
        run_query,
        f"""SELECT * FROM {CATALOG}.{SCHEMA}.service_manual_chunks
            WHERE chunk_id = '{safe_id}' LIMIT 1""",
    )
    if not rows:
        raise HTTPException(404, f"Chunk {chunk_id} not found")
    return rows[0]


# ─── Session Check ───────────────────────────────────────────────────────

@app.get("/api/check-session")
async def check_session(request: Request):
    """Check if the user's OBO token is still valid."""
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        # Running locally or no OBO — treat as valid (no expiry to track)
        return {"valid": True, "expires_in_seconds": 9999}
    try:
        # Decode JWT payload (no verification — just reading the exp claim)
        import base64
        parts = token.split(".")
        if len(parts) >= 2:
            padding = 4 - len(parts[1]) % 4
            payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * padding))
            exp = payload.get("exp", 0)
            now = _time.time()
            remaining = max(0, int(exp - now))
            return {"valid": remaining > 0, "expires_in_seconds": remaining}
    except Exception as e:
        log.warning("Session check token decode failed: %s", e)
    return {"valid": True, "expires_in_seconds": 9999}


@app.get("/api/force-logout")
async def force_logout():
    """Redirect to trigger a fresh OAuth login."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/", status_code=302)


# ─── Frontend Serving ─────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "..", "frontend", "src")), name="static")


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "..", "frontend", "src", "index.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
    )
