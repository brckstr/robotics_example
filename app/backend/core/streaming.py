"""
MAS (Multi-Agent Supervisor) SSE streaming proxy with action card detection
and MCP tool approval (auto or manual).

SSE event protocol (frontend must handle all of these):
  - thinking:         Reasoning text from intermediate rounds (render as collapsible block)
  - delta:            Text chunk from the final answer (render in answer area)
  - tool_call:        Sub-agent invocation started
  - agent_switch:     MAS switched to a different sub-agent
  - sub_result:       Data returned from a sub-agent
  - action_card:      Entity created/referenced (e.g. PO, exception) — render as card
  - suggested_actions: Follow-up prompts based on tools used
  - mcp_approval:     MCP tools need approval (manual mode) — render approval card
  - error:            Error message
  - session_expired:  OBO token expired; frontend should auto-reload (Gotcha #29)
  - [DONE]:           Stream complete

Phase tracking:
  Text deltas from MAS are buffered per round. Non-final rounds (with pending
  MCP approvals) emit the buffered text as 'thinking'. The final round emits
  text as 'delta'. This gives the frontend a clean separation between
  intermediate reasoning and the actual answer.

MCP Approval:
  When MAS calls an External MCP Server tool, it emits an `mcp_approval_request`
  event and pauses. Two modes:
    - auto_approve=True:  Approves silently and continues in same stream.
    - auto_approve=False: Emits `mcp_approval` event, saves state, and ends stream.
      Frontend sends approval back via next POST and stream resumes.

Keepalive:
  _sse_keepalive() wraps any async generator with periodic SSE comment
  lines (`: keepalive\\n\\n`) every 15 seconds. This prevents the Databricks
  Apps reverse proxy from dropping the connection during long MAS round-trips.

Usage:
    from backend.core.streaming import stream_mas_chat, _sse_keepalive
    return StreamingResponse(_sse_keepalive(stream_mas_chat(message, history, action_card_tables)), media_type="text/event-stream")
"""

import asyncio
import json
import logging
import os

import httpx
from databricks.sdk import WorkspaceClient

from backend.core.lakebase import run_pg_query

log = logging.getLogger("streaming")

w = WorkspaceClient()

MAS_TILE_ID = os.getenv("MAS_TILE_ID", "")


async def _sse_keepalive(gen, interval: float = 10.0):
    """Wrap an async generator with periodic SSE keepalive comments.

    Sends `: keepalive\\n\\n` every `interval` seconds when the inner
    generator hasn't yielded anything.  This prevents the Databricks Apps
    reverse proxy from dropping the connection during long MAS round-trips.

    IMPORTANT: On client disconnect (GeneratorExit), the producer task is
    NOT cancelled.  This lets the inner generator (event_stream) continue
    processing the MAS response and save the final answer to chat history.
    The frontend recovery pattern then polls /api/chat/history to retrieve it.
    """
    queue: asyncio.Queue = asyncio.Queue()
    keepalive_count = 0
    disconnected = False

    async def _producer():
        try:
            async for item in gen:
                await queue.put(item)
        except asyncio.CancelledError:
            log.info("SSE producer cancelled")
        except Exception as exc:
            await queue.put(exc)
        finally:
            await queue.put(None)

    task = asyncio.create_task(_producer())
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=interval)
            except asyncio.TimeoutError:
                keepalive_count += 1
                log.info("SSE keepalive #%d sent", keepalive_count)
                yield ": keepalive\n\n"
                continue
            if item is None:
                break
            if isinstance(item, Exception):
                raise item
            yield item
    except GeneratorExit:
        disconnected = True
        log.warning("SSE client disconnected after %d keepalives — backend continues in background", keepalive_count)
    finally:
        if not disconnected and not task.done():
            task.cancel()


def _get_mas_auth() -> tuple[str, str]:
    """Get workspace host and SP auth header (fallback when no user token)."""
    host = w.config.host.rstrip("/")
    auth_headers = w.config.authenticate()
    return host, auth_headers.get("Authorization", "")


# ── Action card table config ─────────────────────────────────────────────
# Override from domain routes. Each entry:
#   {"table": "...", "card_type": "...", "id_col": "...",
#    "title_template": "...", "actions": [...], "detail_cols": {...}}
ACTION_CARD_TABLES: list[dict] = []


async def _detect_chat_actions(final_text: str, lakebase_called: bool, tools_called: set) -> list[dict]:
    """Detect actionable entities created/referenced during chat."""
    cards = []

    if lakebase_called and ACTION_CARD_TABLES:
        for tbl_config in ACTION_CARD_TABLES:
            try:
                recent = await asyncio.to_thread(
                    run_pg_query,
                    f"SELECT * FROM {tbl_config['table']} WHERE created_at >= NOW() - INTERVAL '3 minutes' ORDER BY created_at DESC LIMIT 3",
                )
                for row in recent:
                    details = {}
                    for display_key, db_col in tbl_config.get("detail_cols", {}).items():
                        val = row.get(db_col, "")
                        details[display_key] = str(val) if val is not None else ""

                    title = tbl_config.get("title_template", tbl_config["card_type"])
                    try:
                        title = title.format(**row)
                    except (KeyError, IndexError):
                        pass

                    cards.append({
                        "type": "action_card",
                        "card_type": tbl_config["card_type"],
                        "entity_id": row.get(tbl_config["id_col"]),
                        "title": title,
                        "details": details,
                        "actions": tbl_config.get("actions", ["approve", "dismiss"]),
                    })
            except Exception as e:
                log.warning("Action card query error for %s: %s", tbl_config["table"], e)

    # Suggested follow-up actions based on tools used
    followups = []
    tool_names_lower = {t.lower() for t in tools_called}
    if any("weather" in t for t in tool_names_lower):
        followups.append({"label": "Check affected items", "prompt": "Which items are affected by the conditions you just found?"})
    if any("reorder" in t or "calculator" in t for t in tool_names_lower):
        followups.append({"label": "Create order", "prompt": "Create an order based on the calculation you just did"})
    if any("forecast" in t or "demand" in t for t in tool_names_lower):
        followups.append({"label": "Plan transfers", "prompt": "Based on the forecast, what transfers should we plan?"})
    if followups:
        cards.append({"type": "suggested_actions", "actions": followups[:3]})

    return cards


# ── MCP Approval State (for manual approval mode) ────────────────────────
# When MAS requests MCP tool approval and auto_approve is false, we pause
# the stream, send the approval request to the frontend, and save state here.
# When the user approves, the next /api/chat call continues with full context.
_mcp_pending: dict | None = None


def get_mcp_pending() -> dict | None:
    """Return current pending MCP approval state (if any)."""
    return _mcp_pending


def clear_mcp_pending():
    """Clear the pending MCP approval state."""
    global _mcp_pending
    _mcp_pending = None


async def stream_mas_chat(
    message: str | None,
    chat_history: list[dict],
    action_card_tables: list[dict] | None = None,
    user_token: str = "",
    auto_approve_mcp: bool = True,
    start_messages: list[dict] | None = None,
    initial_accumulated: list[dict] | None = None,
    initial_tools_called: set | None = None,
    initial_lakebase_called: bool = False,
    initial_approval_round: int = 0,
):
    """
    Async generator that yields SSE events from a MAS streaming invocation.

    Args:
        message: User message (None when continuing from MCP approval).
        chat_history: List of {"role": "user"|"assistant", "content": "..."} dicts.
        action_card_tables: Optional override for ACTION_CARD_TABLES config.
        user_token: Optional OBO (on-behalf-of-user) token from x-forwarded-access-token.
                    If provided, used for MAS calls (required for MCP tools).
                    If expired (401/403), streams session_expired for frontend auto-refresh.
        auto_approve_mcp: If True, auto-approve MCP tool calls. If False, pause and ask frontend.
        start_messages: Override starting messages (used when resuming from MCP approval).
        initial_accumulated: Previous round output items (used when resuming from MCP approval).
        initial_tools_called: Tools called so far (used when resuming from MCP approval).
        initial_lakebase_called: Whether Lakebase was called (used when resuming from MCP approval).
        initial_approval_round: Starting approval round (used when resuming from MCP approval).
    """
    global _mcp_pending

    if action_card_tables is not None:
        global ACTION_CARD_TABLES
        ACTION_CARD_TABLES = action_card_tables

    endpoint = f"mas-{MAS_TILE_ID}-endpoint" if MAS_TILE_ID else ""
    if not endpoint:
        yield f"data: {json.dumps({'type': 'error', 'text': 'MAS endpoint not configured. Set MAS_TILE_ID in app.yaml.'})}\n\n"
        yield "data: [DONE]\n\n"
        return

    final_text = ""
    lakebase_called = initial_lakebase_called
    tools_called = initial_tools_called or set()
    all_accumulated = initial_accumulated or []
    MAX_APPROVAL_ROUNDS = 10  # safety limit to prevent infinite loops
    approval_round = initial_approval_round

    try:
        host = w.config.host.rstrip("/")
        # Use user token if available, fallback to SP token
        if user_token:
            auth = f"Bearer {user_token}"
            log.info("MAS auth: using USER TOKEN (len=%d)", len(user_token))
        else:
            _, auth = await asyncio.to_thread(_get_mas_auth)
            log.info("MAS auth: using SP TOKEN")
        url = f"{host}/serving-endpoints/{endpoint}/invocations"
        input_messages = start_messages if start_messages else list(chat_history[-10:])

        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            while approval_round <= MAX_APPROVAL_ROUNDS:
                payload = {"input": input_messages, "stream": True, "max_turns": 15}
                round_output_items = []
                pending_approvals = []
                round_text_chunks = []  # Buffer text per round

                async with client.stream(
                    "POST", url,
                    json=payload,
                    headers={"Authorization": auth, "Content-Type": "application/json"},
                ) as resp:
                    # Detect expired OBO token — fallback to SP token before giving up
                    if resp.status_code in (401, 403) and user_token:
                        log.warning("MAS %d with user token — falling back to SP token", resp.status_code)
                        _, auth = await asyncio.to_thread(_get_mas_auth)
                        user_token = ""  # prevent infinite retry
                        continue  # retry the while loop with SP auth
                    elif resp.status_code in (401, 403):
                        # SP token also failed — signal frontend to refresh
                        log.error("MAS %d with SP token — session truly expired", resp.status_code)
                        yield f"data: {json.dumps({'type': 'session_expired'})}\n\n"
                        yield "data: [DONE]\n\n"
                        return
                    resp.raise_for_status()
                    async for line in resp.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        raw = line[6:]
                        if raw == "[DONE]":
                            break
                        try:
                            evt = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        etype = evt.get("type", "")
                        step = evt.get("step", 0)

                        # Text delta — buffer per round (classified at round end)
                        if etype == "response.output_text.delta":
                            delta = evt.get("delta", "")
                            if delta:
                                final_text += delta
                                round_text_chunks.append(delta)

                        # Completed output item
                        elif etype == "response.output_item.done":
                            item = evt.get("item", {})
                            item_type = item.get("type", "")
                            round_output_items.append(item)
                            log.info("STREAM [round=%d step=%d] item_type=%s name=%s", approval_round, step, item_type, item.get("name", ""))

                            if item_type == "function_call":
                                agent_name = item.get("name", "")
                                tools_called.add(agent_name)
                                if "lakebase" in agent_name.lower():
                                    lakebase_called = True
                                call_args = item.get("arguments", "")
                                log.info("TOOL CALL [round=%d]: name=%s args=%s", approval_round, agent_name, str(call_args)[:500])
                                yield f"data: {json.dumps({'type': 'tool_call', 'agent': agent_name, 'step': step})}\n\n"

                            elif item_type == "mcp_approval_request":
                                tool_name = item.get("name", "unknown")
                                server_label = item.get("server_label", "")
                                log.info("MCP APPROVAL REQUEST: tool=%s server=%s id=%s", tool_name, server_label, item.get("id"))
                                pending_approvals.append(item)
                                tools_called.add(f"mcp:{server_label}:{tool_name}")
                                if "lakebase" in server_label.lower():
                                    lakebase_called = True
                                yield f"data: {json.dumps({'type': 'tool_call', 'agent': f'{server_label} -> {tool_name}', 'step': step})}\n\n"

                            elif item_type == "function_call_output":
                                output_text = item.get("output", "")
                                tool_name = item.get("name", "")
                                log.info("TOOL OUTPUT [round=%d]: name=%s output=%s", approval_round, tool_name, output_text[:300] if output_text else "EMPTY")
                                if output_text and len(output_text) > 5:
                                    yield f"data: {json.dumps({'type': 'sub_result', 'text': output_text[:2000], 'step': step})}\n\n"

                            elif item_type == "message":
                                content = item.get("content", [])
                                role = item.get("role", "")
                                log.info("MESSAGE [round=%d step=%d] role=%s blocks=%s", approval_round, step, role, [b.get("type") for b in content])
                                for block in content:
                                    text_val = block.get("text", "")
                                    if text_val.startswith("<name>") and text_val.endswith("</name>"):
                                        agent_name = text_val[6:-7]
                                        yield f"data: {json.dumps({'type': 'agent_switch', 'agent': agent_name, 'step': step})}\n\n"
                                    elif text_val and len(text_val) > 5 and not text_val.startswith("<"):
                                        yield f"data: {json.dumps({'type': 'sub_result', 'text': text_val, 'step': step})}\n\n"
                                # Capture final text from message items (accept both block types, relaxed role check)
                                if step > 1 and role in ("assistant", ""):
                                    for block in content:
                                        block_type = block.get("type", "text")
                                        if block_type in ("output_text", "text") and block.get("text"):
                                            final_text = block["text"]
                                            log.info("FINAL TEXT captured from message (step=%d, role=%s, len=%d)", step, role, len(final_text))

                # ── Flush buffered text with correct type ──
                log.info("FLUSH [round=%d] pending_approvals=%d round_chunks=%d final_text_len=%d", approval_round, len(pending_approvals), len(round_text_chunks), len(final_text))
                if not pending_approvals:
                    # Final round — emit text as delta (answer)
                    if round_text_chunks:
                        for chunk in round_text_chunks:
                            yield f"data: {json.dumps({'type': 'delta', 'text': chunk})}\n\n"
                    elif final_text:
                        # Fallback: MAS sent text via message items, not streaming deltas
                        yield f"data: {json.dumps({'type': 'delta', 'text': final_text})}\n\n"
                    break

                # Non-final round — emit text as thinking (reasoning)
                round_thinking = "".join(round_text_chunks)
                if round_thinking:
                    yield f"data: {json.dumps({'type': 'thinking', 'text': round_thinking})}\n\n"

                # Keepalive to prevent proxy timeout during approval round-trip
                yield ": keepalive\n\n"

                # ── Auto-approve mode: approve silently and continue in same stream ──
                if auto_approve_mcp:
                    approval_round += 1
                    log.info("AUTO-APPROVING %d MCP tool(s) (round %d)", len(pending_approvals), approval_round)
                    input_messages = list(chat_history[-10:])
                    for item in all_accumulated:
                        input_messages.append(item)
                    for item in round_output_items:
                        input_messages.append(item)
                    for req in pending_approvals:
                        input_messages.append({
                            "type": "mcp_approval_response",
                            "id": f"approval-{approval_round}-{req.get('id', '')}",
                            "approval_request_id": req.get("id", ""),
                            "approve": True,
                        })
                    all_accumulated.extend(round_output_items)
                    continue  # Loop to next MAS invocation

                # ── Manual approval mode: pause and ask frontend ──
                approval_round += 1
                all_accumulated.extend(round_output_items)

                # Save state for when approval comes back
                _mcp_pending = {
                    "accumulated": all_accumulated,
                    "pending": pending_approvals,
                    "tools_called": tools_called,
                    "lakebase_called": lakebase_called,
                    "round": approval_round,
                }

                # Send approval request to frontend
                approval_tools = []
                for p in pending_approvals:
                    args_raw = p.get("arguments", "{}")
                    try:
                        args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
                    except (json.JSONDecodeError, TypeError):
                        args = {}
                    approval_tools.append({
                        "name": p.get("name", "unknown"),
                        "server": p.get("server_label", ""),
                        "arguments": args,
                    })
                yield f"data: {json.dumps({'type': 'mcp_approval', 'tools': approval_tools})}\n\n"
                log.info("MCP APPROVAL PAUSE: round=%d tools=%s", approval_round, [t["name"] for t in approval_tools])

                # End this stream — frontend will send approval and start a new stream
                yield "data: [DONE]\n\n"
                return  # Exit without action cards — those come after final answer

            else:
                # while condition failed (MAX_APPROVAL_ROUNDS) — emit whatever text we have
                log.warning("MAS hit MAX_ROUNDS=%d, emitting final_text as delta", MAX_APPROVAL_ROUNDS)
                if final_text:
                    yield f"data: {json.dumps({'type': 'delta', 'text': final_text})}\n\n"

    except Exception as e:
        log.error("MAS stream error: %s", e)
        yield f"data: {json.dumps({'type': 'error', 'text': str(e)})}\n\n"

    # Emit action cards
    try:
        action_cards = await _detect_chat_actions(final_text, lakebase_called, tools_called)
        for card in action_cards:
            yield f"data: {json.dumps(card)}\n\n"
    except Exception as e:
        log.warning("Action card detection error: %s", e)

    yield "data: [DONE]\n\n"
