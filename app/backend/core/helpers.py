"""
Shared helper functions: input sanitization and MAS response parsing.

Usage:
    from backend.core import _safe, _extract_agent_response
"""

import json
import re

from fastapi import HTTPException

_SAFE_RE = re.compile(r"^[\w\s\-.,#&'()/%]+$")


def _safe(val: str) -> str:
    """Whitelist regex for filter values injected into SQL. Raises 400 on bad input."""
    if not _SAFE_RE.match(val):
        raise HTTPException(400, "Invalid filter value")
    return val


def _extract_agent_response(data: dict) -> str:
    """Parse MAS Agent Bricks response — output[].content[].text format."""
    # Agent Bricks v1 format: output is a list of messages
    if isinstance(data.get("output"), list):
        for msg in data["output"]:
            if msg.get("role") == "assistant":
                for block in msg.get("content", []):
                    if block.get("type") == "output_text" and block.get("text"):
                        return block["text"]
    # Fallback: output as plain string
    if isinstance(data.get("output"), str):
        return data["output"]
    # Legacy chat completions format
    if isinstance(data.get("choices"), list):
        for c in data["choices"]:
            msg = c.get("message", {})
            if msg.get("content"):
                return msg["content"]
    return json.dumps(data, indent=2)
