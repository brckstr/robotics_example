"""
Health check + OBO session validation endpoints.

Usage:
    from backend.core.health import health_router
    app.include_router(health_router)
"""

import asyncio
import base64
import json
import time

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from backend.core.lakehouse import run_query, w
from backend.core.lakebase import run_pg_query

health_router = APIRouter()


@health_router.get("/api/health")
async def health_check():
    """Returns {status: "healthy"|"degraded", checks: {sdk, sql_warehouse, lakebase}}."""
    checks = {}
    try:
        w.current_user.me()
        checks["sdk"] = "ok"
    except Exception as e:
        checks["sdk"] = str(e)
    try:
        await asyncio.to_thread(run_query, "SELECT 1")
        checks["sql_warehouse"] = "ok"
    except Exception as e:
        checks["sql_warehouse"] = str(e)
    try:
        await asyncio.to_thread(run_pg_query, "SELECT 1")
        checks["lakebase"] = "ok"
    except Exception as e:
        checks["lakebase"] = str(e)
    ok = all(v == "ok" for v in checks.values())
    return {"status": "healthy" if ok else "degraded", "checks": checks}


@health_router.get("/api/check-session")
async def check_session(request: Request):
    """Check if the user's OBO token is still valid by decoding the JWT exp claim.

    Returns {valid: true, expires_in_seconds: N} or {valid: false, reason: "expired"}.
    The frontend polls this periodically to proactively detect token expiry
    and prompt re-login before the user hits a stale-token error.
    """
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        return {"valid": False, "reason": "no_token"}
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {"valid": False, "reason": "invalid_format"}
        payload = parts[1]
        payload += "=" * (4 - len(payload) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(payload))
        exp = decoded.get("exp", 0)
        now = time.time()
        if now >= exp:
            return {"valid": False, "reason": "expired", "expired_ago_seconds": int(now - exp)}
        return {"valid": True, "expires_in_seconds": int(exp - now)}
    except Exception:
        return {"valid": False, "reason": "decode_error"}


@health_router.get("/api/force-logout")
async def force_logout(request: Request):
    """Clear ALL cookies for this origin (including HttpOnly proxy session cookies
    that the backend never sees) using the Clear-Site-Data header, then redirect
    to /.  The Databricks Apps proxy will find no valid session and kick off a
    fresh OAuth flow, giving the user a new OBO token.

    Clear-Site-Data is supported by Chrome, Edge, and Firefox — covers all
    typical demo/enterprise browsers.
    """
    response = RedirectResponse(url="/", status_code=302)
    response.headers["Clear-Site-Data"] = '"cookies"'
    for cookie_name in request.cookies:
        response.delete_cookie(key=cookie_name, path="/")
    return response
