"""
Example: Supply Chain Domain Routes
====================================
Reference implementation showing all route patterns used in a production supply-chain demo.
NOT imported by the scaffold -- copy and adapt these patterns for your domain.

Patterns demonstrated:
1. Lakehouse read endpoints (Delta Lake via Statement Execution API)
2. Lakebase CRUD endpoints (PostgreSQL)
3. Paginated + filterable list endpoints
4. Detail endpoints with parallel queries
5. Pydantic models for request validation
6. asyncio.gather() for parallel data fetching
7. asyncio.to_thread() for all blocking I/O
8. _safe() input validation for dynamic SQL

IMPORTANT: This file is a reference only. It will NOT run standalone.
Copy individual patterns into your main.py and adjust table/column names.
"""

import asyncio
import json
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# -- Core imports from the scaffold --
# These are the only functions you need from backend.core.
from backend.core import run_query, run_pg_query, write_pg, _safe

app = FastAPI()


# =============================================================================
# PYDANTIC MODELS -- Request validation for write endpoints
# =============================================================================
# Use BaseModel subclasses for POST and PATCH bodies. FastAPI auto-validates
# and returns 422 with details on bad input. Keep fields Optional for PATCH
# bodies so callers can send partial updates.

class ShipmentTrackingCreate(BaseModel):
    """POST body for adding a tracking event to a shipment."""
    shipment_id: str
    status: str
    location_description: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    temperature_f: Optional[float] = None
    notes: Optional[str] = None
    updated_by: str = "system"


class ExceptionCreate(BaseModel):
    """POST body for creating a shipment exception."""
    shipment_id: str
    exception_type: str              # delay, temperature_excursion, damage, customs_hold
    severity: str = "medium"         # low, medium, high, critical
    description: str
    assigned_to: Optional[str] = None


class ExceptionUpdate(BaseModel):
    """PATCH body -- all fields Optional so callers send only what changed."""
    status: Optional[str] = None     # open, acknowledged, resolved, cancelled
    resolution: Optional[str] = None
    assigned_to: Optional[str] = None


class PurchaseOrderCreate(BaseModel):
    """POST body for creating a purchase order between facilities."""
    po_number: str
    supplier_facility_id: str
    destination_facility_id: str
    product_id: str
    quantity: float
    unit_cost_usd: Optional[float] = None
    requested_date: Optional[str] = None
    expected_date: Optional[str] = None
    created_by: str = "system"


class PurchaseOrderUpdate(BaseModel):
    """PATCH body for purchase order status changes."""
    status: Optional[str] = None     # draft, submitted, approved, shipped, received, cancelled
    expected_date: Optional[str] = None


class WorkflowUpdate(BaseModel):
    """PATCH body for approving/dismissing an agent workflow."""
    status: Optional[str] = None     # approved, dismissed


class NoteCreate(BaseModel):
    """POST body for adding a note to any entity (shipment, PO, exception, etc.)."""
    entity_type: str                 # shipment, purchase_order, exception, facility
    entity_id: str
    note_text: str
    author: str = "system"


class AgentActionUpdate(BaseModel):
    """PATCH body for approving/dismissing an agent-proposed action."""
    status: Optional[str] = None     # approved, dismissed, expired


# =============================================================================
# PATTERN 1: KPI METRICS -- asyncio.gather() for parallel aggregation queries
# =============================================================================
# Use this when a dashboard needs multiple independent KPI values from different
# tables. Each query runs in a separate thread via asyncio.to_thread(), and
# asyncio.gather() waits for all of them concurrently.
#
# Key points:
#   - Each run_query() call is blocking (it calls the SDK synchronously)
#   - asyncio.to_thread() pushes it to a threadpool so the event loop stays free
#   - asyncio.gather() runs N queries in parallel instead of sequentially
#   - Always guard against empty results: `q[0]["col"] if q else 0`

@app.get("/api/supply-chain/metrics")
async def get_metrics():
    """Return KPI metrics for the command center dashboard.

    Runs 5 independent Lakehouse queries in parallel and assembles a flat dict.
    Each query targets a different Delta Lake table with aggregation.
    """
    q_ship, q_otd, q_fill, q_exc, q_inv = await asyncio.gather(
        # -- Shipment volume + cost (last 30 days) --
        asyncio.to_thread(run_query, """
            SELECT COUNT(*) as total_shipments,
                   SUM(cost_usd) as total_cost,
                   ROUND(AVG(distance_miles), 0) as avg_distance
            FROM shipments
            WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30)
        """),
        # -- On-time delivery rate (last 90 days, delivered only) --
        asyncio.to_thread(run_query, """
            SELECT ROUND(
                SUM(CASE WHEN actual_delivery_date <= expected_delivery_date THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100, 1
            ) as on_time_pct
            FROM shipments
            WHERE status = 'delivered'
              AND ship_date >= DATE_SUB(CURRENT_DATE(), 90)
        """),
        # -- Order fill rate (last 90 days) --
        asyncio.to_thread(run_query, """
            SELECT ROUND(AVG(fulfillment_rate) * 100, 1) as avg_fill_rate
            FROM orders
            WHERE order_date >= DATE_SUB(CURRENT_DATE(), 90)
        """),
        # -- Active exceptions count (last 30 days) --
        asyncio.to_thread(run_query, """
            SELECT COUNT(*) as active_exceptions
            FROM shipments
            WHERE status IN ('delayed', 'cancelled')
              AND ship_date >= DATE_SUB(CURRENT_DATE(), 30)
        """),
        # -- Total inventory at latest snapshot --
        asyncio.to_thread(run_query, """
            SELECT ROUND(SUM(quantity_on_hand), 0) as total_inventory
            FROM inventory
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory)
        """),
    )

    # Assemble flat response -- guard every access against empty result sets
    return {
        "total_shipments_30d": q_ship[0]["total_shipments"] if q_ship else 0,
        "total_cost_30d": q_ship[0]["total_cost"] if q_ship else 0,
        "avg_distance": q_ship[0]["avg_distance"] if q_ship else 0,
        "on_time_delivery_pct": q_otd[0]["on_time_pct"] if q_otd else 0,
        "avg_fill_rate_pct": q_fill[0]["avg_fill_rate"] if q_fill else 0,
        "active_exceptions": q_exc[0]["active_exceptions"] if q_exc else 0,
        "total_inventory": q_inv[0]["total_inventory"] if q_inv else 0,
    }


# =============================================================================
# PATTERN 2: FILTERABLE LIST -- Optional params, _safe(), WHERE clause building
# =============================================================================
# Use this pattern for any list endpoint that supports frontend filter dropdowns.
# Every user-supplied string value MUST pass through _safe() before being
# interpolated into SQL. Build the WHERE clause dynamically from Optional params.
#
# Key points:
#   - Use Optional[str] = None for filter params (FastAPI omits them from the URL)
#   - _safe(val) raises HTTP 400 if the value contains injection characters
#   - Build a list of WHERE fragments, join with AND, prepend "WHERE " only if non-empty
#   - Pagination: page/per_page -> LIMIT/OFFSET; return total count alongside data
#   - Whitelist allowed sort columns to prevent injection in ORDER BY

@app.get("/api/supply-chain/shipments")
async def get_shipments(
    status: Optional[str] = None,
    division: Optional[str] = None,
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    carrier: Optional[str] = None,
    transport_mode: Optional[str] = None,
    cold_only: bool = False,
    sort: str = "ship_date",
    order: str = "DESC",
    page: int = 1,
    per_page: int = 25,
):
    """Paginated, filterable shipment list with JOINs.

    Returns {shipments: [...], total: N, page: N, per_page: N} for frontend
    to render a data table with pagination controls.
    """
    # -- Build WHERE clause from optional filters --
    where = []
    if status:
        where.append(f"s.status = '{_safe(status)}'")
    if division:
        where.append(f"p.division = '{_safe(division)}'")
    if origin:
        where.append(f"s.origin_facility_id = '{_safe(origin)}'")
    if destination:
        where.append(f"s.destination_facility_id = '{_safe(destination)}'")
    if carrier:
        where.append(f"s.carrier = '{_safe(carrier)}'")
    if transport_mode:
        where.append(f"s.transport_mode = '{_safe(transport_mode)}'")
    if cold_only:
        where.append("s.temp_min_f IS NOT NULL")
    clause = "WHERE " + " AND ".join(where) if where else ""

    # -- Whitelist sort columns to prevent injection --
    allowed_sorts = {"ship_date", "cost_usd", "distance_miles", "quantity", "status"}
    sort_col = f"s.{sort}" if sort in allowed_sorts else "s.ship_date"
    dir_ = "ASC" if order.upper() == "ASC" else "DESC"
    offset = (page - 1) * per_page

    # -- Parallel queries: data page + total count --
    q_data, q_count = await asyncio.gather(
        asyncio.to_thread(run_query, f"""
            SELECT s.*, fo.facility_name as origin_name, fd.facility_name as dest_name,
                   p.product_name, p.product_category
            FROM shipments s
            JOIN facilities fo ON s.origin_facility_id = fo.facility_id
            JOIN facilities fd ON s.destination_facility_id = fd.facility_id
            JOIN products p ON s.product_id = p.product_id
            {clause}
            ORDER BY {sort_col} {dir_}
            LIMIT {per_page} OFFSET {offset}
        """),
        asyncio.to_thread(run_query, f"""
            SELECT COUNT(*) as total
            FROM shipments s
            JOIN products p ON s.product_id = p.product_id
            {clause}
        """),
    )

    return {
        "shipments": q_data,
        "total": q_count[0]["total"] if q_count else 0,
        "page": page,
        "per_page": per_page,
    }


# =============================================================================
# PATTERN 3: DETAIL ENDPOINT -- Parallel Lakehouse + Lakebase via gather()
# =============================================================================
# Use this when a detail view needs data from BOTH Delta Lake (historical record)
# and Lakebase (live operational state). One gather() call fires both queries.
#
# Key points:
#   - Validate the path parameter with _safe() immediately
#   - Raise HTTP 404 if the primary record is not found
#   - Merge Lakebase results (tracking events, notes, etc.) into the response dict

@app.get("/api/supply-chain/shipments/{shipment_id}")
async def get_shipment_detail(shipment_id: str):
    """Single shipment with full detail from Lakehouse + live tracking from Lakebase.

    Combines the immutable Delta Lake record (origin, destination, product, cost)
    with live Lakebase tracking events in a single response.
    """
    sid = _safe(shipment_id)

    # -- Parallel: Delta Lake detail + Lakebase tracking events --
    q_ship, q_track = await asyncio.gather(
        asyncio.to_thread(run_query, f"""
            SELECT s.*,
                   fo.facility_name as origin_name,
                   fo.latitude as origin_lat, fo.longitude as origin_lon,
                   fd.facility_name as dest_name,
                   fd.latitude as dest_lat, fd.longitude as dest_lon,
                   p.product_name, p.product_category, p.requires_cold_chain
            FROM shipments s
            JOIN facilities fo ON s.origin_facility_id = fo.facility_id
            JOIN facilities fd ON s.destination_facility_id = fd.facility_id
            JOIN products p ON s.product_id = p.product_id
            WHERE s.shipment_id = '{sid}'
        """),
        # Lakebase query uses parameterized %s (safe by default)
        asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM live_shipment_tracking WHERE shipment_id = %s ORDER BY created_at ASC",
            (sid,),
        ),
    )

    if not q_ship:
        raise HTTPException(404, "Shipment not found")

    result = q_ship[0]
    result["tracking_events"] = q_track
    return result


# =============================================================================
# PATTERN 4: LAKEBASE CRUD -- Create, Read, Update with parameterized queries
# =============================================================================
# Lakebase (PostgreSQL) handles operational/transactional data that changes
# frequently: tracking events, exceptions, purchase orders, notes.
#
# Key points:
#   - POST: Use write_pg() with INSERT ... RETURNING * to get the created row back
#   - GET: Use run_pg_query() with %s params for safe filtering
#   - PATCH: Build SET clause dynamically from non-None fields
#   - ALWAYS use %s parameterized queries for Lakebase (not f-string interpolation)
#   - write_pg() auto-commits; returns the RETURNING row or {"affected": N}

# --- Tracking Events: GET + POST ---

@app.get("/api/tracking/{shipment_id}")
async def get_tracking(shipment_id: str):
    """Get all tracking events for a shipment, ordered chronologically."""
    return await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM live_shipment_tracking WHERE shipment_id = %s ORDER BY created_at ASC",
        (_safe(shipment_id),),
    )


@app.post("/api/tracking")
async def add_tracking(body: ShipmentTrackingCreate):
    """Add a new tracking event. Returns the created row via RETURNING *.

    The RETURNING * clause is key -- it gives the caller the server-generated
    fields (id, created_at) without a second query.
    """
    row = await asyncio.to_thread(
        write_pg,
        """INSERT INTO live_shipment_tracking
           (shipment_id, status, location_description, latitude, longitude,
            temperature_f, notes, updated_by)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (body.shipment_id, body.status, body.location_description,
         body.latitude, body.longitude, body.temperature_f,
         body.notes, body.updated_by),
    )
    return row


# --- Exceptions: GET with optional filter + POST + PATCH ---

@app.get("/api/exceptions")
async def get_exceptions(status: Optional[str] = None):
    """List exceptions, optionally filtered by status.

    Pattern: branch on whether the filter is provided. Both paths use
    parameterized queries -- _safe() is belt-and-suspenders on top of %s.
    """
    if status:
        return await asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM shipment_exceptions WHERE status = %s ORDER BY created_at DESC",
            (_safe(status),),
        )
    return await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM shipment_exceptions ORDER BY created_at DESC",
    )


@app.post("/api/exceptions")
async def create_exception(body: ExceptionCreate):
    """Create a new exception. Returns the created row."""
    return await asyncio.to_thread(
        write_pg,
        """INSERT INTO shipment_exceptions
           (shipment_id, exception_type, severity, description, assigned_to)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING *""",
        (body.shipment_id, body.exception_type, body.severity,
         body.description, body.assigned_to),
    )


@app.patch("/api/exceptions/{exception_id}")
async def update_exception(exception_id: int, body: ExceptionUpdate):
    """Update an exception -- dynamic SET clause from non-None fields.

    Pattern for PATCH endpoints:
    1. Collect (column = %s) fragments and values for each non-None field
    2. Add side-effect columns (e.g. resolved_at = NOW() when status = 'resolved')
    3. Raise 400 if nothing to update
    4. Append the WHERE id as the last param
    5. Use RETURNING * to get the updated row
    """
    sets, vals = [], []
    if body.status:
        sets.append("status = %s")
        vals.append(body.status)
        # Side effect: set resolved_at timestamp when resolving
        if body.status == "resolved":
            sets.append("resolved_at = NOW()")
    if body.resolution:
        sets.append("resolution = %s")
        vals.append(body.resolution)
    if body.assigned_to:
        sets.append("assigned_to = %s")
        vals.append(body.assigned_to)
    if not sets:
        raise HTTPException(400, "No fields to update")

    vals.append(exception_id)
    return await asyncio.to_thread(
        write_pg,
        f"UPDATE shipment_exceptions SET {', '.join(sets)} WHERE exception_id = %s RETURNING *",
        tuple(vals),
    )


# --- Purchase Orders: GET + POST + PATCH ---

@app.get("/api/purchase-orders")
async def get_purchase_orders(status: Optional[str] = None):
    """List purchase orders with optional status filter."""
    if status:
        return await asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM purchase_orders WHERE status = %s ORDER BY created_at DESC",
            (_safe(status),),
        )
    return await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM purchase_orders ORDER BY created_at DESC",
    )


@app.post("/api/purchase-orders")
async def create_purchase_order(body: PurchaseOrderCreate):
    """Create a draft purchase order between two facilities."""
    return await asyncio.to_thread(
        write_pg,
        """INSERT INTO purchase_orders
           (po_number, supplier_facility_id, destination_facility_id, product_id,
            quantity, unit_cost_usd, requested_date, expected_date, created_by)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (body.po_number, body.supplier_facility_id, body.destination_facility_id,
         body.product_id, body.quantity, body.unit_cost_usd,
         body.requested_date, body.expected_date, body.created_by),
    )


@app.patch("/api/purchase-orders/{po_id}")
async def update_purchase_order(po_id: int, body: PurchaseOrderUpdate):
    """Update PO status or expected date. Always bumps updated_at."""
    sets, vals = [], []
    if body.status:
        sets.append("status = %s")
        vals.append(body.status)
    if body.expected_date:
        sets.append("expected_date = %s")
        vals.append(body.expected_date)
    # Always update the timestamp on any change
    sets.append("updated_at = NOW()")
    if not vals:
        raise HTTPException(400, "No fields to update")

    vals.append(po_id)
    return await asyncio.to_thread(
        write_pg,
        f"UPDATE purchase_orders SET {', '.join(sets)} WHERE po_id = %s RETURNING *",
        tuple(vals),
    )


# =============================================================================
# PATTERN 5: FILTERABLE LIST (no pagination) -- Inventory with JOINs
# =============================================================================
# Simpler variant of Pattern 2: filters but no pagination. Good for bounded
# datasets (inventory snapshots, fleet vehicles, facilities) where the total
# row count is manageable for the frontend.

@app.get("/api/supply-chain/inventory")
async def get_inventory(
    facility_id: Optional[str] = None,
    product_category: Optional[str] = None,
    below_reorder_only: bool = False,
):
    """Current inventory with optional filters. Always scoped to latest snapshot."""
    # Start with a mandatory base condition (latest snapshot)
    where = ["i.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory)"]
    if facility_id:
        where.append(f"i.facility_id = '{_safe(facility_id)}'")
    if product_category:
        where.append(f"p.product_category = '{_safe(product_category)}'")
    if below_reorder_only:
        where.append("i.below_reorder = true")
    clause = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT i.*, f.facility_name, f.facility_type,
               p.product_name, p.product_category
        FROM inventory i
        JOIN facilities f ON i.facility_id = f.facility_id
        JOIN products p ON i.product_id = p.product_id
        {clause}
        ORDER BY i.days_of_supply ASC
    """
    return await asyncio.to_thread(run_query, sql)


@app.get("/api/supply-chain/inventory/alerts")
async def get_inventory_alerts():
    """Top 50 items below reorder point, sorted by urgency (days of supply)."""
    sql = """
        SELECT i.facility_id, f.facility_name, i.product_id, p.product_name,
               p.product_category, i.quantity_available, i.reorder_point,
               i.days_of_supply, i.storage_utilization_pct
        FROM inventory i
        JOIN facilities f ON i.facility_id = f.facility_id
        JOIN products p ON i.product_id = p.product_id
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory)
          AND i.below_reorder = true
        ORDER BY i.days_of_supply ASC
        LIMIT 50
    """
    return await asyncio.to_thread(run_query, sql)


# =============================================================================
# PATTERN 6: WORKFLOW ENDPOINTS -- List, Detail, Approve/Dismiss with side effects
# =============================================================================
# Workflows represent agent-initiated actions that need human approval.
# The PATCH endpoint has side effects: approving a workflow also advances
# its linked purchase order or exception to the next status.
#
# Key points:
#   - GET list returns enriched workflow objects (headline, reasoning chain)
#   - GET detail fetches a single workflow and enriches it
#   - PATCH approve/dismiss updates the workflow AND cascades to linked entities
#   - Side effects use separate write_pg() calls after the primary update

@app.get("/api/workflows")
async def get_workflows(status: Optional[str] = None, limit: int = 20):
    """List recent workflows, optionally filtered by status."""
    if status:
        rows = await asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM workflows WHERE status = %s ORDER BY created_at DESC LIMIT %s",
            (_safe(status), limit),
        )
    else:
        rows = await asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM workflows ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
    # Enrich each workflow with business-readable headline
    return [_enrich_workflow(wf) for wf in rows]


@app.get("/api/workflows/{workflow_id}")
async def get_workflow(workflow_id: int):
    """Get a single workflow with full reasoning chain and enrichment."""
    rows = await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM workflows WHERE workflow_id = %s",
        (workflow_id,),
    )
    if not rows:
        raise HTTPException(404, "Workflow not found")
    return _enrich_workflow(rows[0])


@app.patch("/api/workflows/{workflow_id}")
async def update_workflow(workflow_id: int, body: WorkflowUpdate):
    """Approve or dismiss a workflow, with cascading side effects.

    Side effects on approve:
      - Linked purchase order -> status = 'submitted'
    Side effects on dismiss:
      - Linked purchase order (if still draft) -> status = 'cancelled'
      - Linked exception (if still open) -> status = 'cancelled'
    """
    if not body.status:
        raise HTTPException(400, "status is required")

    sets = ["status = %s"]
    vals = [body.status]
    # Set completed_at for terminal statuses
    if body.status in ("approved", "dismissed", "failed"):
        sets.append("completed_at = NOW()")
    vals.append(workflow_id)

    row = await asyncio.to_thread(
        write_pg,
        f"UPDATE workflows SET {', '.join(sets)} WHERE workflow_id = %s RETURNING *",
        tuple(vals),
    )

    # -- Side effect: advance linked purchase order on approve --
    if body.status == "approved" and row and row.get("result_po_id"):
        await asyncio.to_thread(
            write_pg,
            "UPDATE purchase_orders SET status = 'submitted', updated_at = NOW() WHERE po_id = %s",
            (row["result_po_id"],),
        )

    # -- Side effect: cancel linked entities on dismiss --
    if body.status == "dismissed" and row:
        if row.get("result_po_id"):
            await asyncio.to_thread(
                write_pg,
                "UPDATE purchase_orders SET status = 'cancelled', updated_at = NOW() "
                "WHERE po_id = %s AND status = 'draft'",
                (row["result_po_id"],),
            )
        if row.get("result_exception_id"):
            await asyncio.to_thread(
                write_pg,
                "UPDATE shipment_exceptions SET status = 'cancelled', resolved_at = NOW() "
                "WHERE exception_id = %s AND status = 'open'",
                (row["result_exception_id"],),
            )

    return row


def _enrich_workflow(wf: dict) -> dict:
    """Add a business-readable headline to a workflow dict.

    This helper runs in-process (no I/O) so it does NOT need asyncio.to_thread().
    Call it on each row after the Lakebase query returns.
    """
    wtype = wf.get("workflow_type", "")
    entity_id = wf.get("entity_id", "") or ""
    summary = wf.get("summary", "") or ""

    # Build a human-readable headline from the workflow type
    headlines = {
        "auto_reorder": f"Auto-Reorder: {entity_id}" if entity_id else "Auto-Reorder Triggered",
        "delay_response": f"Delay Response: Shipment {entity_id}" if entity_id else "Delay Response",
        "cold_chain_escalation": f"Cold Chain Alert: {entity_id}" if entity_id else "Cold Chain Alert",
    }
    wf["headline"] = headlines.get(wtype, summary[:80])

    # Parse reasoning_chain from JSON string if needed
    chain = wf.get("reasoning_chain") or []
    if isinstance(chain, str):
        try:
            chain = json.loads(chain)
        except Exception:
            chain = []
    wf["reasoning_chain"] = chain

    return wf


# =============================================================================
# PATTERN 7: FILTERS ENDPOINT -- Parallel DISTINCT queries for dropdowns
# =============================================================================
# Feed this to the frontend on page load so filter dropdowns are populated
# with real values from the data. One gather() call for all filter dimensions.
#
# Key points:
#   - Each query is a simple SELECT DISTINCT ... ORDER BY
#   - Flatten each result list to a plain list of strings for the frontend
#   - This is a great candidate for caching (values change infrequently)

@app.get("/api/supply-chain/filters")
async def get_filters():
    """All distinct filter values for frontend dropdowns, fetched in parallel."""
    q_div, q_reg, q_ftype, q_pcat, q_carrier, q_mode, q_status = await asyncio.gather(
        asyncio.to_thread(run_query, "SELECT DISTINCT division FROM facilities ORDER BY division"),
        asyncio.to_thread(run_query, "SELECT DISTINCT region FROM facilities ORDER BY region"),
        asyncio.to_thread(run_query, "SELECT DISTINCT facility_type FROM facilities ORDER BY facility_type"),
        asyncio.to_thread(run_query, "SELECT DISTINCT product_category FROM products ORDER BY product_category"),
        asyncio.to_thread(run_query, "SELECT DISTINCT carrier FROM shipments ORDER BY carrier"),
        asyncio.to_thread(run_query, "SELECT DISTINCT transport_mode FROM shipments ORDER BY transport_mode"),
        asyncio.to_thread(run_query, "SELECT DISTINCT status FROM shipments ORDER BY status"),
    )

    # Flatten each query result (list of dicts) to a plain list of strings
    return {
        "divisions": [r["division"] for r in q_div],
        "regions": [r["region"] for r in q_reg],
        "facility_types": [r["facility_type"] for r in q_ftype],
        "product_categories": [r["product_category"] for r in q_pcat],
        "carriers": [r["carrier"] for r in q_carrier],
        "transport_modes": [r["transport_mode"] for r in q_mode],
        "shipment_statuses": [r["status"] for r in q_status],
    }


# =============================================================================
# PATTERN 8: NOTES ENDPOINT -- Generic entity notes (GET + POST)
# =============================================================================
# A single notes table serves all entity types. The entity_type + entity_id
# pair acts as a polymorphic foreign key. This avoids creating separate notes
# tables for shipments, POs, exceptions, etc.
#
# Key points:
#   - GET: Filter by entity_type AND entity_id (both required in the path)
#   - POST: Pydantic model validates the body; write_pg() returns the new row
#   - _safe() on path params is belt-and-suspenders (psycopg2 %s is already safe)

@app.get("/api/notes/{entity_type}/{entity_id}")
async def get_notes(entity_type: str, entity_id: str):
    """Get all notes for a specific entity, newest first."""
    return await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM notes WHERE entity_type = %s AND entity_id = %s ORDER BY created_at DESC",
        (_safe(entity_type), _safe(entity_id)),
    )


@app.post("/api/notes")
async def add_note(body: NoteCreate):
    """Add a note to any entity. Returns the created row."""
    return await asyncio.to_thread(
        write_pg,
        """INSERT INTO notes (entity_type, entity_id, note_text, author)
           VALUES (%s, %s, %s, %s)
           RETURNING *""",
        (body.entity_type, body.entity_id, body.note_text, body.author),
    )


# =============================================================================
# PATTERN 9: AGENT ACTIONS ENDPOINT -- GET with filters + PATCH status
# =============================================================================
# Agent actions are proposals created by the MAS (Multi-Agent Supervisor) that
# need human approval. The GET endpoint supports status filtering and a limit
# param. The PATCH endpoint is minimal -- just a status update.
#
# Key points:
#   - limit param with a sensible default (20) prevents unbounded queries
#   - %s parameterized for both status AND limit (Lakebase handles int params)
#   - PATCH is intentionally simple -- no dynamic SET building needed

@app.get("/api/agent-actions")
async def get_agent_actions(status: Optional[str] = None, limit: int = 20):
    """List recent agent-proposed actions, optionally filtered by status."""
    if status:
        return await asyncio.to_thread(
            run_pg_query,
            "SELECT * FROM agent_actions WHERE status = %s ORDER BY created_at DESC LIMIT %s",
            (_safe(status), limit),
        )
    return await asyncio.to_thread(
        run_pg_query,
        "SELECT * FROM agent_actions ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )


@app.patch("/api/agent-actions/{action_id}")
async def update_agent_action(action_id: int, body: AgentActionUpdate):
    """Approve or dismiss an agent-proposed action."""
    if not body.status:
        raise HTTPException(400, "status is required")
    return await asyncio.to_thread(
        write_pg,
        "UPDATE agent_actions SET status = %s WHERE action_id = %s RETURNING *",
        (body.status, action_id),
    )


# =============================================================================
# PATTERN 10: AGGREGATE OVERVIEW -- Multiple parallel Lakebase queries for a
#             dashboard page that mixes KPIs, grouped counts, and recent lists
# =============================================================================
# Similar to Pattern 1 (KPI Metrics) but targets Lakebase instead of Lakehouse.
# Useful for operational dashboards that show workflow status breakdowns,
# recent activity feeds, and summary KPIs -- all from PostgreSQL.

@app.get("/api/agent-overview")
async def get_agent_overview():
    """Aggregated agent activity dashboard: KPIs + recent workflows + actions."""
    q_wf_status, q_aa_status, q_wf_types, q_aa_24h, q_recent_wf, q_recent_aa = await asyncio.gather(
        # Workflow status breakdown
        asyncio.to_thread(run_pg_query,
            "SELECT status, COUNT(*) as cnt FROM workflows GROUP BY status"),
        # Agent action status breakdown
        asyncio.to_thread(run_pg_query,
            "SELECT status, COUNT(*) as cnt FROM agent_actions GROUP BY status"),
        # Workflow types in last 7 days
        asyncio.to_thread(run_pg_query,
            "SELECT workflow_type, COUNT(*) as cnt FROM workflows "
            "WHERE created_at >= NOW() - INTERVAL '7 days' GROUP BY workflow_type"),
        # Agent actions in last 24 hours
        asyncio.to_thread(run_pg_query,
            "SELECT COUNT(*) as cnt FROM agent_actions "
            "WHERE created_at >= NOW() - INTERVAL '24 hours'"),
        # Recent workflows for the activity feed
        asyncio.to_thread(run_pg_query,
            "SELECT * FROM workflows ORDER BY created_at DESC LIMIT 30"),
        # Recent agent actions
        asyncio.to_thread(run_pg_query,
            "SELECT * FROM agent_actions ORDER BY created_at DESC LIMIT 10"),
    )

    # Pivot status counts into a flat dict for easy frontend consumption
    wf_counts = {r["status"]: r["cnt"] for r in q_wf_status}
    aa_counts = {r["status"]: r["cnt"] for r in q_aa_status}

    kpis = {
        "pending_approval": wf_counts.get("pending_approval", 0),
        "in_progress": wf_counts.get("in_progress", 0),
        "workflows_7d": sum(r["cnt"] for r in q_wf_types) if q_wf_types else 0,
        "dismissed_7d": wf_counts.get("dismissed", 0),
        "agent_actions_24h": q_aa_24h[0]["cnt"] if q_aa_24h else 0,
        "total_workflows": sum(wf_counts.values()),
    }

    # Enrich workflow objects with headlines
    workflows = [_enrich_workflow(wf) for wf in q_recent_wf]

    return {
        "kpis": kpis,
        "workflows": workflows,
        "agent_actions_recent": q_recent_aa,
    }


# =============================================================================
# QUICK REFERENCE: Pattern summary
# =============================================================================
#
# | Pattern | When to use                              | Core function  |
# |---------|------------------------------------------|----------------|
# | 1. KPI  | Dashboard top-line metrics               | run_query      |
# | 2. List | Filterable tables with pagination         | run_query      |
# | 3. Detail | Single record with related data        | run_query + run_pg_query |
# | 4. CRUD | Create/update operational records         | write_pg       |
# | 5. Filtered list | Bounded lists with JOINs        | run_query      |
# | 6. Workflow | Approve/dismiss with side effects     | write_pg       |
# | 7. Filters | Dropdown values for filter UI          | run_query      |
# | 8. Notes | Polymorphic notes on any entity          | run_pg_query + write_pg |
# | 9. Actions | Agent proposals needing approval       | run_pg_query + write_pg |
# | 10. Overview | Aggregated dashboard from Lakebase   | run_pg_query   |
#
# Rules:
#   - Lakehouse (run_query):  Delta Lake tables, historical/analytical data
#   - Lakebase (run_pg_query/write_pg): PostgreSQL, operational/transactional data
#   - ALWAYS use asyncio.to_thread() around run_query/run_pg_query/write_pg
#   - ALWAYS use asyncio.gather() when you have 2+ independent queries
#   - ALWAYS validate user input with _safe() before SQL interpolation
#   - ALWAYS use %s parameterized queries for Lakebase (never f-string values)
#   - Use Pydantic BaseModel for POST/PATCH bodies
#   - Use RETURNING * on INSERT/UPDATE to avoid a second query
#   - Raise HTTPException(400/404/500) for error cases
