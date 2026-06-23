# Databricks notebook source
# MAGIC %md
# MAGIC # Physical AI Fleet Health Console — One-Notebook Deploy
# MAGIC
# MAGIC Provisions every resource for the Amazon Robotics fleet health demo from inside a Databricks workspace.
# MAGIC
# MAGIC **Prereqs**
# MAGIC   1. This repo cloned into the workspace as a Git folder (Repos) or uploaded under `/Workspace/Users/...`.
# MAGIC   2. A pre-provisioned serverless SQL warehouse.
# MAGIC   3. Run this notebook as a user who can create catalogs, schemas, Lakebase instances, apps, Genie spaces, and MAS.
# MAGIC
# MAGIC **What gets created (in order)**
# MAGIC   * Phase A — Delta Lake: catalog schema, 4 Delta tables (via `02_generate_data.py` submitted as a serverless job).
# MAGIC   * Phase B — Lakebase: instance, database, core+domain schemas, seed rows.
# MAGIC   * Phase C — AI layer: Genie Space (4-step PATCH dance), shared `lakebase-mcp-server` app (or reuse), UC HTTP connection, MAS (POST then PATCH per Gotcha #34).
# MAGIC   * Phase D — App deploy: app create → first deploy → register resources → redeploy → explicit permission grants → health verify.
# MAGIC
# MAGIC **Idempotent-ish.** Re-running checks for existing resources before creating. Safe to re-run after a partial failure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 — Install / upgrade dependencies
# MAGIC
# MAGIC Gotcha #22: the serverless runtime ships an older `databricks-sdk` without `w.database`. Upgrade first.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk psycopg2-binary requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configuration
# MAGIC
# MAGIC Edit these constants for your workspace. Everything else flows from this cell.

# COMMAND ----------

# ─── Required workspace identifiers ─────────────────────────────────────────
CATALOG               = "amz_robotics_9868sm_catalog"   # FEVM-provisioned UC catalog
SCHEMA                = "fleet_health"
# Warehouse is auto-discovered by name in cell 2 — set the substring to match.
# FEVM auto-provisions "Serverless Starter Warehouse"; "Starter Warehouse" matches it.
WAREHOUSE_NAME_MATCH  = "Starter Warehouse"             # case-insensitive substring

# ─── Lakebase ───────────────────────────────────────────────────────────────
LAKEBASE_INSTANCE     = "amz-robotics-db"               # hyphens (Gotcha #5)
LAKEBASE_DATABASE     = "amz_robotics"
LAKEBASE_CAPACITY     = "CU_1"                          # required (Gotcha #30)

# ─── App names ──────────────────────────────────────────────────────────────
MCP_APP_NAME          = "lakebase-mcp-server"           # shared across demos
FLEET_APP_NAME        = "amz-robotics-fleet-health"
UC_HTTP_CONN_NAME     = "amz_robotics_mcp_conn"

# ─── Source code paths in the Workspace ────────────────────────────────────
# Auto-detect from this notebook's path if it lives inside the cloned repo.
# Override these constants if your layout differs.
import os, re, json, time, uuid, urllib.parse

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
NOTEBOOK_PATH         = _ctx.notebookPath().get()
# notebooks/04_deploy_all → repo root is two dirs up.
REPO_ROOT             = os.path.dirname(os.path.dirname(NOTEBOOK_PATH))   # /Workspace/...../amz_robotics
APP_SOURCE_PATH       = f"{REPO_ROOT}/app"
MCP_APP_SOURCE_PATH   = f"{REPO_ROOT}/lakebase-mcp-server/app"
DATAGEN_NOTEBOOK_PATH = f"{REPO_ROOT}/notebooks/02_generate_data"
CORE_SCHEMA_FILE      = f"/Workspace{REPO_ROOT}/lakebase/core_schema.sql"
DOMAIN_SCHEMA_FILE    = f"/Workspace{REPO_ROOT}/lakebase/domain_schema.sql"

# ─── Genie + MAS identity ──────────────────────────────────────────────────
GENIE_SPACE_TITLE     = "Fleet Telemetry"
GENIE_TABLES_SORTED   = [
    f"{CATALOG}.{SCHEMA}.anomalies",     # alphabetical sort REQUIRED (Gotcha #10)
    f"{CATALOG}.{SCHEMA}.robots",
    f"{CATALOG}.{SCHEMA}.telemetry",
]
MAS_NAME              = "fleet-health-supervisor"
MAS_INSTRUCTIONS      = (
    "You are the Fleet Health copilot for Amazon Robotics RME teams. Reduce operator cognitive load "
    "by reasoning over telemetry, service manuals, and historical remediations, and render at the depth "
    "the role needs (RME Tech vs RME Lead). Every retrieval is governed by Unity Catalog; every diagnostic "
    "persists in Lakebase so memory survives detection-model upgrades (Sparrow v4 -> v5).\n\n"
    "Use fleet-telemetry-genie for analytical queries. Use mcp-lakebase-connection to write work orders, "
    "persist diagnostics to agent_memory, and update statuses. Tool names: insert_record, update_records, "
    "execute_sql, read_query.\n\n"
    "Domain: Sparrow (item pick arm), Hercules (drive unit), Proteus (AMR), Sequoia (gantry). "
    "FCs: BFI4, PDX9, SHV1, CMH1, PHL7, ABE8. RME = Reliability and Maintenance Engineering."
)

print("Notebook path :", NOTEBOOK_PATH)
print("Repo root     :", REPO_ROOT)
print("App source    :", APP_SOURCE_PATH)
print("MCP source    :", MCP_APP_SOURCE_PATH)
print("Catalog       :", CATALOG)
print("Schema        :", SCHEMA)
print("Warehouse     :", f"resolved by name in next cell (match='{WAREHOUSE_NAME_MATCH}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — SDK + REST helpers
# MAGIC
# MAGIC Inside a workspace notebook `WorkspaceClient()` auto-authenticates. We layer a small helper for raw REST calls the typed SDK doesn't cover yet (Genie, MAS, UC connections).

# COMMAND ----------

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppResource, AppResourceSqlWarehouse, AppResourceServingEndpoint, AppResourceDatabase, ComputeSize

w = WorkspaceClient()

WORKSPACE_URL = w.config.host.rstrip("/")
print("Workspace     :", WORKSPACE_URL)
print("Identity      :", w.current_user.me().user_name)


def resolve_warehouse_id(name_match: str) -> str:
    """Find a SQL warehouse by case-insensitive substring match on its name.

    Picks the first RUNNING serverless warehouse if multiple match; otherwise
    the first match overall. Raises if none found.
    """
    matches = [wh for wh in w.warehouses.list()
               if name_match.lower() in (wh.name or "").lower()]
    if not matches:
        names = [wh.name for wh in w.warehouses.list()]
        raise RuntimeError(
            f"No warehouse name contains '{name_match}'. "
            f"Available: {names}"
        )
    # Prefer serverless + running when picking among multiple matches
    matches.sort(key=lambda wh: (
        0 if (wh.enable_serverless_compute and str(wh.state) in ("State.RUNNING", "RUNNING")) else
        1 if wh.enable_serverless_compute else
        2
    ))
    chosen = matches[0]
    if len(matches) > 1:
        print(f"  (multiple matches; picked '{chosen.name}' state={chosen.state})")
    return chosen.id, chosen.name


WAREHOUSE_ID, WAREHOUSE_NAME = resolve_warehouse_id(WAREHOUSE_NAME_MATCH)
print(f"Warehouse     : {WAREHOUSE_NAME}  ({WAREHOUSE_ID})")


def _headers():
    auth = w.config.authenticate()  # {'Authorization': 'Bearer …'}
    return {**auth, "Content-Type": "application/json"}


def api(method: str, path: str, body=None, expect_ok=True):
    """Lightweight wrapper for raw Databricks REST calls.

    method: GET / POST / PATCH / DELETE
    path  : starts with /api/... ; trailing query params allowed.
    body  : dict (will be JSON-encoded) or None.
    """
    url = WORKSPACE_URL + path
    r = requests.request(method.upper(), url, headers=_headers(),
                         data=json.dumps(body) if body is not None else None,
                         timeout=120)
    if expect_ok and not r.ok:
        raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:600]}")
    if r.text.strip():
        try:
            return r.json()
        except json.JSONDecodeError:
            return r.text
    return None


def workspace_read_text(path: str) -> str:
    """Read a file from the workspace via /api/2.0/workspace/export (raw)."""
    out = api("GET", f"/api/2.0/workspace/export?path={urllib.parse.quote(path)}&format=SOURCE&direct_download=true")
    if isinstance(out, str):
        return out
    # JSON response with base64 content
    import base64
    return base64.b64decode(out["content"]).decode("utf-8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase A · Delta Lake data
# MAGIC
# MAGIC One Statement-Execution call per statement (Gotcha #21), then submit `02_generate_data.py` as a one-shot serverless job and wait.

# COMMAND ----------

def run_sql(stmt: str, warehouse_id: str = WAREHOUSE_ID):
    """Execute a single Databricks SQL statement and wait for completion."""
    resp = api("POST", "/api/2.0/sql/statements",
               body={"statement": stmt, "warehouse_id": warehouse_id, "wait_timeout": "30s"})
    state = resp.get("status", {}).get("state")
    if state != "SUCCEEDED":
        # Poll if still pending/running
        sid = resp["statement_id"]
        for _ in range(60):
            time.sleep(2)
            resp = api("GET", f"/api/2.0/sql/statements/{sid}")
            if resp.get("status", {}).get("state") in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                break
    if resp.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {resp}")
    return resp


print(f"Creating schema {CATALOG}.{SCHEMA} …")
run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
        f"COMMENT 'Amazon Robotics Physical AI Fleet Health Console'")
print("  schema ready.")

# COMMAND ----------

# Submit 02_generate_data.py as a one-shot serverless run.

from databricks.sdk.service.jobs import SubmitTask, NotebookTask, JobEnvironment
from databricks.sdk.service.compute import Environment

print("Submitting data-generation job (serverless) …")
run = w.jobs.submit(
    run_name="amz-robotics-fleet-data-gen",
    tasks=[SubmitTask(
        task_key="generate_data",
        notebook_task=NotebookTask(notebook_path=DATAGEN_NOTEBOOK_PATH),
        environment_key="serverless_env",
    )],
    environments=[JobEnvironment(environment_key="serverless_env", spec=Environment(client="2"))],
).result()

print(f"  data-gen completed in {(run.run_duration or 0)/1000:.0f}s · state={run.state.life_cycle_state} · result={run.state.result_state}")

# Sanity: row counts
counts = run_sql(
    f"SELECT 'robots' t, COUNT(*) n FROM {CATALOG}.{SCHEMA}.robots "
    f"UNION ALL SELECT 'telemetry', COUNT(*) FROM {CATALOG}.{SCHEMA}.telemetry "
    f"UNION ALL SELECT 'anomalies', COUNT(*) FROM {CATALOG}.{SCHEMA}.anomalies "
    f"UNION ALL SELECT 'service_manual_chunks', COUNT(*) FROM {CATALOG}.{SCHEMA}.service_manual_chunks"
)
for row in counts.get("result", {}).get("data_array", []):
    print(f"  {row[0]:25s} {int(row[1]):>8,d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase B · Lakebase
# MAGIC
# MAGIC Create instance (Gotcha #30: capacity required), wait for `AVAILABLE` not `RUNNING` (Gotcha #31), create database, apply schemas, seed.

# COMMAND ----------

from databricks.sdk.service.database import DatabaseInstance

existing = None
try:
    existing = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    print(f"  reusing instance {LAKEBASE_INSTANCE} (state={existing.state})")
except Exception:
    pass

if existing is None:
    print(f"Creating Lakebase instance {LAKEBASE_INSTANCE} (capacity {LAKEBASE_CAPACITY}) — ~6 min …")
    w.database.create_database_instance(
        database_instance=DatabaseInstance(name=LAKEBASE_INSTANCE, capacity=LAKEBASE_CAPACITY)
    )

# Poll until AVAILABLE (Gotcha #31)
for i in range(60):
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    if str(inst.state) in ("DatabaseInstanceState.AVAILABLE", "AVAILABLE"):
        break
    print(f"  state={inst.state} (waiting…)")
    time.sleep(15)
else:
    raise RuntimeError("Lakebase instance did not become AVAILABLE")

PG_HOST = inst.read_write_dns
print(f"  AVAILABLE — {PG_HOST}")

# COMMAND ----------

# Get a Lakebase credential via the SDK (no request_id needed — Gotcha #36 only hits the CLI form)
# Use the running user's email as the PG user — serverless ephemeral spark-* users have no role (Gotcha #32).
import psycopg2

cred = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
PG_TOKEN = cred.token
PG_USER  = w.current_user.me().user_name


def pg_exec(sql: str, db: str = LAKEBASE_DATABASE, params=None):
    """Execute one or more SQL statements against Lakebase."""
    conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=db,
                            user=PG_USER, password=PG_TOKEN, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        try:
            return cur.fetchall()
        except psycopg2.ProgrammingError:
            return None
    finally:
        cur.close(); conn.close()


def pg_exec_script(sql_text: str, db: str = LAKEBASE_DATABASE):
    """Run a multi-statement SQL script, ignoring blank/comment-only chunks."""
    conn = psycopg2.connect(host=PG_HOST, port=5432, dbname=db,
                            user=PG_USER, password=PG_TOKEN, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql_text)
    finally:
        cur.close(); conn.close()


# Create the database in the postgres administrative DB (must connect to 'databricks_postgres')
try:
    pg_exec(f'CREATE DATABASE {LAKEBASE_DATABASE};', db="databricks_postgres")
    print(f"  created database {LAKEBASE_DATABASE}")
except psycopg2.errors.DuplicateDatabase:
    print(f"  database {LAKEBASE_DATABASE} already exists")
except Exception as e:
    # Some workspaces use a different admin DB name; try 'postgres' as fallback
    try:
        pg_exec(f'CREATE DATABASE {LAKEBASE_DATABASE};', db="postgres")
        print(f"  created database {LAKEBASE_DATABASE}")
    except psycopg2.errors.DuplicateDatabase:
        print(f"  database {LAKEBASE_DATABASE} already exists")
    except Exception as e2:
        print(f"  WARN: could not create database via admin DB ({e}); trying direct connect …")
        try:
            pg_exec("SELECT 1")  # if the DB exists, this succeeds
            print(f"  database {LAKEBASE_DATABASE} already exists")
        except Exception:
            raise e2

# COMMAND ----------

# Apply core + domain schemas (multi-statement files; psycopg2 handles them fine)
print("Applying core_schema.sql …")
pg_exec_script(workspace_read_text(CORE_SCHEMA_FILE))
print("Applying domain_schema.sql …")
pg_exec_script(workspace_read_text(DOMAIN_SCHEMA_FILE))
print("  Lakebase tables:")
for (t,) in (pg_exec("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename") or []):
    print(f"    - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lakebase seed
# MAGIC
# MAGIC Seed `work_orders`, `agent_memory`, `workflows`, `agent_actions`, `notes` with deterministic synthetic rows.

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

random.seed(20260612)
NOW = datetime.now(timezone.utc)

FC_SITES = ["BFI4", "PDX9", "SHV1", "CMH1", "PHL7", "ABE8"]
FAMILIES = ["Sparrow", "Hercules", "Proteus", "Sequoia"]
TECHS    = ["A. Chen", "B. Kumar", "C. Rodriguez", "D. Patel", "E. Thompson",
            "F. Nakamura", "G. Lee", "H. Patel", "I. Brown", "J. Singh"]
SIGNALS = {
    "Sparrow":  ["vibration", "joint_torque", "joint_temperature", "vacuum_pressure", "camera_health"],
    "Hercules": ["battery_soh", "motor_current", "wheel_encoder", "nav_odometry"],
    "Proteus":  ["lidar_health", "battery_soh", "drive_motor", "safety_sensor", "dock_alignment"],
    "Sequoia":  ["rail_position", "lift_motor", "tote_actuator", "gantry_temperature"],
}
MODELS = {
    "Sparrow":  ["sparrow-detect-v4", "sparrow-detect-v5"],
    "Hercules": ["hercules-detect-v3"],
    "Proteus":  ["proteus-detect-v2"],
    "Sequoia":  ["sequoia-detect-v2"],
}

robots = []
for fam in FAMILIES:
    for site in FC_SITES:
        n = 12 if fam == "Hercules" else 6
        for i in range(n):
            robots.append({"robot_id": f"{fam[:3].upper()}-{site}-{i:03d}", "family": fam, "fc_site": site})
random.shuffle(robots)

DIAG = {
    "vacuum_pressure":   ("Vacuum pressure dropped to {v:.1f} kPa during pick cycles.",
                          "Replaced gripper seal AR-SPR-VAC-005 and inline filter AR-SPR-FLT-002."),
    "vibration":         ("Joint 5 sustained vibration at {v:.2f} mm/s RMS.",
                          "Scheduled preventive bearing replacement; updated PdM threshold."),
    "joint_torque":      ("Joint 4 torque spiked to {v:.1f} Nm.", "Re-lubricated harmonic gear."),
    "joint_temperature": ("Joint temperature {v:.1f} C exceeds 65 C ceiling.", "Cleared vent blockage."),
    "camera_health":     ("Computer vision health {v:.1f}.", "Cleaned IR illuminator and recalibrated."),
    "battery_soh":       ("Battery SoH {v:.1f}%.", "Replaced battery AR-HRC-BAT-018."),
    "motor_current":     ("Motor current {v:.1f} A.", "Removed wheel debris, lubricated axle."),
    "wheel_encoder":     ("Wheel encoder {v:.1f}%.", "Recalibrated nav."),
    "nav_odometry":      ("Nav odometry {v:.1f}%.", "Re-baselined fiducial map."),
    "lidar_health":      ("LiDAR health {v:.1f}.", "Cleaned LiDAR window."),
    "safety_sensor":     ("Safety sensor {v:.1f}%.", "Replaced sensor module."),
    "dock_alignment":    ("Dock offset {v:.2f} cm.", "Cleaned fiducials, ran proteus_dock_recal."),
    "drive_motor":       ("Drive motor current {v:.1f} A.", "Cleared fault, resumed."),
    "rail_position":     ("Rail position error {v:.2f} mm.", "Re-aligned gantry rail."),
    "lift_motor":        ("Lift motor {v:.1f} A.", "Cleared rail debris."),
    "tote_actuator":     ("Tote actuator cycle health {v:.1f}%.", "Replaced AR-SEQ-TOT-009."),
    "gantry_temperature":("Gantry enclosure {v:.1f} C.", "Replaced enclosure fan."),
}

WO_STATUSES = ["completed"]*12 + ["in_progress"]*4 + ["open"]*3 + ["awaiting_parts"]
print("Seeding work_orders …")
wo_n = 0
for r in robots[:120]:
    fam = r["family"]
    sig = random.choice([s for s in SIGNALS[fam] if s in DIAG])
    diag, remed = DIAG[sig]
    val = random.uniform(20.0, 80.0)
    status = random.choice(WO_STATUSES)
    severity = random.choices(["low","medium","high","critical"], weights=[10,35,35,20])[0]
    priority = "urgent" if severity == "critical" else "high" if severity == "high" else "normal"
    created = NOW - timedelta(days=random.randint(0,90), hours=random.randint(0,23))
    resolved = created + timedelta(hours=random.randint(2,72)) if status == "completed" else None
    wo_num = f"WO-{fam[:3].upper()}-{created.strftime('%y%m')}-{wo_n:04d}"
    pg_exec(
        """INSERT INTO work_orders (wo_number, robot_id, family, fc_site, source_anomaly_id,
            severity, priority, title, root_cause, remediation_steps, parts_used, manual_refs,
            technician, status, created_by, created_at, resolved_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s)
           ON CONFLICT (wo_number) DO NOTHING""",
        params=(
            wo_num, r["robot_id"], fam, r["fc_site"], f"ANOM-{random.randint(1,3000):06d}",
            severity, priority, f"{sig.replace('_',' ').title()} anomaly — {r['robot_id']}",
            diag.format(v=val), remed,
            json.dumps([{"part": "AR-SPR-VAC-005", "qty": 1}] if "vacuum" in sig else []),
            json.dumps([{"section": sig.replace("_"," ").title(), "page": random.randint(8,64)}]),
            random.choice(TECHS), status,
            "agent" if random.random() < 0.6 else random.choice(TECHS),
            created, resolved,
        ),
    )
    wo_n += 1
print(f"  inserted {wo_n} work_orders")

print("Seeding agent_memory …")
mem_n = 0
for r in robots[:80]:
    fam = r["family"]
    sig = random.choice([s for s in SIGNALS[fam] if s in DIAG])
    diag, remed = DIAG[sig]
    val = random.uniform(20.0, 80.0)
    persona = random.choices(["rme_tech","rme_lead"], weights=[70,30])[0]
    outcome = random.choices(["resolved","recurring","escalated","no_action","pending"], weights=[60,15,10,10,5])[0]
    model_v = random.choice(MODELS[fam])    # critical: mix of v4 + v5 for Sparrow
    created = NOW - timedelta(days=random.randint(0,120))
    pg_exec(
        """INSERT INTO agent_memory (robot_id, family, fc_site, signal, persona, diagnostic,
            remediation, outcome, model_version, source_anomaly_id, confidence, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        params=(r["robot_id"], fam, r["fc_site"], sig, persona, diag.format(v=val), remed,
                outcome, model_v, f"ANOM-{random.randint(1,3000):06d}",
                round(random.uniform(0.72, 0.97), 2), created),
    )
    mem_n += 1
print(f"  inserted {mem_n} agent_memory rows")

print("Seeding workflows …")
wf_n = 0
wf_types = ["work_order_create", "anomaly_escalate", "memory_persist"]
for _ in range(20):
    r = random.choice(robots)
    sig = random.choice([s for s in SIGNALS[r["family"]] if s in DIAG])
    severity = random.choices(["medium","high","critical"], weights=[40,40,20])[0]
    wf_type = random.choice(wf_types)
    status = random.choices(["pending_approval","in_progress"], weights=[70,30])[0]
    summary = {
        "work_order_create": f"Recommend opening a work order for {r['robot_id']} ({sig.replace('_',' ')} anomaly).",
        "anomaly_escalate":  f"Escalating recurring {sig.replace('_',' ')} cluster on {r['family']} at {r['fc_site']}.",
        "memory_persist":    f"Persist diagnostic for {r['robot_id']} so future {r['family']} upgrades retain context.",
    }[wf_type]
    headline = {
        "work_order_create": f"Open WO for {r['robot_id']}",
        "anomaly_escalate":  f"Escalate {r['family']} cluster at {r['fc_site']}",
        "memory_persist":    f"Persist {r['family']} diagnostic",
    }[wf_type]
    reasoning = [
        {"agent":"fleet-telemetry-genie","step":"queried recent anomalies","result":"found pattern"},
        {"agent":"service-manuals-ka","step":"retrieved remediation","result":"matched section"},
    ]
    pg_exec(
        """INSERT INTO workflows (workflow_type, trigger_source, severity, summary, reasoning_chain,
            entity_type, entity_id, status, headline)
           VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)""",
        params=(wf_type, "monitor", severity, summary, json.dumps(reasoning),
                "robot", r["robot_id"], status, headline),
    )
    wf_n += 1
print(f"  inserted {wf_n} workflows")

print("Seeding agent_actions …")
aa_n = 0
for _ in range(50):
    r = random.choice(robots)
    sev = random.choices(["low","medium","high","critical"], weights=[20,50,25,5])[0]
    st  = random.choices(["executed","dismissed","pending","failed"], weights=[70,15,10,5])[0]
    pg_exec(
        """INSERT INTO agent_actions (action_type, severity, entity_type, entity_id,
            description, action_taken, status)
           VALUES (%s,%s,%s,%s,%s,%s,%s)""",
        params=(random.choice(["anomaly_triage","work_order_open","manual_lookup",
                               "memory_persist","redeploy_recommend"]),
                sev, "robot", r["robot_id"],
                f"Reviewed {r['family']} {r['robot_id']} signal cluster",
                "Created work order and notified RME tech" if st == "executed" else "No action taken",
                st),
    )
    aa_n += 1
print(f"  inserted {aa_n} agent_actions")

print("Seeding notes …")
n_n = 0
for _ in range(30):
    r = random.choice(robots)
    text = random.choice([
        f"PM compliance verified by {random.choice(TECHS)} on last visit.",
        f"Note: this {r['family']} has flagged {random.choice(SIGNALS[r['family']]).replace('_',' ')} twice this month.",
        "Operator reports occasional noise during pick — investigated, no fault found.",
        "Coordination with Inbound: tote weight calibration adjusted.",
    ])
    pg_exec(
        "INSERT INTO notes (entity_type, entity_id, note_text, author) VALUES (%s,%s,%s,%s)",
        params=("robot", r["robot_id"], text,
                random.choice(["agent","rme-tech","rme-lead"] + TECHS)),
    )
    n_n += 1
print(f"  inserted {n_n} notes")

print("\n✅ Lakebase seed complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase C · AI layer
# MAGIC
# MAGIC Genie Space (Gotcha #10 PATCH dance) · shared Lakebase MCP app · UC HTTP connection · MAS (Gotcha #34 POST then PATCH).

# COMMAND ----------

# ─── Genie Space ────────────────────────────────────────────────────────────
print("Creating Genie Space …")
gs = api("POST", "/api/2.0/genie/spaces",
         body={"serialized_space": json.dumps({"version": 2}), "warehouse_id": WAREHOUSE_ID})
GENIE_SPACE_ID = gs["space_id"]
print(f"  space_id: {GENIE_SPACE_ID}")

# Title + description
api("PATCH", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}",
    body={"title": GENIE_SPACE_TITLE,
          "description": "Query Amazon Robotics fleet — robots, telemetry, anomalies."})

# Tables — MUST go through serialized_space, sorted alphabetically (Gotcha #10)
serialized = json.dumps({"version": 2, "data_sources": {
    "tables": [{"identifier": t} for t in GENIE_TABLES_SORTED]
}})
api("PATCH", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}",
    body={"serialized_space": serialized})

# Instructions (PATCH response may not echo it back — verify by GET)
api("PATCH", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}", body={"instructions": (
    "You are the Fleet Telemetry agent for Amazon Robotics RME teams. "
    "Robot families: Sparrow (item pick arm), Hercules (drive unit), Proteus (AMR), Sequoia (gantry). "
    "FCs: BFI4, PDX9, SHV1, CMH1, PHL7, ABE8. detect_model_version tags anomalies (sparrow-detect-v4 vs v5) "
    "demonstrating fleet memory across model upgrades. auto_resolved = agent-resolved; "
    "downtime_minutes drives $-avoided at $150/min."
)})

# Grant CAN_RUN — endpoint is /permissions/genie/{id}, NOT /permissions/genie/spaces/{id} (Gotcha #11)
api("PATCH", f"/api/2.0/permissions/genie/{GENIE_SPACE_ID}",
    body={"access_control_list": [{"group_name": "users", "permission_level": "CAN_RUN"}]})

# Verify
ggs = api("GET", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}?include_serialized_space=true")
attached = json.loads(ggs.get("serialized_space","{}")).get("data_sources",{}).get("tables",[])
print(f"  attached tables: {[t['identifier'] for t in attached]}")

# COMMAND ----------

# ─── Shared lakebase-mcp-server app (create if missing) ────────────────────
print("Lakebase MCP app …")
try:
    mcp_app = w.apps.get(name=MCP_APP_NAME)
    print(f"  reusing existing app ({mcp_app.compute_status.state})")
except Exception:
    print(f"  creating {MCP_APP_NAME} …")
    w.apps.create_and_wait(app=App(name=MCP_APP_NAME))
    mcp_app = w.apps.get(name=MCP_APP_NAME)

MCP_APP_URL          = mcp_app.url
MCP_SP_CLIENT_ID     = mcp_app.service_principal_client_id
MCP_SP_NUMERIC_ID    = mcp_app.service_principal_id
print(f"  url: {MCP_APP_URL}")
print(f"  SP client_id: {MCP_SP_CLIENT_ID}  numeric_id: {MCP_SP_NUMERIC_ID}")

# Deploy code (idempotent — overwrites)
print(f"  deploying source from {MCP_APP_SOURCE_PATH} …")
w.apps.deploy_and_wait(app_name=MCP_APP_NAME, source_code_path=MCP_APP_SOURCE_PATH)
print(f"  deployed.")

# Register database resource (preserve any existing — Gotcha #9 union)
existing_resources = list(mcp_app.resources or [])
already_registered = any(getattr(r, "database", None)
                         and r.database.instance_name == LAKEBASE_INSTANCE
                         and r.database.database_name == LAKEBASE_DATABASE
                         for r in existing_resources)
if not already_registered:
    new_res = AppResource(
        name="database" if not existing_resources else f"database-{len(existing_resources)+1}",
        database=AppResourceDatabase(instance_name=LAKEBASE_INSTANCE,
                                     database_name=LAKEBASE_DATABASE,
                                     permission="CAN_CONNECT_AND_CREATE"),
    )
    full = existing_resources + [new_res]
    w.apps.update(name=MCP_APP_NAME, app=App(name=MCP_APP_NAME, resources=full))
    # Redeploy so the SP role is created in Lakebase (Gotcha #33)
    print(f"  re-deploying to materialize SP role …")
    w.apps.deploy_and_wait(app_name=MCP_APP_NAME, source_code_path=MCP_APP_SOURCE_PATH)

# Grant CAN_USE on the MCP app to the users group (Gotcha #17 — required for MAS proxy)
api("PATCH", f"/api/2.0/permissions/apps/{MCP_APP_NAME}",
    body={"access_control_list": [{"group_name": "users", "permission_level": "CAN_USE"}]})
print("  CAN_USE granted to users group.")

# Grant Lakebase table access to the MCP SP (now that the SP role exists)
pg_exec(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{MCP_SP_CLIENT_ID}"')
pg_exec(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{MCP_SP_CLIENT_ID}"')
pg_exec(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{MCP_SP_CLIENT_ID}"')
pg_exec(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{MCP_SP_CLIENT_ID}"')
print(f"  Lakebase table grants → MCP SP {MCP_SP_CLIENT_ID}")

# COMMAND ----------

# ─── SP OAuth secret + UC HTTP connection ──────────────────────────────────
print("Creating SP OAuth secret for the MCP app …")
sec = api("POST", f"/api/2.0/accounts/servicePrincipals/{MCP_SP_NUMERIC_ID}/credentials/secrets", body={})
MCP_SP_SECRET = sec["secret"]
print(f"  secret created (id={sec.get('id','?')[:8]}…)")

print("Creating UC HTTP connection …")
try:
    api("DELETE", f"/api/2.1/unity-catalog/connections/{UC_HTTP_CONN_NAME}", expect_ok=False)
except Exception:
    pass

conn = api("POST", "/api/2.1/unity-catalog/connections", body={
    "name": UC_HTTP_CONN_NAME,
    "connection_type": "HTTP",
    "options": {
        "host": MCP_APP_URL,                              # MUST include https:// (Gotcha #18)
        "port": "443",
        "base_path": f"/db/{LAKEBASE_DATABASE}/mcp/",     # trailing slash (Gotcha #15)
        "client_id": MCP_SP_CLIENT_ID,
        "client_secret": MCP_SP_SECRET,
        "oauth_scope": "all-apis",
        "token_endpoint": f"{WORKSPACE_URL}/oidc/v1/token",   # required (Gotcha #44)
        "is_mcp_connection": "true",
    },
    "comment": "Lakebase MCP for Amazon Robotics fleet health demo",
})
print(f"  connection: {conn['name']} (base_path={conn['options']['base_path']})")

# COMMAND ----------

# ─── MAS (Gotcha #34 — POST simpler agents, PATCH MCP) ─────────────────────
print("Creating MAS (Genie agent only via POST) …")
mas_post = api("POST", "/api/2.0/multi-agent-supervisors", body={
    "name": MAS_NAME,
    "description": "Multi-agent supervisor for the Physical AI Fleet Health Console.",
    "instructions": MAS_INSTRUCTIONS,
    "agents": [{
        "agent_type": "genie-space",
        "genie_space": {"id": GENIE_SPACE_ID},
        "name": "fleet-telemetry-genie",
        "description": ("Natural-language SQL over robots, telemetry, and anomalies. "
                        "Use for fleet KPIs, anomaly counts, robot-family comparisons, "
                        "FC-site filters, trend questions."),
    }],
})
MAS_TILE_ID_FULL = mas_post["multi_agent_supervisor"]["tile"]["tile_id"]
MAS_TILE_ID      = MAS_TILE_ID_FULL[:8]                   # used in endpoint name + app.yaml
MAS_ENDPOINT     = f"mas-{MAS_TILE_ID}-endpoint"
print(f"  tile_id (full): {MAS_TILE_ID_FULL}")
print(f"  endpoint:       {MAS_ENDPOINT}")

print("PATCHing MAS to add external-mcp-server agent (Gotcha #34) …")
api("PATCH", f"/api/2.0/multi-agent-supervisors/{MAS_TILE_ID_FULL}", body={
    "name": MAS_NAME,
    "agents": [
        {
            "agent_type": "genie-space",
            "genie_space": {"id": GENIE_SPACE_ID},
            "name": "fleet-telemetry-genie",
            "description": ("Natural-language SQL over robots, telemetry, and anomalies. "
                            "Use for fleet KPIs, anomaly counts, robot-family comparisons, "
                            "FC-site filters, trend questions."),
        },
        {
            "agent_type": "external-mcp-server",
            "external_mcp_server": {"connection_name": UC_HTTP_CONN_NAME},
            "name": "mcp-lakebase-connection",
            "description": ("Write operational data to Lakebase. insert_record / update_records / "
                            "execute_sql / read_query. Tables: work_orders, agent_memory, notes, "
                            "agent_actions, workflows. Always include model_version when writing "
                            "agent_memory."),
        },
    ],
})
print("  MAS has both sub-agents now.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase D · App deploy + verify
# MAGIC
# MAGIC App create → first deploy → register resources (Gotcha #8) → redeploy (Gotcha #23 PGHOST injection) → explicit grants (Gotcha #25 MAS CAN_QUERY, Gotcha #33 Lakebase SP grants).

# COMMAND ----------

# Patch app.yaml + demo-config.yaml in the workspace copy of /app so the discovered IDs are live.
# (This is harmless if you have not yet uploaded a stale copy.)
app_yaml_path     = f"/Workspace{APP_SOURCE_PATH}/app.yaml"
demo_config_path  = f"/Workspace{APP_SOURCE_PATH}/demo-config.yaml"

def _patch_inplace(workspace_path, replacements):
    """Read a workspace file, apply (regex, replacement) pairs, write it back."""
    text = workspace_read_text(workspace_path)
    for pat, repl in replacements:
        text = re.sub(pat, repl, text)
    api("POST", "/api/2.0/workspace/import", body={
        "path": workspace_path,
        "format": "AUTO",
        "language": "PYTHON",     # ignored for non-py
        "content": __import__("base64").b64encode(text.encode("utf-8")).decode("ascii"),
        "overwrite": True,
    })

try:
    _patch_inplace(app_yaml_path, [
        (r'(name: MAS_TILE_ID\s*\n\s*value: ")[^"]*(")',     fr'\g<1>{MAS_TILE_ID}\g<2>'),
        (r'(name: GENIE_SPACE_ID\s*\n\s*value: ")[^"]*(")',  fr'\g<1>{GENIE_SPACE_ID}\g<2>'),
        (r'(name: KA_TILE_ID\s*\n\s*value: ")[^"]*(")',      r'\g<1>\g<2>'),
        (r'name:\s*"mas-[A-Za-z0-9_-]+-endpoint"',           f'name: "{MAS_ENDPOINT}"'),
    ])
    print(f"  patched {app_yaml_path}")
except Exception as e:
    print(f"  WARN: could not patch app.yaml: {e} (continuing — env vars will use defaults)")

# COMMAND ----------

# Create the fleet app (or reuse) and run the deploy → register-resources → redeploy cycle.
print(f"Fleet app: {FLEET_APP_NAME}")
try:
    fapp = w.apps.get(name=FLEET_APP_NAME)
    print(f"  reusing existing app ({fapp.compute_status.state})")
except Exception:
    print(f"  creating …")
    w.apps.create_and_wait(app=App(name=FLEET_APP_NAME))
    fapp = w.apps.get(name=FLEET_APP_NAME)

FLEET_SP_CLIENT_ID = fapp.service_principal_client_id
print(f"  fleet app SP: {FLEET_SP_CLIENT_ID}")
print(f"  url:          {fapp.url}")

# Pre-grant catalog/schema/warehouse so first deploy can read at startup
api("PATCH", f"/api/2.1/unity-catalog/permissions/catalog/{CATALOG}",
    body={"changes": [{"principal": FLEET_SP_CLIENT_ID, "add": ["USE_CATALOG"]}]})
api("PATCH", f"/api/2.1/unity-catalog/permissions/schema/{CATALOG}.{SCHEMA}",
    body={"changes": [{"principal": FLEET_SP_CLIENT_ID, "add": ["USE_SCHEMA", "SELECT"]}]})
api("PATCH", f"/api/2.0/permissions/warehouses/{WAREHOUSE_ID}",
    body={"access_control_list": [{"service_principal_name": FLEET_SP_CLIENT_ID,
                                   "permission_level": "CAN_USE"}]})

# First deploy
print("  first deploy …")
w.apps.deploy_and_wait(app_name=FLEET_APP_NAME, source_code_path=APP_SOURCE_PATH)

# Register resources (Gotcha #8 — declarative app.yaml is NOT auto-registered)
print("  registering 3 resources …")
w.apps.update(name=FLEET_APP_NAME, app=App(name=FLEET_APP_NAME, resources=[
    AppResource(name="sql-warehouse",
                sql_warehouse=AppResourceSqlWarehouse(id=WAREHOUSE_ID, permission="CAN_USE")),
    AppResource(name="mas-endpoint",
                serving_endpoint=AppResourceServingEndpoint(name=MAS_ENDPOINT, permission="CAN_QUERY")),
    AppResource(name="database",
                database=AppResourceDatabase(instance_name=LAKEBASE_INSTANCE,
                                             database_name=LAKEBASE_DATABASE,
                                             permission="CAN_CONNECT_AND_CREATE")),
]))

# Redeploy to inject PGHOST/PGPORT/PGDATABASE/PGUSER (Gotcha #23)
print("  redeploying to inject PGHOST/PGPORT/PGDATABASE/PGUSER …")
w.apps.deploy_and_wait(app_name=FLEET_APP_NAME, source_code_path=APP_SOURCE_PATH)

# COMMAND ----------

# Grant MAS endpoint CAN_QUERY by UUID (Gotcha #25 — name doesn't work for permissions API)
print("Looking up MAS endpoint UUID …")
ep = next((e for e in w.serving_endpoints.list() if e.name == MAS_ENDPOINT), None)
if not ep:
    raise RuntimeError(f"Could not find serving endpoint {MAS_ENDPOINT}")
MAS_ENDPOINT_UUID = ep.id
print(f"  uuid: {MAS_ENDPOINT_UUID}")

api("PATCH", f"/api/2.0/permissions/serving-endpoints/{MAS_ENDPOINT_UUID}",
    body={"access_control_list": [{"service_principal_name": FLEET_SP_CLIENT_ID,
                                   "permission_level": "CAN_QUERY"}]})
print("  CAN_QUERY granted on MAS endpoint.")

# Grant Lakebase table access to the fleet app SP — possible only AFTER redeploy (Gotcha #33)
pg_exec(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{FLEET_SP_CLIENT_ID}"')
pg_exec(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{FLEET_SP_CLIENT_ID}"')
pg_exec(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{FLEET_SP_CLIENT_ID}"')
pg_exec(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{FLEET_SP_CLIENT_ID}"')
print(f"  Lakebase table grants → fleet SP {FLEET_SP_CLIENT_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary
# MAGIC
# MAGIC Open the app URL in a browser (OAuth login required — `curl` will get a 302 to the OAuth challenge, that is expected).
# MAGIC Hit `/api/health` after login to confirm SDK, SQL warehouse, and Lakebase all report `ok`.

# COMMAND ----------

fapp = w.apps.get(name=FLEET_APP_NAME)
print("━" * 70)
print(f"  Demo:        Physical AI Fleet Health Console")
print(f"  App URL:     {fapp.url}")
print(f"  App status:  {fapp.app_status.state}  /  compute {fapp.compute_status.state}")
print("─" * 70)
print(f"  Catalog:                 {CATALOG}.{SCHEMA}")
print(f"  Lakebase instance/db:    {LAKEBASE_INSTANCE} / {LAKEBASE_DATABASE}")
print(f"  Genie Space ID:          {GENIE_SPACE_ID}")
print(f"  MAS tile (short / full): {MAS_TILE_ID} / {MAS_TILE_ID_FULL}")
print(f"  MAS endpoint:            {MAS_ENDPOINT}")
print(f"  MCP app URL:             {MCP_APP_URL}")
print(f"  MCP SP client_id:        {MCP_SP_CLIENT_ID}")
print(f"  UC HTTP connection:      {UC_HTTP_CONN_NAME}  (base_path /db/{LAKEBASE_DATABASE}/mcp/)")
print(f"  Fleet app SP client_id:  {FLEET_SP_CLIENT_ID}")
print("━" * 70)
print("\nNext step: open the App URL in your browser, OAuth login, and try the demo.")
print("If `/api/health` is anything other than healthy, see docs/DEPLOYMENT_GUIDE.md.")
