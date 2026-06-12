#!/usr/bin/env python3
"""
Physical AI Fleet Health Console — Lakebase Seeder

Run AFTER 02_generate_data.py has produced Delta tables AND after
the Lakebase instance + database exist and schemas have been applied:

    databricks database create-database-instance amz-robotics-db --capacity CU_1 --profile=DEFAULT
    databricks psql amz-robotics-db --profile=DEFAULT -- -c "CREATE DATABASE amz_robotics;"
    databricks psql amz-robotics-db --profile=DEFAULT -- -d amz_robotics -f ../lakebase/core_schema.sql
    databricks psql amz-robotics-db --profile=DEFAULT -- -d amz_robotics -f ../lakebase/domain_schema.sql

Then run this script LOCALLY (not on a serverless notebook — Gotcha #32):

    python notebooks/03_seed_lakebase.py

The script obtains a DB credential via the CLI (Gotcha #36 — request_id required)
and connects directly via psycopg2.
"""

import json
import random
import subprocess
import sys
from datetime import datetime, timedelta, timezone

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. Run: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# ─── Config ──────────────────────────────────────────────────────────────────

INSTANCE_NAME = "amz-robotics-db"
DATABASE_NAME = "amz_robotics"
PROFILE       = "DEFAULT"
PG_USER       = "gary.burgett@databricks.com"   # the human running the seed; replace if different
PG_SSLMODE    = "require"

random.seed(20260528)

NOW = datetime.now(timezone.utc)

# ─── Domain constants (must match 02_generate_data.py) ───────────────────────

FC_SITES   = ["BFI4", "PDX9", "SHV1", "CMH1", "PHL7", "ABE8"]
FAMILIES   = ["Sparrow", "Hercules", "Proteus", "Sequoia"]
TECHNICIANS = [
    "A. Chen", "B. Kumar", "C. Rodriguez", "D. Patel", "E. Thompson",
    "F. Nakamura", "G. Lee", "H. Patel", "I. Brown", "J. Singh",
]
DETECT_MODELS_BY_FAMILY = {
    "Sparrow":  ["sparrow-detect-v4", "sparrow-detect-v5"],
    "Hercules": ["hercules-detect-v3"],
    "Proteus":  ["proteus-detect-v2"],
    "Sequoia":  ["sequoia-detect-v2"],
}
SIGNALS_BY_FAMILY = {
    "Sparrow":  ["vibration", "joint_torque", "joint_temperature", "vacuum_pressure", "camera_health"],
    "Hercules": ["battery_soh", "motor_current", "wheel_encoder", "nav_odometry"],
    "Proteus":  ["lidar_health", "battery_soh", "drive_motor", "safety_sensor", "dock_alignment"],
    "Sequoia":  ["rail_position", "lift_motor", "tote_actuator", "gantry_temperature"],
}

# Diagnostic / remediation templates per (family, signal)
DIAG_TEMPLATES = {
    ("Sparrow",  "vacuum_pressure"): (
        "Vacuum pressure dropped to {val:.1f} kPa during pick cycles, well below the 65 kPa floor.",
        "Replaced gripper interface seal (AR-SPR-VAC-005) and inline filter (AR-SPR-FLT-002). Recalibrated suction baseline.",
    ),
    ("Sparrow",  "vibration"): (
        "Joint 5 sustained vibration at {val:.2f} mm/s RMS — early bearing wear signature.",
        "Scheduled preventive bearing replacement at next maintenance window. Updated PdM threshold.",
    ),
    ("Sparrow",  "joint_torque"): (
        "Joint 4 torque spiked to {val:.1f} Nm during standard 2-kg pick.",
        "Re-lubricated harmonic gear (AR-LUB-HG-100), checked belt tension. Resumed nominal operation.",
    ),
    ("Sparrow",  "joint_temperature"): (
        "Joint temperature held {val:.1f} C through normal-load picks — exceeds 65 C ceiling.",
        "Reduced cycle rate temporarily, inspected ventilation grille for blockage. Cleared dust accumulation.",
    ),
    ("Sparrow",  "camera_health"): (
        "Computer vision health score fell to {val:.1f}.",
        "Cleaned IR illuminator and lens, recalibrated via RME console > Vision > Calibrate.",
    ),
    ("Hercules", "battery_soh"): (
        "Battery State of Health declined to {val:.1f}% — below 70% replacement threshold.",
        "Scheduled battery swap (AR-HRC-BAT-018). Robot pulled from outbound until next maintenance window.",
    ),
    ("Hercules", "motor_current"): (
        "Drive motor sustained {val:.1f} A on level floor — likely wheel binding.",
        "Inspected drive wheel, removed debris, lubricated axle. Confirmed return to baseline current.",
    ),
    ("Hercules", "wheel_encoder"): (
        "Wheel encoder accuracy dropped to {val:.1f}% — pose estimation degraded.",
        "Recalibrated with hercules_calib_nav.py at the maintenance bay mat.",
    ),
    ("Hercules", "nav_odometry"): (
        "Nav odometry accuracy at {val:.1f}% — repeated near-collisions logged.",
        "Re-baselined fiducial map for the affected aisle. Verified collision-avoidance reactive layer.",
    ),
    ("Proteus",  "lidar_health"): (
        "LiDAR health score at {val:.1f} — likely window contamination.",
        "Cleaned LiDAR window with isopropyl wipe (AR-PRO-LCW-001). Score returned to >98.",
    ),
    ("Proteus",  "safety_sensor"): (
        "Safety sensor availability {val:.1f}% — Proteus restricted from human-occupied zones.",
        "Replaced sensor module, ran safety re-validation, returned robot to full operation.",
    ),
    ("Proteus",  "dock_alignment"): (
        "Dock offset at {val:.2f} cm causing failed GoCart handoffs.",
        "Cleaned dock fiducials, ran proteus_dock_recal. Offset back within tolerance.",
    ),
    ("Proteus",  "drive_motor"): (
        "Drive motor current {val:.1f} A — possible binding or load anomaly.",
        "Verified wheel free rotation, checked motor mount torque. Cleared fault and resumed.",
    ),
    ("Proteus",  "battery_soh"): (
        "Battery SoH at {val:.1f}%. Replaced under standard battery program.",
        "Scheduled and completed battery swap.",
    ),
    ("Sequoia",  "lift_motor"): (
        "Lift motor at {val:.1f} A — investigated rail debris between bins 4-6.",
        "Cleared rail debris, returned lift to nominal current.",
    ),
    ("Sequoia",  "tote_actuator"): (
        "Tote actuator cycle health at {val:.1f}% — failure predicted within 500 cycles.",
        "Replaced actuator (AR-SEQ-TOT-009) during scheduled maintenance.",
    ),
    ("Sequoia",  "rail_position"): (
        "Rail position error at {val:.2f} mm — likely alignment drift.",
        "Re-aligned gantry rail, retorqued mounts to spec.",
    ),
    ("Sequoia",  "gantry_temperature"): (
        "Gantry enclosure temperature {val:.1f} C — cooling fan slowed.",
        "Replaced enclosure fan, cleared dust filters. Temp returned to nominal.",
    ),
}

# ─── Helper: load a Lakebase token via CLI ───────────────────────────────────


def get_db_token() -> str:
    """Call `databricks database generate-database-credential` — Gotcha #36 requires request_id."""
    payload = json.dumps({"instance_names": [INSTANCE_NAME], "request_id": "seed"})
    out = subprocess.check_output(
        ["databricks", "database", "generate-database-credential",
         "--profile", PROFILE, "--json", payload],
        text=True,
    )
    return json.loads(out)["token"]


def get_pg_host() -> str:
    out = subprocess.check_output(
        ["databricks", "database", "get-database-instance", INSTANCE_NAME,
         "--profile", PROFILE, "-o", "json"],
        text=True,
    )
    return json.loads(out)["read_write_dns"]


# ─── Connect ─────────────────────────────────────────────────────────────────

print(f"Fetching DB credential for {INSTANCE_NAME} …")
token = get_db_token()
host = get_pg_host()
print(f"Connecting to {host}/{DATABASE_NAME} as {PG_USER} …")

conn = psycopg2.connect(
    host=host, port=5432, dbname=DATABASE_NAME,
    user=PG_USER, password=token, sslmode=PG_SSLMODE,
)
conn.autocommit = True
cur = conn.cursor()
print("Connected.")

# ─── Build a small reference list of fake robot_ids to seed against ──────────

robots_ref = []
for fam in FAMILIES:
    for site in FC_SITES:
        for i in range(_count := 12 if fam == "Hercules" else 6):
            robots_ref.append({
                "robot_id": f"{fam[:3].upper()}-{site}-{i:03d}",
                "family": fam,
                "fc_site": site,
            })
random.shuffle(robots_ref)

# ─── Seed: work_orders (~120 rows) ───────────────────────────────────────────

WO_STATUSES_WEIGHTED = (
    ["completed"] * 12 + ["in_progress"] * 4 + ["open"] * 3 +
    ["awaiting_parts"] * 1 + ["cancelled"] * 0
)

print("Seeding work_orders …")
wo_count = 0
for r in robots_ref[:120]:
    fam = r["family"]
    fam_signals = SIGNALS_BY_FAMILY[fam]
    signal = random.choice(fam_signals)
    if (fam, signal) not in DIAG_TEMPLATES:
        signal = next(s for s in fam_signals if (fam, s) in DIAG_TEMPLATES)
    diag_tpl, remed = DIAG_TEMPLATES[(fam, signal)]
    val = random.uniform(20.0, 80.0)
    status = random.choice(WO_STATUSES_WEIGHTED)
    severity = random.choices(
        ["low", "medium", "high", "critical"], weights=[10, 35, 35, 20]
    )[0]
    priority = "urgent" if severity == "critical" else "high" if severity == "high" else "normal"
    created = NOW - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
    resolved = created + timedelta(hours=random.randint(2, 72)) if status == "completed" else None
    wo_num = f"WO-{fam[:3].upper()}-{created.strftime('%y%m')}-{wo_count:04d}"
    title = f"{signal.replace('_', ' ').title()} anomaly — {r['robot_id']}"
    cur.execute(
        """INSERT INTO work_orders
           (wo_number, robot_id, family, fc_site, source_anomaly_id, severity, priority,
            title, root_cause, remediation_steps, parts_used, manual_refs, technician,
            status, created_by, created_at, resolved_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s)""",
        (
            wo_num, r["robot_id"], fam, r["fc_site"],
            f"ANOM-{random.randint(1, 3000):06d}",
            severity, priority, title,
            diag_tpl.format(val=val), remed,
            json.dumps([{"part": "AR-SPR-VAC-005", "qty": 1}] if "vacuum" in signal else []),
            json.dumps([{"section": signal.replace("_", " ").title(), "page": random.randint(8, 64)}]),
            random.choice(TECHNICIANS), status,
            "agent" if random.random() < 0.6 else random.choice(TECHNICIANS),
            created, resolved,
        ),
    )
    wo_count += 1
print(f"  inserted {wo_count} work_orders")

# ─── Seed: agent_memory (~80 rows, mixed model_version) ──────────────────────

print("Seeding agent_memory …")
mem_count = 0
# Split: 40 rows under sparrow-detect-v4 (older — pre-upgrade) and 40 distributed across v5 + other families
for r in robots_ref[:80]:
    fam = r["family"]
    fam_signals = SIGNALS_BY_FAMILY[fam]
    signal = random.choice(fam_signals)
    if (fam, signal) not in DIAG_TEMPLATES:
        signal = next(s for s in fam_signals if (fam, s) in DIAG_TEMPLATES)
    diag_tpl, remed = DIAG_TEMPLATES[(fam, signal)]
    val = random.uniform(20.0, 80.0)
    persona = random.choices(["rme_tech", "rme_lead"], weights=[70, 30])[0]
    outcome = random.choices(
        ["resolved", "recurring", "escalated", "no_action", "pending"],
        weights=[60, 15, 10, 10, 5],
    )[0]
    # The differentiator: half the Sparrow rows are pre-upgrade (v4), half are post (v5)
    if fam == "Sparrow":
        model_version = random.choice(["sparrow-detect-v4", "sparrow-detect-v5"])
    else:
        model_version = DETECT_MODELS_BY_FAMILY[fam][0]
    created = NOW - timedelta(days=random.randint(0, 120))
    cur.execute(
        """INSERT INTO agent_memory
           (robot_id, family, fc_site, signal, persona, diagnostic, remediation,
            outcome, model_version, source_anomaly_id, confidence, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            r["robot_id"], fam, r["fc_site"], signal, persona,
            diag_tpl.format(val=val), remed,
            outcome, model_version,
            f"ANOM-{random.randint(1, 3000):06d}",
            round(random.uniform(0.72, 0.97), 2),
            created,
        ),
    )
    mem_count += 1
print(f"  inserted {mem_count} agent_memory rows")

# ─── Seed: workflows (~20 rows, mix in-progress + pending_approval) ──────────

print("Seeding workflows …")
wf_count = 0
workflow_types = ["work_order_create", "anomaly_escalate", "memory_persist"]
for i in range(20):
    r = random.choice(robots_ref)
    fam = r["family"]
    fam_signals = SIGNALS_BY_FAMILY[fam]
    signal = random.choice(fam_signals)
    severity = random.choices(["medium", "high", "critical"], weights=[40, 40, 20])[0]
    wf_type = random.choice(workflow_types)
    status = random.choices(["pending_approval", "in_progress"], weights=[70, 30])[0]
    summary_map = {
        "work_order_create": f"Recommend opening a work order for {r['robot_id']} ({signal.replace('_',' ')} anomaly).",
        "anomaly_escalate":   f"Escalating recurring {signal.replace('_',' ')} anomaly cluster on {fam} family at {r['fc_site']}.",
        "memory_persist":     f"Persist diagnostic for {r['robot_id']} so future {fam} upgrades retain context.",
    }
    headline_map = {
        "work_order_create": f"Open WO for {r['robot_id']}",
        "anomaly_escalate":   f"Escalate {fam} cluster at {r['fc_site']}",
        "memory_persist":     f"Persist {fam} diagnostic",
    }
    reasoning = [
        {"agent": "fleet-telemetry-genie", "step": "queried recent anomalies", "result": "found pattern"},
        {"agent": "service-manuals-ka", "step": "retrieved remediation", "result": "matched section"},
    ]
    cur.execute(
        """INSERT INTO workflows
           (workflow_type, trigger_source, severity, summary, reasoning_chain,
            entity_type, entity_id, status, headline)
           VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)""",
        (
            wf_type, "monitor", severity,
            summary_map[wf_type], json.dumps(reasoning),
            "robot", r["robot_id"], status, headline_map[wf_type],
        ),
    )
    wf_count += 1
print(f"  inserted {wf_count} workflows")

# ─── Seed: agent_actions (~50 rows) ──────────────────────────────────────────

print("Seeding agent_actions …")
aa_count = 0
for i in range(50):
    r = random.choice(robots_ref)
    severity = random.choices(["low", "medium", "high", "critical"], weights=[20, 50, 25, 5])[0]
    status   = random.choices(["executed", "dismissed", "pending", "failed"], weights=[70, 15, 10, 5])[0]
    cur.execute(
        """INSERT INTO agent_actions
           (action_type, severity, entity_type, entity_id, description, action_taken, status)
           VALUES (%s,%s,%s,%s,%s,%s,%s)""",
        (
            random.choice([
                "anomaly_triage", "work_order_open", "manual_lookup",
                "memory_persist", "redeploy_recommend",
            ]),
            severity, "robot", r["robot_id"],
            f"Reviewed {r['family']} {r['robot_id']} signal cluster",
            "Created work order and notified RME tech" if status == "executed" else "No action taken",
            status,
        ),
    )
    aa_count += 1
print(f"  inserted {aa_count} agent_actions")

# ─── Seed: notes (~30 rows) ──────────────────────────────────────────────────

print("Seeding notes …")
n_count = 0
for i in range(30):
    r = random.choice(robots_ref)
    cur.execute(
        """INSERT INTO notes (entity_type, entity_id, note_text, author)
           VALUES (%s,%s,%s,%s)""",
        (
            "robot", r["robot_id"],
            random.choice([
                f"PM compliance verified by {random.choice(TECHNICIANS)} on last visit.",
                f"Note: this {r['family']} has flagged {random.choice(SIGNALS_BY_FAMILY[r['family']]).replace('_',' ')} twice this month.",
                "Operator reports occasional noise during pick — investigated, no fault found.",
                "Coordination with Inbound: tote weight calibration adjusted.",
            ]),
            random.choice(["agent", "rme-tech", "rme-lead"] + TECHNICIANS),
        ),
    )
    n_count += 1
print(f"  inserted {n_count} notes")

cur.close()
conn.close()
print("\n✅ Seed complete.")
