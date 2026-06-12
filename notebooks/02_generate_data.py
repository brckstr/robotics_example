# Databricks notebook source
# Physical AI Fleet Health Console — Data Generation
# Generates Delta Lake tables for the Amazon Robotics fleet health demo.
# Hash-based deterministic generation — re-running produces identical data.

# COMMAND ----------

CATALOG = "amz_robotics_9868sm_catalog"
SCHEMA = "fleet_health"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

import hashlib
from datetime import date, timedelta
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, DateType, LongType, BooleanType, TimestampType,
)

TODAY = date.today()


def _hash_float(seed: str, lo: float, hi: float) -> float:
    h = int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16)
    return lo + (h / 0xFFFFFFFF) * (hi - lo)


def _hash_int(seed: str, lo: int, hi: int) -> int:
    return int(_hash_float(seed, lo, hi + 0.999))


def _hash_choice(seed: str, options: list):
    return options[_hash_int(seed, 0, len(options) - 1)]


def _hash_weighted(seed: str, options: list, weights: list):
    h = _hash_float(seed, 0.0, 1.0)
    cumulative = 0.0
    total = sum(weights)
    for i, w in enumerate(weights):
        cumulative += w / total
        if h <= cumulative:
            return options[i]
    return options[-1]


def _hash_bool(seed: str, true_prob: float) -> bool:
    return _hash_float(seed, 0.0, 1.0) < true_prob


# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS — Amazon Robotics fleet domain
# ═══════════════════════════════════════════════════════════════════════════

# Fulfillment Center sites — sortable + non-sortable + next-gen mix
FC_SITES = [
    {"site": "BFI4", "region": "Pacific NW",  "type": "next_gen",     "fleet_size": 180, "criticality": "high"},
    {"site": "PDX9", "region": "Pacific NW",  "type": "sortable",     "fleet_size": 140, "criticality": "medium"},
    {"site": "SHV1", "region": "South",       "type": "next_gen",     "fleet_size": 220, "criticality": "high"},
    {"site": "CMH1", "region": "Midwest",     "type": "sortable",     "fleet_size": 110, "criticality": "medium"},
    {"site": "PHL7", "region": "Northeast",   "type": "non_sortable", "fleet_size": 90,  "criticality": "medium"},
    {"site": "ABE8", "region": "Northeast",   "type": "next_gen",     "fleet_size": 160, "criticality": "high"},
]

# Robot families with firmware lineages
FAMILIES = [
    {
        "name": "Sparrow",
        "kind": "item_pick_arm",
        "firmware_versions": ["sparrow-fw-3.4", "sparrow-fw-3.5", "sparrow-fw-3.6"],
        "detect_models": ["sparrow-detect-v4", "sparrow-detect-v5"],
        "signals": ["vibration", "joint_torque", "joint_temperature", "vacuum_pressure", "camera_health"],
    },
    {
        "name": "Hercules",
        "kind": "drive_unit",
        "firmware_versions": ["hercules-fw-2.7", "hercules-fw-2.8"],
        "detect_models": ["hercules-detect-v3"],
        "signals": ["battery_soh", "motor_current", "wheel_encoder", "nav_odometry"],
    },
    {
        "name": "Proteus",
        "kind": "amr",
        "firmware_versions": ["proteus-fw-1.5", "proteus-fw-1.6"],
        "detect_models": ["proteus-detect-v2"],
        "signals": ["lidar_health", "battery_soh", "drive_motor", "safety_sensor", "dock_alignment"],
    },
    {
        "name": "Sequoia",
        "kind": "storage_gantry",
        "firmware_versions": ["sequoia-fw-1.2", "sequoia-fw-1.3"],
        "detect_models": ["sequoia-detect-v2"],
        "signals": ["rail_position", "lift_motor", "tote_actuator", "gantry_temperature"],
    },
]

ROBOT_STATUSES = ["operational", "degraded", "maintenance", "offline"]
ROBOT_STATUS_WEIGHTS = [80, 12, 5, 3]

ANOMALY_SEVERITIES = ["low", "medium", "high", "critical"]
ANOMALY_SEVERITY_WEIGHTS = [40, 35, 18, 7]

ANOMALY_STATUSES = ["open", "investigating", "resolved", "false_positive"]
ANOMALY_STATUS_WEIGHTS = [25, 15, 55, 5]

HISTORY_DAYS = 180  # 6 months
START_DATE = TODAY - timedelta(days=HISTORY_DAYS)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 1: robots
# ═══════════════════════════════════════════════════════════════════════════

robots = []
robot_seq = 0
for site in FC_SITES:
    site_name = site["site"]
    # Distribute families across the FC (roughly even, weighted by family)
    family_alloc = {
        "Sparrow":  int(site["fleet_size"] * 0.30),
        "Hercules": int(site["fleet_size"] * 0.45),
        "Proteus":  int(site["fleet_size"] * 0.15),
        "Sequoia":  int(site["fleet_size"] * 0.10),
    }
    for fam in FAMILIES:
        for i in range(family_alloc[fam["name"]]):
            robot_seq += 1
            seed_base = f"robot:{site_name}:{fam['name']}:{i}"
            robot_id = f"{fam['name'][:3].upper()}-{site_name}-{i:03d}"
            firmware = _hash_choice(seed_base + ":fw", fam["firmware_versions"])
            status = _hash_weighted(seed_base + ":status", ROBOT_STATUSES, ROBOT_STATUS_WEIGHTS)
            install_date = TODAY - timedelta(days=_hash_int(seed_base + ":install", 30, 720))
            robots.append(Row(
                robot_id=robot_id,
                family=fam["name"],
                kind=fam["kind"],
                fc_site=site_name,
                region=site["region"],
                fc_type=site["type"],
                firmware_version=firmware,
                detect_model=_hash_choice(seed_base + ":dm", fam["detect_models"]),
                status=status,
                install_date=install_date,
                hours_operated=_hash_int(seed_base + ":hrs", 500, 12000),
            ))

robots_schema = StructType([
    StructField("robot_id", StringType(), False),
    StructField("family", StringType(), False),
    StructField("kind", StringType(), False),
    StructField("fc_site", StringType(), False),
    StructField("region", StringType(), False),
    StructField("fc_type", StringType(), False),
    StructField("firmware_version", StringType(), False),
    StructField("detect_model", StringType(), False),
    StructField("status", StringType(), False),
    StructField("install_date", DateType(), False),
    StructField("hours_operated", IntegerType(), False),
])

robots_df = spark.createDataFrame(robots, robots_schema)
robots_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.robots")
print(f"robots: {robots_df.count()} rows")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 2: telemetry  (daily-aggregated readings per robot per signal)
# ═══════════════════════════════════════════════════════════════════════════

# To keep the volume reasonable, sample ~25% of robots per day at one reading per signal.
# Total ~ robots(800) * 0.25 * signals(~5) * days(180) ~= 180k rows.

telemetry_rows = []
robot_specs = [(r["robot_id"], r["family"], r["fc_site"], r["status"]) for r in robots]

# Signal baseline ranges (healthy operating range)
SIGNAL_RANGES = {
    "vibration":          (0.5, 2.0),     # mm/s RMS — Sparrow joints
    "joint_torque":       (10.0, 60.0),   # Nm
    "joint_temperature":  (35.0, 65.0),   # C
    "vacuum_pressure":    (50.0, 85.0),   # kPa (suction)
    "camera_health":      (95.0, 100.0),  # score 0-100
    "battery_soh":        (70.0, 99.0),   # state of health %
    "motor_current":      (5.0, 15.0),    # A
    "wheel_encoder":      (98.0, 100.0),  # accuracy %
    "nav_odometry":       (97.0, 100.0),  # accuracy %
    "lidar_health":       (95.0, 100.0),
    "drive_motor":        (5.0, 25.0),    # A
    "safety_sensor":      (98.0, 100.0),  # availability %
    "dock_alignment":     (0.0, 2.5),     # cm offset
    "rail_position":      (0.0, 1.5),     # mm error
    "lift_motor":         (8.0, 30.0),    # A
    "tote_actuator":      (0.0, 100.0),   # cycle health %
    "gantry_temperature": (20.0, 55.0),
}

for r_robot_id, r_family, r_site, r_status in robot_specs:
    fam_signals = next(f["signals"] for f in FAMILIES if f["name"] == r_family)
    # Sample 30 days from the 180-day window per robot (deterministic stride)
    for day_idx in range(0, HISTORY_DAYS, 6):
        reading_date = START_DATE + timedelta(days=day_idx)
        for sig in fam_signals:
            seed = f"tel:{r_robot_id}:{sig}:{day_idx}"
            lo, hi = SIGNAL_RANGES[sig]
            # Inject degradation for "degraded" robots — drift toward boundary
            if r_status == "degraded":
                lo = lo + (hi - lo) * 0.4
                hi = hi + (hi - lo) * 0.2
            value = _hash_float(seed, lo, hi)
            health_score = max(0.0, min(100.0, 100.0 - abs(value - (lo + hi) / 2) / max(1.0, (hi - lo)) * 25.0))
            telemetry_rows.append(Row(
                robot_id=r_robot_id,
                family=r_family,
                fc_site=r_site,
                signal=sig,
                reading_date=reading_date,
                reading_value=round(value, 3),
                health_score=round(health_score, 1),
            ))

telemetry_schema = StructType([
    StructField("robot_id", StringType(), False),
    StructField("family", StringType(), False),
    StructField("fc_site", StringType(), False),
    StructField("signal", StringType(), False),
    StructField("reading_date", DateType(), False),
    StructField("reading_value", DoubleType(), False),
    StructField("health_score", DoubleType(), False),
])

telemetry_df = spark.createDataFrame(telemetry_rows, telemetry_schema)
telemetry_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.telemetry")
print(f"telemetry: {telemetry_df.count()} rows")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 3: anomalies  (detected events across the 6-month window)
# ═══════════════════════════════════════════════════════════════════════════

# Generate ~3000 anomalies — ~5-10 per active robot over 6 months
anomaly_rows = []
anomaly_seq = 0

for r in robots:
    family_def = next(f for f in FAMILIES if f["name"] == r["family"])
    fam_signals = family_def["signals"]
    detect_models = family_def["detect_models"]

    # More anomalies for degraded/maintenance robots
    base_count = {"operational": 3, "degraded": 9, "maintenance": 12, "offline": 2}[r["status"]]
    count = base_count + _hash_int(f"acount:{r['robot_id']}", 0, 4)

    for i in range(count):
        anomaly_seq += 1
        seed = f"anom:{r['robot_id']}:{i}"
        detected_offset = _hash_int(seed + ":offset", 0, HISTORY_DAYS - 1)
        detected_at = TODAY - timedelta(days=detected_offset)
        severity = _hash_weighted(seed + ":sev", ANOMALY_SEVERITIES, ANOMALY_SEVERITY_WEIGHTS)
        status = _hash_weighted(seed + ":stat", ANOMALY_STATUSES, ANOMALY_STATUS_WEIGHTS)
        # Older anomalies more likely to be resolved
        if detected_offset > 14 and status == "open":
            status = "resolved"
        signal = _hash_choice(seed + ":sig", fam_signals)
        model_version = _hash_choice(seed + ":mv", detect_models)
        confidence = round(_hash_float(seed + ":conf", 0.60, 0.99), 2)
        # Auto-resolved fraction — for the "autonomous resolution rate" KPI
        auto_resolved = (status == "resolved") and _hash_bool(seed + ":auto", 0.65)
        # Estimated downtime impact in minutes
        downtime_minutes = {"low": 5, "medium": 15, "high": 60, "critical": 180}[severity]
        downtime_minutes = downtime_minutes + _hash_int(seed + ":dt", 0, downtime_minutes // 2)
        if status not in ("resolved", "false_positive"):
            downtime_minutes = 0  # not yet accumulated
        anomaly_rows.append(Row(
            anomaly_id=f"ANOM-{anomaly_seq:06d}",
            robot_id=r["robot_id"],
            family=r["family"],
            fc_site=r["fc_site"],
            signal=signal,
            severity=severity,
            detected_at=detected_at,
            detect_model_version=model_version,
            confidence=confidence,
            status=status,
            auto_resolved=auto_resolved,
            downtime_minutes=downtime_minutes,
        ))

anomaly_schema = StructType([
    StructField("anomaly_id", StringType(), False),
    StructField("robot_id", StringType(), False),
    StructField("family", StringType(), False),
    StructField("fc_site", StringType(), False),
    StructField("signal", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("detected_at", DateType(), False),
    StructField("detect_model_version", StringType(), False),
    StructField("confidence", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("auto_resolved", BooleanType(), False),
    StructField("downtime_minutes", IntegerType(), False),
])

anomalies_df = spark.createDataFrame(anomaly_rows, anomaly_schema)
anomalies_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.anomalies")
print(f"anomalies: {anomalies_df.count()} rows")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# TABLE 4: service_manual_chunks  (RAG corpus, governed via UC)
# ═══════════════════════════════════════════════════════════════════════════

# Synthetic manual chunks: family + manual section + page + body
# Real demo would point a Knowledge Assistant at this Delta table.

MANUAL_SECTIONS = {
    "Sparrow": [
        ("Vacuum / Suction System", "vacuum_pressure",
         "Inspect the vacuum generator manifold. With suction enabled, expected kPa is 65-85. "
         "Below 60 kPa indicates a leak — check the gripper interface seal (P/N AR-SPR-VAC-005) "
         "and the in-line filter (P/N AR-SPR-FLT-002). Replace filter every 2000 cycles."),
        ("Joint Health", "joint_torque",
         "Joint torque exceeding 60 Nm during a standard pick cycle indicates mechanical resistance. "
         "Inspect harmonic gear lubrication (P/N AR-LUB-HG-100) and check belt tension. "
         "Recommended torque calibration interval: 90 days."),
        ("Vision System", "camera_health",
         "Camera exposure errors typically indicate dust on the IR illuminator or condensation on the lens. "
         "Clean with dry lint-free cloth (do not use solvents). Recalibrate via the RME console > Vision > Calibrate."),
        ("Vibration Diagnostics", "vibration",
         "Sustained vibration above 2.0 mm/s RMS on joint 4 or joint 5 is a leading indicator of bearing wear. "
         "Schedule preventive bearing replacement at 8000 operating hours."),
    ],
    "Hercules": [
        ("Battery Maintenance", "battery_soh",
         "Battery State of Health below 70% requires replacement to maintain the 8-hour shift envelope. "
         "Use balanced charging mode for cells with imbalance > 50 mV (replacement P/N AR-HRC-BAT-018)."),
        ("Drive Motor Diagnostics", "motor_current",
         "Sustained motor current above 15 A on level floor indicates excessive load or wheel binding. "
         "Inspect drive wheel for debris and check axle bearing play. Lubricate axle quarterly."),
        ("Navigation & Odometry", "wheel_encoder",
         "Wheel encoder accuracy below 98% degrades pose estimation. Recalibrate via 'hercules_calib_nav.py' "
         "with the robot on the calibration mat at the maintenance bay."),
    ],
    "Proteus": [
        ("LiDAR Maintenance", "lidar_health",
         "Proteus uses a 360° rotating LiDAR. Health score below 95% suggests window contamination — "
         "clean with isopropyl wipe (P/N AR-PRO-LCW-001). Internal failure requires full LiDAR swap."),
        ("Safety Sensor Audit", "safety_sensor",
         "All Proteus units operate in human-occupied workflows. Any safety sensor below 100% availability "
         "must trigger an immediate work order — robot is restricted to non-human zones until repaired."),
        ("Dock Alignment", "dock_alignment",
         "Dock alignment offset above 2.5 cm causes failed GoCart handoffs. Verify dock fiducials are clean "
         "and run 'proteus_dock_recal' from the RME console."),
    ],
    "Sequoia": [
        ("Rail & Lift", "lift_motor",
         "Lift motor current above 30 A indicates either an overweight tote (escalate to Inbound) or "
         "rail debris. Inspect the rail track between bins 4-6 most commonly."),
        ("Tote Handling", "tote_actuator",
         "Tote actuator cycle health below 90% predicts a failure within ~500 cycles. "
         "Replace per P/N AR-SEQ-TOT-009 during the next scheduled maintenance window."),
    ],
}

CATALOG_URI_PREFIX = f"uc://catalogs/{CATALOG}/schemas/{SCHEMA}/volumes/manuals"

manual_rows = []
chunk_seq = 0
for family, sections in MANUAL_SECTIONS.items():
    page = 1
    for section, signal, body in sections:
        chunk_seq += 1
        # Split body into 2-3 chunks per section so the KA has multiple retrievable units
        sentences = [s.strip() for s in body.split(".") if s.strip()]
        for chunk_i, sent_group in enumerate([sentences[:2], sentences[2:]] if len(sentences) > 2 else [sentences]):
            if not sent_group:
                continue
            chunk_seq += 1
            chunk_text = ". ".join(sent_group) + "."
            manual_rows.append(Row(
                chunk_id=f"CHK-{chunk_seq:05d}",
                family=family,
                manual_section=section,
                page=page,
                signal_tag=signal,
                chunk_text=chunk_text,
                source_uri=f"{CATALOG_URI_PREFIX}/{family.lower()}_manual.pdf",
                source_doc=f"{family} Service Manual",
            ))
            page += 1

manual_schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("family", StringType(), False),
    StructField("manual_section", StringType(), False),
    StructField("page", IntegerType(), False),
    StructField("signal_tag", StringType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("source_uri", StringType(), False),
    StructField("source_doc", StringType(), False),
])

manual_df = spark.createDataFrame(manual_rows, manual_schema)
manual_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.service_manual_chunks")
print(f"service_manual_chunks: {manual_df.count()} rows")

# COMMAND ----------

# Verify all tables
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)
for t in ["robots", "telemetry", "anomalies", "service_manual_chunks"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.{t}").collect()[0]["n"]
    print(f"{t}: {cnt:,} rows")
