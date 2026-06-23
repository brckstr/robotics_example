"""
Fleet Diagnostic Guidance Chain — deterministic pre-flight checks for the
Amazon Robotics Fleet Health copilot.

A common failure mode for agentic systems on physical fleets is "premature
remediation": the agent jumps to a fix before it has the data needed to
defend the recommendation. Operators get burned, then they stop trusting
the agent.

This chain enforces a deterministic policy:
  1. Classify the user question into (robot_family, issue_type) using an LLM.
  2. Look up the required pre-flight metric checks in a hand-curated policy
     registry — no LLM in the loop for the lookup itself, so the rules are
     auditable and reviewable by an RME Lead.
  3. Return a structured payload that the downstream agent (MAS supervisor)
     uses to enrich its prompt: "before answering, you MUST run these
     queries and cite the results."

Result: the agent always grounds remediation in fresh telemetry, the
operator can replay every decision, and the RME team (or Project Eluna
team) can audit and update the policy without retraining anything.

This module is designed to be deployed three ways:

  (a) As a LangChain Runnable inside a Databricks model-serving endpoint
      (mlflow.langchain.log_model). The MAS supervisor calls it as a
      sub-agent before invoking Genie or the Knowledge Assistant.

  (b) As a Unity Catalog Python function (CREATE FUNCTION ... LANGUAGE
      PYTHON) so the agent can call it via the MAS supervisor's
      unity-catalog-function tool type.

  (c) Standalone — run this file directly (`python fleet_guidance_chain.py`)
      to print example outputs for the demo team.

The policy registry lives at the top of this file and is the only thing an
RME Lead needs to touch when a new failure mode is identified.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Literal, Optional

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableLambda, RunnablePassthrough
from pydantic import BaseModel, Field

# ─────────────────────────────────────────────────────────────────────────────
# 1. Policy registry — the deterministic part
# ─────────────────────────────────────────────────────────────────────────────
#
# Schema:  policy[robot_family][issue_type] = CheckPolicy(...)
#
# Each policy lists:
#   required_checks      — telemetry/Lakebase queries the agent MUST run.
#   manual_sections      — KA chunks the agent MUST cite before a remediation.
#   blocking_questions   — open questions for the operator if data is missing.
#   never_recommend_until — guardrail clause embedded verbatim in the system
#                          prompt the agent receives back.

RobotFamily = Literal["Sparrow", "Hercules", "Proteus", "Sequoia"]
IssueType = Literal[
    "battery", "vibration", "vacuum", "joint_thermal", "vision",
    "navigation", "lidar", "safety_sensor", "dock_alignment", "gantry_rail",
    "tote_handling", "unknown",
]


@dataclass(frozen=True)
class CheckPolicy:
    required_checks: tuple[str, ...]
    manual_sections: tuple[str, ...]
    blocking_questions: tuple[str, ...]
    never_recommend_until: str


POLICY: dict[RobotFamily, dict[IssueType, CheckPolicy]] = {
    "Sparrow": {
        "vacuum": CheckPolicy(
            required_checks=(
                "telemetry: vacuum_pressure trend, last 14 days, by robot_id",
                "telemetry: pick cycle count since last filter change",
                "lakebase: most recent work_order on this robot with signal='vacuum_pressure'",
                "lakebase: agent_memory rows where signal='vacuum_pressure' AND family='Sparrow'",
            ),
            manual_sections=(
                "Sparrow Service Manual — Vacuum / Suction System",
            ),
            blocking_questions=(
                "When was the gripper seal (AR-SPR-VAC-005) last replaced?",
                "Has the inline filter (AR-SPR-FLT-002) been changed in the last 2,000 cycles?",
            ),
            never_recommend_until=(
                "Never recommend a vacuum-pressure remediation without first reporting "
                "the 14-day pressure trend, the cycle count since last filter change, "
                "and any prior agent_memory entries for vacuum_pressure on the same robot."
            ),
        ),
        "vibration": CheckPolicy(
            required_checks=(
                "telemetry: vibration RMS by joint, last 30 days, weekly aggregation",
                "telemetry: joint_torque values for the same joints over the same window",
                "robots: hours_operated and firmware_version for this robot",
                "lakebase: agent_memory rows where signal='vibration' on same family + similar joint",
            ),
            manual_sections=(
                "Sparrow Service Manual — Vibration Diagnostics",
                "Sparrow Service Manual — Joint Health",
            ),
            blocking_questions=(
                "What is the cumulative operating hours? Bearing replacement is scheduled at 8,000 hrs.",
                "Has the harmonic gear been re-lubricated in the last 90 days?",
            ),
            never_recommend_until=(
                "Never recommend bearing replacement without the 30-day RMS trend, "
                "the correlated joint_torque values, and the robot's total operating hours."
            ),
        ),
        "joint_thermal": CheckPolicy(
            required_checks=(
                "telemetry: joint_temperature trend by joint, last 7 days",
                "telemetry: joint_torque correlated with the same joint",
                "robots: ambient FC temperature if available, last 24h",
            ),
            manual_sections=("Sparrow Service Manual — Joint Health",),
            blocking_questions=(
                "Is the FC HVAC system operating normally for this site?",
                "When was the joint cooling vent last cleared of dust?",
            ),
            never_recommend_until=(
                "Never blame the joint motor without first checking torque correlation "
                "and FC ambient conditions — could be environmental."
            ),
        ),
        "vision": CheckPolicy(
            required_checks=(
                "telemetry: camera_health score trend, last 14 days",
                "lakebase: notes on this robot mentioning 'illuminator' or 'lens' in last 30 days",
            ),
            manual_sections=("Sparrow Service Manual — Vision System",),
            blocking_questions=(
                "Has the IR illuminator been cleaned in the last 30 days?",
                "Any condensation reported on the lens?",
            ),
            never_recommend_until=(
                "Never recommend camera replacement without ruling out contamination first."
            ),
        ),
    },
    "Hercules": {
        "battery": CheckPolicy(
            required_checks=(
                "telemetry: battery_soh trend, last 60 days, weekly aggregation",
                "telemetry: motor_current correlated with same time window",
                "robots: hours_operated and install_date for this robot",
                "lakebase: agent_memory rows where signal='battery_soh' AND family='Hercules'",
                "lakebase: work_orders history for this robot involving battery",
            ),
            manual_sections=("Hercules Service Manual — Battery Maintenance",),
            blocking_questions=(
                "What is the maximum charge reached in the last 30 days? (rule of thumb: <85% → degradation)",
                "How many full charge cycles since install?",
                "Is the charging station throwing balance warnings for this robot? (cells with imbalance >50mV need balanced charging)",
                "Is the SoH decline linear or stepped? Stepped suggests a single cell failure, linear suggests end of life.",
            ),
            never_recommend_until=(
                "Never recommend battery replacement (AR-HRC-BAT-018) without first reporting: "
                "(1) the 60-day SoH trend, (2) the maximum charge reached in the last 30 days, "
                "(3) full charge cycle count since install, and (4) any imbalance warnings "
                "from the charging station. Replacing a battery that is actually a charging-station "
                "fault is a $4K mistake and a multi-hour redeploy hit."
            ),
        ),
        "navigation": CheckPolicy(
            required_checks=(
                "telemetry: wheel_encoder accuracy and nav_odometry accuracy, last 7 days",
                "telemetry: motor_current to rule out wheel binding",
                "lakebase: work_orders involving 'navigation' or 'fiducial' on this robot",
            ),
            manual_sections=("Hercules Service Manual — Navigation & Odometry",),
            blocking_questions=(
                "When was the fiducial map for this aisle last re-baselined?",
                "Have nearby Hercules robots seen the same degradation? (could be infrastructure)",
            ),
            never_recommend_until=(
                "Never recommend an encoder recalibration without first checking whether the issue "
                "is robot-local (this robot only) vs aisle-wide (fiducial map drift)."
            ),
        ),
    },
    "Proteus": {
        "lidar": CheckPolicy(
            required_checks=(
                "telemetry: lidar_health trend, last 14 days",
                "telemetry: safety_sensor availability over same window",
                "lakebase: notes mentioning 'lidar' or 'window' in last 30 days",
            ),
            manual_sections=("Proteus Service Manual — LiDAR Maintenance",),
            blocking_questions=(
                "Has the LiDAR window been cleaned with isopropyl wipe in the last 14 days?",
                "Is the degradation in one rotation arc or all-around?",
            ),
            never_recommend_until=(
                "Never recommend full LiDAR swap (~$6K) without first verifying that "
                "isopropyl cleaning was attempted and that the degradation is internal "
                "(all-arcs) rather than external (single-arc contamination)."
            ),
        ),
        "safety_sensor": CheckPolicy(
            required_checks=(
                "telemetry: safety_sensor availability, last 24h",
                "lakebase: any open exceptions on this robot",
            ),
            manual_sections=("Proteus Service Manual — Safety Sensor Audit",),
            blocking_questions=(
                "Has the robot been restricted to non-human zones since the alert?",
            ),
            never_recommend_until=(
                "ANY safety sensor below 100% MUST trigger a work order immediately. "
                "The robot is restricted from human-occupied zones until repaired. "
                "Do not recommend continued operation."
            ),
        ),
        "dock_alignment": CheckPolicy(
            required_checks=(
                "telemetry: dock_alignment offset trend, last 7 days",
                "lakebase: GoCart handoff failure rate (work_orders mentioning 'handoff')",
            ),
            manual_sections=("Proteus Service Manual — Dock Alignment",),
            blocking_questions=(
                "Are dock fiducials clean?",
                "Has proteus_dock_recal been run since the issue started?",
            ),
            never_recommend_until=(
                "Never recommend dock hardware replacement without first running "
                "proteus_dock_recal and reporting the resulting offset."
            ),
        ),
    },
    "Sequoia": {
        "gantry_rail": CheckPolicy(
            required_checks=(
                "telemetry: rail_position error trend, last 14 days",
                "telemetry: lift_motor current during the same window",
                "lakebase: notes mentioning 'rail debris' or 'bin 4' or 'bin 5' or 'bin 6'",
            ),
            manual_sections=("Sequoia Service Manual — Rail & Lift",),
            blocking_questions=(
                "Has the rail track between bins 4-6 been visually inspected?",
                "Is the lift current spike correlated with specific tote weights?",
            ),
            never_recommend_until=(
                "Never recommend gantry hardware replacement without first ruling out "
                "rail debris and overweight totes (escalate to Inbound if weight is the issue)."
            ),
        ),
        "tote_handling": CheckPolicy(
            required_checks=(
                "telemetry: tote_actuator cycle health, last 30 days",
                "robots: cumulative cycles since last actuator replacement",
            ),
            manual_sections=("Sequoia Service Manual — Tote Handling",),
            blocking_questions=(
                "What is the cumulative cycle count? Failure predicted within 500 cycles when health <90%.",
            ),
            never_recommend_until=(
                "Never recommend tote actuator (AR-SEQ-TOT-009) replacement without first "
                "reporting the 30-day cycle health trend and remaining cycles to predicted failure."
            ),
        ),
    },
}


def lookup_policy(family: RobotFamily, issue: IssueType) -> Optional[CheckPolicy]:
    """Pure-Python policy lookup. Returns None if no policy is defined for this combination."""
    return POLICY.get(family, {}).get(issue)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Pydantic models — typed I/O for the chain
# ─────────────────────────────────────────────────────────────────────────────


class Classification(BaseModel):
    """LLM output: extracted robot family + issue type from the user question."""
    robot_family: RobotFamily = Field(
        description="One of Sparrow, Hercules, Proteus, Sequoia. Use 'Sparrow' as a fallback if truly ambiguous."
    )
    issue_type: IssueType = Field(
        description=(
            "One of: battery, vibration, vacuum, joint_thermal, vision, navigation, "
            "lidar, safety_sensor, dock_alignment, gantry_rail, tote_handling, unknown."
        )
    )
    robot_id: Optional[str] = Field(
        default=None,
        description="Specific robot identifier if mentioned (e.g., SPA-BFI4-007). Null if not specified."
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Classifier confidence 0.0–1.0. Below 0.6 should be treated as 'unknown' by the caller."
    )


class GuidancePayload(BaseModel):
    """Final structured output the caller (MAS supervisor) consumes."""
    classification: Classification
    policy_matched: bool
    required_checks: list[str] = Field(default_factory=list)
    manual_sections: list[str] = Field(default_factory=list)
    blocking_questions: list[str] = Field(default_factory=list)
    system_prompt_supplement: str = Field(
        default="",
        description=(
            "A block of text the MAS supervisor should append to its system "
            "prompt for this turn. Encodes the never_recommend_until rule and "
            "the list of mandatory checks."
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Classifier — LLM-based, returns a typed Classification
# ─────────────────────────────────────────────────────────────────────────────

CLASSIFIER_SYSTEM_PROMPT = """You classify Amazon Robotics RME diagnostic questions \
into a robot family and issue type. Robot families are: Sparrow (item-pick arm), \
Hercules (drive unit), Proteus (autonomous mobile robot), Sequoia (storage gantry). \
Issue types: battery, vibration, vacuum, joint_thermal, vision, navigation, lidar, \
safety_sensor, dock_alignment, gantry_rail, tote_handling, unknown.

Inference cues:
  * "SPA-..." or "Sparrow" or "vacuum" or "joint" → Sparrow.
  * "HRC-..." or "Hercules" or "battery" or "wheel" → Hercules.
  * "PRO-..." or "Proteus" or "lidar" or "GoCart" or "dock" → Proteus.
  * "SEQ-..." or "Sequoia" or "rail" or "tote" or "gantry" → Sequoia.

Return ONLY the JSON object — no commentary."""


def build_classifier_chain() -> Runnable:
    """Build the LLM-backed classifier chain.

    Uses ChatDatabricks pointing at a Foundation Model endpoint by default.
    Override the endpoint via the MODEL_ENDPOINT env var, or swap in
    `ChatOpenAI` / `ChatAnthropic` for local testing.
    """
    parser = PydanticOutputParser(pydantic_object=Classification)
    fmt_instructions = parser.get_format_instructions()

    prompt = ChatPromptTemplate.from_messages([
        ("system", CLASSIFIER_SYSTEM_PROMPT + "\n\n" + fmt_instructions),
        ("user", "{question}"),
    ])

    llm = _build_llm()
    return prompt | llm | parser


def _build_llm():
    """Resolve a chat LLM from environment.

    Default: Databricks Foundation Model (`databricks-claude-opus-4-7`) via
    ChatDatabricks. Falls back to OpenAI if `OPENAI_API_KEY` is set and the
    Databricks integration isn't available (useful for local CI smoke tests).
    """
    endpoint = os.environ.get("MODEL_ENDPOINT", "databricks-claude-opus-4-7")
    try:
        from langchain_databricks import ChatDatabricks
        return ChatDatabricks(endpoint=endpoint, temperature=0.0, max_tokens=400)
    except ImportError:
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
                              temperature=0.0, max_tokens=400)
        except ImportError as e:
            raise RuntimeError(
                "Install one of: langchain-databricks (preferred on Databricks) "
                "or langchain-openai (for local smoke tests)."
            ) from e


# ─────────────────────────────────────────────────────────────────────────────
# 4. The chain itself — classify → lookup → assemble guidance
# ─────────────────────────────────────────────────────────────────────────────


def build_guidance_chain() -> Runnable:
    """Full LCEL chain: question (str) → GuidancePayload."""
    classifier = build_classifier_chain()

    def _to_guidance(cls: Classification) -> GuidancePayload:
        policy = lookup_policy(cls.robot_family, cls.issue_type)
        if policy is None or cls.confidence < 0.6:
            return GuidancePayload(
                classification=cls,
                policy_matched=False,
                system_prompt_supplement=(
                    "No deterministic policy matched this question "
                    f"(family={cls.robot_family}, issue={cls.issue_type}, "
                    f"confidence={cls.confidence:.2f}). Proceed with general diagnostic "
                    "reasoning but flag to the operator that this case lacks an "
                    "approved pre-flight checklist — record findings in agent_memory "
                    "so an RME Lead can add a policy entry."
                ),
            )
        prompt_supp = _format_system_prompt_supplement(cls, policy)
        return GuidancePayload(
            classification=cls,
            policy_matched=True,
            required_checks=list(policy.required_checks),
            manual_sections=list(policy.manual_sections),
            blocking_questions=list(policy.blocking_questions),
            system_prompt_supplement=prompt_supp,
        )

    return classifier | RunnableLambda(_to_guidance)


def _format_system_prompt_supplement(cls: Classification, policy: CheckPolicy) -> str:
    checks = "\n".join(f"  - {c}" for c in policy.required_checks)
    sections = "\n".join(f"  - {s}" for s in policy.manual_sections)
    questions = "\n".join(f"  - {q}" for q in policy.blocking_questions)
    robot = cls.robot_id or f"<unspecified {cls.robot_family}>"
    return (
        f"### Mandatory pre-flight for {cls.robot_family} / {cls.issue_type} "
        f"(robot: {robot}, classifier confidence {cls.confidence:.2f})\n\n"
        f"GUARDRAIL: {policy.never_recommend_until}\n\n"
        f"REQUIRED CHECKS — run these BEFORE proposing remediation, and cite each in your answer:\n{checks}\n\n"
        f"MANUAL SECTIONS to retrieve via the Knowledge Assistant:\n{sections}\n\n"
        f"BLOCKING QUESTIONS — if the operator hasn't already answered these, ask them first:\n{questions}\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. MLflow PyFunc wrapper — for deployment as a Databricks model-serving endpoint
# ─────────────────────────────────────────────────────────────────────────────


class FleetGuidanceModel:
    """MLflow PyFunc-compatible wrapper.

    Deploy with:

        import mlflow
        from examples.fleet_guidance_chain import FleetGuidanceModel, build_guidance_chain

        with mlflow.start_run():
            mlflow.langchain.log_model(
                lc_model=build_guidance_chain(),
                artifact_path="fleet_guidance_chain",
                input_example={"question": "Why is SPA-BFI4-007 throwing vacuum alarms?"},
                pip_requirements=[
                    "langchain-core>=0.3",
                    "langchain-databricks>=0.1",
                    "pydantic>=2.0",
                ],
            )

    Then serve as an MLflow model-serving endpoint and reference it as a
    serving_endpoint sub-agent in the MAS config — OR register a wrapping UC
    function that calls the endpoint, so the MAS supervisor can invoke this
    guidance step before delegating to Genie + the Knowledge Assistant.
    """

    def __init__(self):
        self._chain = build_guidance_chain()

    def predict(self, context, model_input):
        question = (
            model_input["question"]
            if isinstance(model_input, dict)
            else model_input.iloc[0]["question"]
        )
        return self._chain.invoke({"question": question}).model_dump()


# ─────────────────────────────────────────────────────────────────────────────
# 6. Smoke test — runs without an LLM by stubbing classification
# ─────────────────────────────────────────────────────────────────────────────


def _smoke_test():
    """Run a few canonical examples through the lookup directly, then through
    the full chain if an LLM is available. Useful for the demo team to verify
    policy coverage without spinning up a model-serving endpoint."""

    print("=" * 78)
    print("FLEET GUIDANCE CHAIN — smoke test")
    print("=" * 78)

    # Direct policy lookup (no LLM needed)
    canonical = [
        ("Hercules", "battery"),
        ("Sparrow",  "vacuum"),
        ("Sparrow",  "vibration"),
        ("Proteus",  "lidar"),
        ("Proteus",  "safety_sensor"),
        ("Sequoia",  "tote_handling"),
        ("Sparrow",  "navigation"),     # missing — Sparrow has no navigation policy
    ]
    for family, issue in canonical:
        p = lookup_policy(family, issue)
        status = "✓ policy" if p else "✗ no policy (will fall through to general reasoning)"
        print(f"  [{family:8s}] {issue:18s} {status}")

    # Optional: full chain run (requires either Databricks Foundation Model
    # access or OPENAI_API_KEY in the environment)
    if os.environ.get("FLEET_GUIDANCE_LIVE_TEST") == "1":
        print()
        print("─" * 78)
        print("LIVE LLM TEST (FLEET_GUIDANCE_LIVE_TEST=1)")
        print("─" * 78)
        chain = build_guidance_chain()
        for q in [
            "Why is HRC-BFI4-003 showing a battery state of health drop? Should I swap the battery?",
            "SPA-SHV1-019 is throwing vacuum_pressure alarms during pick cycles. What do I do?",
            "Proteus-12 at PDX9 — LiDAR health score is 87. Replace the LiDAR?",
            "Hello, is this thing on?",  # should classify as unknown
        ]:
            print(f"\n>>> {q}")
            out = chain.invoke({"question": q})
            print(f"  family={out.classification.robot_family} "
                  f"issue={out.classification.issue_type} "
                  f"confidence={out.classification.confidence:.2f} "
                  f"matched={out.policy_matched}")
            if out.policy_matched:
                print("  required_checks:")
                for c in out.required_checks:
                    print(f"    - {c}")
                print("  blocking_questions:")
                for q in out.blocking_questions:
                    print(f"    - {q}")
                print("  system_prompt_supplement (first 240 chars):")
                print("    " + out.system_prompt_supplement[:240].replace("\n", "\n    ") + "…")

    print()
    print("Done. Set FLEET_GUIDANCE_LIVE_TEST=1 with model creds in env to run the LLM path.")


if __name__ == "__main__":
    _smoke_test()
