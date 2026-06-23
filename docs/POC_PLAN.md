# Amazon Robotics // Databricks

## Agentic Fleet Health & Predictive Maintenance POC

---

### Key Contacts

**Databricks Key Contacts:**

| Name | Role |
|------|------|
| @Greg Lockett | Account Executive |
| @Prashant Upadhyay | Account Executive (Additional) |
| @Gary Burgett | Solutions Architect (Primary SA) |
| TBD | SA Manager |
| TBD | Specialist SA — Generative AI / Agent Bricks |
| TBD | Specialist SA — Unity Catalog / Governance |
| TBD | Specialist SA — Lakebase / OLTP |
| TBD | Engineering Overlay — Agent Bricks PM |
| TBD | Engineering Overlay — Unity Catalog PM |

**Amazon Robotics Key Stakeholders:**

| Name | Title / Role |
|------|-------------|
| TBD | VP, RME Engineering (Executive Sponsor) |
| TBD | RME Operations Lead (Decision Maker) |
| TBD | Physical AI Tech Lead (Technical Decision Maker) |
| TBD | Project Eluna PM (Alignment Partner) |
| TBD | InfoSec Lead (Gating Approver) |
| TBD | Data Platform Engineer — S3 / Iceberg / Glue |
| TBD | RME Technician (User Persona — Frontline) |
| TBD | RME Lead / Area Manager (User Persona — Leadership) |

---

### Executive Alignment

Executive engagement cadence is being established alongside this POC. Initial alignment touchpoints are placeholders to be confirmed jointly between the Amazon Robotics RME / Physical AI leadership and Databricks executive sponsors.

- **TBD:** TBD (VP, RME Engineering) and TBD (Databricks Field CTO / GM) — Initial alignment call (target: pre-kickoff)
- **TBD:** TBD (Physical AI Tech Lead) and TBD (Databricks GenAI Field Engineering Leader) — Technical alignment on agent architecture (target: end of Phase 1)
- **TBD:** TBD (RME Operations Lead) and TBD (Databricks VP, Manufacturing & Logistics) — Mid-POC business review (target: end of Phase 2)

**Next Alignment:**

- **TBD:** Joint executive readout at end of Phase 3 — VP, RME Engineering + Physical AI leadership with Databricks senior leadership

---

### Summary & Scope

Amazon Robotics operates the world's largest "Physical AI" fleet — over 1M robots globally across drive units (Hercules, Pegasus, Proteus), robotic arms (Robin, Cardinal, Sparrow), storage gantries (Sequoia), and the newest multi-arm Blue Jay workstations. The Reliability, Maintenance & Engineering (RME) organization is responsible for keeping that fleet available, and the Physical AI team is extending its agentic operator-assist work (Project Eluna, currently piloting at a Tennessee FC) to reduce cognitive load across more operational personas. The team's data foundation today sits on S3, Iceberg, and Glue, with service manuals, RME runbooks, and work-order history scattered across systems that were not built for governed retrieval or agent-driven workflows.

Amazon Robotics is evaluating Databricks — specifically Unity Catalog, Databricks AI (Agent Bricks, Genie, MAS), and Lakebase — to close three concrete gaps that block production rollout of agentic fleet-health applications: (1) governance, lineage, and auditability strong enough to clear formal InfoSec review; (2) persistent agent memory that survives detection-model upgrades (the Sparrow v4 → v5 transition is a stated concern); and (3) governed retrieval over service manuals + RME procedures with full source-to-recommendation traceability. The POC is positioned to **echo and extend Project Eluna's cognitive-load-reduction frame**, while differentiating on UC lineage, Lakebase memory across model swaps, and RAG over the RME knowledge corpus. The active competitive alternative under evaluation is the AWS internal default: Bedrock Agents + Lookout for Equipment + IoT SiteWise + S3 Knowledge Bases.

---

### Business Use Cases

**Fleet Health — Persona-Aware Diagnostics**

- **Overview:** A single Sparrow vibration anomaly today produces the same alert payload whether the consumer is an RME Technician walking the floor or an RME Lead managing shift availability. The technician needs steps, parts, and a manual snippet; the lead needs root cause, trend, SLA risk, and shift redeploy options. Mirrors Eluna's cognitive-load-reduction frame and extends it to two distinct RME personas.
- **Goals:**
  - Render the same underlying agent diagnostic at two depths (RME Technician, RME Lead) without re-querying upstream data.
  - Demonstrate ≥60% autonomous resolution rate on a holdout anomaly set (in line with the Cox/Druva Bedrock benchmark of 63%).

**Fleet Health — Persistent Memory Across Model Upgrades**

- **Overview:** When Sparrow detection-model v4 ships to v5, today's tooling loses institutional memory of every diagnostic, remediation, and outcome — the agent effectively starts cold. Amazon Robotics has explicitly named this gap. Lakebase persists every agent decision tagged by model version so that a v4 → v5 model swap retains operational context.
- **Goals:**
  - Persist 100% of agent diagnostics, remediations, and outcomes to `agent_memory` tagged by `model_version`.
  - Demonstrate memory carry-forward across a simulated Sparrow v4 → v5 detection-model upgrade — the agent surfaces relevant historical remediations from v4-era cases when triaging v5-era anomalies.

**Fleet Health — Governed Retrieval Over Service Manuals + RME Procedures**

- **Overview:** RAG over robot service docs, RME runbooks, and prior remediation notes — with every retrieval, source chunk, and downstream recommendation traceable through Unity Catalog lineage. Directly supports the InfoSec review that is currently gating production pilot expansion.
- **Goals:**
  - End-to-end UC lineage from sensor reading → manual chunk → agent recommendation → work order, replayable for any agent decision.
  - Formal InfoSec review approval against documented lineage and access policies.

**Fleet Health — Cross-Platform Fleet View**

- **Overview:** Sparrow (item-pick arm), Hercules (drive unit), Proteus (AMR), and Sequoia (gantry) each emit a distinct signal vocabulary today. RME leaders need a single fleet view that reasons across all four robot families to compare reliability, prioritize PM, and redeploy capacity. Demonstrates UC's edge over a per-robot-family stack.
- **Goals:**
  - Unified Genie + agent surface that answers natural-language questions across all four robot families with consistent KPIs.
  - Cross-family MTBF and availability comparison delivered in <30 seconds of agent response time.

---

### Positive Business Outcomes

**Outcome #1: Governed Agentic Apps That Clear InfoSec by Design**

- **Description:** Unity Catalog provides end-to-end lineage from raw telemetry through service-manual retrieval to agent recommendation and downstream work-order writeback. Amazon Robotics RME and Physical AI teams can replay any agent decision against its sources for audit, satisfy InfoSec review requirements without bolting on a separate governance layer, and expand from one-FC pilots to fleet-wide rollout without rebuilding the trust story.

**Outcome #2: Institutional Memory That Survives Model Upgrades**

- **Description:** Every diagnostic, remediation, and outcome is persisted in Lakebase tagged by detection-model version. When Sparrow v4 ships to v5 — or when any other component model is swapped — the fleet retains its operational memory. Agents start v5 already informed by v4-era cases, eliminating the cold-start penalty that today recurs with every model release.

**Outcome #3: Persona-Aware Operator Experience That Extends Eluna's Cognitive-Load Frame**

- **Description:** A single agent reasoning loop renders at the right depth for the consumer — step-by-step remediation and part numbers for an RME Technician, root cause and shift redeploy options for an RME Lead. Reduces the time each persona spends translating between agent output and their own decision context, and gives RME leaders a consistent surface across all four robot families (Sparrow, Hercules, Proteus, Sequoia).

**Outcome #4: Measurable Fleet Availability and Downtime Avoidance**

- **Description:** Agent-assisted diagnostics target a 30–40% downtime reduction (McKinsey benchmark for predictive maintenance), translating to an estimated $150K–$400K of avoided downtime and SLA penalties per fulfillment center per year, with payback inside 60–90 days after the first prevented critical failure. The 63% autonomous resolution benchmark from Cox/Druva on AWS Bedrock is matched or exceeded on Databricks Agent Bricks with the added differentiators of UC governance and persistent memory.

---

### Project Summary

As a next step, Amazon Robotics and Databricks will engage in a Proof of Concept Evaluation that will run for approximately 10 weeks starting post-funding approval (kickoff in Week 0, final readout end of Week 10). The goal of this Evaluation is to demonstrate the value Databricks can provide as measured by the evaluation criteria below, with explicit positioning against the AWS Bedrock Agents + Lookout for Equipment + IoT SiteWise + S3 Knowledge Bases stack currently in evaluation. The POC is sized to land before the **2027-03-01 production target** captured in the existing Use Case Object (UCO `aAvVp000001hD4fKAE`, currently U3 Evaluating).

A reference architecture has already been deployed in the FE Vending Machine workspace (`fevm-amz-robotics-9868sm`) as the [Physical AI Fleet Health Console](https://amz-robotics-fleet-health-7474653834780775.aws.databricksapps.com). It includes the four Delta tables (898 robots, ~119k telemetry rows, ~5.5k anomalies, 20 service-manual chunks), Lakebase tables (`work_orders` and `agent_memory` with a mix of `sparrow-detect-v4` and `sparrow-detect-v5` rows to demonstrate upgrade-survival), a Genie Space over the fleet tables, and a MAS supervisor wired to Genie and the Lakebase MCP. This reference architecture is the baseline the POC will extend from. The Knowledge Assistant over `service_manual_chunks` is in scope for Phase 1 — it is intentionally deferred from the reference architecture so it can be exercised end-to-end during the POC.

---

### POC Scope & Success Criteria

| OKR | Business Value | Scope | Success Criteria | Status |
|-----|---------------|-------|-----------------|--------|
| **Phase 1 — Governance Foundation (~3 weeks)** | | | | |
| **OKR #1 — Unity Catalog ingestion from existing sources** | InfoSec gating today blocks production rollout of agent apps. Establishing UC as the governed surface across telemetry, manuals, and work orders is the prerequisite for every other workload. | **Data Sources/Tables:** S3 + Iceberg + Glue → UC tables `robots`, `telemetry`, `anomalies`, `service_manual_chunks`; **Framework:** Unity Catalog, External Locations, Lakeflow Connect | Ingest ≥6 months of historical telemetry for ≥4 robot families (Sparrow, Hercules, Proteus, Sequoia); ≥898 robots and ≥119k telemetry rows landed in UC; all four target tables registered with Iceberg-compatible metadata | |
| **OKR #2 — End-to-end lineage replay for any agent decision** | RME and Physical AI leaders must be able to defend any agent recommendation against source evidence — required for InfoSec review and operator trust. | **Data Sources/Tables:** `robots`, `telemetry`, `anomalies`, `service_manual_chunks`, `work_orders`, `agent_memory`; **Framework:** Unity Catalog lineage, system tables | Lineage trace replay from any work order back to source telemetry reading and manual chunk in ≤60 seconds; 100% of agent-created work orders traceable to a specific anomaly and ≥1 manual chunk; documented lineage diagram delivered to InfoSec | |
| **OKR #3 — InfoSec review approval against documented governance posture** | Without a formal InfoSec sign-off, pilot expansion past one FC is blocked. | **Frameworks:** UC access controls, audit logs, lineage, secret management, network controls | Formal InfoSec review approval signed off in writing; documented mapping of all UC controls to Amazon Robotics InfoSec requirements; zero open critical findings at end of Phase 1 | |
| **Phase 2 — Persona-Aware Agent + Persistent Memory (~3 weeks)** | | | | |
| **OKR #4 — Persona-aware diagnostic rendering** | RME Technicians and RME Leads consume the same underlying anomaly very differently. Reducing translation cost is the direct extension of Eluna's cognitive-load reduction frame. | **Tables/Data Sources to Query:** `anomalies`, `telemetry`, `service_manual_chunks`, `agent_memory`; **Framework:** Agent Bricks MAS, Genie Space, Knowledge Assistant, Lakebase MCP | Single agent diagnostic renders at two persona depths (RME Tech, RME Lead) on the Incidents page persona switcher; persona switch latency <2 seconds; ≥30 holdout anomalies validated by SMEs as appropriate-depth at both personas | |
| **OKR #5 — Autonomous resolution rate on holdout anomaly set** | The Cox/Druva on Bedrock benchmark is 63% autonomous resolution. Matching or beating that with Databricks' governance + memory advantages defines the win narrative. | **Tables/Data Sources to Query:** `anomalies` (holdout set), `service_manual_chunks`, `agent_memory`; **Framework:** Agent Bricks MAS with Genie + KA + Lakebase MCP sub-agents | ≥60% autonomous resolution rate on a SME-curated holdout set of ≥100 anomalies spanning all four robot families; ≥50% of those resolutions create a complete agent-authored work order without human edits | |
| **OKR #6 — Memory carry-forward across a simulated model upgrade** | Amazon Robotics explicitly identified memory loss across model upgrades as a gap. Lakebase tagging by `model_version` is the direct answer. | **Tables/Data Sources to Query:** `agent_memory` (with `sparrow-detect-v4` and `sparrow-detect-v5` rows); **Framework:** Lakebase + MAS, model-version tags | 100% of agent diagnostics persisted to `agent_memory` tagged by `model_version`; on a v4 → v5 simulated upgrade, the agent surfaces ≥3 relevant prior v4-era cases per v5-era anomaly triaged; SME validation of carry-forward relevance ≥80% | |
| **Phase 3 — Production Pilot at One FC (~4 weeks)** | | | | |
| **OKR #7 — MTTR reduction vs baseline at a single FC site** | Direct measurable business outcome — the headline economic story tied to the McKinsey 30–40% downtime-reduction benchmark and $150K–$400K per-FC value. | **Tables/Data Sources to Query:** `work_orders` (production), `anomalies` (production), `agent_memory`; **Framework:** Full stack — UC + Lakebase + MAS + Genie + KA | ≥30% MTTR reduction on agent-assisted work orders vs 30-day pre-POC baseline at one FC (suggested BFI4 or SHV1); ≥20 work orders authored by the agent during the pilot window; downtime-avoided dollar value estimated against the $150K–$400K per-FC annual envelope | |
| **OKR #8 — SLA-risk ahead-of-time alerting on critical anomalies** | RME Leads need lead time on SLA-threatening incidents — the leadership-persona surface of the wow moment. | **Tables/Data Sources to Query:** `anomalies`, `telemetry`, `work_orders`; **Framework:** MAS, Lakebase MCP, Genie | ≥80% of critical-severity anomalies flagged with an SLA-risk score before the SLA window closes; alerting precision ≥70% (low false-positive rate confirmed by RME Lead) | |
| **OKR #9 — Live integration with RME work-order system + persona exit interviews** | Production-readiness depends on closing the loop with the system of record and validating the experience with both target personas. | **Frameworks:** Lakebase MCP writes; integration with the FC's RME work-order system | Live read/write integration deployed at one FC; clean exit interview with ≥1 RME Technician and ≥1 RME Lead validating the persona-aware experience; written go/no-go recommendation for production rollout | |

---

### Tasks and Timeline

**Abbreviation Dictionary:**

- POC — Proof of Concept
- SA — Solutions Architect
- AE — Account Executive
- PS — Professional Services
- UC — Unity Catalog
- MAS — Multi-Agent Supervisor (Agent Bricks)
- KA — Knowledge Assistant
- MCP — Model Context Protocol (Lakebase MCP server)
- RME — Reliability, Maintenance & Engineering
- FC — Fulfillment Center
- AMR — Autonomous Mobile Robot
- MTBF — Mean Time Between Failures
- MTTR — Mean Time To Repair
- SLA — Service Level Agreement
- PM — Preventive Maintenance
- SoH — State of Health (battery)
- LiDAR — Light Detection And Ranging

The timeline below is normalized to "weeks post-funding approval." Phase 1 starts Week 1, kickoff occurs in Week 0.

| Task | Amazon Robotics Owner | Databricks Owner | Comments | Target Due | Status | Notes |
|------|----------------------|-----------------|----------|-----------|--------|-------|
| **Week 0 — Kickoff & Environment Setup** | | | | | | |
| Kickoff call — full team alignment on scope, success criteria, schedule | RME Operations Lead, Physical AI Tech Lead | @Gary Burgett, @Greg Lockett | Both sides; record decisions | Week 0 | | |
| Workspace creation and Unity Catalog bootstrap | Data Platform Engineer | @Gary Burgett | FEVM reference workspace exists; provision customer workspace per InfoSec | Week 0 | | |
| Access provisioning and connectivity validation (S3, Iceberg, Glue) | Data Platform Engineer | @Gary Burgett | Service principals, OAuth M2M, network paths | Week 0 | | |
| Identify holdout anomaly set (≥100 anomalies, all four robot families) | RME Operations Lead | @Gary Burgett | SME-curated; ground truth for OKR #5 | Week 0 | | |
| **Phase 1 — Governance Foundation (Weeks 1–3)** | | | | | | |
| Ingest 6 months of telemetry + anomalies for Sparrow / Hercules / Proteus / Sequoia | Data Platform Engineer | @Gary Burgett | Lakeflow Connect from S3 + Iceberg | Week 2 | | |
| Ingest service manuals + RME procedural runbooks into `service_manual_chunks` | RME Operations Lead | @Gary Burgett | RAG corpus; chunking strategy aligned with manual structure | Week 2 | | |
| Register all tables, external locations, and access policies in UC | Data Platform Engineer | @Gary Burgett | Includes Iceberg metadata compatibility | Week 2 | | |
| Build lineage trace replay tool — sensor → chunk → recommendation → work order | Physical AI Tech Lead | @Gary Burgett | UC lineage + system tables | Week 3 | | |
| Deliver documented lineage diagram + governance posture write-up to InfoSec | RME Operations Lead, InfoSec Lead | @Gary Burgett, @Greg Lockett | Inputs to formal review | Week 3 | | |
| Formal InfoSec review and sign-off | InfoSec Lead | @Greg Lockett | OKR #3 gating event | Week 3 | | |
| Phase 1 checkpoint review with execs | RME Operations Lead | @Gary Burgett, @Greg Lockett | Go / no-go for Phase 2 | Week 3 | | |
| **Phase 2 — Persona-Aware Agent + Persistent Memory (Weeks 4–6)** | | | | | | |
| Deploy Knowledge Assistant over `service_manual_chunks` | Physical AI Tech Lead | @Gary Burgett | Deferred from reference architecture; in scope for POC | Week 4 | | |
| Wire MAS supervisor with Genie + KA + Lakebase MCP sub-agents | Physical AI Tech Lead | @Gary Burgett | Reference MAS already exists; add KA | Week 4 | | |
| Implement persona-aware rendering on Incidents page (RME Tech ↔ RME Lead) | Physical AI Tech Lead | @Gary Burgett | The wow moment | Week 5 | | |
| Persistence to `agent_memory` tagged by `model_version` (full coverage) | Physical AI Tech Lead | @Gary Burgett | Lakebase MCP writes | Week 5 | | |
| Run holdout anomaly set; measure autonomous resolution rate | RME Operations Lead | @Gary Burgett | OKR #5; ≥100 anomalies | Week 6 | | |
| Simulate Sparrow v4 → v5 upgrade; validate memory carry-forward | Physical AI Tech Lead | @Gary Burgett | OKR #6; SME validation panel | Week 6 | | |
| Phase 2 checkpoint review with execs | Physical AI Tech Lead | @Gary Burgett, @Greg Lockett | Go / no-go for Phase 3 | Week 6 | | |
| **Phase 3 — Production Pilot at One FC (Weeks 7–10)** | | | | | | |
| Select FC site (target: BFI4 or SHV1) and confirm scope with site leadership | RME Operations Lead | @Gary Burgett, @Greg Lockett | Site-specific InfoSec confirmations | Week 7 | | |
| Stand up live integration with the FC's RME work-order system | Data Platform Engineer | @Gary Burgett | Read + write through Lakebase MCP | Week 7 | | |
| Capture 30-day pre-POC MTTR baseline at the pilot FC | RME Operations Lead | @Gary Burgett | Required for OKR #7 comparison | Week 7 | | |
| Run live pilot — agent-assisted diagnostics, work-order authoring | RME Operations Lead, RME Technicians | @Gary Burgett | Live shadow then live writes | Weeks 8–9 | | |
| SLA-risk alerting validation on critical anomalies | RME Operations Lead | @Gary Burgett | OKR #8; precision review with RME Lead | Week 9 | | |
| Exit interviews — RME Technician and RME Lead | RME Operations Lead | @Gary Burgett | OKR #9; structured feedback template | Week 10 | | |
| Compile evaluation results vs success criteria | Physical AI Tech Lead | @Gary Burgett | Fills Evaluation Results table | Week 10 | | |
| **Week 10 — Final Readout** | | | | | | |
| Final readout meeting — full team on both sides | VP RME Engineering, Physical AI Tech Lead | @Gary Burgett, @Greg Lockett, @Prashant Upadhyay | Includes go/no-go recommendation | Week 10 | | |
| Post-POC survey + commercial next steps | RME Operations Lead | @Greg Lockett | CSAT + production-rollout commercial path | Week 10 | | |

---

### POC Staffing Plan

Databricks and Amazon Robotics will jointly collaborate on this POC with the following resources.

**Databricks Resources:**

| Name | Role | Focus Area | Engagement Model |
|------|------|-----------|-----------------|
| @Gary Burgett | Primary Lead SA | All — end-to-end ownership | Full time dedicated |
| @Greg Lockett | Account Executive | Executive alignment, commercials, InfoSec coordination | Full time overlay |
| @Prashant Upadhyay | Account Executive (Additional) | Commercial coverage, exec relationships | Part time overlay |
| TBD | SA Manager | PM interface, escalation path | Full time overlay |
| TBD | Specialist SA — GenAI / Agent Bricks | OKRs #4–6 (persona-aware agent, memory) | Part time overlay |
| TBD | Specialist SA — Unity Catalog / Governance | OKRs #1–3 (ingestion, lineage, InfoSec) | Part time overlay |
| TBD | Specialist SA — Lakebase / OLTP | OKR #6, Phase 3 work-order integration | Part time overlay |
| TBD | Engineering Overlay — Agent Bricks PM | Roadmap alignment for OKR #6 (model-version memory) | As needed overlay |
| TBD | Engineering Overlay — UC PM | InfoSec review escalation | As needed overlay |

**Amazon Robotics Resources:**

| Name | Role | Focus Area | Engagement Model |
|------|------|-----------|-----------------|
| TBD | VP, RME Engineering | Executive Sponsor | Part time overlay |
| TBD | RME Operations Lead | Business decision maker, success-criteria sign-off | Full time overlay |
| TBD | Physical AI Tech Lead | Technical decision maker, agent architecture lead | Full time overlay |
| TBD | Project Eluna PM | Alignment partner — Eluna positioning, no overlap | Part time overlay |
| TBD | InfoSec Lead | OKR #3 sign-off, governance posture review | Part time overlay |
| TBD | Data Platform Engineer | Phase 1 ingestion, Phase 3 integration | Full time dedicated |
| TBD | RME Technician (Persona) | OKR #4 persona validation, Phase 3 exit interview | As needed overlay |
| TBD | RME Lead (Persona) | OKR #4 persona validation, OKR #8 alerting precision review, Phase 3 exit interview | Part time overlay |

**Meeting Schedule:**

- Kickoff Call Week 0 with full team on both sides
- Daily 15-minute standup (Databricks SA + Physical AI Tech Lead + Data Platform Engineer)
- Weekly Progress Report on Friday with all leads
- Phase-end checkpoint reviews at end of Weeks 3, 6, and 10
- Executive alignment touchpoints at end of Phases 1, 2, and 3
- Final readout in Week 10 with full team on both sides

---

### Current State & Architecture

Amazon Robotics' current data foundation for RME and Physical AI workloads is built on AWS-native components:

**Key Components & Challenges:**

- **S3 + Iceberg + Glue** — Robot telemetry from drive units, arms, gantries, and AMRs lands in S3 and is catalogued in Glue. Iceberg is the table format. The architecture is operational but was not built for governed retrieval, agent workflows, or end-to-end lineage from sensor to recommendation.
- **Service manuals and RME runbooks** — Robot service docs and RME procedural runbooks live in document stores outside the analytics platform. There is no governed RAG corpus today; retrieval is ad-hoc and not auditable.
- **Work-order history** — RME work orders live in the FC operational system of record. Today there is no agent-driven authoring or write-back path; remediations are manually entered.
- **Detection models (Sparrow v4, transitioning to v5)** — Component-specific detection models are versioned in the ML stack, but operational memory of agent diagnostics and remediations does not persist across model swaps. This is the "memory that survives model changes" gap that Amazon Robotics has explicitly named.
- **Project Eluna (Oct 2025)** — Amazon Robotics' own agentic operator-assist system, piloting at a Tennessee FC. Eluna pioneers the "cognitive load reduction" framing this POC echoes. The POC is positioned as a complementary extension that adds UC governance, Lakebase memory, and RAG over the RME knowledge corpus — not as a competitor to Eluna.
- **InfoSec gating** — Formal InfoSec review approval is pending and gates expansion past current pilot scope. The Databricks POC must clear the same bar.

**Competitive context — the active alternative under evaluation:** AWS Bedrock Agents (orchestration) + Lookout for Equipment (anomaly detection) + IoT SiteWise (telemetry) + S3 Knowledge Bases (RAG corpus). Databricks differentiation is intentionally focused on three concrete gaps in that stack: unified UC governance and lineage spanning telemetry + manuals + work orders; model flexibility (not locked to Lookout); and Lakebase agent memory that persists across model version upgrades.

**Reference architecture (deployed):** The [Physical AI Fleet Health Console](https://amz-robotics-fleet-health-7474653834780775.aws.databricksapps.com) is already running in the FEVM workspace `fevm-amz-robotics-9868sm` with the three-layer architecture (Delta Lake analytics + Lakebase OLTP + shared Lakebase MCP server) and MAS Agent Bricks orchestrating Genie and Lakebase MCP sub-agents. This is the baseline the POC will extend from.

---

### Mutual Success Plan

| Objective | Actions | Owner | Timeline | Status |
|-----------|---------|-------|----------|--------|
| Funding / Budget Approval | Confirm POC funding internally at Amazon Robotics; finalize commercial terms | TBD (VP RME Engineering), @Greg Lockett | Pre–Week 0 | |
| Executive Alignment — Kickoff | Schedule VP RME Engineering / Databricks Field CTO alignment | @Greg Lockett, @Prashant Upadhyay | Pre–Week 0 | |
| Workspace & Environment Provisioned | Customer workspace, UC, Lakebase, connectivity to S3/Iceberg/Glue | @Gary Burgett, TBD (Data Platform Engineer) | Week 0 | |
| Holdout Anomaly Set Defined | SME-curated ≥100 anomalies across four robot families with ground-truth resolutions | TBD (RME Operations Lead) | Week 0 | |
| Phase 1 Completion — UC + Lineage + InfoSec | UC ingestion, lineage replay tool, InfoSec sign-off | Joint | Weeks 1–3 | |
| Executive Alignment — Phase 1 | Tech-lead-level alignment on agent architecture | @Gary Burgett, TBD (Physical AI Tech Lead) | End of Week 3 | |
| Phase 2 Completion — Persona Agent + Memory | KA deployed, persona switcher live, holdout run, v4→v5 carry-forward validated | Joint | Weeks 4–6 | |
| Executive Alignment — Phase 2 | Mid-POC business review | @Greg Lockett, TBD (RME Operations Lead) | End of Week 6 | |
| Phase 3 Completion — One-FC Pilot | Live integration, MTTR reduction, SLA-risk alerting, persona exit interviews | Joint | Weeks 7–10 | |
| Final Readout | Present results vs success criteria; go/no-go recommendation for production rollout | @Gary Burgett, @Greg Lockett | Week 10 | |
| Post-POC Survey | CSAT survey to both personas + leadership | @Greg Lockett | Week 10 | |
| Commercial Next Steps | Production rollout commercial path aligned to 2027-03-01 target | @Greg Lockett, @Prashant Upadhyay | Weeks 10–12 | |

---

### Evaluation Results

*To be filled out during and post Proof of Concept*

| OKR | Target | Result | Status |
|-----|--------|--------|--------|
| OKR #1 — UC ingestion from existing sources | ≥6 months historical telemetry, ≥4 robot families, ≥898 robots, ≥119k telemetry rows | | |
| OKR #2 — End-to-end lineage replay | Lineage replay ≤60s; 100% agent work orders traceable to anomaly + ≥1 manual chunk | | |
| OKR #3 — InfoSec review approval | Signed-off InfoSec approval; zero open critical findings | | |
| OKR #4 — Persona-aware diagnostic rendering | Persona switch <2s; ≥30 anomalies SME-validated at both depths | | |
| OKR #5 — Autonomous resolution rate | ≥60% on holdout set of ≥100 anomalies; ≥50% with no-edit agent work orders | | |
| OKR #6 — Memory carry-forward across v4→v5 | 100% diagnostics tagged by model_version; ≥3 v4-era cases surfaced per v5 anomaly; ≥80% SME relevance | | |
| OKR #7 — MTTR reduction at pilot FC | ≥30% MTTR reduction vs 30-day pre-POC baseline; ≥20 agent-authored work orders | | |
| OKR #8 — SLA-risk ahead-of-time alerting | ≥80% of critical anomalies flagged pre-SLA; ≥70% precision per RME Lead | | |
| OKR #9 — Live RME integration + exit interviews | Live integration deployed; ≥1 RME Technician + ≥1 RME Lead exit interview; written go/no-go recommendation | | |

---

### Resources

- [Physical AI Fleet Health Console — deployed reference architecture](https://amz-robotics-fleet-health-7474653834780775.aws.databricksapps.com)
- [Databricks Documentation](https://docs.databricks.com)
- [Unity Catalog Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Agent Bricks Documentation](https://docs.databricks.com/en/generative-ai/agent-bricks.html)
- [Lakebase Documentation](https://docs.databricks.com/en/lakebase/index.html)
- [Genie Documentation](https://docs.databricks.com/en/genie/index.html)
- [Lakeflow Connect Documentation](https://docs.databricks.com/en/ingestion/lakeflow-connect/index.html)
- [Iceberg on Databricks](https://docs.databricks.com/en/delta/uniform.html)
- [Unity Catalog lineage and system tables](https://docs.databricks.com/en/admin/system-tables/lineage.html)

---

### Notes

*Running notes section — to be filled during POC*

- Use case mapping reference: Amazon Robotics UCO `aAvVp000001hD4fKAE` (U3 Evaluating), Account `0016100001SQS0sAAH`. Related UCO `aAvVp000001ivA5KAI` (Amazon Robotics — GenAI, U1) sits adjacent.
- Production target: 2027-03-01 (per SFDC). POC is sized to land ahead of that target with commercial close in the intervening window.
- Eluna positioning rule of engagement: echo, do not compete. Use Eluna's "cognitive load reduction" framing in exec materials; differentiate on UC lineage, Lakebase memory, and RAG over manuals.
- Robot families in scope for Phase 1 ingestion: Sparrow (item-pick arm), Hercules (drive unit), Proteus (AMR), Sequoia (gantry).
- Detection model upgrade simulated for OKR #6: Sparrow v4 (`sparrow-detect-v4`) → v5 (`sparrow-detect-v5`). The reference workspace already has a mix of v4 and v5 rows in `agent_memory` to seed the demonstration.
- Suggested pilot FC: BFI4 or SHV1 (Shreveport is the Sequoia 30M-item site — strong story-fit for gantry use cases; BFI4 has tighter SLA coverage). Confirm with RME Operations Lead in Week 0.
