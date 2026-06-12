-- Physical AI Fleet Health Console — Domain Lakebase tables
-- Apply this AFTER core_schema.sql.
--
-- 2 domain tables:
--   work_orders  — RME work orders the agent creates/updates via Lakebase MCP
--   agent_memory — persistent diagnostics that survive model upgrades (Sparrow v4 → v5)

-- ============================================================
-- Table: work_orders
-- RME (Reliability and Maintenance Engineering) work orders
-- Agent writes new orders via Lakebase MCP; updates via PATCH /api/work-orders/{id}
-- ============================================================
CREATE TABLE IF NOT EXISTS work_orders (
    work_order_id       SERIAL PRIMARY KEY,
    wo_number           VARCHAR(50) NOT NULL UNIQUE,
    robot_id            VARCHAR(50) NOT NULL,
    family              VARCHAR(30) NOT NULL
                        CHECK (family IN ('Sparrow', 'Hercules', 'Proteus', 'Sequoia')),
    fc_site             VARCHAR(10) NOT NULL,
    source_anomaly_id   VARCHAR(50),
    severity            VARCHAR(20) NOT NULL DEFAULT 'medium'
                        CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    priority            VARCHAR(20) NOT NULL DEFAULT 'normal'
                        CHECK (priority IN ('low', 'normal', 'high', 'urgent')),
    title               VARCHAR(200) NOT NULL,
    root_cause          TEXT,
    remediation_steps   TEXT,
    parts_used          JSONB DEFAULT '[]',
    manual_refs         JSONB DEFAULT '[]',
    technician          VARCHAR(100),
    status              VARCHAR(30) NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'in_progress', 'awaiting_parts', 'completed', 'cancelled')),
    created_by          VARCHAR(100) NOT NULL DEFAULT 'agent',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_work_orders_status   ON work_orders(status);
CREATE INDEX IF NOT EXISTS idx_work_orders_robot    ON work_orders(robot_id);
CREATE INDEX IF NOT EXISTS idx_work_orders_fc_site  ON work_orders(fc_site);
CREATE INDEX IF NOT EXISTS idx_work_orders_severity ON work_orders(severity);

-- ============================================================
-- Table: agent_memory
-- Persistent diagnostics + remediations + outcomes.
-- model_version tags every entry so the demo can show institutional knowledge
-- carrying forward across Sparrow v4 → v5 (and future) detection-model upgrades.
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_memory (
    memory_id           SERIAL PRIMARY KEY,
    robot_id            VARCHAR(50) NOT NULL,
    family              VARCHAR(30) NOT NULL
                        CHECK (family IN ('Sparrow', 'Hercules', 'Proteus', 'Sequoia')),
    fc_site             VARCHAR(10),
    signal              VARCHAR(50),
    persona             VARCHAR(20) NOT NULL DEFAULT 'rme_tech'
                        CHECK (persona IN ('rme_tech', 'rme_lead')),
    diagnostic          TEXT NOT NULL,
    remediation         TEXT,
    outcome             VARCHAR(30) NOT NULL DEFAULT 'pending'
                        CHECK (outcome IN ('pending', 'resolved', 'recurring', 'escalated', 'no_action')),
    model_version       VARCHAR(50) NOT NULL,
    source_anomaly_id   VARCHAR(50),
    source_work_order_id INTEGER,
    confidence          NUMERIC(3, 2) CHECK (confidence BETWEEN 0 AND 1),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_memory_robot          ON agent_memory(robot_id);
CREATE INDEX IF NOT EXISTS idx_agent_memory_family         ON agent_memory(family);
CREATE INDEX IF NOT EXISTS idx_agent_memory_model_version  ON agent_memory(model_version);
CREATE INDEX IF NOT EXISTS idx_agent_memory_outcome        ON agent_memory(outcome);
