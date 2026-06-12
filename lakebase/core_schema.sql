-- Core Lakebase schema — required by scaffold core modules.
-- These 3 tables support notes, agent action tracking, and workflow management.
-- Apply this BEFORE domain_schema.sql.

-- ============================================================
-- Table: notes
-- Free-text notes attached to any domain entity
-- ============================================================
CREATE TABLE IF NOT EXISTS notes (
    note_id     SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id   VARCHAR(50) NOT NULL,
    note_text   TEXT NOT NULL,
    author      VARCHAR(100) NOT NULL DEFAULT 'system',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notes_entity ON notes(entity_type, entity_id);

-- ============================================================
-- Table: agent_actions
-- Autonomous agent actions from proactive monitoring
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_actions (
    action_id       SERIAL PRIMARY KEY,
    action_type     VARCHAR(50) NOT NULL,
    severity        VARCHAR(20) NOT NULL DEFAULT 'medium'
                    CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    entity_type     VARCHAR(50),
    entity_id       VARCHAR(50),
    description     TEXT NOT NULL,
    action_taken    TEXT,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'executed', 'dismissed', 'failed')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_actions_status ON agent_actions(status);
CREATE INDEX IF NOT EXISTS idx_agent_actions_type ON agent_actions(action_type);

-- ============================================================
-- Table: workflows
-- Multi-step autonomous workflow executions
-- ============================================================
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id     SERIAL PRIMARY KEY,
    workflow_type   VARCHAR(50) NOT NULL,
    trigger_source  VARCHAR(30) NOT NULL DEFAULT 'monitor'
                    CHECK (trigger_source IN ('monitor', 'chat', 'manual')),
    severity        VARCHAR(20) NOT NULL DEFAULT 'medium',
    summary         TEXT NOT NULL,
    reasoning_chain JSONB NOT NULL DEFAULT '[]',
    entity_type     VARCHAR(50),
    entity_id       VARCHAR(50),
    status          VARCHAR(20) NOT NULL DEFAULT 'in_progress'
                    CHECK (status IN ('in_progress', 'pending_approval', 'approved', 'dismissed', 'failed')),
    result_data     JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);
CREATE INDEX IF NOT EXISTS idx_workflows_type ON workflows(workflow_type);

-- ============================================================
-- Table: exceptions
-- Generic exception/alert management — works for any domain
-- (supply chain alerts, maintenance issues, compliance flags, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS exceptions (
    exception_id    SERIAL PRIMARY KEY,
    entity_type     VARCHAR(50) NOT NULL,
    entity_id       VARCHAR(50) NOT NULL,
    exception_type  VARCHAR(50) NOT NULL,
    severity        VARCHAR(20) NOT NULL DEFAULT 'medium'
                    CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    description     TEXT NOT NULL,
    assigned_to     VARCHAR(100),
    status          VARCHAR(20) NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'acknowledged', 'resolved', 'escalated', 'cancelled')),
    resolution      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_exceptions_status ON exceptions(status);
CREATE INDEX IF NOT EXISTS idx_exceptions_severity ON exceptions(severity);
CREATE INDEX IF NOT EXISTS idx_exceptions_entity ON exceptions(entity_type, entity_id);

-- ============================================================
-- Table: chat_sessions
-- Persistent chat sessions for the AI advisor
-- ============================================================
CREATE TABLE IF NOT EXISTS chat_sessions (
    session_id  VARCHAR(50) PRIMARY KEY,
    title       VARCHAR(200) DEFAULT 'New conversation',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Table: chat_messages
-- Individual messages within a chat session
-- ============================================================
CREATE TABLE IF NOT EXISTS chat_messages (
    message_id  SERIAL PRIMARY KEY,
    session_id  VARCHAR(50) NOT NULL REFERENCES chat_sessions(session_id) ON DELETE CASCADE,
    role        VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant')),
    content     TEXT NOT NULL,
    metadata    JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat_messages(session_id, created_at);

-- ============================================================
-- Grants — grant access to the app service principal
-- Replace <APP_SP_CLIENT_ID> with your Databricks App's SP client ID.
-- Find it via: databricks apps get <app-name> --profile=<profile> | jq '.service_principal_client_id'
-- ============================================================
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO "<APP_SP_CLIENT_ID>";
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "<APP_SP_CLIENT_ID>";
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "<APP_SP_CLIENT_ID>";
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "<APP_SP_CLIENT_ID>";

-- ============================================================
-- Idempotent ALTERs — extend workflows table for enrichment
-- ============================================================
DO $$ BEGIN
  ALTER TABLE workflows ADD COLUMN IF NOT EXISTS result_exception_id INTEGER;
  ALTER TABLE workflows ADD COLUMN IF NOT EXISTS headline VARCHAR(200);
  ALTER TABLE workflows ADD COLUMN IF NOT EXISTS enriched_summary TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
