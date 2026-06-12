-- Databricks notebook source
-- Physical AI Fleet Health Console — Schema Setup
-- Run this first to create the catalog schema.
--
-- ═══════════════════════════════════════════════════════════════════════════════
-- IMPORTANT: Multi-Statement Execution
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- This file contains multiple SQL statements separated by "-- COMMAND ----------".
--
--   * NOTEBOOK UI:  Works fine — the notebook UI splits on "-- COMMAND ----------"
--     and sends each statement individually. Just click "Run All".
--
--   * API / CLI (Statement Execution API):  The Databricks Statement Execution API
--     (POST /api/2.0/sql/statements) only supports a SINGLE statement per request.
--     Sending multiple statements separated by ";" will fail with a parse error.
--     You must execute each statement below as a separate API call.
--
-- ═══════════════════════════════════════════════════════════════════════════════

-- COMMAND ----------

-- IMPORTANT: Statement 1 of 4 — Set catalog context
USE CATALOG amz_robotics_9868sm_catalog;

-- COMMAND ----------

-- IMPORTANT: Statement 2 of 4 — Create schema
CREATE SCHEMA IF NOT EXISTS fleet_health
COMMENT 'Amazon Robotics Physical AI Fleet Health Console — robots, telemetry, anomalies, and service manual chunks for the RME copilot';

-- COMMAND ----------

-- IMPORTANT: Statement 3 of 4 — Set schema context
USE SCHEMA fleet_health;

-- COMMAND ----------

-- IMPORTANT: Statement 4 of 4 — Verify schema is ready
SELECT current_catalog(), current_schema();
