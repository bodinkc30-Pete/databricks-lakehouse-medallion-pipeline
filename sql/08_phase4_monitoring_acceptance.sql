-- Project 07 Phase 4: READ-ONLY independent monitoring acceptance.
-- All metrics use synthetic portfolio Job metadata and Phase 3 QA audit.
-- Alert outbox records are NOT externally delivered.
WITH runs AS (
  SELECT
      COUNT(*) AS monitored_runs,
      COUNT(DISTINCT run_id) AS distinct_run_ids,
      SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
      SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
      SUM(CASE WHEN run_duration_ms >= 70000 THEN 1 ELSE 0 END) AS slow_runs,
      SUM(rows_processed) AS total_observed_processed_rows,
      MAX(run_duration_ms) AS max_run_duration_ms
  FROM workspace.project07_sandbox.p4_run_metrics
),
alerts AS (
  SELECT
      COUNT(*) AS queued_alerts,
      COUNT(DISTINCT alert_key) AS distinct_alert_keys,
      SUM(CASE WHEN alert_type = 'JOB_FAILED' THEN 1 ELSE 0 END) AS failure_alerts,
      SUM(CASE WHEN alert_type = 'DURATION_WARNING' THEN 1 ELSE 0 END) AS duration_alerts,
      SUM(CASE WHEN delivery_status = 'NOT_CONFIGURED' THEN 1 ELSE 0 END) AS undelivered_alerts
  FROM workspace.project07_sandbox.p4_alert_outbox
),
incidents AS (
  SELECT
      COUNT(*) AS incidents,
      SUM(CASE WHEN current_state = 'RECOVERED' THEN 1 ELSE 0 END) AS recovered_incidents,
      SUM(CASE WHEN current_state = 'OPEN' THEN 1 ELSE 0 END) AS open_incidents
  FROM workspace.project07_sandbox.p4_incident_state
),
monitor AS (
  SELECT
      COUNT(*) AS monitor_attempts,
      SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) AS successful_monitor_attempts
  FROM workspace.project07_sandbox.p4_monitor_audit
),
gold AS (
  SELECT
      active_snapshot_id, version_no, row_count, unit_count, source_amount
  FROM workspace.project07_sandbox.p3_publish_manifest
  WHERE pipeline_name = 'project07_p3_gold'
)
SELECT
    r.*, a.*, i.*, m.*,
    g.active_snapshot_id AS gold_snapshot,
    g.version_no AS gold_version,
    g.row_count AS gold_rows,
    g.unit_count AS gold_units,
    g.source_amount AS gold_source_amount
FROM runs r
CROSS JOIN alerts a
CROSS JOIN incidents i
CROSS JOIN monitor m
CROSS JOIN gold g;
