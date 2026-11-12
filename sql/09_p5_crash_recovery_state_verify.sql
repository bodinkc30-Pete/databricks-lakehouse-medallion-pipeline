-- Project 07 Recovery Lab: READ ONLY acceptance of committed data,
-- checkpoint/audit consistency and the previously published Gold version.
-- Run once immediately AFTER intentionally failed Job and again AFTER recovery.
-- Source data are fully synthetic; amount is not recognized sales revenue.
WITH p5_data AS (
  SELECT
      COUNT(*) AS data_rows,
      COUNT(DISTINCT struct(order_id, sku_id)) AS distinct_business_keys,
      SUM(quantity) AS total_units,
      SUM(order_amount) AS total_source_amount,
      SUM(CASE WHEN last_batch_id = 'p5_incremental_20260203'
               THEN 1 ELSE 0 END) AS batch_rows_applied
  FROM workspace.project07_sandbox.p5_recovery_target
),
checkpoint AS (
  SELECT
      COUNT(*) AS checkpoint_rows,
      MAX(last_source_updated_at) AS last_source_updated_at,
      MAX(last_batch_id) AS checkpoint_batch_id
  FROM workspace.project07_sandbox.p5_recovery_watermark
  WHERE pipeline_name = 'project07_p5_crash_recovery'
),
attempts AS (
  SELECT
      COUNT(*) AS completed_audit_attempts,
      SUM(CASE WHEN result = 'RECOVERED' THEN 1 ELSE 0 END) AS recovered_attempts,
      SUM(CASE WHEN result = 'REPLAY_NOOP' THEN 1 ELSE 0 END) AS replay_attempts,
      SUM(CASE WHEN inserted_rows > 0 OR updated_rows > 0 THEN 1 ELSE 0 END)
          AS non_idempotent_attempts
  FROM workspace.project07_sandbox.p5_recovery_audit
),
published AS (
  SELECT
      active_snapshot_id AS historical_gold_snapshot,
      version_no AS historical_gold_version,
      row_count AS historical_gold_rows,
      unit_count AS historical_gold_units,
      source_amount AS historical_gold_source_amount
  FROM workspace.project07_sandbox.p3_publish_manifest
  WHERE pipeline_name = 'project07_p3_gold'
)
SELECT p.*, c.*, a.*, g.*
FROM p5_data p
CROSS JOIN checkpoint c
CROSS JOIN attempts a
CROSS JOIN published g;
