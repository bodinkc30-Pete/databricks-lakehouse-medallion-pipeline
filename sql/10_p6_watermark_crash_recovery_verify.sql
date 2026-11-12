-- Project 07 Failure Recovery Lab02: independent READ-ONLY acceptance.
-- One row summarizes data, checkpoint, completion, state events and legacy Gold.
WITH target_metrics AS (
 SELECT
   COUNT(*) AS target_rows,
   COUNT(DISTINCT struct(order_id, sku_id)) AS unique_keys,
   SUM(quantity) AS units,
   SUM(order_amount) AS source_amount,
   SUM(CASE WHEN last_batch_id = 'p6_incremental_20260204' THEN 1 ELSE 0 END)
     AS applied_batch_rows
 FROM workspace.project07_sandbox.p6_recovery_target
),
checkpoint AS (
 SELECT
   COUNT(*) AS watermark_rows,
   MAX(last_source_updated_at) AS watermark_ts,
   MAX(last_batch_id) AS watermark_batch_id
 FROM workspace.project07_sandbox.p6_recovery_watermark
 WHERE pipeline_name = 'project07_p6_lab02'
),
completions AS (
 SELECT
   COUNT(*) AS completed_batch_rows,
   MAX(completed_by_attempt) AS completed_by_attempt
 FROM workspace.project07_sandbox.p6_completion_audit
 WHERE batch_id = 'p6_incremental_20260204'
),
state_metrics AS (
 SELECT
   COUNT(*) AS state_event_rows,
   COUNT(DISTINCT attempt_id) AS state_attempts,
   SUM(CASE WHEN state = 'STARTED' THEN 1 ELSE 0 END) AS started_events,
   SUM(CASE WHEN state = 'MERGED' THEN 1 ELSE 0 END) AS merged_events,
   SUM(CASE WHEN state = 'CHECKPOINTED' THEN 1 ELSE 0 END) AS checkpointed_events,
   SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_events,
   SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_events,
   SUM(CASE WHEN state = 'COMPLETED' AND (inserted_rows > 0 OR updated_rows > 0)
            THEN 1 ELSE 0 END) AS unsafe_completions
 FROM workspace.project07_sandbox.p6_state_events
 WHERE batch_id = 'p6_incremental_20260204'
),
last_event AS (
 SELECT state AS most_recent_state, run_mode AS most_recent_mode
 FROM (
   SELECT state, run_mode,
          ROW_NUMBER() OVER (
             ORDER BY recorded_at DESC, step_index DESC, event_id DESC
          ) AS rank_no
   FROM workspace.project07_sandbox.p6_state_events
   WHERE batch_id = 'p6_incremental_20260204'
 )
 WHERE rank_no = 1
),
published_gold AS (
 SELECT
   active_snapshot_id AS original_gold_snapshot,
   version_no AS original_gold_version,
   row_count AS original_gold_rows,
   unit_count AS original_gold_units
 FROM workspace.project07_sandbox.p3_publish_manifest
 WHERE pipeline_name = 'project07_p3_gold'
)
SELECT t.*, w.*, c.*, s.*, l.*, g.*
FROM target_metrics t
CROSS JOIN checkpoint w
CROSS JOIN completions c
CROSS JOIN state_metrics s
CROSS JOIN last_event l
CROSS JOIN published_gold g;
