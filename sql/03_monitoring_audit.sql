-- Extracted from the Project 07 monitoring section.
-- Persists pipeline run status, row metrics, failures and duration.

CREATE TABLE IF NOT EXISTS workspace.gold.pipeline_run_audit (
    run_id STRING,
    pipeline_name STRING,
    batch_id STRING,
    run_status STRING,
    failed_step STRING,
    error_message STRING,
    rows_read BIGINT,
    rows_written BIGINT,
    rows_rejected BIGINT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds DOUBLE
)
USING DELTA;

SELECT
    pipeline_name,
    batch_id,
    run_status,
    rows_read,
    rows_written,
    rows_rejected,
    failed_step,
    error_message,
    duration_seconds,
    started_at,
    completed_at
FROM workspace.gold.pipeline_run_audit
ORDER BY started_at DESC;
