-- Project 07 Phase 2: READ-ONLY acceptance query.
-- Synthetic-only tables in the dedicated workspace.project07_sandbox schema.
-- source_order_amount is a synthetic measure, not recognized revenue.
WITH target_metrics AS (
    SELECT
        COUNT(*) AS target_rows,
        COUNT(DISTINCT struct(order_id, sku_id)) AS distinct_business_keys,
        SUM(quantity) AS target_units,
        SUM(order_amount) AS target_source_amount
    FROM workspace.project07_sandbox.p2_incremental_order_lines
),
watermark_metrics AS (
    SELECT
        MAX(last_source_updated_at) AS last_source_updated_at,
        COUNT(*) AS watermark_rows
    FROM workspace.project07_sandbox.p2_watermark
    WHERE pipeline_name = 'project07_p2_order_lines'
)
SELECT
    a.phase,
    a.batch_id,
    a.result,
    a.source_rows,
    a.inserted_rows,
    a.updated_rows,
    a.unchanged_rows,
    a.target_rows_before,
    a.target_rows_after,
    t.target_rows,
    t.distinct_business_keys,
    t.target_units,
    t.target_source_amount,
    w.last_source_updated_at,
    w.watermark_rows
FROM workspace.project07_sandbox.p2_batch_audit AS a
CROSS JOIN target_metrics AS t
CROSS JOIN watermark_metrics AS w
ORDER BY
    a.executed_at ASC,
    a.phase ASC;
