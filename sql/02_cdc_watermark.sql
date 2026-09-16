-- Extracted from the Project 07 Databricks notebook.
-- Delta watermark and Change Data Feed checkpoint state.

CREATE TABLE IF NOT EXISTS workspace.gold.pipeline_watermark (
    pipeline_name STRING,
    last_processed_at TIMESTAMP
)
USING DELTA;

INSERT INTO workspace.gold.pipeline_watermark
SELECT
    'fact_order_lines_incremental',
    TIMESTAMP('2026-08-13 08:00:00')
WHERE NOT EXISTS (
    SELECT 1
    FROM workspace.gold.pipeline_watermark
    WHERE pipeline_name = 'fact_order_lines_incremental'
);

SELECT
    f.order_id,
    f.sku_id,
    f.created_time,
    f.quantity,
    f.sku_subtotal_after_discount
FROM workspace.gold.fact_order_lines AS f
CROSS JOIN workspace.gold.pipeline_watermark AS w
WHERE w.pipeline_name = 'fact_order_lines_incremental'
  AND f.created_time > w.last_processed_at
ORDER BY f.created_time;

CREATE TABLE IF NOT EXISTS workspace.gold.cdf_checkpoint (
    pipeline_name STRING,
    last_processed_version BIGINT
)
USING DELTA;

INSERT INTO workspace.gold.cdf_checkpoint
SELECT
    'fact_order_lines_cdf_downstream',
    7
WHERE NOT EXISTS (
    SELECT 1
    FROM workspace.gold.cdf_checkpoint
    WHERE pipeline_name = 'fact_order_lines_cdf_downstream'
);

UPDATE workspace.gold.cdf_checkpoint
SET last_processed_version = 8
WHERE pipeline_name = 'fact_order_lines_cdf_downstream';

SELECT *
FROM workspace.gold.cdf_checkpoint
WHERE pipeline_name = 'fact_order_lines_cdf_downstream';
