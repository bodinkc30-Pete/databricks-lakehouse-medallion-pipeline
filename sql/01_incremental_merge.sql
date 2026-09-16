-- Extracted from the Databricks notebook used in Project 07.
-- Demonstrates Delta Lake MERGE / UPSERT for incremental processing.

MERGE INTO workspace.gold.fact_order_lines_merge_sandbox AS target
USING (
    SELECT * FROM VALUES
    (
        '585523356187723174',
        '1732064364367938743',
        2,
        180.91
    ),
    (
        '585522819869935571',
        '1732059819924817079',
        2,
        597.18
    ),
    (
        'NEW_585523356187723174',
        'NEW_1732064364367938743',
        1,
        170.91
    ),
    (
        'NEW_585522819869935571',
        'NEW_1732059819924817079',
        1,
        587.18
    )
) AS source (
    order_id,
    sku_id,
    quantity,
    sku_subtotal_after_discount
)
ON target.order_id = source.order_id
AND target.sku_id = source.sku_id
WHEN MATCHED THEN
UPDATE SET
    target.quantity = source.quantity,
    target.sku_subtotal_after_discount = source.sku_subtotal_after_discount
WHEN NOT MATCHED THEN
INSERT (
    order_id,
    sku_id,
    quantity,
    sku_subtotal_after_discount
)
VALUES (
    source.order_id,
    source.sku_id,
    source.quantity,
    source.sku_subtotal_after_discount
);
