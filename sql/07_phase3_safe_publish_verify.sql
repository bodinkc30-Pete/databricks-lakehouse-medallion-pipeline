-- Project 07 Phase 3. READ-ONLY query showing the active QA-approved snapshot.
-- All amounts are synthetic SOURCE measures, NOT recognized revenue.
SELECT
    m.active_snapshot_id,
    m.version_no AS active_version,
    m.row_count AS manifest_rows,
    m.unit_count AS manifest_units,
    m.source_amount AS manifest_source_amount,
    (SELECT COUNT(*) FROM workspace.project07_sandbox.p3_current_gold_order_lines) AS visible_rows,
    (SELECT COUNT(DISTINCT struct(order_id, sku_id))
       FROM workspace.project07_sandbox.p3_current_gold_order_lines) AS visible_unique_keys,
    (SELECT SUM(quantity)
       FROM workspace.project07_sandbox.p3_current_gold_order_lines) AS visible_units,
    (SELECT SUM(order_amount)
       FROM workspace.project07_sandbox.p3_current_gold_order_lines) AS visible_source_amount,
    (SELECT COUNT(*) FROM workspace.project07_sandbox.p3_quality_audit
       WHERE result = 'FAIL') AS failed_qa_attempts,
    (SELECT COUNT(*) FROM workspace.project07_sandbox.p3_quality_audit
       WHERE result = 'PASS') AS successful_qa_attempts,
    (SELECT COUNT(*) FROM workspace.project07_sandbox.p3_rejected_rows) AS quarantined_issue_rows,
    (SELECT COUNT(*) FROM workspace.project07_sandbox.p3_publish_audit
       WHERE result = 'PASS') AS successful_publishes
FROM workspace.project07_sandbox.p3_publish_manifest m
WHERE m.pipeline_name = 'project07_p3_gold';
