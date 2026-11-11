-- Project 07 Phase 1: read-only Databricks SQL Star Schema acceptance audit.
-- All tables are synthetic-only and located in workspace.project07_sandbox.
-- source_order_amount is NOT recognized revenue.
SELECT
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_fact_order_lines) AS fact_line_rows,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_fact_orders) AS fact_order_rows,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_dim_sku) AS dim_sku_rows,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_dim_product) AS dim_product_rows,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_dim_date) AS dim_date_rows,
  (SELECT SUM(quantity) FROM workspace.project07_sandbox.gold_fact_order_lines) AS total_units,
  (SELECT SUM(source_order_amount) FROM workspace.project07_sandbox.gold_fact_order_lines) AS total_source_amount,
  (SELECT SUM(source_order_amount) FROM workspace.project07_sandbox.gold_fact_orders) AS order_source_amount,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_fact_order_lines l
      LEFT ANTI JOIN workspace.project07_sandbox.gold_dim_sku s ON l.sku_id = s.sku_id) AS missing_sku_fk,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_fact_order_lines l
      LEFT ANTI JOIN workspace.project07_sandbox.gold_dim_product p ON l.product_key = p.product_key) AS missing_product_fk,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_fact_order_lines l
      LEFT ANTI JOIN workspace.project07_sandbox.gold_dim_date d ON l.date_key = d.date_key) AS missing_date_fk,
  (SELECT COUNT(*) FROM workspace.project07_sandbox.gold_fact_order_lines l
      LEFT ANTI JOIN workspace.project07_sandbox.gold_fact_orders o ON l.order_id = o.order_id) AS missing_order_fk;
