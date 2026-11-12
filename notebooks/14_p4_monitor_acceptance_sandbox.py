# Databricks notebook source
# Project07 Phase4: read-only operational acceptance after monitoring ingest.
# No writes. No external alert delivery.
from pyspark.sql import functions as F

SCHEMA = "workspace.project07_sandbox"
RUNS = SCHEMA + ".p4_run_metrics"
ALERTS = SCHEMA + ".p4_alert_outbox"
INCIDENTS = SCHEMA + ".p4_incident_state"
AUDIT = SCHEMA + ".p4_monitor_audit"
P3_MANIFEST = SCHEMA + ".p3_publish_manifest"
P3_VIEW = SCHEMA + ".p3_current_gold_order_lines"

runs = spark.table(RUNS)
assert runs.count() == 4, "P4_ACCEPTANCE_FAILED: incorrect monitored run count"
assert runs.select("run_id").distinct().count() == 4
assert runs.filter(F.col("result_state") == "FAILED").count() == 2
assert runs.filter(F.col("result_state") == "SUCCESS").count() == 2
assert runs.filter(F.col("run_duration_ms") >= 70000).count() == 1
assert runs.filter(F.col("rows_processed").isNull()).count() == 0
assert runs.filter(F.col("rows_processed") == 5270).count() == 1, \
    "P4_ACCEPTANCE_FAILED: duplicate-injection row count not recorded"
assert runs.filter(F.col("rows_processed") == 5269).count() == 3
alerts = spark.table(ALERTS)
assert alerts.count() == 3 and alerts.select("alert_key").distinct().count() == 3
assert alerts.filter(F.col("alert_type") == "JOB_FAILED").count() == 2
assert alerts.filter(F.col("alert_type") == "DURATION_WARNING").count() == 1
assert alerts.filter(F.col("delivery_status") != "NOT_CONFIGURED").count() == 0
incidents = spark.table(INCIDENTS)
assert incidents.count() == 2
assert incidents.filter(F.col("current_state") != "RECOVERED").count() == 0
audit = spark.table(AUDIT)
assert audit.count() >= 1
assert audit.filter(F.col("result") != "PASS").count() == 0
manifest = spark.table(P3_MANIFEST).where(F.col("pipeline_name") == "project07_p3_gold").collect()
assert len(manifest) == 1
assert manifest[0]["active_snapshot_id"] == "p3_v2_valid"
assert manifest[0]["version_no"] == 2
view = spark.table(P3_VIEW)
assert view.count() == 5269
totals = view.agg(
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount")
).first()
assert totals["units"] == 13176
assert str(totals["amount"]) == "5265860.00"

print("P4_ACCEPTANCE_RESULT=PASS")
print("P4_RUNS_MONITORED=4")
print("P4_FAILED_JOB_ALERTS=2")
print("P4_DURATION_WARNINGS=1")
print("P4_DUPLICATE_ALERT_KEYS=0")
print("P4_INCIDENTS_RECOVERED=2")
print("P4_REPLAY_SAFE_ALERT_OUTBOX=True")
print("P4_PUBLISHED_GOLD=p3_v2_valid")
print("P4_GOLD_ROWS=5269")
print("P4_GOLD_UNITS=13176")
print("P4_ALERT_DELIVERY=NOT_CONFIGURED")
