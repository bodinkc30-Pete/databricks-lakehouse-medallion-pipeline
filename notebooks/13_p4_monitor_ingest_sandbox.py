# Databricks notebook source
# Project07 Phase4: real Databricks Jobs API metadata -> monitoring + alert outbox.
# Reads sanitized run-events JSON (collected by src/p4_collect_job_runs.py).
# WRITES ONLY workspace.project07_sandbox.p4_* .
# No external email/webhook messages; outbound delivery is NOT_CONFIGURED.
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

SCHEMA = "workspace.project07_sandbox"
SOURCE = "/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/p4_job_events.jsonl"
RUNS = SCHEMA + ".p4_run_metrics"
ALERTS = SCHEMA + ".p4_alert_outbox"
INCIDENTS = SCHEMA + ".p4_incident_state"
AUDIT = SCHEMA + ".p4_monitor_audit"
P3_QA = SCHEMA + ".p3_quality_audit"
P3_MANIFEST = SCHEMA + ".p3_publish_manifest"
assert all(t.startswith(SCHEMA + ".p4_") for t in (RUNS, ALERTS, INCIDENTS, AUDIT))
SLOW_THRESHOLD_MS = 70000
EXPECTED_RUN_IDS = [856548117138939, 392396837723218,
                    1116006042981211, 889964702448996]

raw = spark.read.option("multiLine", "false").json(SOURCE)
assert raw.count() == 4, "P4_SOURCE_CONTRACT_FAILED: expected four real run events"
assert sorted([int(r["run_id"]) for r in raw.select("run_id").collect()]) == sorted(EXPECTED_RUN_IDS)
assert raw.select("run_id").distinct().count() == 4
assert raw.filter(
    (F.col("job_id") != F.lit(824168112776513)) |
    (F.col("is_synthetic") != F.lit(True)) |
    (F.col("run_duration_ms") <= 0) |
    (~F.col("result_state").isin("SUCCESS", "FAILED"))
).limit(1).count() == 0, "P4_SOURCE_CONTRACT_FAILED: invalid metadata"

# Reconcile actual processed row counts from the Phase 3 QA audit, not estimates.
qa_rows = (spark.table(P3_QA)
           .groupBy("scenario")
           .agg(F.max("actual_rows").cast("bigint").alias("rows_processed")))
assert qa_rows.count() == 4, "P4_QA_AUDIT_CONTRACT_FAILED: expected four scenarios"
incoming = raw.join(qa_rows, on="scenario", how="inner")
assert incoming.count() == 4
assert incoming.filter(F.col("rows_processed").isNull()).limit(1).count() == 0
incoming = incoming.select(
    F.col("run_id").cast("bigint"),
    F.col("job_id").cast("bigint"),
    F.col("scenario").cast("string"),
    F.to_timestamp("started_at_utc").alias("started_at"),
    F.to_timestamp("ended_at_utc").alias("ended_at"),
    F.col("run_duration_ms").cast("bigint"),
    F.col("result_state").cast("string"),
    F.col("failed_task").cast("string"),
    F.col("failed_qa_attempts").cast("bigint"),
    F.col("gold_publish_skipped").cast("boolean"),
    F.col("gold_publish_succeeded").cast("boolean"),
    F.col("rows_processed").cast("bigint"),
).withColumn("event_hash", F.sha2(F.concat_ws(
    "|", F.col("run_id").cast("string"), F.col("scenario"),
    F.col("run_duration_ms").cast("string"), F.col("result_state"),
    F.col("failed_task"), F.col("rows_processed").cast("string")
), 256))
assert incoming.filter(F.col("started_at").isNull() | F.col("ended_at").isNull()).count() == 0
incoming.createOrReplaceTempView("p4_incoming_events")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUNS} (
    run_id BIGINT,
    job_id BIGINT,
    scenario STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    run_duration_ms BIGINT,
    result_state STRING,
    failed_task STRING,
    failed_qa_attempts BIGINT,
    gold_publish_skipped BOOLEAN,
    gold_publish_succeeded BOOLEAN,
    rows_processed BIGINT,
    event_hash STRING,
    first_seen_at TIMESTAMP
) USING DELTA
""")
# Historical API polls may be replayed. A stable event_hash prevents duplicates
# and avoids rewriting unchanged observed runs.
spark.sql(f"""
MERGE INTO {RUNS} t
USING p4_incoming_events s
ON t.run_id = s.run_id
WHEN MATCHED AND t.event_hash <> s.event_hash THEN UPDATE SET
    t.job_id=s.job_id, t.scenario=s.scenario,
    t.started_at=s.started_at, t.ended_at=s.ended_at,
    t.run_duration_ms=s.run_duration_ms, t.result_state=s.result_state,
    t.failed_task=s.failed_task, t.failed_qa_attempts=s.failed_qa_attempts,
    t.gold_publish_skipped=s.gold_publish_skipped,
    t.gold_publish_succeeded=s.gold_publish_succeeded,
    t.rows_processed=s.rows_processed, t.event_hash=s.event_hash
WHEN NOT MATCHED THEN INSERT (
    run_id, job_id, scenario, started_at, ended_at,
    run_duration_ms, result_state, failed_task, failed_qa_attempts,
    gold_publish_skipped, gold_publish_succeeded, rows_processed,
    event_hash, first_seen_at
) VALUES (
    s.run_id,s.job_id,s.scenario,s.started_at,s.ended_at,
    s.run_duration_ms,s.result_state,s.failed_task,s.failed_qa_attempts,
    s.gold_publish_skipped,s.gold_publish_succeeded,s.rows_processed,
    s.event_hash,current_timestamp()
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ALERTS} (
    alert_key STRING,
    run_id BIGINT,
    alert_type STRING,
    severity STRING,
    task_key STRING,
    duration_ms BIGINT,
    delivery_status STRING,
    first_detected_at TIMESTAMP
) USING DELTA
""")
# Actual state-based incidents; no invented platform outages.
events = spark.table(RUNS)
failures = (events.filter(F.col("result_state") == "FAILED")
    .select(F.concat_ws(":", F.col("run_id").cast("string"),
                        F.lit("JOB_FAILED")).alias("alert_key"),
            "run_id", F.lit("JOB_FAILED").alias("alert_type"),
            F.lit("HIGH").alias("severity"), F.col("failed_task").alias("task_key"),
            F.col("run_duration_ms").alias("duration_ms")))
slow = (events.filter(F.col("run_duration_ms") >= SLOW_THRESHOLD_MS)
    .select(F.concat_ws(":", F.col("run_id").cast("string"),
                        F.lit("DURATION_WARNING")).alias("alert_key"),
            "run_id", F.lit("DURATION_WARNING").alias("alert_type"),
            F.lit("WARNING").alias("severity"), F.lit("").alias("task_key"),
            F.col("run_duration_ms").alias("duration_ms")))
alerts = failures.unionByName(slow)
assert alerts.select("alert_key").distinct().count() == alerts.count(), "P4_DUPLICATE_ALERT_KEY"
assert alerts.count() == 3, "P4_ALERT_CONTRACT_FAILED: expected 2 errors and 1 slow run"
alerts.createOrReplaceTempView("p4_candidates")
spark.sql(f"""
MERGE INTO {ALERTS} t
USING p4_candidates s
ON t.alert_key = s.alert_key
WHEN NOT MATCHED THEN INSERT (
    alert_key,run_id,alert_type,severity,task_key,
    duration_ms,delivery_status,first_detected_at
) VALUES (
    s.alert_key,s.run_id,s.alert_type,s.severity,s.task_key,
    s.duration_ms,'NOT_CONFIGURED',current_timestamp()
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {INCIDENTS} (
    incident_id STRING,
    failing_run_id BIGINT,
    scenario STRING,
    failed_task STRING,
    detected_at TIMESTAMP,
    current_state STRING,
    recovered_by_run_id BIGINT,
    recovery_confirmed_at TIMESTAMP
) USING DELTA
""")
manifest = spark.table(P3_MANIFEST).where(F.col("pipeline_name") == "project07_p3_gold").collect()
assert len(manifest) == 1 and manifest[0]["active_snapshot_id"] == "p3_v2_valid", \
    "P4_RECOVERY_VERIFY_FAILED: active Gold not validated v2"
recovery = events.filter(
    (F.col("scenario") == "recovery")
    & (F.col("result_state") == "SUCCESS")
    & F.col("gold_publish_succeeded")
).select("run_id","ended_at").collect()
assert len(recovery) == 1, "P4_RECOVERY_VERIFY_FAILED: no successful recovery run"
recover_run_id = recovery[0]["run_id"]
recovery_time = recovery[0]["ended_at"]
bad = events.filter(F.col("result_state") == "FAILED")
incidents = (bad.select(
    F.concat(F.lit("P4-"), F.col("run_id").cast("string")).alias("incident_id"),
    F.col("run_id").alias("failing_run_id"),
    F.col("scenario"), F.col("failed_task"), F.col("ended_at").alias("detected_at"),
).withColumn("current_state", F.when(F.col("detected_at") < F.lit(recovery_time),
                                    F.lit("RECOVERED")).otherwise(F.lit("OPEN")))
 .withColumn("recovered_by_run_id", F.when(F.col("current_state") == "RECOVERED",
                                         F.lit(recover_run_id).cast("bigint"))
             .otherwise(F.lit(None).cast("bigint")))
 .withColumn("recovery_confirmed_at",
             F.when(F.col("current_state") == "RECOVERED", F.lit(recovery_time))
              .otherwise(F.lit(None).cast("timestamp"))))
assert incidents.count() == 2
assert incidents.filter(F.col("current_state") != "RECOVERED").count() == 0
incidents.createOrReplaceTempView("p4_incidents_incoming")
spark.sql(f"""
MERGE INTO {INCIDENTS} t
USING p4_incidents_incoming s
ON t.incident_id = s.incident_id
WHEN MATCHED AND t.current_state <> s.current_state THEN UPDATE SET
    t.current_state=s.current_state,
    t.recovered_by_run_id=s.recovered_by_run_id,
    t.recovery_confirmed_at=s.recovery_confirmed_at
WHEN NOT MATCHED THEN INSERT (
    incident_id,failing_run_id,scenario,failed_task,detected_at,
    current_state,recovered_by_run_id,recovery_confirmed_at
) VALUES (
    s.incident_id,s.failing_run_id,s.scenario,s.failed_task,s.detected_at,
    s.current_state,s.recovered_by_run_id,s.recovery_confirmed_at
)
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT} (
    attempt_id STRING,
    source_runs BIGINT,
    distinct_target_runs BIGINT,
    alert_records BIGINT,
    recovered_incidents BIGINT,
    execution_time TIMESTAMP,
    result STRING
) USING DELTA
""")
run_count = spark.table(RUNS).count()
alert_count = spark.table(ALERTS).count()
incidents_df = spark.table(INCIDENTS)
incident_count = incidents_df.filter(F.col("current_state") == "RECOVERED").count()
assert run_count == 4 and alert_count == 3 and incident_count == 2
assert spark.table(RUNS).select("run_id").distinct().count() == 4
assert spark.table(ALERTS).select("alert_key").distinct().count() == 3
import uuid
audit_schema=StructType([
    StructField("attempt_id",StringType(),False),
    StructField("source_runs",LongType(),False),
    StructField("distinct_target_runs",LongType(),False),
    StructField("alert_records",LongType(),False),
    StructField("recovered_incidents",LongType(),False),
    StructField("execution_time",TimestampType(),False),
    StructField("result",StringType(),False),
])
row=(str(uuid.uuid4()),4,run_count,alert_count,incident_count,
     datetime.now(timezone.utc).replace(tzinfo=None),"PASS")
spark.createDataFrame([row],audit_schema).write.format("delta").mode("append").saveAsTable(AUDIT)

print("P4_MONITOR_RESULT=PASS")
print("P4_REAL_JOB_RUNS=4")
print("P4_FAILING_JOB_RUNS=2")
print("P4_SLOW_RUNS=1")
print("P4_QUEUED_ALERTS=3")
print("P4_RECOVERED_INCIDENTS=2")
print("P4_ALERT_DELIVERY=NOT_CONFIGURED (no notifications sent)")
print("P4_PUBLISHED_GOLD_SNAPSHOT=p3_v2_valid")
