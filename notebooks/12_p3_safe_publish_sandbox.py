# Databricks notebook source
# Project 07 Phase 3, Task 3: QA-gated, pointer-switched versioned Gold.
# This notebook MUST have depends_on on the QA Task.
# Only one Delta manifest row changes to publish a fully staged snapshot.
# This does NOT claim a cross-table ACID transaction for all data sources.
from datetime import datetime, timezone
from decimal import Decimal
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DecimalType, TimestampType
)

dbutils.widgets.dropdown("scenario", "baseline", ["baseline", "fail_duplicate", "fail_null", "recovery"])
scenario = dbutils.widgets.get("scenario")
allowed = {
    "baseline": ("p3_v1", 1, 13175, "5265850.00"),
    "recovery": ("p3_v2_valid", 2, 13176, "5265860.00"),
}
assert scenario in allowed, "P3_PUBLISH_REJECTED: only validated healthy scenarios allowed"
snapshot_id, version_no, expected_units, expected_amount = allowed[scenario]

SCHEMA = "workspace.project07_sandbox"
STAGE = SCHEMA + ".p3_snapshot_lines"
QA_AUDIT = SCHEMA + ".p3_quality_audit"
MANIFEST = SCHEMA + ".p3_publish_manifest"
PUBLISH_AUDIT = SCHEMA + ".p3_publish_audit"
VIEW = SCHEMA + ".p3_current_gold_order_lines"
assert all(x.startswith(SCHEMA + ".p3_")
           for x in (STAGE, QA_AUDIT, MANIFEST, PUBLISH_AUDIT, VIEW))

qa = (spark.table(QA_AUDIT)
      .where((F.col("snapshot_id") == snapshot_id)
             & (F.col("scenario") == scenario))
      .orderBy(F.desc("checked_at")).limit(1).collect())
assert len(qa) == 1 and qa[0]["result"] == "PASS", \
    "P3_PUBLISH_BLOCKED: no successful QA gate for requested snapshot"

stage = spark.table(STAGE).where(F.col("snapshot_id") == snapshot_id)
assert stage.count() == 5269
assert stage.select("order_id", "sku_id").distinct().count() == 5269
metrics = stage.agg(
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert metrics["units"] == expected_units
assert str(metrics["amount"]) == expected_amount

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST} (
    pipeline_name STRING,
    active_snapshot_id STRING,
    version_no INT,
    published_at TIMESTAMP,
    row_count BIGINT,
    unit_count BIGINT,
    source_amount DECIMAL(20,2)
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PUBLISH_AUDIT} (
    publish_attempt_id STRING,
    scenario STRING,
    candidate_snapshot_id STRING,
    resulting_active_snapshot_id STRING,
    candidate_version INT,
    result STRING,
    recorded_at TIMESTAMP
) USING DELTA
""")
manifest = spark.table(MANIFEST).filter(
    F.col("pipeline_name") == "project07_p3_gold"
).select("active_snapshot_id", "version_no").collect()
assert len(manifest) <= 1, "P3_PUBLISH_BLOCKED: duplicated manifest pointer"
if manifest and int(manifest[0]["version_no"]) > version_no:
    raise RuntimeError("P3_STALE_PUBLICATION_REJECTED: do not regress published version")

candidate_schema = StructType([
    StructField("pipeline_name", StringType(), False),
    StructField("active_snapshot_id", StringType(), False),
    StructField("version_no", IntegerType(), False),
    StructField("row_count", LongType(), False),
    StructField("unit_count", LongType(), False),
    StructField("source_amount", DecimalType(20,2), False),
])
spark.createDataFrame([(
    "project07_p3_gold", snapshot_id, version_no, 5269,
    int(expected_units), Decimal(expected_amount),
)], candidate_schema).createOrReplaceTempView("p3_validated_publish_candidate")

# This is the ONLY mutation used to switch consumers to a new complete snapshot.
# Versioned staging rows are immutable and already QA-validated.
spark.sql(f"""
MERGE INTO {MANIFEST} AS t
USING p3_validated_publish_candidate AS s
ON t.pipeline_name = s.pipeline_name
WHEN MATCHED AND s.version_no > t.version_no THEN UPDATE SET
    t.active_snapshot_id = s.active_snapshot_id,
    t.version_no = s.version_no,
    t.row_count = s.row_count,
    t.unit_count = s.unit_count,
    t.source_amount = s.source_amount,
    t.published_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, active_snapshot_id, version_no,
    published_at, row_count, unit_count, source_amount
) VALUES (
    s.pipeline_name, s.active_snapshot_id, s.version_no,
    current_timestamp(), s.row_count, s.unit_count, s.source_amount
)
""")
spark.sql(f"""
CREATE VIEW IF NOT EXISTS {VIEW} AS
SELECT s.order_id, s.sku_id, s.order_status, s.quantity,
       s.order_amount, s.source_updated_at, s.snapshot_id
FROM {STAGE} s
JOIN {MANIFEST} m
  ON s.snapshot_id = m.active_snapshot_id
WHERE m.pipeline_name = 'project07_p3_gold'
""")
pointer = spark.table(MANIFEST).where(
    F.col("pipeline_name") == "project07_p3_gold"
).collect()
assert len(pointer) == 1, "P3_MANIFEST_CONTRACT_FAILED"
active = pointer[0]["active_snapshot_id"]
assert active == snapshot_id and pointer[0]["version_no"] == version_no, \
    "P3_MANIFEST_CONTRACT_FAILED: snapshot not current"
published = spark.table(VIEW)
assert published.count() == 5269, "P3_PUBLISH_RECONCILIATION_FAILED: count"
final = published.agg(
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert final["units"] == expected_units
assert str(final["amount"]) == expected_amount

publish_schema = StructType([
    StructField("publish_attempt_id", StringType(), False),
    StructField("scenario", StringType(), False),
    StructField("candidate_snapshot_id", StringType(), False),
    StructField("resulting_active_snapshot_id", StringType(), False),
    StructField("candidate_version", IntegerType(), False),
    StructField("result", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
])
spark.createDataFrame([(
    str(uuid.uuid4()), scenario, snapshot_id, active, version_no,
    "PASS", datetime.now(timezone.utc).replace(tzinfo=None),
)], publish_schema).write.format("delta").mode("append").saveAsTable(PUBLISH_AUDIT)
print("P3_PUBLISH_RESULT=PASS")
print("P3_PUBLISHED_SNAPSHOT=" + active)
print("P3_MANIFEST_VERSION=" + str(version_no))
print("P3_PUBLISHED_ROWS=5269")
print("P3_PUBLISHED_UNITS=" + str(expected_units))
print("P3_PUBLISHED_AMOUNT=" + expected_amount)
print("P3_VISIBLE_GOLD_VIEW=" + VIEW)
