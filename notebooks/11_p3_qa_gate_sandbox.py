# Databricks notebook source
# Project07 Phase3, Task2: audit, quarantine, reject invalid snapshots.
# This notebook intentionally fails in fail_duplicate and fail_null scenarios;
# dependent publish task must be SKIPPED. No existing Gold is overwritten.
from datetime import datetime, timezone
from decimal import Decimal
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType, TimestampType
)

dbutils.widgets.dropdown("scenario", "baseline", ["baseline", "fail_duplicate", "fail_null", "recovery"])
scenario = dbutils.widgets.get("scenario")
ids = {
    "baseline": ("p3_v1", 1, 13175, "5265850.00"),
    "fail_duplicate": ("p3_v2_bad_duplicate", 2, 13176, "5265860.00"),
    "fail_null": ("p3_v2_bad_null", 2, 13176, "5265860.00"),
    "recovery": ("p3_v2_valid", 2, 13176, "5265860.00"),
}
assert scenario in ids
snapshot_id, version_no, expected_units, expected_amount = ids[scenario]

SCHEMA = "workspace.project07_sandbox"
STAGE = SCHEMA + ".p3_snapshot_lines"
AUDIT = SCHEMA + ".p3_quality_audit"
REJECTED = SCHEMA + ".p3_rejected_rows"
SKU_DIM = SCHEMA + ".gold_dim_sku"
assert AUDIT.startswith(SCHEMA + ".p3_")
assert REJECTED.startswith(SCHEMA + ".p3_")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT} (
    qa_run_id STRING,
    scenario STRING,
    snapshot_id STRING,
    version_no INT,
    actual_rows BIGINT,
    expected_rows BIGINT,
    duplicate_groups BIGINT,
    null_rows BIGINT,
    invalid_rows BIGINT,
    orphan_sku_rows BIGINT,
    units BIGINT,
    source_amount DECIMAL(20,2),
    result STRING,
    checked_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REJECTED} (
    snapshot_id STRING,
    issue_code STRING,
    order_id STRING,
    sku_id STRING,
    detected_at TIMESTAMP
) USING DELTA
""")

required_fields = {
    "snapshot_id", "version_no", "order_id", "sku_id", "order_status",
    "quantity", "order_amount", "source_updated_at", "row_hash"
}
available = set(spark.table(STAGE).columns)
assert required_fields.issubset(available), "P3_SCHEMA_DRIFT_DETECTED"

data = spark.table(STAGE).where(F.col("snapshot_id") == snapshot_id)
actual_rows = data.count()
expected_rows = 5269

duplicate_keys = (data.groupBy("order_id", "sku_id")
                  .count().filter(F.col("count") > 1).select("order_id", "sku_id"))
duplicate_groups = duplicate_keys.count()
null_pred = (
    F.col("order_id").isNull() | F.col("sku_id").isNull()
    | F.col("order_status").isNull() | F.col("quantity").isNull()
    | F.col("order_amount").isNull() | F.col("source_updated_at").isNull()
    | F.col("row_hash").isNull()
)
invalid_pred = (
    (F.col("quantity") <= 0) | (F.col("order_amount") < 0)
    | (~F.col("order_status").isin("Completed", "Processing", "Cancelled"))
)
null_rows = data.filter(null_pred).count()
invalid_rows = data.filter(invalid_pred).count()
orphan_skus = data.select("sku_id").join(
    spark.table(SKU_DIM).select("sku_id"), on="sku_id", how="left_anti"
)
orphan_count = orphan_skus.count()
total = data.agg(
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount")
).first()
units = int(total["units"] or 0)
amount = Decimal(str(total["amount"] or "0.00"))

# Quarantine evidence is additive; never delete source or silently dedupe.
if duplicate_groups:
    (duplicate_keys.withColumn("snapshot_id", F.lit(snapshot_id))
     .withColumn("issue_code", F.lit("DUPLICATE_BUSINESS_KEY"))
     .withColumn("detected_at", F.current_timestamp())
     .select("snapshot_id", "issue_code", "order_id", "sku_id", "detected_at")
     .write.format("delta").mode("append").saveAsTable(REJECTED))
if null_rows:
    (data.filter(null_pred).select("order_id", "sku_id")
     .withColumn("snapshot_id", F.lit(snapshot_id))
     .withColumn("issue_code", F.lit("MISSING_REQUIRED_VALUE"))
     .withColumn("detected_at", F.current_timestamp())
     .select("snapshot_id", "issue_code", "order_id", "sku_id", "detected_at")
     .write.format("delta").mode("append").saveAsTable(REJECTED))
if invalid_rows:
    (data.filter(invalid_pred).select("order_id", "sku_id")
     .withColumn("snapshot_id", F.lit(snapshot_id))
     .withColumn("issue_code", F.lit("INVALID_MEASURE_OR_STATUS"))
     .withColumn("detected_at", F.current_timestamp())
     .select("snapshot_id", "issue_code", "order_id", "sku_id", "detected_at")
     .write.format("delta").mode("append").saveAsTable(REJECTED))

ok = (
    actual_rows == expected_rows and duplicate_groups == 0
    and null_rows == 0 and invalid_rows == 0 and orphan_count == 0
    and units == expected_units and amount == Decimal(expected_amount)
)
qa_run_id = str(uuid.uuid4())
aud_schema = StructType([
    StructField("qa_run_id", StringType(), False),
    StructField("scenario", StringType(), False),
    StructField("snapshot_id", StringType(), False),
    StructField("version_no", LongType(), False),
    StructField("actual_rows", LongType(), False),
    StructField("expected_rows", LongType(), False),
    StructField("duplicate_groups", LongType(), False),
    StructField("null_rows", LongType(), False),
    StructField("invalid_rows", LongType(), False),
    StructField("orphan_sku_rows", LongType(), False),
    StructField("units", LongType(), False),
    StructField("source_amount", DecimalType(20,2), False),
    StructField("result", StringType(), False),
    StructField("checked_at", TimestampType(), False),
])
# For table writes, use Spark schema matching the destination (INT vs BIGINT)
# via explicit cast below, rather than relying on an implicit schema migration.
row = (
    qa_run_id, scenario, snapshot_id, version_no,
    actual_rows, expected_rows, duplicate_groups, null_rows, invalid_rows,
    orphan_count, units, amount, "PASS" if ok else "FAIL",
    datetime.now(timezone.utc).replace(tzinfo=None)
)
aud = spark.createDataFrame([row], aud_schema).withColumn(
    "version_no", F.col("version_no").cast("int")
)
aud.write.format("delta").mode("append").saveAsTable(AUDIT)
print("P3_QA_RESULT=" + ("PASS" if ok else "FAIL"))
print("P3_QA_RUN_ID=" + qa_run_id)
print("P3_QA_SNAPSHOT=" + snapshot_id)
print("P3_QA_ACTUAL_ROWS=" + str(actual_rows))
print("P3_QA_DUPLICATE_GROUPS=" + str(duplicate_groups))
print("P3_QA_NULL_ROWS=" + str(null_rows))
print("P3_QA_INVALID_ROWS=" + str(invalid_rows))
print("P3_QA_ORPHAN_SKU_ROWS=" + str(orphan_count))
print("P3_QA_UNITS=" + str(units))
print("P3_QA_SOURCE_AMOUNT=" + str(amount))
if not ok:
    raise RuntimeError(
        "P3_QA_GATE_REJECTED: invalid snapshot, downstream Gold publish blocked; "
        "audit and rejected-row evidence retained."
    )
