# Databricks notebook source
# Project 07 Phase 2 - LIVE synthetic-only incremental MERGE.
# ALLOWED WRITES: workspace.project07_sandbox.p2_* ONLY.
# DO NOT EDIT/RUN against historical Bronze, Silver or Gold tables.
# Batch 1: bootstrap 5,249 synthetic lines
# Batch 2: 20 synthetic inserts + 3 source corrections
# Batch 3: replay identical Batch 2. Expected zero data mutations.
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DecimalType
)

dbutils.widgets.dropdown("phase", "bootstrap", ["bootstrap", "delta", "replay"])
phase = dbutils.widgets.get("phase").strip().lower()
assert phase in ("bootstrap", "delta", "replay"), "Unsupported phase"

SCHEMA = "workspace.project07_sandbox"
SOURCE_TABLE = SCHEMA + ".silver_orders"
TARGET = SCHEMA + ".p2_incremental_order_lines"
WATERMARK = SCHEMA + ".p2_watermark"
AUDIT = SCHEMA + ".p2_batch_audit"
assert all(name.startswith("workspace.project07_sandbox.p2_")
           for name in (TARGET, WATERMARK, AUDIT))
assert SOURCE_TABLE == "workspace.project07_sandbox.silver_orders"

batch_id = "bootstrap_20260201" if phase == "bootstrap" else "incremental_20260202"
source_ts = "2026-02-01 00:00:00" if phase == "bootstrap" else "2026-02-02 00:00:00"
source = spark.table(SOURCE_TABLE)
assert source.count() == 5249, "P2_SOURCE_CONTRACT_FAILED: existing Silver has changed"
assert source.select("order_id", "sku_id").distinct().count() == 5249
assert source.filter(~F.col("order_id").startswith("SYN-ORDER-")).limit(1).count() == 0

def normalize(df, modified_at):
    basic = df.select(
        F.col("order_id").cast("string"),
        F.col("sku_id").cast("string"),
        F.col("order_status").cast("string"),
        F.col("quantity").cast("bigint"),
        F.col("order_amount").cast("decimal(18,2)"),
    ).withColumn("source_updated_at", F.to_timestamp(F.lit(modified_at)))
    return basic.withColumn("row_hash", F.sha2(F.concat_ws(
        "|", F.col("order_id"), F.col("sku_id"), F.col("order_status"),
        F.col("quantity").cast("string"), F.col("order_amount").cast("string")
    ), 256))

if phase == "bootstrap":
    incoming = normalize(source, source_ts)
else:
    # Three deliberately corrected existing synthetic records.
    corrections = (source.where(F.col("order_id").isin(
            "SYN-ORDER-00000001", "SYN-ORDER-00000004", "SYN-ORDER-00000007"))
        .select("order_id", "sku_id", "order_status", "quantity", "order_amount")
        .withColumn("order_status", F.lit("Completed"))
        .withColumn("quantity", F.col("quantity") + F.lit(1))
        .withColumn("order_amount",
                    (F.col("order_amount") + F.lit(50)).cast("decimal(18,2)")))
    # Real source systems would supply new records. Here we fabricate deterministic
    # new order lines in Spark; no private customer/order records are read.
    i = F.col("id")
    new_records = (spark.range(5250, 5270)
        .select(
            F.format_string("SYN-ORDER-%08d", i.cast("int")).alias("order_id"),
            F.format_string("SYN-SKU-%04d", ((i % 53) + 1).cast("int")).alias("sku_id"),
            F.when((i % 3) == 0, "Completed")
             .when((i % 3) == 1, "Processing")
             .otherwise("Cancelled").alias("order_status"),
            ((i % 4) + 1).cast("bigint").alias("quantity"),
            (((i % 19) + 1) * 100).cast("decimal(18,2)").alias("order_amount"),
        ))
    incoming = normalize(corrections.unionByName(new_records), source_ts)

incoming = incoming.cache()
incoming_count = incoming.count()
expected_incoming = 5249 if phase == "bootstrap" else 23
assert incoming_count == expected_incoming, "P2_DQ_FAILED: unexpected batch row count"
assert incoming.select("order_id", "sku_id").distinct().count() == incoming_count, \
    "P2_DQ_FAILED: duplicate business keys in incoming batch"
assert incoming.where(
    F.col("order_id").isNull() | F.col("sku_id").isNull() |
    F.col("source_updated_at").isNull() | F.col("row_hash").isNull() |
    F.col("quantity").isNull() | (F.col("quantity") <= 0) |
    F.col("order_amount").isNull() | (F.col("order_amount") < 0)
).limit(1).count() == 0, "P2_DQ_FAILED: invalid input row"

# Create dedicated managed tables if absent; NEVER overwrite legacy Phase 1 tables.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    order_id STRING,
    sku_id STRING,
    order_status STRING,
    quantity BIGINT,
    order_amount DECIMAL(18,2),
    source_updated_at TIMESTAMP,
    row_hash STRING,
    last_batch_id STRING,
    applied_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK} (
    pipeline_name STRING,
    last_source_updated_at TIMESTAMP,
    last_batch_id STRING,
    recorded_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT} (
    attempt_id STRING,
    batch_id STRING,
    phase STRING,
    source_rows BIGINT,
    inserted_rows BIGINT,
    updated_rows BIGINT,
    unchanged_rows BIGINT,
    target_rows_before BIGINT,
    target_rows_after BIGINT,
    total_quantity BIGINT,
    total_source_amount DECIMAL(20,2),
    source_updated_at TIMESTAMP,
    executed_at TIMESTAMP,
    result STRING
) USING DELTA
""")

watermark_rows = spark.table(WATERMARK).where(
    F.col("pipeline_name") == "project07_p2_order_lines"
).select("last_source_updated_at").collect()
assert len(watermark_rows) <= 1, "P2_WATERMARK_FAILED: multiple watermark rows"
last_watermark = watermark_rows[0][0] if watermark_rows else None
incoming_timestamp = datetime.strptime(source_ts, "%Y-%m-%d %H:%M:%S")
if last_watermark is not None and incoming_timestamp < last_watermark:
    raise ValueError("P2_STALE_BATCH_REJECTED: source timestamp precedes watermark")

target_before = spark.table(TARGET)
before_rows = target_before.count()
# Determine expected changes against the current snapshot, pre-MERGE.
comparison = incoming.alias("s").join(
    target_before.select("order_id", "sku_id", "row_hash", "source_updated_at").alias("t"),
    on=["order_id", "sku_id"], how="left"
)
changes = comparison.agg(
    F.sum(F.when(F.col("t.row_hash").isNull(), 1).otherwise(0)).alias("inserted"),
    F.sum(F.when(
        F.col("t.row_hash").isNotNull()
        & (F.col("s.source_updated_at") > F.col("t.source_updated_at"))
        & (F.col("s.row_hash") != F.col("t.row_hash")), 1
    ).otherwise(0)).alias("updated")
).first()
inserted = int(changes["inserted"] or 0)
updated = int(changes["updated"] or 0)
unchanged = incoming_count - inserted - updated

# Database performs an atomic MERGE for the target table itself.
# Hash and timestamp guards make an unchanged or replayed batch a no-op.
incoming.createOrReplaceTempView("p2_incoming_orders")
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING p2_incoming_orders AS s
ON t.order_id = s.order_id AND t.sku_id = s.sku_id
WHEN MATCHED AND s.source_updated_at > t.source_updated_at
  AND s.row_hash <> t.row_hash
THEN UPDATE SET
    t.order_status = s.order_status,
    t.quantity = s.quantity,
    t.order_amount = s.order_amount,
    t.source_updated_at = s.source_updated_at,
    t.row_hash = s.row_hash,
    t.last_batch_id = '{batch_id}',
    t.applied_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    order_id, sku_id, order_status, quantity, order_amount,
    source_updated_at, row_hash, last_batch_id, applied_at
) VALUES (
    s.order_id, s.sku_id, s.order_status, s.quantity, s.order_amount,
    s.source_updated_at, s.row_hash, '{batch_id}', current_timestamp()
)
""")

after = spark.table(TARGET)
after_count = after.count()
expected_total = 5249 if phase == "bootstrap" else 5269
assert after_count == expected_total, \
    "P2_RECONCILIATION_FAILED: unexpected final row count"
assert after.select("order_id", "sku_id").distinct().count() == after_count, \
    "P2_RECONCILIATION_FAILED: duplicate target keys"
totals = after.agg(
    F.sum("quantity").alias("total_quantity"),
    F.sum("order_amount").cast("decimal(20,2)").alias("total_source_amount")
).first()
# On replay, every incoming row must be unchanged, including applied_at.
if phase == "replay":
    assert inserted == 0 and updated == 0 and unchanged == 23, \
        "P2_IDEMPOTENCY_FAILED: replay applied mutations"

# Only advance/check the watermark after successful target validation.
spark.sql(f"""
MERGE INTO {WATERMARK} AS t
USING (SELECT 'project07_p2_order_lines' AS pipeline_name,
              timestamp('{source_ts}') AS last_source_updated_at,
              '{batch_id}' AS last_batch_id) AS s
ON t.pipeline_name = s.pipeline_name
WHEN MATCHED AND s.last_source_updated_at > t.last_source_updated_at
THEN UPDATE SET
    t.last_source_updated_at = s.last_source_updated_at,
    t.last_batch_id = s.last_batch_id,
    t.recorded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_source_updated_at, last_batch_id, recorded_at
) VALUES (
    s.pipeline_name, s.last_source_updated_at, s.last_batch_id, current_timestamp()
)
""")
final_watermark = spark.table(WATERMARK).where(
    F.col("pipeline_name") == "project07_p2_order_lines"
).first()["last_source_updated_at"]
assert final_watermark >= incoming_timestamp, "P2_WATERMARK_FAILED: not advanced"

# Each attempt is auditable; no data-record mutation is needed for replay.
audit_schema = StructType([
    StructField("attempt_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("phase", StringType(), False),
    StructField("source_rows", LongType(), False),
    StructField("inserted_rows", LongType(), False),
    StructField("updated_rows", LongType(), False),
    StructField("unchanged_rows", LongType(), False),
    StructField("target_rows_before", LongType(), False),
    StructField("target_rows_after", LongType(), False),
    StructField("total_quantity", LongType(), False),
    StructField("total_source_amount", DecimalType(20,2), False),
    StructField("source_updated_at", TimestampType(), False),
    StructField("executed_at", TimestampType(), False),
    StructField("result", StringType(), False),
])
attempt_id = str(uuid.uuid4())
record = (
    attempt_id, batch_id, phase, incoming_count, inserted, updated, unchanged,
    before_rows, after_count, int(totals["total_quantity"]),
    Decimal(str(totals["total_source_amount"])), incoming_timestamp,
    datetime.now(timezone.utc).replace(tzinfo=None), "PASS"
)
spark.createDataFrame([record], audit_schema).write.format("delta").mode("append").saveAsTable(AUDIT)

print("P2_RESULT=PASS")
print("P2_PHASE=" + phase)
print("P2_BATCH_ID=" + batch_id)
print("P2_ATTEMPT_ID=" + attempt_id)
print("P2_SOURCE_ROWS=" + str(incoming_count))
print("P2_INSERTED=" + str(inserted))
print("P2_UPDATED=" + str(updated))
print("P2_UNCHANGED=" + str(unchanged))
print("P2_TARGET_ROWS_BEFORE=" + str(before_rows))
print("P2_TARGET_ROWS_AFTER=" + str(after_count))
print("P2_TOTAL_QUANTITY=" + str(totals["total_quantity"]))
print("P2_SOURCE_ORDER_AMOUNT=" + str(totals["total_source_amount"]))
print("P2_WATERMARK=" + str(final_watermark))
print("P2_WRITES=workspace.project07_sandbox.p2_* only")
incoming.unpersist()
