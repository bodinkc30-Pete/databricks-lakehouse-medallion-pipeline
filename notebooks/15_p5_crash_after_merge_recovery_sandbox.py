# Databricks notebook source
# PROJECT 07 - STEP 1 FAILURE RECOVERY LAB: crash after Delta MERGE.
# This is a synthetic-only, manually triggered fault-injection exercise.
# Allowed writes: workspace.project07_sandbox.p5_* (new dedicated tables ONLY).
# READ ONLY: historical Phase 2 and Phase 3 data and Gold manifest.
#
# Run order:
#   mode = inject_crash  --> expected Job FAILED *after* a successful MERGE
#   mode = recover       --> expected Job SUCCESS; no duplicate data writes
#   mode = replay        --> expected Job SUCCESS; another no-op MERGE
#
# No DROP / TRUNCATE / OVERWRITE and no external notifications.
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
    DecimalType,
)

dbutils.widgets.dropdown(
    "mode", "inject_crash", ["inject_crash", "recover", "replay"]
)
mode = dbutils.widgets.get("mode").strip()
assert mode in {"inject_crash", "recover", "replay"}, "P5_INVALID_MODE"

SCHEMA = "workspace.project07_sandbox"
SOURCE = SCHEMA + ".p2_incremental_order_lines"
GOLD_MANIFEST = SCHEMA + ".p3_publish_manifest"
TARGET = SCHEMA + ".p5_recovery_target"
WATERMARK = SCHEMA + ".p5_recovery_watermark"
AUDIT = SCHEMA + ".p5_recovery_audit"
assert all(name.startswith("workspace.project07_sandbox.p5_")
           for name in (TARGET, WATERMARK, AUDIT)), "P5_UNSAFE_WRITE_TARGET"

PIPELINE_NAME = "project07_p5_crash_recovery"
BATCH_ID = "p5_incremental_20260203"
PREVIOUS_TS = "2026-02-02 00:00:00"
BATCH_TS = "2026-02-03 00:00:00"
BATCH_TIMESTAMP = datetime.strptime(BATCH_TS, "%Y-%m-%d %H:%M:%S")
EXPECTED_BASE_ROWS = 5269
EXPECTED_FINAL_ROWS = 5274
EXPECTED_FINAL_UNITS = 13192
EXPECTED_FINAL_AMOUNT = "5270890.00"
EXPECTED_BATCH_ROWS = 7

# Confirm the exact synthetic Phase 2 dataset before any P5 mutation.
source = spark.table(SOURCE)
source_counts = source.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id", "sku_id").alias("keys"),
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert source_counts["rows"] == source_counts["keys"] == EXPECTED_BASE_ROWS
assert source_counts["units"] == 13175
assert str(source_counts["amount"]) == "5265850.00"
assert source.filter(~F.col("order_id").startswith("SYN-ORDER-")).limit(1).count() == 0

# Assert that the old trusted Gold snapshot is intact before the exercise.
manifest_before = spark.table(GOLD_MANIFEST).where(
    F.col("pipeline_name") == "project07_p3_gold"
).collect()
assert len(manifest_before) == 1
assert manifest_before[0]["active_snapshot_id"] == "p3_v2_valid"
assert manifest_before[0]["version_no"] == 2

# CTAS happens only once to create an isolated clone of P2 rows; subsequent
# failure/recovery runs reuse the same Delta state without reseeding.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET}
USING DELTA AS
SELECT order_id, sku_id, order_status, quantity, order_amount,
       source_updated_at, row_hash, last_batch_id, applied_at
FROM {SOURCE}
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
    mode STRING,
    batch_id STRING,
    before_watermark TIMESTAMP,
    after_watermark TIMESTAMP,
    target_rows_before BIGINT,
    target_rows_after BIGINT,
    source_rows BIGINT,
    inserted_rows BIGINT,
    updated_rows BIGINT,
    unchanged_rows BIGINT,
    total_quantity BIGINT,
    total_source_amount DECIMAL(20,2),
    result STRING,
    recorded_at TIMESTAMP
) USING DELTA
""")

# Seed this independent P5 watermark only if the row is absent.
spark.sql(f"""
MERGE INTO {WATERMARK} AS t
USING (
    SELECT '{PIPELINE_NAME}' AS pipeline_name,
           timestamp('{PREVIOUS_TS}') AS last_source_updated_at,
           'p5_initial_checkpoint' AS last_batch_id
) AS s
ON t.pipeline_name = s.pipeline_name
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_source_updated_at, last_batch_id, recorded_at
) VALUES (
    s.pipeline_name, s.last_source_updated_at, s.last_batch_id,
    current_timestamp()
)
""")
watermark_before_rows = spark.table(WATERMARK).where(
    F.col("pipeline_name") == PIPELINE_NAME
).collect()
assert len(watermark_before_rows) == 1, "P5_CHECKPOINT_DUPLICATED"
watermark_before = watermark_before_rows[0]["last_source_updated_at"]

# Deterministic batch: two P2 existing order-line corrections + five brand-new
# synthetic rows, all uniquely identified at (order_id, sku_id) grain.
corrections = (
    source.where(F.col("order_id").isin(
        "SYN-ORDER-00005250", "SYN-ORDER-00005251"
    ))
    .select("order_id", "sku_id", "order_status", "quantity", "order_amount")
    .withColumn("order_status", F.lit("Completed"))
    .withColumn("quantity", (F.col("quantity") + 2).cast("bigint"))
    .withColumn(
        "order_amount",
        (F.col("order_amount") + F.lit(20)).cast("decimal(18,2)")
    )
)
assert corrections.count() == 2, "P5_SOURCE_CORRECTION_CONTRACT_FAILED"
i = F.col("id")
new_rows = spark.range(5270, 5275).select(
    F.format_string("SYN-ORDER-%08d", i.cast("int")).alias("order_id"),
    F.format_string("SYN-SKU-%04d", ((i % 53) + 1).cast("int")).alias("sku_id"),
    F.when((i % 3) == 0, "Completed")
     .when((i % 3) == 1, "Processing")
     .otherwise("Cancelled").alias("order_status"),
    ((i % 4) + 1).cast("bigint").alias("quantity"),
    (((i % 19) + 1) * 100).cast("decimal(18,2)").alias("order_amount"),
)
incoming = (
    corrections.unionByName(new_rows)
    .withColumn("source_updated_at", F.to_timestamp(F.lit(BATCH_TS)))
    .withColumn("row_hash", F.sha2(F.concat_ws(
        "|", F.col("order_id"), F.col("sku_id"), F.col("order_status"),
        F.col("quantity").cast("string"),
        F.col("order_amount").cast("string")
    ), 256))
).cache()
assert incoming.count() == EXPECTED_BATCH_ROWS
assert incoming.select("order_id", "sku_id").distinct().count() == EXPECTED_BATCH_ROWS
assert incoming.filter(
    F.col("order_id").isNull() | F.col("sku_id").isNull()
    | F.col("source_updated_at").isNull() | F.col("row_hash").isNull()
    | F.col("quantity").isNull() | (F.col("quantity") <= 0)
    | F.col("order_amount").isNull() | (F.col("order_amount") < 0)
).count() == 0, "P5_DQ_FAILED"

target_before = spark.table(TARGET)
before_count = target_before.count()
audit_before = spark.table(AUDIT).count()

if mode == "inject_crash":
    assert before_count == EXPECTED_BASE_ROWS, \
        "P5_INJECTION_REFUSED: not a pristine isolated target"
    assert watermark_before == datetime.strptime(PREVIOUS_TS, "%Y-%m-%d %H:%M:%S")
    assert audit_before == 0, "P5_INJECTION_REFUSED: prior audit exists"
elif mode == "recover":
    assert before_count == EXPECTED_FINAL_ROWS, \
        "P5_RECOVERY_REFUSED: the injected MERGE was not persisted"
    assert watermark_before == datetime.strptime(PREVIOUS_TS, "%Y-%m-%d %H:%M:%S"), \
        "P5_RECOVERY_REFUSED: expected checkpoint to lag committed data"
    assert audit_before == 0, "P5_RECOVERY_REFUSED: unexpected earlier completed audit"
else:  # replay
    assert before_count == EXPECTED_FINAL_ROWS
    assert watermark_before == BATCH_TIMESTAMP
    assert audit_before >= 1, "P5_REPLAY_REFUSED: missing recovery audit"

# Use exactly the same guarded MERGE as Phase 2; a successful transaction
# persists even if the notebook/Job throws immediately afterward.
comparison = incoming.alias("s").join(
    target_before.select(
        "order_id", "sku_id", "row_hash", "source_updated_at"
    ).alias("t"),
    on=["order_id", "sku_id"], how="left",
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
unchanged = EXPECTED_BATCH_ROWS - inserted - updated

if mode == "inject_crash":
    assert (inserted, updated, unchanged) == (5, 2, 0)
else:
    assert (inserted, updated, unchanged) == (0, 0, 7), \
        "P5_IDEMPOTENCY_FAILED: committed rows unexpectedly changed"

incoming.createOrReplaceTempView("p5_recovery_batch")
spark.sql(f"""
MERGE INTO {TARGET} AS t
USING p5_recovery_batch AS s
ON t.order_id = s.order_id AND t.sku_id = s.sku_id
WHEN MATCHED AND s.source_updated_at > t.source_updated_at
  AND s.row_hash <> t.row_hash
THEN UPDATE SET
    t.order_status = s.order_status,
    t.quantity = s.quantity,
    t.order_amount = s.order_amount,
    t.source_updated_at = s.source_updated_at,
    t.row_hash = s.row_hash,
    t.last_batch_id = '{BATCH_ID}',
    t.applied_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    order_id, sku_id, order_status, quantity, order_amount,
    source_updated_at, row_hash, last_batch_id, applied_at
) VALUES (
    s.order_id, s.sku_id, s.order_status, s.quantity, s.order_amount,
    s.source_updated_at, s.row_hash, '{BATCH_ID}', current_timestamp()
)
""")

# Verify the commit *before* intentionally terminating the Job.
target_after = spark.table(TARGET)
after_count = target_after.count()
stats = target_after.agg(
    F.countDistinct("order_id", "sku_id").alias("distinct_keys"),
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert after_count == stats["distinct_keys"] == EXPECTED_FINAL_ROWS
assert stats["units"] == EXPECTED_FINAL_UNITS
assert str(stats["amount"]) == EXPECTED_FINAL_AMOUNT

print("P5_TARGET_MERGE_COMMITTED=YES")
print("P5_MODE=" + mode)
print("P5_DATA_ROWS_AFTER=" + str(after_count))
print("P5_INSERTED=" + str(inserted))
print("P5_UPDATED=" + str(updated))
print("P5_UNCHANGED=" + str(unchanged))
print("P5_DATA_UNITS_AFTER=" + str(stats["units"]))
print("P5_DATA_AMOUNT_AFTER=" + str(stats["amount"]))

if mode == "inject_crash":
    print("P5_FAILURE_POINT=AFTER_COMMITTED_MERGE_BEFORE_WATERMARK_AND_AUDIT")
    assert spark.table(WATERMARK).where(
        F.col("pipeline_name") == PIPELINE_NAME
    ).first()["last_source_updated_at"] == datetime.strptime(
        PREVIOUS_TS, "%Y-%m-%d %H:%M:%S"
    )
    assert spark.table(AUDIT).count() == 0
    incoming.unpersist()
    raise RuntimeError(
        "P5_INJECTED_CRASH_AFTER_MERGE: Delta data committed; "
        "watermark and completion audit deliberately not written. "
        "Run recovery with the SAME batch ID."
    )

# Recovery: the already-committed MERGE is a data-level no-op. Only now
# advance the independent P5 checkpoint, and then record an attempt audit.
spark.sql(f"""
MERGE INTO {WATERMARK} AS t
USING (
    SELECT '{PIPELINE_NAME}' AS pipeline_name,
           timestamp('{BATCH_TS}') AS last_source_updated_at,
           '{BATCH_ID}' AS last_batch_id
) AS s
ON t.pipeline_name = s.pipeline_name
WHEN MATCHED AND s.last_source_updated_at > t.last_source_updated_at
THEN UPDATE SET
    t.last_source_updated_at = s.last_source_updated_at,
    t.last_batch_id = s.last_batch_id,
    t.recorded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_source_updated_at, last_batch_id, recorded_at
) VALUES (
    s.pipeline_name, s.last_source_updated_at, s.last_batch_id,
    current_timestamp()
)
""")
watermark_after = spark.table(WATERMARK).where(
    F.col("pipeline_name") == PIPELINE_NAME
).collect()
assert len(watermark_after) == 1
assert watermark_after[0]["last_source_updated_at"] == BATCH_TIMESTAMP

audit_schema = StructType([
    StructField("attempt_id", StringType(), False),
    StructField("mode", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("before_watermark", TimestampType(), False),
    StructField("after_watermark", TimestampType(), False),
    StructField("target_rows_before", LongType(), False),
    StructField("target_rows_after", LongType(), False),
    StructField("source_rows", LongType(), False),
    StructField("inserted_rows", LongType(), False),
    StructField("updated_rows", LongType(), False),
    StructField("unchanged_rows", LongType(), False),
    StructField("total_quantity", LongType(), False),
    StructField("total_source_amount", DecimalType(20, 2), False),
    StructField("result", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
])
row = (
    str(uuid.uuid4()), mode, BATCH_ID, watermark_before, BATCH_TIMESTAMP,
    before_count, after_count, EXPECTED_BATCH_ROWS, inserted, updated,
    unchanged, int(stats["units"]), Decimal(str(stats["amount"])),
    "RECOVERED" if mode == "recover" else "REPLAY_NOOP",
    datetime.now(timezone.utc).replace(tzinfo=None),
)
spark.createDataFrame([row], schema=audit_schema) \
    .write.format("delta").mode("append").saveAsTable(AUDIT)

final_audit = spark.table(AUDIT)
expected_audits = 1 if mode == "recover" else audit_before + 1
assert final_audit.count() == expected_audits
assert final_audit.filter(
    (F.col("inserted_rows") != 0) | (F.col("updated_rows") != 0)
).count() == 0, "P5_RECOVERY_AUDIT_MISMATCH"
assert spark.table(GOLD_MANIFEST).where(
    F.col("pipeline_name") == "project07_p3_gold"
).first()["active_snapshot_id"] == "p3_v2_valid", \
    "P5_GOLD_UNEXPECTEDLY_CHANGED"
incoming.unpersist()

print("P5_RECOVERY_RESULT=PASS")
print("P5_WATERMARK=" + str(BATCH_TIMESTAMP))
print("P5_COMPLETED_AUDIT_ATTEMPTS=" + str(final_audit.count()))
print("P5_GOLD_UNCHANGED=p3_v2_valid")
print("P5_WRITES=workspace.project07_sandbox.p5_* only")
