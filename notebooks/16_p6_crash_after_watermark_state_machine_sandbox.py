# Databricks notebook source
# Project 07 — Failure Recovery Lab 02: crash AFTER checkpoint, BEFORE completion.
# Synthetic-only supervised experiment. Only new workspace.project07_sandbox.p6_*
# objects are written; all earlier phase tables and published Gold are READ ONLY.
# Run manually in order: inject_crash (EXPECTED FAIL), recover, replay (PASS).
#
# Crash injection uses an intentionally raised RuntimeError; not a real compute
# shutdown. State transitions are committed as separate Delta transactions.
# This event-sourced state machine is NOT a cross-table atomic transaction.
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, DecimalType,
)

dbutils.widgets.dropdown(
    "mode", "inject_crash", ["inject_crash", "recover", "replay"]
)
mode = dbutils.widgets.get("mode").strip()
assert mode in {"inject_crash", "recover", "replay"}, "P6_INVALID_MODE"

SCHEMA = "workspace.project07_sandbox"
SOURCE = SCHEMA + ".p5_recovery_target"
GOLD_MANIFEST = SCHEMA + ".p3_publish_manifest"
TARGET = SCHEMA + ".p6_recovery_target"
WATERMARK = SCHEMA + ".p6_recovery_watermark"
EVENTS = SCHEMA + ".p6_state_events"
COMPLETION = SCHEMA + ".p6_completion_audit"
assert all(
    name.startswith("workspace.project07_sandbox.p6_")
    for name in (TARGET, WATERMARK, EVENTS, COMPLETION)
), "P6_UNSAFE_WRITE_TARGET"

PIPELINE = "project07_p6_lab02"
BATCH_ID = "p6_incremental_20260204"
INITIAL_TS = "2026-02-03 00:00:00"
BATCH_TS = "2026-02-04 00:00:00"
INITIAL_DATE = datetime.strptime(INITIAL_TS, "%Y-%m-%d %H:%M:%S")
BATCH_DATE = datetime.strptime(BATCH_TS, "%Y-%m-%d %H:%M:%S")
BASE_ROWS, FINAL_ROWS, BATCH_ROWS = 5274, 5277, 4
EXPECTED_UNITS, EXPECTED_AMOUNT = 13200, "5275100.00"

# Fail before writing anything if the known synthetic source or trusted Gold
# have drifted. A fresh isolated target starts as a CTAS clone of this source.
source = spark.table(SOURCE)
stats = source.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id", "sku_id").alias("distinct_keys"),
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert stats["rows"] == stats["distinct_keys"] == BASE_ROWS
assert stats["units"] == 13192 and str(stats["amount"]) == "5270890.00"
assert source.filter(
    ~F.col("order_id").startswith("SYN-ORDER-")
).limit(1).count() == 0, "P6_PRIVATE_SOURCE_REFUSED"

gold = spark.table(GOLD_MANIFEST).where(
    F.col("pipeline_name") == "project07_p3_gold"
).collect()
assert len(gold) == 1
assert gold[0]["active_snapshot_id"] == "p3_v2_valid"
assert gold[0]["version_no"] == 2

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
CREATE TABLE IF NOT EXISTS {EVENTS} (
    event_id STRING,
    attempt_id STRING,
    batch_id STRING,
    run_mode STRING,
    state STRING,
    step_index INT,
    target_rows BIGINT,
    inserted_rows BIGINT,
    updated_rows BIGINT,
    watermark_at_event TIMESTAMP,
    error_code STRING,
    recorded_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {COMPLETION} (
    batch_id STRING,
    completed_by_attempt STRING,
    target_rows BIGINT,
    total_units BIGINT,
    total_source_amount DECIMAL(20,2),
    last_source_updated_at TIMESTAMP,
    completed_at TIMESTAMP
) USING DELTA
""")
# Seed ONLY when this dedicated lab has no watermark, never rewrite a checkpoint.
spark.sql(f"""
MERGE INTO {WATERMARK} t
USING (
    SELECT '{PIPELINE}' AS pipeline_name,
           timestamp('{INITIAL_TS}') AS last_source_updated_at,
           'p6_initial_checkpoint' AS last_batch_id
) s
ON t.pipeline_name = s.pipeline_name
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_source_updated_at, last_batch_id, recorded_at
) VALUES (
    s.pipeline_name, s.last_source_updated_at, s.last_batch_id,
    current_timestamp()
)
""")
watermark_rows = spark.table(WATERMARK).where(
    F.col("pipeline_name") == PIPELINE
).collect()
assert len(watermark_rows) == 1, "P6_DUPLICATE_WATERMARK"
checkpoint_before = watermark_rows[0]["last_source_updated_at"]
target_before = spark.table(TARGET)
before_rows = target_before.count()
events_before = spark.table(EVENTS)
existing_events = events_before.count()
completion_before = spark.table(COMPLETION).where(
    F.col("batch_id") == BATCH_ID
).count()

# The status ledger is a durable append-only transition log. No attempt may
# jump states, and re-injection of a batch that already committed is refused.
if mode == "inject_crash":
    assert before_rows == BASE_ROWS, "P6_INJECTION_REFUSED: target not pristine"
    assert checkpoint_before == INITIAL_DATE
    assert existing_events == 0 and completion_before == 0, \
        "P6_INJECTION_REFUSED: state history already exists"
elif mode == "recover":
    assert before_rows == FINAL_ROWS, "P6_RECOVERY_REFUSED: MERGE not committed"
    assert checkpoint_before == BATCH_DATE, \
        "P6_RECOVERY_REFUSED: checkpoint was not committed"
    assert completion_before == 0, "P6_RECOVERY_REFUSED: batch already completed"
    assert events_before.filter(
        (F.col("batch_id") == BATCH_ID) & (F.col("state") == "FAILED")
    ).count() == 1, "P6_RECOVERY_REFUSED: no verified failure marker"
    assert events_before.filter(F.col("state") == "COMPLETED").count() == 0
else:
    assert before_rows == FINAL_ROWS and checkpoint_before == BATCH_DATE
    assert completion_before == 1, "P6_REPLAY_REFUSED: missing completion"
    assert events_before.filter(
        (F.col("batch_id") == BATCH_ID) & (F.col("state") == "COMPLETED")
    ).count() >= 1

# Fabricate exactly one corrected synthetic order plus three novel order keys.
correction = (
    source.where(F.col("order_id") == "SYN-ORDER-00005252")
    .select("order_id", "sku_id", "order_status", "quantity", "order_amount")
    .withColumn("order_status", F.lit("Completed"))
    .withColumn("quantity", (F.col("quantity") + 1).cast("bigint"))
    .withColumn(
        "order_amount",
        (F.col("order_amount") + F.lit(10)).cast("decimal(18,2)")
    )
)
assert correction.count() == 1, "P6_CORRECTION_SOURCE_MISSING"
i = F.col("id")
new_rows = spark.range(5275, 5278).select(
    F.format_string("SYN-ORDER-%08d", i.cast("int")).alias("order_id"),
    F.format_string("SYN-SKU-%04d", ((i % 53) + 1).cast("int")).alias("sku_id"),
    F.when((i % 3) == 0, "Completed")
     .when((i % 3) == 1, "Processing")
     .otherwise("Cancelled").alias("order_status"),
    ((i % 4) + 1).cast("bigint").alias("quantity"),
    (((i % 19) + 1) * 100).cast("decimal(18,2)").alias("order_amount"),
)
incoming = (
    correction.unionByName(new_rows)
    .withColumn("source_updated_at", F.to_timestamp(F.lit(BATCH_TS)))
    .withColumn("row_hash", F.sha2(F.concat_ws(
        "|", F.col("order_id"), F.col("sku_id"), F.col("order_status"),
        F.col("quantity").cast("string"),
        F.col("order_amount").cast("string"),
    ), 256))
).cache()
assert incoming.count() == BATCH_ROWS
assert incoming.select("order_id", "sku_id").distinct().count() == BATCH_ROWS
assert incoming.where(
    F.col("order_id").isNull() | F.col("sku_id").isNull()
    | F.col("row_hash").isNull() | F.col("source_updated_at").isNull()
    | F.col("quantity").isNull() | (F.col("quantity") <= 0)
    | F.col("order_amount").isNull() | (F.col("order_amount") < 0)
).count() == 0, "P6_DATA_QUALITY_FAILED"

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("attempt_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("run_mode", StringType(), False),
    StructField("state", StringType(), False),
    StructField("step_index", IntegerType(), False),
    StructField("target_rows", LongType(), False),
    StructField("inserted_rows", LongType(), False),
    StructField("updated_rows", LongType(), False),
    StructField("watermark_at_event", TimestampType(), True),
    StructField("error_code", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
])
ALLOWED_NEXT = {
    None: "STARTED",
    "STARTED": "MERGED",
    "MERGED": "CHECKPOINTED",
    "CHECKPOINTED": ("FAILED", "COMPLETED"),
}
attempt_id = str(uuid.uuid4())


def transition(state, target_rows, ins, upd, checkpoint, error=""):
    """Validate each transition before persisting a separate Delta event."""
    prior = (
        spark.table(EVENTS)
        .where(F.col("attempt_id") == attempt_id)
        .orderBy(F.desc("step_index"))
        .limit(1).collect()
    )
    previous = prior[0]["state"] if prior else None
    allowed = ALLOWED_NEXT.get(previous)
    allowed_states = (allowed,) if isinstance(allowed, str) else allowed
    assert allowed_states and state in allowed_states, \
        "P6_INVALID_STATE_TRANSITION"
    step_index = 1 if not prior else int(prior[0]["step_index"]) + 1
    row = (
        str(uuid.uuid4()), attempt_id, BATCH_ID, mode, state, step_index,
        int(target_rows), int(ins), int(upd), checkpoint, error,
        datetime.now(timezone.utc).replace(tzinfo=None),
    )
    spark.createDataFrame([row], schema=event_schema) \
        .write.format("delta").mode("append").saveAsTable(EVENTS)
    assert spark.table(EVENTS).where(
        (F.col("attempt_id") == attempt_id)
        & (F.col("step_index") == step_index)
        & (F.col("state") == state)
    ).count() == 1, "P6_STATE_PERSISTENCE_FAILED"
    print("P6_STATE=" + state + " step=" + str(step_index))


transition("STARTED", before_rows, 0, 0, checkpoint_before)

# With the same business key and exact batch timestamp, a restart is an
# idempotent data-level no-op, even though the checkpoint already advanced.
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
    ).otherwise(0)).alias("updated"),
).first()
inserted = int(changes["inserted"] or 0)
updated = int(changes["updated"] or 0)
unchanged = BATCH_ROWS - inserted - updated
if mode == "inject_crash":
    assert (inserted, updated, unchanged) == (3, 1, 0), \
        "P6_INITIAL_CHANGE_CONTRACT_FAILED"
else:
    assert (inserted, updated, unchanged) == (0, 0, 4), \
        "P6_RECOVERY_IDEMPOTENCY_FAILED"

incoming.createOrReplaceTempView("p6_incoming_batch")
spark.sql(f"""
MERGE INTO {TARGET} t
USING p6_incoming_batch s
ON t.order_id=s.order_id AND t.sku_id=s.sku_id
WHEN MATCHED AND s.source_updated_at > t.source_updated_at
  AND s.row_hash <> t.row_hash
THEN UPDATE SET
    t.order_status=s.order_status,
    t.quantity=s.quantity,
    t.order_amount=s.order_amount,
    t.source_updated_at=s.source_updated_at,
    t.row_hash=s.row_hash,
    t.last_batch_id='{BATCH_ID}',
    t.applied_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    order_id,sku_id,order_status,quantity,order_amount,
    source_updated_at,row_hash,last_batch_id,applied_at
) VALUES (
    s.order_id,s.sku_id,s.order_status,s.quantity,s.order_amount,
    s.source_updated_at,s.row_hash,'{BATCH_ID}',current_timestamp()
)
""")
after = spark.table(TARGET)
totals = after.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id", "sku_id").alias("distinct_keys"),
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert totals["rows"] == totals["distinct_keys"] == FINAL_ROWS
assert totals["units"] == EXPECTED_UNITS
assert str(totals["amount"]) == EXPECTED_AMOUNT
transition("MERGED", FINAL_ROWS, inserted, updated, checkpoint_before)

# The injection fails only AFTER the checkpoint transaction has committed.
spark.sql(f"""
MERGE INTO {WATERMARK} t
USING (
    SELECT '{PIPELINE}' AS pipeline_name,
           timestamp('{BATCH_TS}') AS last_source_updated_at,
           '{BATCH_ID}' AS last_batch_id
) s
ON t.pipeline_name=s.pipeline_name
WHEN MATCHED AND s.last_source_updated_at > t.last_source_updated_at
THEN UPDATE SET
    t.last_source_updated_at=s.last_source_updated_at,
    t.last_batch_id=s.last_batch_id,
    t.recorded_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pipeline_name,last_source_updated_at,last_batch_id,recorded_at
) VALUES (
    s.pipeline_name,s.last_source_updated_at,s.last_batch_id,
    current_timestamp()
)
""")
watermark_after_rows = spark.table(WATERMARK).where(
    F.col("pipeline_name") == PIPELINE
).collect()
assert len(watermark_after_rows) == 1
assert watermark_after_rows[0]["last_source_updated_at"] == BATCH_DATE
transition("CHECKPOINTED", FINAL_ROWS, inserted, updated, BATCH_DATE)

if mode == "inject_crash":
    # Intentionally record the FAILED event before raising, for a reproducible
    # training incident. A true hard crash may instead leave CHECKPOINTED as
    # the latest event; real recovery must also handle that incomplete state.
    assert spark.table(COMPLETION).count() == 0
    transition(
        "FAILED", FINAL_ROWS, inserted, updated, BATCH_DATE,
        "P6_INJECTED_CRASH_AFTER_WATERMARK",
    )
    print("P6_WATERMARK_COMMITTED=YES")
    print("P6_COMPLETION_AUDIT_ROWS=0")
    print("P6_FAILURE_POINT=AFTER_WATERMARK_BEFORE_COMPLETION_AUDIT")
    incoming.unpersist()
    raise RuntimeError(
        "P6_INJECTED_CRASH_AFTER_WATERMARK: Delta MERGE and checkpoint "
        "committed, completion audit deliberately NOT written. "
        "Recover the SAME batch despite the advanced watermark."
    )

# Completion is idempotent by batch_id. Do not create a second completion
# entry when the identical batch is replayed.
spark.sql(f"""
MERGE INTO {COMPLETION} t
USING (
    SELECT '{BATCH_ID}' AS batch_id,
           '{attempt_id}' AS completed_by_attempt,
           CAST({FINAL_ROWS} AS BIGINT) AS target_rows,
           CAST({EXPECTED_UNITS} AS BIGINT) AS total_units,
           CAST({EXPECTED_AMOUNT} AS DECIMAL(20,2)) AS total_source_amount,
           timestamp('{BATCH_TS}') AS last_source_updated_at
) s
ON t.batch_id=s.batch_id
WHEN NOT MATCHED THEN INSERT (
    batch_id,completed_by_attempt,target_rows,total_units,
    total_source_amount,last_source_updated_at,completed_at
) VALUES (
    s.batch_id,s.completed_by_attempt,s.target_rows,s.total_units,
    s.total_source_amount,s.last_source_updated_at,current_timestamp()
)
""")
completions = spark.table(COMPLETION).where(F.col("batch_id") == BATCH_ID)
recorded = completions.collect()
assert len(recorded) == 1, "P6_DUPLICATE_BATCH_COMPLETION"
assert recorded[0]["target_rows"] == FINAL_ROWS
assert recorded[0]["total_units"] == EXPECTED_UNITS
assert str(recorded[0]["total_source_amount"]) == EXPECTED_AMOUNT

transition("COMPLETED", FINAL_ROWS, inserted, updated, BATCH_DATE)
# Verify exact states of this attempt, and committed Gold from the older lab.
observed = (
    spark.table(EVENTS)
    .where(F.col("attempt_id") == attempt_id)
    .orderBy("step_index")
    .select("state").collect()
)
assert [r["state"] for r in observed] == [
    "STARTED", "MERGED", "CHECKPOINTED", "COMPLETED"
]
assert spark.table(GOLD_MANIFEST).where(
    F.col("pipeline_name") == "project07_p3_gold"
).first()["active_snapshot_id"] == "p3_v2_valid"
incoming.unpersist()

print("P6_LAB02_RESULT=PASS")
print("P6_MODE=" + mode)
print("P6_INSERTED=" + str(inserted))
print("P6_UPDATED=" + str(updated))
print("P6_UNCHANGED=" + str(unchanged))
print("P6_FINAL_ROWS=" + str(FINAL_ROWS))
print("P6_FINAL_UNITS=" + str(EXPECTED_UNITS))
print("P6_FINAL_SOURCE_AMOUNT=" + EXPECTED_AMOUNT)
print("P6_COMPLETION_AUDIT_ROWS=" + str(completions.count()))
print("P6_GOLD_UNCHANGED=p3_v2_valid")
