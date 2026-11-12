# Databricks notebook source
# Project 07 Phase 3, Task 1: create immutable versioned synthetic fact snapshots.
# WRITES only to workspace.project07_sandbox.p3_*; READS Phase 2 synthetic target.
# Scenarios: baseline -> fail_duplicate -> fail_null -> recovery.
from pyspark.sql import functions as F

dbutils.widgets.dropdown("scenario", "baseline", ["baseline", "fail_duplicate", "fail_null", "recovery"])
scenario = dbutils.widgets.get("scenario")
mapping = {
    "baseline": ("p3_v1", 1, 5269, 13175, "5265850.00"),
    "fail_duplicate": ("p3_v2_bad_duplicate", 2, 5270, None, None),
    "fail_null": ("p3_v2_bad_null", 2, 5269, 13176, "5265860.00"),
    "recovery": ("p3_v2_valid", 2, 5269, 13176, "5265860.00"),
}
assert scenario in mapping, "P3_UNSUPPORTED_SCENARIO"
snapshot_id, version_no, expected_rows, expected_units, expected_amount = mapping[scenario]
SCHEMA = "workspace.project07_sandbox"
SOURCE = SCHEMA + ".p2_incremental_order_lines"
STAGE = SCHEMA + ".p3_snapshot_lines"
assert STAGE.startswith(SCHEMA + ".p3_")

incoming = spark.table(SOURCE).select(
    "order_id", "sku_id", "order_status", "quantity",
    "order_amount", "source_updated_at",
)
source_total = incoming.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id", "sku_id").alias("distinct_keys"),
    F.sum("quantity").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
).first()
assert source_total["rows"] == source_total["distinct_keys"] == 5269
assert source_total["units"] == 13175
assert str(source_total["amount"]) == "5265850.00"
assert incoming.filter(~F.col("order_id").startswith("SYN-ORDER-")).limit(1).count() == 0

if scenario != "baseline":
    # Simulate a legitimate corrected order record for the prospective v2 release.
    key = "SYN-ORDER-00005250"
    assert incoming.filter(F.col("order_id") == key).count() == 1
    incoming = (incoming.withColumn(
        "quantity", F.when(F.col("order_id") == key, F.col("quantity") + 1)
                     .otherwise(F.col("quantity")))
        .withColumn("order_amount",
            F.when(F.col("order_id") == key, F.col("order_amount") + F.lit(10))
             .otherwise(F.col("order_amount")).cast("decimal(18,2)")
        ))
if scenario == "fail_duplicate":
    # Deliberate failure injection: same business key appears twice.
    duplicate = incoming.filter(F.col("order_id") == "SYN-ORDER-00005250")
    incoming = incoming.unionByName(duplicate)
if scenario == "fail_null":
    # Deliberate failure injection: a required identifier becomes NULL.
    incoming = incoming.withColumn(
        "order_id", F.when(F.col("order_id") == "SYN-ORDER-00005250",
                           F.lit(None).cast("string")).otherwise(F.col("order_id")))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STAGE} (
    snapshot_id STRING,
    version_no INT,
    order_id STRING,
    sku_id STRING,
    order_status STRING,
    quantity BIGINT,
    order_amount DECIMAL(18,2),
    source_updated_at TIMESTAMP,
    row_hash STRING,
    staged_at TIMESTAMP
) USING DELTA
""")

# Important: snapshot IDs are immutable. Never overwrite or silently repair a
# failed snapshot; a recovery creates its own new, separately identifiable ID.
existing = spark.table(STAGE).where(F.col("snapshot_id") == snapshot_id).count()
if existing == 0:
    incoming = incoming.select(
        F.lit(snapshot_id).alias("snapshot_id"),
        F.lit(version_no).cast("int").alias("version_no"),
        "order_id", "sku_id", "order_status",
        F.col("quantity").cast("bigint").alias("quantity"),
        F.col("order_amount").cast("decimal(18,2)").alias("order_amount"),
        "source_updated_at",
    ).withColumn(
        "row_hash", F.sha2(F.concat_ws(
            "|", F.coalesce("order_id", F.lit("<NULL>")), "sku_id",
            "order_status", F.col("quantity").cast("string"),
            F.col("order_amount").cast("string")
        ), 256)
    ).withColumn("staged_at", F.current_timestamp())
    incoming.write.format("delta").mode("append").saveAsTable(STAGE)
    print("P3_STAGE_ACTION=APPEND_NEW_IMMUTABLE_SNAPSHOT")
else:
    print("P3_STAGE_ACTION=REUSE_EXISTING_SNAPSHOT")
post = spark.table(STAGE).where(F.col("snapshot_id") == snapshot_id)
assert post.count() == expected_rows, "P3_STAGE_CONTRACT_FAILED: snapshot row count"
if expected_units is not None:
    totals = post.agg(
        F.sum("quantity").alias("units"),
        F.sum("order_amount").cast("decimal(20,2)").alias("amount"),
    ).first()
    assert totals["units"] == expected_units
    assert str(totals["amount"]) == expected_amount

print("P3_STAGE_RESULT=PASS")
print("P3_SCENARIO=" + scenario)
print("P3_SNAPSHOT_ID=" + snapshot_id)
print("P3_STAGE_ROWS=" + str(expected_rows))
