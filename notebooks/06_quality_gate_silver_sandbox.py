# Databricks notebook source
# PROJECT 07 | Task 3/4: stop Gold publication on invalid Silver data.
from pyspark.sql import functions as F

dbutils.widgets.dropdown("inject_duplicate", "false", ["false", "true"])
inject_duplicate = dbutils.widgets.get("inject_duplicate").lower() == "true"
SOURCE = "workspace.project07_sandbox.silver_orders"
assert SOURCE.startswith("workspace.project07_sandbox.")
clean = spark.table(SOURCE)
df = clean.unionByName(clean.limit(1)) if inject_duplicate else clean
agg = df.agg(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct(F.col("order_id"), F.col("sku_id"))).alias("unique_keys"),
    F.sum(F.when(F.col("order_id").isNull() | F.col("sku_id").isNull()
        | (F.trim(F.col("order_id")) == "") | (F.trim(F.col("sku_id")) == ""), 1).otherwise(0)).alias("missing_keys"),
    F.sum(F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), 1).otherwise(0)).alias("bad_qty"),
    F.sum(F.when(F.col("order_amount").isNull() | (F.col("order_amount") < 0), 1).otherwise(0)).alias("bad_amount"),
).first()
issues = []
if agg["rows"] != 5249: issues.append("row_count")
if agg["rows"] != agg["unique_keys"]: issues.append("duplicate_keys")
for rule in ("missing_keys", "bad_qty", "bad_amount"):
    if agg[rule]: issues.append(rule)
print("STAGE=QA_GATE")
print("INJECT_DUPLICATE=" + str(inject_duplicate))
print("ROWS=" + str(agg["rows"]))
print("DUPLICATES=" + str(agg["rows"] - agg["unique_keys"]))
if issues:
    print("QA_RESULT=FAIL")
    raise ValueError("DQ_GATE_FAILED: " + ",".join(issues))
print("QA_RESULT=PASS")
