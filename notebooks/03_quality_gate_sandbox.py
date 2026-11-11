# Databricks notebook source
# Project 07 — QA/DQ Gate. Reads ONLY fabricated synthetic CSV; writes no tables.
from pyspark.sql import functions as F

dbutils.widgets.dropdown("inject_duplicate", "false", ["false", "true"])
inject_duplicate = dbutils.widgets.get("inject_duplicate").lower() == "true"
source_path = "/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/synthetic_orders.csv"

source_df = spark.read.option("header", "true").option("inferSchema", "false").csv(source_path)
required_columns = {"Order ID", "SKU ID", "Quantity", "Order Amount", "Phone #", "Detail Address"}
missing_columns = sorted(required_columns - set(source_df.columns))
if missing_columns or len(source_df.columns) != 65:
    raise ValueError("DQ_GATE_FAILED: invalid schema")

# For the Failure Lab, duplicate an in-memory synthetic row only.
# This does NOT modify the source CSV or any Delta table.
df = source_df
if inject_duplicate:
    df = df.unionByName(source_df.limit(1))

checks = df.agg(
    F.count("*").alias("source_rows"),
    F.countDistinct(F.struct(F.col("Order ID"), F.col("SKU ID"))).alias("unique_keys"),
    F.sum(F.when(F.col("Order ID").isNull() | (F.trim(F.col("Order ID")) == "")
                 | F.col("SKU ID").isNull() | (F.trim(F.col("SKU ID")) == ""), 1).otherwise(0)).alias("missing_keys"),
    F.sum(F.when(F.col("Quantity").cast("int").isNull()
                 | (F.col("Quantity").cast("int") <= 0)
                 | (F.col("Quantity").cast("double") != F.col("Quantity").cast("int")), 1).otherwise(0)).alias("bad_quantity"),
    F.sum(F.when(F.col("Order Amount").cast("double").isNull()
                 | F.isnan(F.col("Order Amount").cast("double"))
                 | (F.col("Order Amount").cast("double") < 0), 1).otherwise(0)).alias("bad_amount"),
    F.sum(F.when((F.trim(F.coalesce(F.col("Phone #"), F.lit(""))) != "")
                 | (F.trim(F.coalesce(F.col("Detail Address"), F.lit(""))) != ""), 1).otherwise(0)).alias("pii_populated")
).first()

count = checks["source_rows"]
duplicates = count - checks["unique_keys"]
print("PROJECT=07")
print("QA_GATE=SPARK_SYNTHETIC_ORDERS")
print("INJECT_DUPLICATE=" + str(inject_duplicate))
print("SOURCE_ROWS=" + str(count))
print("DUPLICATE_KEYS=" + str(duplicates))
print("MISSING_KEYS=" + str(checks["missing_keys"]))
print("BAD_QUANTITY=" + str(checks["bad_quantity"]))
print("BAD_AMOUNT=" + str(checks["bad_amount"]))
print("PII_POPULATED=" + str(checks["pii_populated"]))

errors = []
if not inject_duplicate and count != 5249:
    errors.append("row_count")
if duplicates:
    errors.append("duplicate_keys")
for name in ("missing_keys", "bad_quantity", "bad_amount", "pii_populated"):
    if checks[name]:
        errors.append(name)
if errors:
    print("QA_RESULT=FAIL")
    raise ValueError("DQ_GATE_FAILED: " + ",".join(errors))

print("QA_RESULT=PASS")
