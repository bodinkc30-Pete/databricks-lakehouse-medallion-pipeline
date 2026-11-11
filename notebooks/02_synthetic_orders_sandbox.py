# Databricks notebook source
# Project 07: first safe live PySpark job (read-only)
# Data source: SYNTHETIC orders only. No actual customer fields or production tables.
from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/synthetic_orders.csv"
print("PROJECT=07")
print("DATA_CLASSIFICATION=SYNTHETIC_ONLY")
print("PIPELINE_MODE=READ_ONLY")

orders = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(SOURCE_PATH)
)

required = {"Order ID", "SKU ID", "Quantity", "Order Amount", "Phone #", "Detail Address"}
missing = sorted(required - set(orders.columns))
assert not missing, "Missing source columns: " + ", ".join(missing)
assert len(orders.columns) == 65, "Expected 65 source columns"

stats = orders.select(
    F.count("*").alias("rows"),
    F.countDistinct(F.struct(F.col("Order ID"), F.col("SKU ID"))).alias("unique_keys"),
    F.sum(F.when(F.col("Order ID").isNull() | (F.trim(F.col("Order ID")) == ""), 1).otherwise(0)).alias("null_order_ids"),
    F.sum(F.when(F.col("Phone #").isNotNull() & (F.trim(F.col("Phone #")) != ""), 1).otherwise(0)).alias("nonempty_phones"),
    F.sum(F.when(F.col("Detail Address").isNotNull() & (F.trim(F.col("Detail Address")) != ""), 1).otherwise(0)).alias("nonempty_addresses"),
).first()
assert stats["rows"] == 5249, "Wrong synthetic row count"
assert stats["unique_keys"] == 5249, "Duplicate Order ID / SKU ID"
assert stats["null_order_ids"] == 0, "Order key missing"
assert stats["nonempty_phones"] == 0, "Sensitive field unexpectedly filled"
assert stats["nonempty_addresses"] == 0, "Sensitive field unexpectedly filled"
print("SOURCE_ROWS=" + str(stats["rows"]))
print("SOURCE_COLUMNS=" + str(len(orders.columns)))
print("UNIQUE_ORDER_SKU=" + str(stats["unique_keys"]))
print("SENSITIVE_FIELDS_EMPTY=True")
print("RESULT=PASS")
