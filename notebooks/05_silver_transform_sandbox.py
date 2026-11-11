# Databricks notebook source
# PROJECT 07 | Task 2/4: normalize and redact synthetic order lines.
from pyspark.sql import functions as F

SOURCE = "workspace.project07_sandbox.bronze_orders"
TARGET = "workspace.project07_sandbox.silver_orders"
assert SOURCE.startswith("workspace.project07_sandbox.")
assert TARGET.startswith("workspace.project07_sandbox.")

bronze = spark.table(SOURCE)
silver = bronze.select(
    F.col("order_id"),
    F.col("sku_id"),
    F.col("order_status"),
    F.col("quantity").cast("int").alias("quantity"),
    F.col("order_amount").cast("decimal(18,2)").alias("order_amount"),
    F.col("_source_system"),
    F.col("_ingested_at"),
)
assert silver.count() == 5249, "Silver row-count mismatch"
# No customer/recipient/tax identifiers in Silver and downstream Gold.
assert set(silver.columns) == {"order_id","sku_id","order_status","quantity","order_amount","_source_system","_ingested_at"}
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
print("STAGE=SILVER")
print("SOURCE_ROWS=5249")
print("TARGET_TABLE=" + TARGET)
print("RESULT=PASS")
