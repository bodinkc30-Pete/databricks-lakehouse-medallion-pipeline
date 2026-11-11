# Databricks notebook source
# PROJECT 07 | Task 1/4: synthetic Bronze ingestion. Sandbox tables ONLY.
import re
from pyspark.sql import functions as F

SOURCE = "/Volumes/workspace/bronze/source_files/project07_portfolio_sandbox/synthetic_orders.csv"
TABLE = "workspace.project07_sandbox.bronze_orders"
assert TABLE.startswith("workspace.project07_sandbox."), "Unsafe write target"

raw = spark.read.option("header", "true").option("inferSchema", "false").csv(SOURCE)
required = {"Order ID", "SKU ID", "Quantity", "Order Amount", "Order Status", "Phone #", "Detail Address"}
assert len(raw.columns) == 65 and required.issubset(set(raw.columns)), "Bronze schema mismatch"
assert raw.count() == 5249, "Unexpected source count"
assert raw.where(F.col("Order ID").isNull() | ~F.col("Order ID").startswith("SYN-ORDER-")).limit(1).count() == 0, "Non-synthetic Order ID"

# Delta-compatible source schema: preserve all 65 fields; normalize names only.
safe_names = [re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") for name in raw.columns]
assert len(set(safe_names)) == len(safe_names), "Name normalization collision"
assert "order_id" in safe_names and "sku_id" in safe_names
bronze = (raw.toDF(*safe_names)
          .withColumn("_source_system", F.lit("project07_synthetic"))
          .withColumn("_ingested_at", F.current_timestamp()))
# Overwrite is confined to this dedicated new sandbox table, NEVER existing project tables.
bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
print("STAGE=BRONZE")
print("SOURCE_ROWS=5249")
print("NORMALIZED_COLUMNS=65")
print("TARGET_TABLE=" + TABLE)
print("RESULT=PASS")
