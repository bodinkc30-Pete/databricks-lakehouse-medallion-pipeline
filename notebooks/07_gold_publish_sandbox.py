# Databricks notebook source
# PROJECT 07 | Task 4/4: publish aggregates only after QA success dependency.
from pyspark.sql import functions as F

SOURCE = "workspace.project07_sandbox.silver_orders"
TARGET = "workspace.project07_sandbox.gold_orders_by_status"
assert SOURCE.startswith("workspace.project07_sandbox.")
assert TARGET.startswith("workspace.project07_sandbox.")

silver = spark.table(SOURCE)
gold = (silver.groupBy("order_status")
        .agg(F.count("*").alias("order_line_count"),
             F.sum("quantity").alias("units"),
             F.sum("order_amount").alias("total_order_amount")))
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
row = spark.table(TARGET).agg(F.sum("order_line_count").alias("rows")).first()
assert row["rows"] == 5249, "Gold reconciliation failed"
print("STAGE=GOLD")
print("SOURCE_ROWS=5249")
print("RECONCILED_ROWS=" + str(row["rows"]))
print("TARGET_TABLE=" + TARGET)
print("RESULT=PASS")
