# Databricks notebook source
# Project 07 / Phase 1: Synthetic Star Schema in a dedicated sandbox.
# READS ONLY: workspace.project07_sandbox.bronze_orders and silver_orders.
# WRITES ONLY: five new workspace.project07_sandbox.gold_* tables.
# Business warning: Order Amount in the synthetic file is not recognized revenue.
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.project07_sandbox"
BRONZE = f"{CATALOG_SCHEMA}.bronze_orders"
SILVER = f"{CATALOG_SCHEMA}.silver_orders"
OUTPUTS = {
    "dim_product": f"{CATALOG_SCHEMA}.gold_dim_product",
    "dim_sku": f"{CATALOG_SCHEMA}.gold_dim_sku",
    "dim_date": f"{CATALOG_SCHEMA}.gold_dim_date",
    "fact_order_lines": f"{CATALOG_SCHEMA}.gold_fact_order_lines",
    "fact_orders": f"{CATALOG_SCHEMA}.gold_fact_orders",
}
assert CATALOG_SCHEMA == "workspace.project07_sandbox"
assert all(name.startswith(CATALOG_SCHEMA + ".") for name in (BRONZE, SILVER, *OUTPUTS.values()))
EXPECTED_LINES = 5249

bronze = spark.table(BRONZE)
silver = spark.table(SILVER)
bronze_count = bronze.count()
silver_count = silver.count()
assert bronze_count == silver_count == EXPECTED_LINES, "GOLD_DQ_FAILED: source row count"
assert bronze.filter(F.col("_source_system") != "project07_synthetic").limit(1).count() == 0
assert silver.filter(~F.col("order_id").startswith("SYN-ORDER-")).limit(1).count() == 0

bronze_attributes = bronze.select(
    "order_id", "sku_id", "seller_sku", "product_name",
    "product_category", "variation", "created_time",
)
assert bronze_attributes.select("order_id", "sku_id").distinct().count() == EXPECTED_LINES
assert silver.select("order_id", "sku_id").distinct().count() == EXPECTED_LINES

# One-to-one join is required; never silently multiply order amounts.
base = silver.join(bronze_attributes, ["order_id", "sku_id"], "inner")
assert base.count() == EXPECTED_LINES, "GOLD_DQ_FAILED: missing or multiplied join rows"
base = base.withColumn(
    "_created_at", F.to_timestamp(F.col("created_time"), "dd/MM/yyyy HH:mm:ss")
).withColumn("_order_date", F.to_date(F.col("_created_at")))
assert base.filter(
    F.col("_created_at").isNull() |
    F.col("product_name").isNull() | (F.trim("product_name") == "") |
    F.col("seller_sku").isNull() | (F.trim("seller_sku") == "")
).limit(1).count() == 0, "GOLD_DQ_FAILED: missing dates/products"

# Product_id is not supplied by the synthetic source. Use deterministic
# derived product_key from normalized synthetic product_name, not a real ID.
def product_key():
    return F.sha2(F.lower(F.trim(F.col("product_name"))), 256)

sku_mapping = base.select(
    "sku_id", "seller_sku", "product_name", "product_category", "variation"
).distinct()
assert sku_mapping.count() == sku_mapping.select("sku_id").distinct().count(), \
    "GOLD_DQ_FAILED: conflicting SKU attributes"

dim_product = sku_mapping.select(
    product_key().alias("product_key"),
    F.col("product_name"),
    F.col("product_category"),
).distinct()
assert dim_product.count() == dim_product.select("product_key").distinct().count(), \
    "GOLD_DQ_FAILED: conflicting product attributes"

dim_sku = sku_mapping.select(
    "sku_id", "seller_sku", product_key().alias("product_key"), "variation"
)
assert dim_sku.select("sku_id").distinct().count() == dim_sku.count()

dated = base.withColumn(
    "date_key", F.date_format("_order_date", "yyyyMMdd").cast("int")
)
dim_date = dated.select(
    "date_key",
    F.col("_order_date").alias("calendar_date")
).distinct().select(
    "date_key",
    "calendar_date",
    F.year("calendar_date").alias("calendar_year"),
    F.quarter("calendar_date").alias("calendar_quarter"),
    F.month("calendar_date").alias("calendar_month"),
    F.dayofmonth("calendar_date").alias("calendar_day"),
)

# Fact grain: ONE synthetic order_id + sku_id per row.
# Raw Order Amount is treated as source amount across ALL order statuses.
fact_lines = dated.select(
    F.sha2(F.concat_ws("||", "order_id", "sku_id"), 256).alias("order_line_key"),
    "order_id",
    "sku_id",
    product_key().alias("product_key"),
    "date_key",
    "order_status",
    F.col("quantity").cast("bigint").alias("quantity"),
    F.col("order_amount").cast("decimal(18,2)").alias("source_order_amount"),
    F.when(F.col("order_status") == "Completed",
           F.col("order_amount").cast("decimal(18,2)"))
     .otherwise(F.lit(0).cast("decimal(18,2)"))
     .alias("completed_order_amount"),
)
assert fact_lines.count() == EXPECTED_LINES
assert fact_lines.select("order_line_key").distinct().count() == EXPECTED_LINES
assert fact_lines.filter(F.col("date_key").isNull()).limit(1).count() == 0

# Validate order-level grain before aggregating: one status/date per order.
order_checks = fact_lines.groupBy("order_id").agg(
    F.countDistinct("order_status").alias("status_versions"),
    F.countDistinct("date_key").alias("date_versions"),
)
assert order_checks.filter(
    (F.col("status_versions") != 1) | (F.col("date_versions") != 1)
).limit(1).count() == 0, "GOLD_DQ_FAILED: conflicting order-level attributes"

fact_orders = fact_lines.groupBy("order_id").agg(
    F.first("date_key").alias("date_key"),
    F.first("order_status").alias("order_status"),
    F.count("*").alias("line_count"),
    F.sum("quantity").alias("total_units"),
    F.sum("source_order_amount").alias("source_order_amount"),
    F.sum("completed_order_amount").alias("completed_order_amount"),
)

# Reconciliation must pass BEFORE writing any Gold model table.
silver_totals = silver.agg(
    F.sum("quantity").cast("bigint").alias("units"),
    F.sum("order_amount").cast("decimal(20,2)").alias("amount")
).first()
line_totals = fact_lines.agg(
    F.sum("quantity").alias("units"),
    F.sum("source_order_amount").cast("decimal(20,2)").alias("amount")
).first()
order_totals = fact_orders.agg(
    F.sum("total_units").alias("units"),
    F.sum("source_order_amount").cast("decimal(20,2)").alias("amount")
).first()
assert silver_totals["units"] == line_totals["units"] == order_totals["units"], \
    "GOLD_DQ_FAILED: unit count reconciliation"
assert silver_totals["amount"] == line_totals["amount"] == order_totals["amount"], \
    "GOLD_DQ_FAILED: source amount reconciliation"
assert line_totals["units"] == 13122
assert len(OUTPUTS) == 5

for dim, left_on, right_on in [
    (dim_sku, "sku_id", "sku_id"),
    (dim_product, "product_key", "product_key"),
    (dim_date, "date_key", "date_key"),
]:
    missing = fact_lines.select(left_on).join(
        dim.select(right_on), on=[left_on], how="left_anti"
    ).limit(1).count()
    assert missing == 0, "GOLD_DQ_FAILED: orphan foreign-key reference"

# Sandbox-only writes. Phase 2 will replace overwrite with versioned MERGE
# and atomic publish; these five writes are NOT a multi-table transaction.
for name, frame in [
    ("dim_product", dim_product),
    ("dim_sku", dim_sku),
    ("dim_date", dim_date),
    ("fact_order_lines", fact_lines),
    ("fact_orders", fact_orders),
]:
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(OUTPUTS[name])
    print("WRITTEN_TABLE=" + OUTPUTS[name])

for name in OUTPUTS:
    print("GOLD_TABLE_COUNT=" + name + ":" + str(spark.table(OUTPUTS[name]).count()))
print("SOURCE_LINES=" + str(EXPECTED_LINES))
print("RECONCILED_UNITS=" + str(line_totals["units"]))
print("RECONCILED_SOURCE_AMOUNT=" + str(line_totals["amount"]))
print("MODEL_GRAIN=order_line:order_id+sku_id;order:order_id")
print("RESULT=PASS")
