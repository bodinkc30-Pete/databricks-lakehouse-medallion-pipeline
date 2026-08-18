# Databricks notebook source
print("Spark version:", spark.version)

data = [
    (1, "Bangkok", 1200.50),
    (2, "Chonburi", 850.00),
    (3, "Rayong", 640.25),
]

columns = ["order_id", "province", "revenue"]

df = spark.createDataFrame(data, columns)

df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, when

df_transformed = (
    df
    .filter(col("revenue") >= 700)
    .withColumn(
        "revenue_level",
        when(col("revenue") >= 1000, "HIGH")
        .otherwise("MEDIUM")
    )
    .select(
        "order_id",
        "province",
        "revenue",
        "revenue_level"
    )
)

df_transformed.show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

df_summary = (
    df
    .groupBy("province")
    .agg(
        count("*").alias("order_count"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("avg_revenue")
    )
)

df_summary.show()


# COMMAND ----------

df.createOrReplaceTempView("orders")

sql_result = spark.sql("""
SELECT
    province,
    COUNT(*) AS order_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue
FROM orders
GROUP BY province
ORDER BY total_revenue DESC
""")

sql_result.show()

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.orders_demo")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.bronze").show(truncate=False)

# COMMAND ----------

delta_orders = spark.table("workspace.bronze.orders_demo")

delta_orders.show()
delta_orders.printSchema()

# COMMAND ----------

orders_path = "/Volumes/workspace/bronze/source_files/orders/ทั้งหมด คำสั่งซื้อ-2026-08-13-12_26.csv"

df_orders_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
)

print("Row count:", df_orders_raw.count())
print("Column count:", len(df_orders_raw.columns))

df_orders_raw.printSchema()
df_orders_raw.show(5, truncate=False)

# COMMAND ----------

for column_name, data_type in df_orders_raw.dtypes:
    print(f"{column_name}: {data_type}")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

orders_schema = StructType([
    StructField("Order ID", StringType(), True),
    StructField("Order Status", StringType(), True),
    StructField("Order Substatus", StringType(), True),
    StructField("Cancelation/Return Type", StringType(), True),
    StructField("Normal or Pre-order", StringType(), True),
    StructField("SKU ID", StringType(), True),
    StructField("Seller SKU", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Variation", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Sku Quantity of return", IntegerType(), True),

    StructField("SKU Unit Original Price", DoubleType(), True),
    StructField("SKU Subtotal Before Discount", DoubleType(), True),
    StructField("SKU Platform Discount", DoubleType(), True),
    StructField("SKU Seller Discount", DoubleType(), True),
    StructField("SKU Subtotal After Discount", DoubleType(), True),
    StructField("Shipping Fee After Discount", DoubleType(), True),
    StructField("Original Shipping Fee", DoubleType(), True),
    StructField("Shipping Fee Seller Discount", DoubleType(), True),
    StructField("Shipping Fee Platform Discount", DoubleType(), True),
    StructField("Payment platform discount", DoubleType(), True),
    StructField("Taxes", DoubleType(), True),
    StructField("Order Amount", DoubleType(), True),
    StructField("Order Refund Amount", DoubleType(), True),

    StructField("Created Time", StringType(), True),
    StructField("Paid Time", StringType(), True),
    StructField("RTS Time", StringType(), True),
    StructField("Shipped Time", StringType(), True),
    StructField("Delivered Time", StringType(), True),
    StructField("Cancelled Time", StringType(), True),

    StructField("Cancel By", StringType(), True),
    StructField("Cancel Reason", StringType(), True),
    StructField("Fulfillment Type", StringType(), True),
    StructField("Warehouse Name", StringType(), True),
    StructField("Tracking ID", StringType(), True),
    StructField("Delivery Option", StringType(), True),
    StructField("Shipping Provider Name", StringType(), True),
    StructField("Buyer Message", StringType(), True),
    StructField("Buyer Username", StringType(), True),
    StructField("Recipient", StringType(), True),
    StructField("Phone #", StringType(), True),
    StructField("Zipcode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Province", StringType(), True),
    StructField("District", StringType(), True),
    StructField("Districts", StringType(), True),
    StructField("Detail Address", StringType(), True),
    StructField("Additional address information", StringType(), True),
    StructField("Payment Method", StringType(), True),
    StructField("Weight(kg)", DoubleType(), True),
    StructField("Product Category", StringType(), True),
    StructField("Package ID", StringType(), True),
    StructField("Seller Note", StringType(), True),
    StructField("Checked Status", StringType(), True),
    StructField("Checked Marked by", StringType(), True),
    StructField("Order Channel", StringType(), True),
    StructField("Creator Handle", StringType(), True),
    StructField("Request Tax Invoice", StringType(), True),
    StructField("Tax Info - Buyer Tax ID", StringType(), True),
    StructField("Tax Info - Type", StringType(), True),
    StructField("Tax Info - Full Name of Buyer", StringType(), True),
    StructField("Tax Info - Email", StringType(), True),
    StructField("Tax Info - Phone Number", StringType(), True),
    StructField("Tax Info - Registered Address", StringType(), True),
    StructField("Tax Info - Address Type", StringType(), True),
])

df_orders_typed = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(orders_path)
)

print("Row count:", df_orders_typed.count())
print("Column count:", len(df_orders_typed.columns))

df_orders_typed.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

batch_id = "2026-08-13_orders_batch_001"

df_orders_bronze = (
    df_orders_typed
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("batch_id", lit(batch_id))
)

df_orders_bronze.select(
    "Order ID",
    "SKU ID",
    "Order Amount",
    "ingested_at",
    "source_file",
    "batch_id"
).show(5, truncate=False)

# COMMAND ----------

(
    df_orders_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.orders_raw")
)

print("Created table: workspace.bronze.orders_raw")
print("Rows:", spark.table("workspace.bronze.orders_raw").count())

# COMMAND ----------

import re

def to_snake_case(column_name):
    column_name = column_name.strip()
    column_name = re.sub(r"[^A-Za-z0-9]+", "_", column_name)
    column_name = re.sub(r"_+", "_", column_name)
    return column_name.strip("_").lower()

df_orders_bronze_clean_names = df_orders_bronze

for old_name in df_orders_bronze.columns:
    new_name = to_snake_case(old_name)
    df_orders_bronze_clean_names = (
        df_orders_bronze_clean_names
        .withColumnRenamed(old_name, new_name)
    )

for column_name in df_orders_bronze_clean_names.columns:
    print(column_name)

# COMMAND ----------

(
    df_orders_bronze_clean_names.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.orders_raw")
)

print("Created table: workspace.bronze.orders_raw")
print("Rows:", spark.table("workspace.bronze.orders_raw").count())

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    trim
)

df_bronze = spark.table("workspace.bronze.orders_raw")

dq_summary = df_bronze.select(
    count("*").alias("total_rows"),

    spark_sum(
        when(col("order_id").isNull(), 1).otherwise(0)
    ).alias("null_order_id"),

    spark_sum(
        when(col("sku_id").isNull(), 1).otherwise(0)
    ).alias("null_sku_id"),

    spark_sum(
        when(col("order_amount").isNull(), 1).otherwise(0)
    ).alias("null_order_amount"),

    spark_sum(
        when(col("order_amount") < 0, 1).otherwise(0)
    ).alias("negative_order_amount"),

    spark_sum(
        when(col("order_id") != trim(col("order_id")), 1).otherwise(0)
    ).alias("dirty_order_id"),

    spark_sum(
        when(col("sku_id") != trim(col("sku_id")), 1).otherwise(0)
    ).alias("dirty_sku_id")
)

dq_summary.show(truncate=False)

duplicate_order_lines = (
    df_bronze
    .groupBy("order_id", "sku_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate order_id + sku_id groups:", duplicate_order_lines)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, sum as spark_sum, when

control_char_check = df_bronze.select(
    spark_sum(
        when(col("order_id").rlike(r"[\t\r\n]"), 1).otherwise(0)
    ).alias("order_id_control_chars"),

    spark_sum(
        when(col("sku_id").rlike(r"[\t\r\n]"), 1).otherwise(0)
    ).alias("sku_id_control_chars"),

    spark_sum(
        when(col("order_id").rlike(r"^\s+|\s+$"), 1).otherwise(0)
    ).alias("order_id_outer_whitespace"),

    spark_sum(
        when(col("sku_id").rlike(r"^\s+|\s+$"), 1).otherwise(0)
    ).alias("sku_id_outer_whitespace")
)

control_char_check.show(truncate=False)

# COMMAND ----------

df_bronze = spark.table("workspace.bronze.orders_raw")

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when

control_char_check = df_bronze.select(
    spark_sum(
        when(col("order_id").rlike(r"[\t\r\n]"), 1).otherwise(0)
    ).alias("order_id_control_chars"),

    spark_sum(
        when(col("sku_id").rlike(r"[\t\r\n]"), 1).otherwise(0)
    ).alias("sku_id_control_chars"),

    spark_sum(
        when(col("order_id").rlike(r"^\s+|\s+$"), 1).otherwise(0)
    ).alias("order_id_outer_whitespace"),

    spark_sum(
        when(col("sku_id").rlike(r"^\s+|\s+$"), 1).otherwise(0)
    ).alias("sku_id_outer_whitespace")
)

control_char_check.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, trim

df_orders_silver = (
    df_bronze
    .withColumn(
        "order_id",
        trim(regexp_replace(col("order_id"), r"[\t\r\n]+", ""))
    )
    .withColumn(
        "sku_id",
        trim(regexp_replace(col("sku_id"), r"[\t\r\n]+", ""))
    )
)


# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when

silver_control_char_check = df_orders_silver.select(
    spark_sum(
        when(col("order_id").rlike(r"[\t\r\n]"), 1).otherwise(0)
    ).alias("order_id_control_chars"),

    spark_sum(
        when(col("sku_id").rlike(r"[\t\r\n]"), 1).otherwise(0)
    ).alias("sku_id_control_chars"),

    spark_sum(
        when(col("order_id").rlike(r"^\s+|\s+$"), 1).otherwise(0)
    ).alias("order_id_outer_whitespace"),

    spark_sum(
        when(col("sku_id").rlike(r"^\s+|\s+$"), 1).otherwise(0)
    ).alias("sku_id_outer_whitespace")
)

silver_control_char_check.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, trim, to_timestamp

timestamp_columns = [
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time",
]

df_orders_silver_ts = df_orders_silver

for column_name in timestamp_columns:
    df_orders_silver_ts = (
        df_orders_silver_ts
        .withColumn(
            column_name,
            trim(
                regexp_replace(
                    col(column_name),
                    r"[\t\r\n]+",
                    ""
                )
            )
        )
        .withColumn(
            column_name,
            to_timestamp(
                col(column_name),
                "dd/MM/yyyy HH:mm:ss"
            )
        )
    )

df_orders_silver_ts.select(
    "order_id",
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time"
).show(10, truncate=False)

df_orders_silver_ts.select(
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time"
).printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, trim

df_bronze = spark.table("workspace.bronze.orders_raw")

df_orders_silver = (
    df_bronze
    .withColumn(
        "order_id",
        trim(regexp_replace(col("order_id"), r"[\t\r\n]+", ""))
    )
    .withColumn(
        "sku_id",
        trim(regexp_replace(col("sku_id"), r"[\t\r\n]+", ""))
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

timestamp_columns = [
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time",
]

df_orders_silver_ts = df_orders_silver

for column_name in timestamp_columns:
    cleaned_value = F.trim(
        F.regexp_replace(
            F.col(column_name),
            r"[\t\r\n]+",
            ""
        )
    )

    df_orders_silver_ts = (
        df_orders_silver_ts
        .withColumn(
            column_name,
            F.try_to_timestamp(
                F.nullif(cleaned_value, F.lit("")),
                F.lit("dd/MM/yyyy HH:mm:ss")
            )
        )
    )

df_orders_silver_ts.select(
    "order_id",
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time"
).show(10, truncate=False)

df_orders_silver_ts.select(
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time"
).printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

timestamp_columns = [
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time",
]

for column_name in timestamp_columns:

    cleaned_source = F.trim(
        F.regexp_replace(
            F.col(column_name),
            r"[\t\r\n]+",
            ""
        )
    )

    parsed_value = F.try_to_timestamp(
        F.nullif(cleaned_source, F.lit("")),
        F.lit("dd/MM/yyyy HH:mm:ss")
    )

    result = (
        df_orders_silver
        .select(
            F.count("*").alias("total_rows"),

            F.sum(
                F.when(
                    cleaned_source.isNull() | (cleaned_source == ""),
                    1
                ).otherwise(0)
            ).alias("source_blank"),

            F.sum(
                F.when(
                    (cleaned_source.isNotNull()) &
                    (cleaned_source != "") &
                    parsed_value.isNull(),
                    1
                ).otherwise(0)
            ).alias("parse_failure")
        )
        .collect()[0]
    )

    print(
        f"{column_name}: "
        f"total={result['total_rows']}, "
        f"blank={result['source_blank']}, "
        f"parse_failure={result['parse_failure']}"
    )

# COMMAND ----------

from pyspark.sql import functions as F

status_summary = (
    df_orders_silver_ts
    .groupBy("order_status")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("paid_time").isNull(), 1).otherwise(0))
            .alias("null_paid_time"),
        F.sum(F.when(F.col("shipped_time").isNull(), 1).otherwise(0))
            .alias("null_shipped_time"),
        F.sum(F.when(F.col("delivered_time").isNull(), 1).otherwise(0))
            .alias("null_delivered_time"),
        F.sum(F.when(F.col("cancelled_time").isNotNull(), 1).otherwise(0))
            .alias("has_cancelled_time")
    )
    .orderBy(F.desc("row_count"))
)

status_summary.show(50, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = spark.table("workspace.bronze.orders_raw")

df_orders_silver = (
    df_bronze
    .withColumn(
        "order_id",
        F.trim(F.regexp_replace(F.col("order_id"), r"[\t\r\n]+", ""))
    )
    .withColumn(
        "sku_id",
        F.trim(F.regexp_replace(F.col("sku_id"), r"[\t\r\n]+", ""))
    )
)

timestamp_columns = [
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time",
]

df_orders_silver_ts = df_orders_silver

for column_name in timestamp_columns:
    cleaned_value = F.trim(
        F.regexp_replace(
            F.col(column_name),
            r"[\t\r\n]+",
            ""
        )
    )

    df_orders_silver_ts = (
        df_orders_silver_ts
        .withColumn(
            column_name,
            F.try_to_timestamp(
                F.nullif(cleaned_value, F.lit("")),
                F.lit("dd/MM/yyyy HH:mm:ss")
            )
        )
    )

print("Rebuilt df_orders_silver_ts")
print("Rows:", df_orders_silver_ts.count())

# COMMAND ----------

from pyspark.sql import functions as F

status_summary = (
    df_orders_silver_ts
    .groupBy("order_status")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(
            F.when(F.col("paid_time").isNull(), 1).otherwise(0)
        ).alias("null_paid_time"),
        F.sum(
            F.when(F.col("shipped_time").isNull(), 1).otherwise(0)
        ).alias("null_shipped_time"),
        F.sum(
            F.when(F.col("delivered_time").isNull(), 1).otherwise(0)
        ).alias("null_delivered_time"),
        F.sum(
            F.when(F.col("cancelled_time").isNotNull(), 1).otherwise(0)
        ).alias("has_cancelled_time")
    )
    .orderBy(F.desc("row_count"))
)

status_summary.show(50, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

business_dq = df_orders_silver_ts.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            (F.col("order_status") == "เสร็จสมบูรณ์") &
            (F.col("delivered_time").isNull()),
            1
        ).otherwise(0)
    ).alias("completed_without_delivered_time"),

    F.sum(
        F.when(
            (F.col("order_status") == "ยกเลิกแล้ว") &
            (F.col("cancelled_time").isNull()),
            1
        ).otherwise(0)
    ).alias("cancelled_without_cancelled_time"),

    F.sum(
        F.when(
            (F.col("order_status") == "ค้างชำระ") &
            (F.col("paid_time").isNotNull()),
            1
        ).otherwise(0)
    ).alias("unpaid_status_with_paid_time"),

    F.sum(
        F.when(
            F.col("delivered_time").isNotNull() &
            F.col("shipped_time").isNull(),
            1
        ).otherwise(0)
    ).alias("delivered_without_shipped_time")
)

business_dq.show(truncate=False)

# COMMAND ----------

(
    df_orders_silver_ts.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.orders_clean")
)

print("Created table: workspace.silver.orders_clean")
print("Rows:", spark.table("workspace.silver.orders_clean").count())

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table("workspace.silver.orders_clean")

df_gold_daily_sales = (
    df_silver
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .withColumn("order_date", F.to_date("created_time"))
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("order_amount").alias("gross_revenue"),
        F.avg("order_amount").alias("avg_order_value")
    )
    .orderBy("order_date")
)

df_gold_daily_sales.show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

order_grain_check = (
    df_silver
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("sku_id").alias("sku_count"),
        F.countDistinct("order_amount").alias("distinct_order_amount_count"),
        F.min("order_amount").alias("min_order_amount"),
        F.max("order_amount").alias("max_order_amount")
    )
)

print(
    "Total distinct orders:",
    order_grain_check.count()
)

print(
    "Orders with multiple rows:",
    order_grain_check
        .filter(F.col("row_count") > 1)
        .count()
)

print(
    "Orders with multiple SKUs:",
    order_grain_check
        .filter(F.col("sku_count") > 1)
        .count()
)

print(
    "Orders with inconsistent order_amount:",
    order_grain_check
        .filter(F.col("distinct_order_amount_count") > 1)
        .count()
)

order_grain_check \
    .filter(F.col("row_count") > 1) \
    .orderBy(F.desc("row_count")) \
    .show(20, truncate=False)

# COMMAND ----------

df_silver = spark.table("workspace.silver.orders_clean")

print("Rows:", df_silver.count())

# COMMAND ----------

from pyspark.sql import functions as F

order_grain_check = (
    df_silver
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("sku_id").alias("sku_count"),
        F.countDistinct("order_amount").alias("distinct_order_amount_count"),
        F.min("order_amount").alias("min_order_amount"),
        F.max("order_amount").alias("max_order_amount")
    )
)

print("Total distinct orders:", order_grain_check.count())

print(
    "Orders with multiple rows:",
    order_grain_check.filter(F.col("row_count") > 1).count()
)

print(
    "Orders with multiple SKUs:",
    order_grain_check.filter(F.col("sku_count") > 1).count()
)

print(
    "Orders with inconsistent order_amount:",
    order_grain_check.filter(
        F.col("distinct_order_amount_count") > 1
    ).count()
)

order_grain_check \
    .filter(F.col("row_count") > 1) \
    .orderBy(F.desc("row_count")) \
    .show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df_fact_orders = (
    df_silver
    .groupBy("order_id")
    .agg(
        F.min("created_time").alias("created_time"),
        F.first("order_status", ignorenulls=True).alias("order_status"),

        F.countDistinct("sku_id").alias("sku_count"),
        F.sum("quantity").alias("total_quantity"),

        F.max("order_amount")
            .cast(DecimalType(18, 2))
            .alias("order_amount"),

        F.max("order_refund_amount")
            .cast(DecimalType(18, 2))
            .alias("order_refund_amount"),

        F.first("province", ignorenulls=True).alias("province"),
        F.first("payment_method", ignorenulls=True).alias("payment_method"),
        F.first("order_channel", ignorenulls=True).alias("order_channel")
    )
)

print("Fact order rows:", df_fact_orders.count())

print(
    "Duplicate order_id:",
    df_fact_orders
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

df_fact_orders.orderBy(
    F.desc("sku_count")
).show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df_gold_daily_sales = (
    df_fact_orders
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .withColumn("order_date", F.to_date("created_time"))
    .groupBy("order_date")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total_quantity").alias("units_sold"),
        F.sum("order_amount")
            .cast(DecimalType(18, 2))
            .alias("gross_revenue"),
        F.avg("order_amount")
            .cast(DecimalType(18, 2))
            .alias("avg_order_value")
    )
    .orderBy("order_date")
)

df_gold_daily_sales.show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_totals = (
    df_fact_orders
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .agg(
        F.count("*").alias("fact_order_count"),
        F.sum("total_quantity").alias("fact_units_sold"),
        F.sum("order_amount")
            .cast(DecimalType(18, 2))
            .alias("fact_revenue")
    )
)

gold_totals = (
    df_gold_daily_sales
    .agg(
        F.sum("order_count").alias("gold_order_count"),
        F.sum("units_sold").alias("gold_units_sold"),
        F.sum("gross_revenue")
            .cast(DecimalType(18, 2))
            .alias("gold_revenue")
    )
)

fact = fact_totals.collect()[0]
gold = gold_totals.collect()[0]

print("FACT")
print("Orders :", fact["fact_order_count"])
print("Units  :", fact["fact_units_sold"])
print("Revenue:", fact["fact_revenue"])

print("\nGOLD")
print("Orders :", gold["gold_order_count"])
print("Units  :", gold["gold_units_sold"])
print("Revenue:", gold["gold_revenue"])

print("\nMATCH")
print("Orders :", fact["fact_order_count"] == gold["gold_order_count"])
print("Units  :", fact["fact_units_sold"] == gold["gold_units_sold"])
print("Revenue:", fact["fact_revenue"] == gold["gold_revenue"])

# COMMAND ----------

(
    df_gold_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.daily_sales")
)

print("Created table: workspace.gold.daily_sales")
print("Rows:", spark.table("workspace.gold.daily_sales").count())

# COMMAND ----------

product_sku_path = "/Volumes/workspace/bronze/source_files/products/product_sku_list.xlsx"

print(product_sku_path)

# COMMAND ----------

import pandas as pd

xls = pd.ExcelFile(product_sku_path)

print("Sheets:", xls.sheet_names)

for sheet_name in xls.sheet_names:
    df_preview = pd.read_excel(product_sku_path, sheet_name=sheet_name)
    print(f"\nSheet: {sheet_name}")
    print("Rows:", len(df_preview))
    print("Columns:", list(df_preview.columns))
    print(df_preview.head())

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

df_sku = pd.read_excel(
    product_sku_path,
    sheet_name="Sheet1",
    header=1
)

print("Rows:", len(df_sku))
print("Columns:", list(df_sku.columns))

print("\nPreview:")
print(df_sku.head(10))

# COMMAND ----------

import pandas as pd

df_sku = pd.read_excel(
    product_sku_path,
    sheet_name="Sheet1",
    header=2
)

print("Rows:", len(df_sku))
print("Columns:", list(df_sku.columns))

print("\nPreview:")
print(df_sku.head(10))

# COMMAND ----------

print("Total rows:", len(df_sku))
print("Distinct SKU ID:", df_sku["SKU ID"].nunique())
print("Distinct Product ID:", df_sku["Product ID"].nunique())

print(
    "Duplicate SKU ID rows:",
    df_sku["SKU ID"].duplicated().sum()
)

product_sku_count = (
    df_sku
    .groupby("Product ID")["SKU ID"]
    .nunique()
    .sort_values(ascending=False)
)

print("\nProducts with multiple SKUs:")
print(product_sku_count[product_sku_count > 1])

# COMMAND ----------

from pyspark.sql import functions as F

df_dim_sku = (
    spark.createDataFrame(
        df_sku[[
            "SKU ID",
            "Product ID",
            "สินค้า",
            "สถานะ"
        ]]
    )
    .withColumnRenamed("SKU ID", "sku_id")
    .withColumnRenamed("Product ID", "product_id")
    .withColumnRenamed("สินค้า", "product_name")
    .withColumnRenamed("สถานะ", "status")
    .withColumn("sku_id", F.col("sku_id").cast("string"))
    .withColumn("product_id", F.col("product_id").cast("string"))
)

print("Rows:", df_dim_sku.count())
print("Distinct SKU:", df_dim_sku.select("sku_id").distinct().count())

df_dim_sku.show(20, truncate=False)
df_dim_sku.printSchema()

# COMMAND ----------

import pandas as pd

product_sku_path = "/Volumes/workspace/bronze/source_files/products/product_sku_list.xlsx"

df_sku = pd.read_excel(
    product_sku_path,
    sheet_name="Sheet1",
    header=2
)

print("Rows:", len(df_sku))
print("Columns:", list(df_sku.columns))

# COMMAND ----------

from pyspark.sql import functions as F

df_dim_sku = (
    spark.createDataFrame(
        df_sku[[
            "SKU ID",
            "Product ID",
            "สินค้า",
            "สถานะ"
        ]]
    )
    .withColumnRenamed("SKU ID", "sku_id")
    .withColumnRenamed("Product ID", "product_id")
    .withColumnRenamed("สินค้า", "product_name")
    .withColumnRenamed("สถานะ", "status")
    .withColumn("sku_id", F.col("sku_id").cast("string"))
    .withColumn("product_id", F.col("product_id").cast("string"))
)

print("Rows:", df_dim_sku.count())
print("Distinct SKU:", df_dim_sku.select("sku_id").distinct().count())

df_dim_sku.show(20, truncate=False)
df_dim_sku.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_orders = spark.table("workspace.silver.orders_clean")

order_skus = (
    df_orders
    .select("sku_id")
    .distinct()
)

coverage = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="left"
    )
    .withColumn(
        "matched",
        F.when(
            F.col("sku_id").isNotNull(),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

# COMMAND ----------

matched_skus = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="inner"
    )
)

unmatched_skus = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="left_anti"
    )
)

total_order_skus = order_skus.count()
matched_count = matched_skus.count()
unmatched_count = unmatched_skus.count()

coverage_pct = (
    matched_count / total_order_skus * 100
    if total_order_skus > 0
    else 0
)

print("Distinct SKU in orders:", total_order_skus)
print("Matched SKU:", matched_count)
print("Unmatched SKU:", unmatched_count)
print(f"Coverage: {coverage_pct:.2f}%")

unmatched_skus.show(50, truncate=False)

# COMMAND ----------

df_orders = spark.table("workspace.silver.orders_clean")

order_skus = (
    df_orders
    .select("sku_id")
    .distinct()
)

print("Distinct SKU in orders:", order_skus.count())

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

product_sku_path = "/Volumes/workspace/bronze/source_files/products/product_sku_list.xlsx"

df_sku = pd.read_excel(
    product_sku_path,
    sheet_name="Sheet1",
    header=2
)

df_dim_sku = (
    spark.createDataFrame(
        df_sku[[
            "SKU ID",
            "Product ID",
            "สินค้า",
            "สถานะ"
        ]]
    )
    .withColumnRenamed("SKU ID", "sku_id")
    .withColumnRenamed("Product ID", "product_id")
    .withColumnRenamed("สินค้า", "product_name")
    .withColumnRenamed("สถานะ", "status")
    .withColumn("sku_id", F.col("sku_id").cast("string"))
    .withColumn("product_id", F.col("product_id").cast("string"))
)

print("dim_sku rows:", df_dim_sku.count())
print("Distinct dim SKU:", df_dim_sku.select("sku_id").distinct().count())

# COMMAND ----------

matched_skus = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="inner"
    )
)

unmatched_skus = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="left_anti"
    )
)

total_order_skus = order_skus.count()
matched_count = matched_skus.count()
unmatched_count = unmatched_skus.count()

coverage_pct = (
    matched_count / total_order_skus * 100
    if total_order_skus > 0
    else 0
)

print("Distinct SKU in orders:", total_order_skus)
print("Matched SKU:", matched_count)
print("Unmatched SKU:", unmatched_count)
print(f"Coverage: {coverage_pct:.2f}%")

unmatched_skus.show(50, truncate=False)

# COMMAND ----------

df_orders = spark.table("workspace.silver.orders_clean")

order_skus = (
    df_orders
    .select("sku_id")
    .distinct()
)

print("Distinct SKU in orders:", order_skus.count())

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

# ------------------------------------------------------------
# 1) Rebuild dim_sku from Excel source
# ------------------------------------------------------------
product_sku_path = "/Volumes/workspace/bronze/source_files/products/product_sku_list.xlsx"

df_sku = pd.read_excel(
    product_sku_path,
    sheet_name="Sheet1",
    header=2
)

df_dim_sku = (
    spark.createDataFrame(
        df_sku[[
            "SKU ID",
            "Product ID",
            "สินค้า",
            "สถานะ"
        ]]
    )
    .withColumnRenamed("SKU ID", "sku_id")
    .withColumnRenamed("Product ID", "product_id")
    .withColumnRenamed("สินค้า", "product_name")
    .withColumnRenamed("สถานะ", "status")
    .withColumn("sku_id", F.col("sku_id").cast("string"))
    .withColumn("product_id", F.col("product_id").cast("string"))
)

# ------------------------------------------------------------
# 2) Rebuild distinct SKU list from Silver orders
# ------------------------------------------------------------
df_orders = spark.table("workspace.silver.orders_clean")

order_skus = (
    df_orders
    .select("sku_id")
    .distinct()
)

# ------------------------------------------------------------
# 3) Match / Unmatched SKU
# ------------------------------------------------------------
matched_skus = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="inner"
    )
)

unmatched_skus = (
    order_skus
    .join(
        df_dim_sku.select("sku_id"),
        on="sku_id",
        how="left_anti"
    )
)

# ------------------------------------------------------------
# 4) Coverage metrics
# ------------------------------------------------------------
total_order_skus = order_skus.count()
matched_count = matched_skus.count()
unmatched_count = unmatched_skus.count()

coverage_pct = (
    matched_count / total_order_skus * 100
    if total_order_skus > 0
    else 0
)

print("dim_sku rows:", df_dim_sku.count())
print("Distinct SKU in orders:", total_order_skus)
print("Matched SKU:", matched_count)
print("Unmatched SKU:", unmatched_count)
print(f"Coverage: {coverage_pct:.2f}%")

print("\nUnmatched SKU:")
unmatched_skus.show(50, truncate=False)

# COMMAND ----------

missing_sku_details = (
    df_orders
    .filter(
        F.col("sku_id").isin(
            "1732728000121373879",
            "1732728000121308343",
            "1732059636552402103"
        )
    )
    .select(
        "sku_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category"
    )
    .distinct()
    .orderBy("sku_id")
)

missing_sku_details.show(50, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# 1) สร้าง historical SKU records จาก orders
historical_skus = (
    df_orders
    .filter(
        F.col("sku_id").isin(
            "1732059636552402103",
            "1732728000121308343",
            "1732728000121373879"
        )
    )
    .groupBy("sku_id")
    .agg(
        F.first("seller_sku", ignorenulls=True).alias("seller_sku"),
        F.first("product_name", ignorenulls=True).alias("product_name"),
        F.first("variation", ignorenulls=True).alias("variation"),
        F.first("product_category", ignorenulls=True).alias("product_category")
    )
    .withColumn("product_id", F.lit(None).cast("string"))
    .withColumn("status", F.lit("Historical"))
    .withColumn("is_historical", F.lit(True))
)

# 2) ปรับ current dim_sku ให้มีโครงสร้างเดียวกัน
current_skus = (
    df_dim_sku
    .withColumn("seller_sku", F.lit(None).cast("string"))
    .withColumn("variation", F.lit(None).cast("string"))
    .withColumn("product_category", F.lit(None).cast("string"))
    .withColumn("is_historical", F.lit(False))
)

# 3) เรียง column ให้เหมือนกันก่อน UNION
dimension_columns = [
    "sku_id",
    "product_id",
    "seller_sku",
    "product_name",
    "variation",
    "product_category",
    "status",
    "is_historical"
]

df_dim_sku_complete = (
    current_skus.select(dimension_columns)
    .unionByName(
        historical_skus.select(dimension_columns)
    )
)

print("Total dim_sku rows:", df_dim_sku_complete.count())
print(
    "Distinct SKU:",
    df_dim_sku_complete.select("sku_id").distinct().count()
)

df_dim_sku_complete.orderBy(
    F.desc("is_historical"),
    "sku_id"
).show(30, truncate=False)

# COMMAND ----------

order_skus = (
    spark.table("workspace.silver.orders_clean")
    .select("sku_id")
    .distinct()
)

matched_skus = (
    order_skus
    .join(
        df_dim_sku_complete.select("sku_id"),
        on="sku_id",
        how="inner"
    )
)

unmatched_skus = (
    order_skus
    .join(
        df_dim_sku_complete.select("sku_id"),
        on="sku_id",
        how="left_anti"
    )
)

total_order_skus = order_skus.count()
matched_count = matched_skus.count()
unmatched_count = unmatched_skus.count()

coverage_pct = (
    matched_count / total_order_skus * 100
    if total_order_skus > 0
    else 0
)

print("Distinct SKU in orders:", total_order_skus)
print("Matched SKU:", matched_count)
print("Unmatched SKU:", unmatched_count)
print(f"Coverage: {coverage_pct:.2f}%")

# COMMAND ----------

(
    df_dim_sku_complete.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.dim_sku")
)

print("Created table: workspace.gold.dim_sku")
print("Rows:", spark.table("workspace.gold.dim_sku").count())

# COMMAND ----------

import pandas as pd

product_list_path = "/Volumes/workspace/bronze/source_files/products/product_list_20260214.xlsx"

xls_product = pd.ExcelFile(product_list_path)

print("Sheets:", xls_product.sheet_names)

for sheet_name in xls_product.sheet_names:
    df_product_preview = pd.read_excel(
        product_list_path,
        sheet_name=sheet_name,
        header=None
    )

    print(f"\nSheet: {sheet_name}")
    print("Rows:", len(df_product_preview))
    print("Columns:", df_product_preview.shape[1])
    print(df_product_preview.head(10))

# COMMAND ----------

import pandas as pd

df_product = pd.read_excel(
    product_list_path,
    sheet_name="Sheet1",
    header=3
)

print("Rows:", len(df_product))
print("Columns:", len(df_product.columns))

print("\nFirst 30 column names:")
for i, column_name in enumerate(df_product.columns[:30], start=1):
    print(i, column_name)

print("\nPreview:")
print(df_product.head())

# COMMAND ----------

print("Total rows:", len(df_product))
print("Distinct Product ID:", df_product["รหัสสินค้า"].nunique())
print("Duplicate Product ID rows:", df_product["รหัสสินค้า"].duplicated().sum())
print("Null Product ID:", df_product["รหัสสินค้า"].isna().sum())

print("\nProduct status counts:")
print(df_product["สถานะรายการสินค้า"].value_counts(dropna=False))

print("\nProduct ID + Name preview:")
print(
    df_product[
        ["รหัสสินค้า", "ชื่อ", "สถานะรายการสินค้า"]
    ].head(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

df_dim_product = (
    spark.createDataFrame(
        df_product[
            ["รหัสสินค้า", "ชื่อ", "สถานะรายการสินค้า"]
        ]
    )
    .withColumnRenamed("รหัสสินค้า", "product_id")
    .withColumnRenamed("ชื่อ", "product_name")
    .withColumnRenamed("สถานะรายการสินค้า", "product_status")
    .withColumn("product_id", F.col("product_id").cast("string"))
)

print("dim_product rows:", df_dim_product.count())
print(
    "Distinct Product:",
    df_dim_product.select("product_id").distinct().count()
)

df_dim_product.show(20, truncate=False)

# COMMAND ----------

df_dim_sku_gold = spark.table("workspace.gold.dim_sku")

sku_product_ids = (
    df_dim_sku_gold
    .filter(F.col("product_id").isNotNull())
    .select("product_id")
    .distinct()
)

matched_products = (
    sku_product_ids
    .join(
        df_dim_product.select("product_id"),
        on="product_id",
        how="inner"
    )
)

unmatched_products = (
    sku_product_ids
    .join(
        df_dim_product.select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

total_product_ids = sku_product_ids.count()
matched_count = matched_products.count()
unmatched_count = unmatched_products.count()

coverage_pct = (
    matched_count / total_product_ids * 100
    if total_product_ids > 0
    else 0
)

print("Product IDs referenced by SKU:", total_product_ids)
print("Matched Product IDs:", matched_count)
print("Unmatched Product IDs:", unmatched_count)
print(f"Coverage: {coverage_pct:.2f}%")

unmatched_products.show(50, truncate=False)

# COMMAND ----------

(
    df_dim_product.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.dim_product")
)

print("Created table: workspace.gold.dim_product")
print("Rows:", spark.table("workspace.gold.dim_product").count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df_silver = spark.table("workspace.silver.orders_clean")

df_fact_orders = (
    df_silver
    .groupBy("order_id")
    .agg(
        F.min("created_time").alias("created_time"),
        F.first("order_status", ignorenulls=True).alias("order_status"),

        F.countDistinct("sku_id").alias("sku_count"),
        F.sum("quantity").alias("total_quantity"),

        F.max("order_amount")
            .cast(DecimalType(18, 2))
            .alias("order_amount"),

        F.max("order_refund_amount")
            .cast(DecimalType(18, 2))
            .alias("order_refund_amount"),

        F.first("province", ignorenulls=True).alias("province"),
        F.first("payment_method", ignorenulls=True).alias("payment_method"),
        F.first("order_channel", ignorenulls=True).alias("order_channel")
    )
)

print("fact_orders rows:", df_fact_orders.count())

print(
    "Duplicate order_id:",
    df_fact_orders
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

df_fact_orders.show(20, truncate=False)

# COMMAND ----------

(
    df_fact_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.fact_orders")
)

print("Created table: workspace.gold.fact_orders")
print("Rows:", spark.table("workspace.gold.fact_orders").count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_orders = spark.table("workspace.gold.fact_orders")
daily_sales = spark.table("workspace.gold.daily_sales")

fact_totals = (
    fact_orders
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .agg(
        F.count("*").alias("fact_order_count"),
        F.sum("total_quantity").alias("fact_units_sold"),
        F.sum("order_amount")
            .cast(DecimalType(18, 2))
            .alias("fact_revenue")
    )
)

daily_totals = (
    daily_sales
    .agg(
        F.sum("order_count").alias("daily_order_count"),
        F.sum("units_sold").alias("daily_units_sold"),
        F.sum("gross_revenue")
            .cast(DecimalType(18, 2))
            .alias("daily_revenue")
    )
)

fact = fact_totals.collect()[0]
daily = daily_totals.collect()[0]

print("FACT_ORDERS")
print("Orders :", fact["fact_order_count"])
print("Units  :", fact["fact_units_sold"])
print("Revenue:", fact["fact_revenue"])

print("\nDAILY_SALES")
print("Orders :", daily["daily_order_count"])
print("Units  :", daily["daily_units_sold"])
print("Revenue:", daily["daily_revenue"])

print("\nRECONCILIATION")
print("Orders match :", fact["fact_order_count"] == daily["daily_order_count"])
print("Units match  :", fact["fact_units_sold"] == daily["daily_units_sold"])
print("Revenue match:", fact["fact_revenue"] == daily["daily_revenue"])

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df_order_lines = spark.table("workspace.silver.orders_clean")
df_dim_sku_gold = spark.table("workspace.gold.dim_sku")
df_dim_product_gold = spark.table("workspace.gold.dim_product")

df_fact_order_lines = (
    df_order_lines.alias("o")
    .join(
        df_dim_sku_gold.alias("s"),
        F.col("o.sku_id") == F.col("s.sku_id"),
        "left"
    )
    .join(
        df_dim_product_gold.alias("p"),
        F.col("s.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.sku_id"),
        F.col("s.product_id"),
        F.col("o.created_time"),
        F.col("o.order_status"),
        F.col("o.quantity"),

        F.col("o.sku_unit_original_price")
            .cast(DecimalType(18, 2))
            .alias("sku_unit_original_price"),

        F.col("o.sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("sku_subtotal_after_discount"),

        F.col("o.order_amount")
            .cast(DecimalType(18, 2))
            .alias("order_amount"),

        F.col("o.seller_sku"),
        F.col("o.variation"),
        F.col("o.product_category"),
        F.col("o.order_channel"),

        F.col("s.product_name").alias("sku_product_name"),
        F.col("s.status").alias("sku_status"),
        F.col("s.is_historical"),

        F.col("p.product_name").alias("product_name"),
        F.col("p.product_status")
    )
)

print("fact_order_lines rows:", df_fact_order_lines.count())

print(
    "Distinct order + sku:",
    df_fact_order_lines
        .select("order_id", "sku_id")
        .distinct()
        .count()
)

df_fact_order_lines.show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

fact_line_dq = df_fact_order_lines.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(F.col("sku_id").isNull(), 1).otherwise(0)
    ).alias("null_sku_id"),

    F.sum(
        F.when(F.col("sku_product_name").isNull(), 1).otherwise(0)
    ).alias("missing_sku_dimension"),

    F.sum(
        F.when(
            F.col("product_id").isNotNull() &
            F.col("product_name").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_product_dimension"),

    F.sum(
        F.when(F.col("quantity") <= 0, 1).otherwise(0)
    ).alias("invalid_quantity"),

    F.sum(
        F.when(
            F.col("sku_subtotal_after_discount") < 0,
            1
        ).otherwise(0)
    ).alias("negative_sku_amount")
)

fact_line_dq.show(truncate=False)

# COMMAND ----------

(
    df_fact_order_lines.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.fact_order_lines")
)

print("Created table: workspace.gold.fact_order_lines")
print("Rows:", spark.table("workspace.gold.fact_order_lines").count())

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
fact_orders = spark.table("workspace.gold.fact_orders")
dim_sku = spark.table("workspace.gold.dim_sku")
dim_product = spark.table("workspace.gold.dim_product")

print("=== GOLD STAR SCHEMA CHECK ===")

print("fact_orders:", fact_orders.count())
print("fact_order_lines:", fact_order_lines.count())
print("dim_sku:", dim_sku.count())
print("dim_product:", dim_product.count())

print("\n=== REFERENTIAL INTEGRITY ===")

missing_orders = (
    fact_order_lines.alias("l")
    .join(
        fact_orders.select("order_id").alias("o"),
        on="order_id",
        how="left_anti"
    )
    .count()
)

missing_skus = (
    fact_order_lines.alias("l")
    .join(
        dim_sku.select("sku_id").alias("s"),
        on="sku_id",
        how="left_anti"
    )
    .count()
)

missing_products = (
    fact_order_lines
    .filter(F.col("product_id").isNotNull())
    .select("product_id")
    .distinct()
    .join(
        dim_product.select("product_id"),
        on="product_id",
        how="left_anti"
    )
    .count()
)

print("Missing Order FK:", missing_orders)
print("Missing SKU FK:", missing_skus)
print("Missing Product FK:", missing_products)

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")

top_skus = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .groupBy(
        "sku_id",
        "seller_sku",
        "sku_product_name"
    )
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("sku_subtotal_after_discount").alias("sku_revenue")
    )
    .orderBy(
        F.desc("units_sold"),
        F.desc("sku_revenue")
    )
)

top_skus.show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")

sku_seller_mapping_check = (
    fact_order_lines
    .groupBy("sku_id")
    .agg(
        F.countDistinct("seller_sku").alias("seller_sku_count"),
        F.collect_set("seller_sku").alias("seller_sku_values"),
        F.count("*").alias("row_count")
    )
    .filter(F.col("seller_sku_count") > 1)
    .orderBy(F.desc("seller_sku_count"))
)

print(
    "SKU IDs with multiple seller_sku:",
    sku_seller_mapping_check.count()
)

sku_seller_mapping_check.show(50, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

target_sku = "1732698562646017207"

seller_sku_timeline = (
    fact_order_lines
    .filter(F.col("sku_id") == target_sku)
    .groupBy("seller_sku")
    .agg(
        F.min("created_time").alias("first_seen"),
        F.max("created_time").alias("last_seen"),
        F.count("*").alias("row_count"),
        F.sum("quantity").alias("units_sold")
    )
    .orderBy("first_seen")
)

seller_sku_timeline.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_sku = spark.table("workspace.gold.dim_sku")

sku_metrics = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .groupBy("sku_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("sku_subtotal_after_discount").alias("sku_revenue")
    )
)

top_skus_corrected = (
    sku_metrics.alias("f")
    .join(
        dim_sku.alias("s"),
        on="sku_id",
        how="left"
    )
    .select(
        "sku_id",
        F.col("s.seller_sku"),
        F.col("s.product_name").alias("sku_product_name"),
        "units_sold",
        "order_count",
        "sku_revenue",
        F.col("s.is_historical")
    )
    .orderBy(
        F.desc("units_sold"),
        F.desc("sku_revenue")
    )
)

top_skus_corrected.show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dim_sku = spark.table("workspace.gold.dim_sku")
order_lines = spark.table("workspace.gold.fact_order_lines")

latest_seller_window = (
    Window
    .partitionBy("sku_id")
    .orderBy(F.col("created_time").desc())
)

latest_seller_sku = (
    order_lines
    .filter(F.col("seller_sku").isNotNull())
    .select(
        "sku_id",
        "seller_sku",
        "created_time"
    )
    .withColumn(
        "rn",
        F.row_number().over(latest_seller_window)
    )
    .filter(F.col("rn") == 1)
    .select(
        "sku_id",
        F.col("seller_sku").alias("latest_seller_sku")
    )
)

df_dim_sku_enriched = (
    dim_sku.alias("d")
    .join(
        latest_seller_sku.alias("l"),
        on="sku_id",
        how="left"
    )
    .select(
        F.col("sku_id"),
        F.col("d.product_id"),

        F.coalesce(
            F.col("l.latest_seller_sku"),
            F.col("d.seller_sku")
        ).alias("seller_sku"),

        F.col("d.product_name"),
        F.col("d.variation"),
        F.col("d.product_category"),
        F.col("d.status"),
        F.col("d.is_historical")
    )
)

print("Rows:", df_dim_sku_enriched.count())

print(
    "Distinct SKU:",
    df_dim_sku_enriched
    .select("sku_id")
    .distinct()
    .count()
)

print(
    "NULL seller_sku:",
    df_dim_sku_enriched
    .filter(F.col("seller_sku").isNull())
    .count()
)

df_dim_sku_enriched.orderBy("sku_id").show(30, truncate=False)

# COMMAND ----------

(
    df_dim_sku_enriched.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.dim_sku")
)

print("Updated table: workspace.gold.dim_sku")
print("Rows:", spark.table("workspace.gold.dim_sku").count())

print(
    "NULL seller_sku:",
    spark.table("workspace.gold.dim_sku")
    .filter(F.col("seller_sku").isNull())
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_sku = spark.table("workspace.gold.dim_sku")
dim_product = spark.table("workspace.gold.dim_product")

product_metrics = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("sku_subtotal_after_discount").alias("product_revenue"),
        F.countDistinct("sku_id").alias("sku_count")
    )
)

top_products = (
    product_metrics.alias("f")
    .join(
        dim_product.alias("p"),
        on="product_id",
        how="left"
    )
    .select(
        "product_id",
        F.col("p.product_name"),
        F.col("p.product_status"),
        "sku_count",
        "units_sold",
        "order_count",
        "product_revenue"
    )
    .orderBy(
        F.desc("units_sold"),
        F.desc("product_revenue")
    )
)

top_products.show(30, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")

# SKU-level totals
sku_totals = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNotNull())
    .agg(
        F.sum("quantity").alias("sku_units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("sku_revenue")
    )
)

# Product-level totals
product_totals = (
    product_metrics
    .agg(
        F.sum("units_sold").alias("product_units"),
        F.sum("product_revenue")
            .cast(DecimalType(18, 2))
            .alias("product_revenue")
    )
)

sku = sku_totals.collect()[0]
product = product_totals.collect()[0]

print("SKU LEVEL")
print("Units  :", sku["sku_units"])
print("Revenue:", sku["sku_revenue"])

print("\nPRODUCT LEVEL")
print("Units  :", product["product_units"])
print("Revenue:", product["product_revenue"])

print("\nRECONCILIATION")
print("Units match  :", sku["sku_units"] == product["product_units"])
print("Revenue match:", sku["sku_revenue"] == product["product_revenue"])

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")

unmapped_product_audit = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNull())
    .groupBy(
        "sku_id",
        "seller_sku",
        "sku_product_name"
    )
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("sku_revenue")
    )
    .orderBy(F.desc("units_sold"))
)

unmapped_product_audit.show(50, truncate=False)

unmapped_totals = (
    unmapped_product_audit
    .agg(
        F.count("*").alias("unmapped_sku_count"),
        F.sum("units_sold").alias("unmapped_units"),
        F.sum("sku_revenue")
            .cast(DecimalType(18, 2))
            .alias("unmapped_revenue")
    )
)

unmapped_totals.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

dim_sku = spark.table("workspace.gold.dim_sku")

df_dim_sku_with_mapping_status = (
    dim_sku
    .withColumn(
        "product_mapping_status",
        F.when(
            F.col("product_id").isNull(),
            F.lit("UNMAPPED_HISTORICAL_PRODUCT")
        ).otherwise(
            F.lit("MAPPED")
        )
    )
)

df_dim_sku_with_mapping_status.groupBy(
    "product_mapping_status"
).count().show()

df_dim_sku_with_mapping_status.filter(
    F.col("product_mapping_status") == "UNMAPPED_HISTORICAL_PRODUCT"
).select(
    "sku_id",
    "seller_sku",
    "product_name",
    "status",
    "is_historical",
    "product_mapping_status"
).show(truncate=False)

# COMMAND ----------

(
    df_dim_sku_with_mapping_status.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.dim_sku")
)

print("Updated table: workspace.gold.dim_sku")
print("Rows:", spark.table("workspace.gold.dim_sku").count())

spark.table("workspace.gold.dim_sku") \
    .groupBy("product_mapping_status") \
    .count() \
    .show()

# COMMAND ----------

(
    df_dim_sku_with_mapping_status.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_sku")
)

print("Updated table: workspace.gold.dim_sku")
print("Rows:", spark.table("workspace.gold.dim_sku").count())

spark.table("workspace.gold.dim_sku") \
    .groupBy("product_mapping_status") \
    .count() \
    .show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_sku = spark.table("workspace.gold.dim_sku")

final_recon = (
    fact_order_lines.alias("f")
    .join(
        dim_sku.select(
            "sku_id",
            "product_mapping_status"
        ).alias("d"),
        on="sku_id",
        how="left"
    )
    .filter(F.col("f.order_status") != "ยกเลิกแล้ว")
    .groupBy("product_mapping_status")
    .agg(
        F.sum("f.quantity").alias("units_sold"),
        F.sum("f.sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("revenue")
    )
)

final_recon.show(truncate=False)

totals = (
    final_recon
    .agg(
        F.sum("units_sold").alias("total_units"),
        F.sum("revenue")
            .cast(DecimalType(18, 2))
            .alias("total_revenue")
    )
)

totals.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_orders = spark.table("workspace.gold.fact_orders")

channel_performance = (
    fact_orders
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .groupBy("order_channel")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total_quantity").alias("units_sold"),

        F.sum("order_amount")
            .cast(DecimalType(18, 2))
            .alias("revenue"),

        F.avg("order_amount")
            .cast(DecimalType(18, 2))
            .alias("avg_order_value")
    )
    .orderBy(F.desc("revenue"))
)

channel_performance.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

channel_totals = (
    channel_performance
    .agg(
        F.sum("order_count").alias("total_orders"),
        F.sum("units_sold").alias("total_units"),
        F.sum("revenue")
            .cast(DecimalType(18, 2))
            .alias("total_revenue")
    )
)

channel_totals.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_orders = spark.table("workspace.gold.fact_orders")

df_channel_performance = (
    fact_orders
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .groupBy("order_channel")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total_quantity").alias("units_sold"),

        F.sum("order_amount")
            .cast(DecimalType(18, 2))
            .alias("revenue"),

        F.avg("order_amount")
            .cast(DecimalType(18, 2))
            .alias("avg_order_value")
    )
)

(
    df_channel_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.channel_performance")
)

print("Created table: workspace.gold.channel_performance")
print("Rows:", spark.table("workspace.gold.channel_performance").count())

spark.table("workspace.gold.channel_performance") \
    .orderBy(F.desc("revenue")) \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_sku = spark.table("workspace.gold.dim_sku")

sku_metrics = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .groupBy("sku_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),

        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("sku_revenue")
    )
)

df_sku_performance = (
    sku_metrics.alias("m")
    .join(
        dim_sku.alias("d"),
        on="sku_id",
        how="left"
    )
    .select(
        F.col("sku_id"),
        F.col("d.product_id"),
        F.col("d.seller_sku"),
        F.col("d.product_name").alias("sku_product_name"),
        F.col("d.status").alias("sku_status"),
        F.col("d.is_historical"),
        F.col("d.product_mapping_status"),
        F.col("m.units_sold"),
        F.col("m.order_count"),
        F.col("m.sku_revenue")
    )
)

(
    df_sku_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.sku_performance")
)

print("Created table: workspace.gold.sku_performance")
print("Rows:", spark.table("workspace.gold.sku_performance").count())

spark.table("workspace.gold.sku_performance") \
    .orderBy(
        F.desc("units_sold"),
        F.desc("sku_revenue")
    ) \
    .show(30, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
sku_performance = spark.table("workspace.gold.sku_performance")
dim_sku = spark.table("workspace.gold.dim_sku")

# -----------------------------
# 1. Fact totals
# -----------------------------
fact_totals = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .agg(
        F.sum("quantity").alias("fact_units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("fact_revenue")
    )
)

# -----------------------------
# 2. SKU performance totals
# -----------------------------
sku_totals = (
    sku_performance
    .agg(
        F.sum("units_sold").alias("sku_units"),
        F.sum("sku_revenue")
            .cast(DecimalType(18, 2))
            .alias("sku_revenue")
    )
)

fact = fact_totals.collect()[0]
sku = sku_totals.collect()[0]

print("FACT ORDER LINES")
print("Units  :", fact["fact_units"])
print("Revenue:", fact["fact_revenue"])

print("\nSKU PERFORMANCE")
print("Units  :", sku["sku_units"])
print("Revenue:", sku["sku_revenue"])

print("\nRECONCILIATION")
print("Units match  :", fact["fact_units"] == sku["sku_units"])
print("Revenue match:", fact["fact_revenue"] == sku["sku_revenue"])

# -----------------------------
# 3. Find SKU with no sales
# -----------------------------
sku_without_sales = (
    dim_sku
    .select(
        "sku_id",
        "seller_sku",
        "product_name",
        "status",
        "is_historical"
    )
    .join(
        sku_performance.select("sku_id"),
        on="sku_id",
        how="left_anti"
    )
)

print("\nSKU IN DIMENSION WITH NO NON-CANCELLED SALES:")
sku_without_sales.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_product = spark.table("workspace.gold.dim_product")

product_metrics = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        F.countDistinct("sku_id").alias("sku_count"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),

        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("product_revenue")
    )
)

df_product_performance = (
    product_metrics.alias("m")
    .join(
        dim_product.alias("d"),
        on="product_id",
        how="left"
    )
    .select(
        F.col("product_id"),
        F.col("d.product_name"),
        F.col("d.product_status"),
        F.col("m.sku_count"),
        F.col("m.units_sold"),
        F.col("m.order_count"),
        F.col("m.product_revenue")
    )
)

(
    df_product_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.product_performance")
)

print("Created table: workspace.gold.product_performance")
print("Rows:", spark.table("workspace.gold.product_performance").count())

spark.table("workspace.gold.product_performance") \
    .orderBy(
        F.desc("units_sold"),
        F.desc("product_revenue")
    ) \
    .show(30, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
product_performance = spark.table("workspace.gold.product_performance")
dim_product = spark.table("workspace.gold.dim_product")

# 1) Fact totals เฉพาะ mapped product
fact_product_totals = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNotNull())
    .agg(
        F.sum("quantity").alias("fact_units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("fact_revenue")
    )
)

# 2) Product performance totals
product_totals = (
    product_performance
    .agg(
        F.sum("units_sold").alias("product_units"),
        F.sum("product_revenue")
            .cast(DecimalType(18, 2))
            .alias("product_revenue")
    )
)

fact = fact_product_totals.collect()[0]
product = product_totals.collect()[0]

print("FACT MAPPED PRODUCTS")
print("Units  :", fact["fact_units"])
print("Revenue:", fact["fact_revenue"])

print("\nPRODUCT PERFORMANCE")
print("Units  :", product["product_units"])
print("Revenue:", product["product_revenue"])

print("\nRECONCILIATION")
print("Units match  :", fact["fact_units"] == product["product_units"])
print("Revenue match:", fact["fact_revenue"] == product["product_revenue"])

# 3) Product ใน dimension ที่ไม่มี mapped non-cancelled sales
product_without_sales = (
    dim_product
    .select(
        "product_id",
        "product_name",
        "product_status"
    )
    .join(
        product_performance.select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

print("\nPRODUCT IN DIMENSION WITH NO MAPPED NON-CANCELLED SALES:")
product_without_sales.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")

promo_candidate_check = (
    fact_order_lines
    .filter(
        F.lower(F.col("sku_product_name"))
        .contains("ซื้อ 1 แถม 1")
    )
    .groupBy(
        "sku_id",
        "seller_sku",
        "sku_product_name"
    )
    .agg(
        F.min("created_time").alias("first_seen"),
        F.max("created_time").alias("last_seen"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("sku_subtotal_after_discount").alias("sku_revenue")
    )
    .orderBy("first_seen")
)

promo_candidate_check.show(50, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

dim_sku = spark.table("workspace.gold.dim_sku")
dim_product = spark.table("workspace.gold.dim_product")

# Normalize ชื่อเพื่อเทียบแบบ deterministic
unmapped_skus = (
    dim_sku
    .filter(F.col("product_id").isNull())
    .select(
        "sku_id",
        "seller_sku",
        "product_name"
    )
    .withColumn(
        "normalized_name",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.col("product_name"),
                    r"\s+",
                    " "
                )
            )
        )
    )
)

products_normalized = (
    dim_product
    .select(
        "product_id",
        "product_name",
        "product_status"
    )
    .withColumn(
        "normalized_name",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.col("product_name"),
                    r"\s+",
                    " "
                )
            )
        )
    )
)

exact_matches = (
    unmapped_skus.alias("s")
    .join(
        products_normalized.alias("p"),
        on="normalized_name",
        how="inner"
    )
    .select(
        F.col("s.sku_id"),
        F.col("s.seller_sku"),
        F.col("s.product_name").alias("sku_product_name"),
        F.col("p.product_id").alias("matched_product_id"),
        F.col("p.product_name").alias("matched_product_name"),
        F.col("p.product_status")
    )
)

print("Exact historical product matches:", exact_matches.count())

exact_matches.show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

dim_sku = spark.table("workspace.gold.dim_sku")

df_dim_sku_enriched_product = (
    dim_sku
    .withColumn(
        "product_id",
        F.when(
            F.col("sku_id") == "1732728000121308343",
            F.lit("1732643634638652599")
        ).otherwise(F.col("product_id"))
    )
    .withColumn(
        "product_mapping_status",
        F.when(
            F.col("sku_id") == "1732728000121308343",
            F.lit("MAPPED_HISTORICAL_EXACT_NAME")
        ).otherwise(F.col("product_mapping_status"))
    )
)

df_dim_sku_enriched_product.select(
    "sku_id",
    "seller_sku",
    "product_id",
    "product_name",
    "product_mapping_status"
).filter(
    F.col("sku_id") == "1732728000121308343"
).show(truncate=False)

print(
    "Remaining unmapped SKU:",
    df_dim_sku_enriched_product
    .filter(F.col("product_id").isNull())
    .count()
)

# COMMAND ----------

(
    df_dim_sku_enriched_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_sku")
)

print("Updated table: workspace.gold.dim_sku")
print("Rows:", spark.table("workspace.gold.dim_sku").count())

spark.table("workspace.gold.dim_sku") \
    .groupBy("product_mapping_status") \
    .count() \
    .show(truncate=False)

print(
    "Remaining NULL product_id:",
    spark.table("workspace.gold.dim_sku")
    .filter(F.col("product_id").isNull())
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

target_sku = "1732728000121308343"

print("DIM_SKU:")
spark.table("workspace.gold.dim_sku") \
    .filter(F.col("sku_id") == target_sku) \
    .select(
        "sku_id",
        "seller_sku",
        "product_id",
        "product_mapping_status"
    ) \
    .show(truncate=False)

print("FACT_ORDER_LINES:")
spark.table("workspace.gold.fact_order_lines") \
    .filter(F.col("sku_id") == target_sku) \
    .select(
        "sku_id",
        "product_id"
    ) \
    .distinct() \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_sku = spark.table("workspace.gold.dim_sku")

df_fact_order_lines_synced = (
    fact_order_lines.alias("f")
    .join(
        dim_sku.select(
            "sku_id",
            F.col("product_id").alias("dim_product_id")
        ).alias("d"),
        on="sku_id",
        how="left"
    )
    .select(
        "sku_id",

        F.coalesce(
            F.col("d.dim_product_id"),
            F.col("f.product_id")
        ).alias("product_id"),

        *[
            F.col(f"f.{c}")
            for c in fact_order_lines.columns
            if c not in ["sku_id", "product_id"]
        ]
    )
)

print("Rows:", df_fact_order_lines_synced.count())

print(
    "NULL product_id:",
    df_fact_order_lines_synced
    .filter(F.col("product_id").isNull())
    .count()
)

df_fact_order_lines_synced \
    .filter(F.col("sku_id") == "1732728000121308343") \
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "sku_product_name"
    ) \
    .distinct() \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

null_product_audit = (
    df_fact_order_lines_synced
    .filter(F.col("product_id").isNull())
    .groupBy(
        "sku_id",
        "seller_sku",
        "sku_product_name"
    )
    .agg(
        F.count("*").alias("fact_row_count"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units"),
        F.min("created_time").alias("first_seen"),
        F.max("created_time").alias("last_seen")
    )
    .orderBy("sku_id")
)

print(
    "Distinct unmapped SKU:",
    null_product_audit.select("sku_id").distinct().count()
)

print(
    "Total unmapped fact rows:",
    df_fact_order_lines_synced
    .filter(F.col("product_id").isNull())
    .count()
)

null_product_audit.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_before = spark.table("workspace.gold.fact_order_lines")
fact_after = df_fact_order_lines_synced

before_totals = (
    fact_before
    .agg(
        F.count("*").alias("rows"),
        F.sum("quantity").alias("units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("revenue")
    )
    .collect()[0]
)

after_totals = (
    fact_after
    .agg(
        F.count("*").alias("rows"),
        F.sum("quantity").alias("units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("revenue")
    )
    .collect()[0]
)

print("BEFORE")
print("Rows   :", before_totals["rows"])
print("Units  :", before_totals["units"])
print("Revenue:", before_totals["revenue"])

print("\nAFTER SYNC")
print("Rows   :", after_totals["rows"])
print("Units  :", after_totals["units"])
print("Revenue:", after_totals["revenue"])

print("\nRECONCILIATION")
print("Rows match   :", before_totals["rows"] == after_totals["rows"])
print("Units match  :", before_totals["units"] == after_totals["units"])
print("Revenue match:", before_totals["revenue"] == after_totals["revenue"])

# COMMAND ----------

from pyspark.sql import functions as F

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_sku = spark.table("workspace.gold.dim_sku")

df_fact_order_lines_synced = (
    fact_order_lines.alias("f")
    .join(
        dim_sku.select(
            "sku_id",
            F.col("product_id").alias("dim_product_id")
        ).alias("d"),
        on="sku_id",
        how="left"
    )
    .select(
        "sku_id",

        F.coalesce(
            F.col("d.dim_product_id"),
            F.col("f.product_id")
        ).alias("product_id"),

        *[
            F.col(f"f.{c}")
            for c in fact_order_lines.columns
            if c not in ["sku_id", "product_id"]
        ]
    )
)

print("Rebuilt df_fact_order_lines_synced")
print("Rows:", df_fact_order_lines_synced.count())

print(
    "NULL product_id:",
    df_fact_order_lines_synced
    .filter(F.col("product_id").isNull())
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_before = spark.table("workspace.gold.fact_order_lines")
fact_after = df_fact_order_lines_synced

before_totals = (
    fact_before
    .agg(
        F.count("*").alias("rows"),
        F.sum("quantity").alias("units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("revenue")
    )
    .collect()[0]
)

after_totals = (
    fact_after
    .agg(
        F.count("*").alias("rows"),
        F.sum("quantity").alias("units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("revenue")
    )
    .collect()[0]
)

print("BEFORE")
print("Rows   :", before_totals["rows"])
print("Units  :", before_totals["units"])
print("Revenue:", before_totals["revenue"])

print("\nAFTER SYNC")
print("Rows   :", after_totals["rows"])
print("Units  :", after_totals["units"])
print("Revenue:", after_totals["revenue"])

print("\nRECONCILIATION")
print("Rows match   :", before_totals["rows"] == after_totals["rows"])
print("Units match  :", before_totals["units"] == after_totals["units"])
print("Revenue match:", before_totals["revenue"] == after_totals["revenue"])

# COMMAND ----------

(
    df_fact_order_lines_synced.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.fact_order_lines")
)

print("Updated table: workspace.gold.fact_order_lines")
print("Rows:", spark.table("workspace.gold.fact_order_lines").count())

print(
    "NULL product_id rows:",
    spark.table("workspace.gold.fact_order_lines")
    .filter(F.col("product_id").isNull())
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
dim_product = spark.table("workspace.gold.dim_product")

product_metrics = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        F.countDistinct("sku_id").alias("sku_count"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("product_revenue")
    )
)

df_product_performance_updated = (
    product_metrics.alias("m")
    .join(
        dim_product.alias("d"),
        on="product_id",
        how="left"
    )
    .select(
        "product_id",
        F.col("d.product_name"),
        F.col("d.product_status"),
        "sku_count",
        "units_sold",
        "order_count",
        "product_revenue"
    )
)

(
    df_product_performance_updated.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.product_performance")
)

print("Updated table: workspace.gold.product_performance")
print("Rows:", spark.table("workspace.gold.product_performance").count())

spark.table("workspace.gold.product_performance") \
    .orderBy(F.desc("units_sold")) \
    .show(30, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

fact_order_lines = spark.table("workspace.gold.fact_order_lines")
product_performance = spark.table("workspace.gold.product_performance")

fact_mapped = (
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNotNull())
    .agg(
        F.sum("quantity").alias("fact_units"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("fact_revenue")
    )
    .collect()[0]
)

product_totals = (
    product_performance
    .agg(
        F.sum("units_sold").alias("product_units"),
        F.sum("product_revenue")
            .cast(DecimalType(18, 2))
            .alias("product_revenue")
    )
    .collect()[0]
)

print("FACT MAPPED PRODUCTS")
print("Units  :", fact_mapped["fact_units"])
print("Revenue:", fact_mapped["fact_revenue"])

print("\nPRODUCT PERFORMANCE")
print("Units  :", product_totals["product_units"])
print("Revenue:", product_totals["product_revenue"])

print("\nRECONCILIATION")
print("Units match  :", fact_mapped["fact_units"] == product_totals["product_units"])
print("Revenue match:", fact_mapped["fact_revenue"] == product_totals["product_revenue"])

print("\nREMAINING UNMAPPED NON-CANCELLED")
(
    fact_order_lines
    .filter(F.col("order_status") != "ยกเลิกแล้ว")
    .filter(F.col("product_id").isNull())
    .groupBy("sku_id", "seller_sku", "sku_product_name")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("sku_subtotal_after_discount")
            .cast(DecimalType(18, 2))
            .alias("revenue")
    )
    .show(truncate=False)
)

# COMMAND ----------

from pyspark.sql import functions as F

fact_orders = spark.table("workspace.gold.fact_orders")
fact_order_lines = spark.table("workspace.gold.fact_order_lines")

print("=== FACT_ORDERS DQ ===")

fact_orders.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0)).alias("null_order_id"),
    F.sum(F.when(F.col("total_quantity") <= 0, 1).otherwise(0)).alias("invalid_quantity"),
    F.sum(F.when(F.col("order_amount") < 0, 1).otherwise(0)).alias("negative_order_amount")
).show(truncate=False)

print("=== FACT_ORDER_LINES DQ ===")

fact_order_lines.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0)).alias("null_order_id"),
    F.sum(F.when(F.col("sku_id").isNull(), 1).otherwise(0)).alias("null_sku_id"),
    F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias("invalid_quantity"),
    F.sum(
        F.when(
            F.col("sku_subtotal_after_discount") < 0,
            1
        ).otherwise(0)
    ).alias("negative_sku_amount")
).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_orders
# MAGIC ALTER COLUMN order_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_orders
# MAGIC ADD CONSTRAINT chk_fact_orders_total_quantity_positive
# MAGIC CHECK (total_quantity > 0);
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_orders
# MAGIC ADD CONSTRAINT chk_fact_orders_order_amount_nonnegative
# MAGIC CHECK (order_amount >= 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_orders
# MAGIC ADD CONSTRAINT chk_fact_orders_order_amount_nonnegative
# MAGIC CHECK (order_amount >= 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW CREATE TABLE workspace.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES workspace.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES workspace.gold.fact_order_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_order_lines
# MAGIC ALTER COLUMN order_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_order_lines
# MAGIC ALTER COLUMN sku_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_order_lines
# MAGIC ADD CONSTRAINT chk_fact_order_lines_quantity_positive
# MAGIC CHECK (quantity > 0);
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_order_lines
# MAGIC ADD CONSTRAINT chk_fact_order_lines_sku_amount_nonnegative
# MAGIC CHECK (sku_subtotal_after_discount >= 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES workspace.gold.fact_order_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO workspace.gold.fact_order_lines (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     product_id,
# MAGIC     created_time,
# MAGIC     order_status,
# MAGIC     quantity,
# MAGIC     sku_unit_original_price,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     order_amount,
# MAGIC     seller_sku,
# MAGIC     variation,
# MAGIC     product_category,
# MAGIC     order_channel,
# MAGIC     sku_product_name,
# MAGIC     sku_status,
# MAGIC     is_historical,
# MAGIC     product_name,
# MAGIC     product_status
# MAGIC )
# MAGIC VALUES (
# MAGIC     'TEST_CONSTRAINT_ORDER_001',
# MAGIC     'TEST_CONSTRAINT_SKU_001',
# MAGIC     NULL,
# MAGIC     current_timestamp(),
# MAGIC     'TEST',
# MAGIC     -1,
# MAGIC     100.00,
# MAGIC     100.00,
# MAGIC     100.00,
# MAGIC     'TEST-SKU',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'TEST',
# MAGIC     'Constraint Test Product',
# MAGIC     'TEST',
# MAGIC     false,
# MAGIC     NULL,
# MAGIC     NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS bad_row_count
# MAGIC FROM workspace.gold.fact_order_lines
# MAGIC WHERE order_id = 'TEST_CONSTRAINT_ORDER_001';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO workspace.gold.fact_order_lines (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     product_id,
# MAGIC     created_time,
# MAGIC     order_status,
# MAGIC     quantity,
# MAGIC     sku_unit_original_price,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     order_amount,
# MAGIC     seller_sku,
# MAGIC     variation,
# MAGIC     product_category,
# MAGIC     order_channel,
# MAGIC     sku_product_name,
# MAGIC     sku_status,
# MAGIC     is_historical,
# MAGIC     product_name,
# MAGIC     product_status
# MAGIC )
# MAGIC VALUES (
# MAGIC     'TEST_CONSTRAINT_ORDER_002',
# MAGIC     'TEST_CONSTRAINT_SKU_002',
# MAGIC     NULL,
# MAGIC     current_timestamp(),
# MAGIC     'TEST',
# MAGIC     1,
# MAGIC     100.00,
# MAGIC     -50.00,
# MAGIC     100.00,
# MAGIC     'TEST-SKU-002',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'TEST',
# MAGIC     'Constraint Test Product 002',
# MAGIC     'TEST',
# MAGIC     false,
# MAGIC     NULL,
# MAGIC     NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS bad_row_count
# MAGIC FROM workspace.gold.fact_order_lines
# MAGIC WHERE order_id = 'TEST_CONSTRAINT_ORDER_002';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.gold.fact_order_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationParameters,
# MAGIC     readVersion,
# MAGIC     isolationLevel,
# MAGIC     isBlindAppend
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY workspace.gold.fact_order_lines
# MAGIC )
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'VERSION_0' AS table_version,
# MAGIC     sku_id,
# MAGIC     product_id,
# MAGIC     seller_sku
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 0
# MAGIC WHERE sku_id = '1732728000121308343'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'VERSION_1' AS table_version,
# MAGIC     sku_id,
# MAGIC     product_id,
# MAGIC     seller_sku
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 1
# MAGIC WHERE sku_id = '1732728000121308343'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'CURRENT' AS table_version,
# MAGIC     sku_id,
# MAGIC     product_id,
# MAGIC     seller_sku
# MAGIC FROM workspace.gold.fact_order_lines
# MAGIC WHERE sku_id = '1732728000121308343';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'VERSION_0' AS table_version,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     COUNT(DISTINCT order_id) AS order_count,
# MAGIC     MIN(product_id) AS product_id
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 0
# MAGIC WHERE sku_id = '1732728000121308343'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'VERSION_1' AS table_version,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     COUNT(DISTINCT order_id) AS order_count,
# MAGIC     MIN(product_id) AS product_id
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 1
# MAGIC WHERE sku_id = '1732728000121308343'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'CURRENT' AS table_version,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     COUNT(DISTINCT order_id) AS order_count,
# MAGIC     MIN(product_id) AS product_id
# MAGIC FROM workspace.gold.fact_order_lines
# MAGIC WHERE sku_id = '1732728000121308343';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'VERSION_0' AS table_version,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     SUM(quantity) AS total_units,
# MAGIC     ROUND(SUM(sku_subtotal_after_discount), 2) AS total_revenue
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'CURRENT' AS table_version,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     SUM(quantity) AS total_units,
# MAGIC     ROUND(SUM(sku_subtotal_after_discount), 2) AS total_revenue
# MAGIC FROM workspace.gold.fact_order_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW fact_order_lines_version_0 AS
# MAGIC SELECT *
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 0;
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS row_count,
# MAGIC     SUM(quantity) AS total_units,
# MAGIC     ROUND(SUM(sku_subtotal_after_discount), 2) AS total_revenue
# MAGIC FROM fact_order_lines_version_0;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     v0.sku_id,
# MAGIC     v0.order_id,
# MAGIC     v0.product_id AS version_0_product_id,
# MAGIC     cur.product_id AS current_product_id
# MAGIC FROM workspace.gold.fact_order_lines VERSION AS OF 0 v0
# MAGIC INNER JOIN workspace.gold.fact_order_lines cur
# MAGIC     ON v0.order_id = cur.order_id
# MAGIC    AND v0.sku_id = cur.sku_id
# MAGIC WHERE NOT (
# MAGIC     v0.product_id <=> cur.product_id
# MAGIC )
# MAGIC ORDER BY v0.sku_id, v0.order_id;

# COMMAND ----------

from pyspark.sql import functions as F

source_fact = spark.table("workspace.gold.fact_order_lines")

df_incremental_source = (
    source_fact
    .orderBy(F.col("created_time").desc())
    .limit(5)
)

print("Incremental source rows:", df_incremental_source.count())

df_incremental_source.select(
    "order_id",
    "sku_id",
    "product_id",
    "created_time",
    "quantity",
    "sku_subtotal_after_discount"
).show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

df_merge_target = (
    spark.table("workspace.gold.fact_order_lines")
    .orderBy(F.col("created_time").desc())
    .limit(10)
)

(
    df_merge_target.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.gold.fact_order_lines_merge_sandbox")
)

print("Created sandbox table: workspace.gold.fact_order_lines_merge_sandbox")
print(
    "Sandbox rows:",
    spark.table("workspace.gold.fact_order_lines_merge_sandbox").count()
)

spark.table("workspace.gold.fact_order_lines_merge_sandbox") \
    .select(
        "order_id",
        "sku_id",
        "product_id",
        "created_time",
        "quantity",
        "sku_subtotal_after_discount"
    ) \
    .orderBy(F.col("created_time").desc()) \
    .show(20, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

sandbox = spark.table("workspace.gold.fact_order_lines_merge_sandbox")

# เอา 2 แถวเดิมมาจำลองเป็นข้อมูลที่ถูกแก้ไข
existing_updates = (
    sandbox
    .orderBy(F.col("created_time").desc())
    .limit(2)
    .withColumn("quantity", F.col("quantity") + 1)
    .withColumn(
        "sku_subtotal_after_discount",
        F.col("sku_subtotal_after_discount") + F.lit(10.00)
    )
)

# สร้าง 2 แถวใหม่จาก schema เดิม
new_rows = (
    sandbox
    .orderBy(F.col("created_time").desc())
    .limit(2)
    .withColumn(
        "order_id",
        F.concat(F.lit("NEW_"), F.col("order_id"))
    )
    .withColumn(
        "sku_id",
        F.concat(F.lit("NEW_"), F.col("sku_id"))
    )
    .withColumn("created_time", F.current_timestamp())
)

df_incremental_batch = existing_updates.unionByName(new_rows)

print("Incremental batch rows:", df_incremental_batch.count())

df_incremental_batch.select(
    "order_id",
    "sku_id",
    "quantity",
    "sku_subtotal_after_discount",
    "created_time"
).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO workspace.gold.fact_order_lines_merge_sandbox AS target
# MAGIC USING (
# MAGIC     SELECT * FROM VALUES
# MAGIC     (
# MAGIC         '585523356187723174',
# MAGIC         '1732064364367938743',
# MAGIC         2,
# MAGIC         180.91
# MAGIC     ),
# MAGIC     (
# MAGIC         '585522819869935571',
# MAGIC         '1732059819924817079',
# MAGIC         2,
# MAGIC         597.18
# MAGIC     ),
# MAGIC     (
# MAGIC         'NEW_585523356187723174',
# MAGIC         'NEW_1732064364367938743',
# MAGIC         1,
# MAGIC         170.91
# MAGIC     ),
# MAGIC     (
# MAGIC         'NEW_585522819869935571',
# MAGIC         'NEW_1732059819924817079',
# MAGIC         1,
# MAGIC         587.18
# MAGIC     )
# MAGIC ) AS source (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount
# MAGIC )
# MAGIC
# MAGIC ON target.order_id = source.order_id
# MAGIC AND target.sku_id = source.sku_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     target.quantity = source.quantity,
# MAGIC     target.sku_subtotal_after_discount = source.sku_subtotal_after_discount
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.order_id,
# MAGIC     source.sku_id,
# MAGIC     source.quantity,
# MAGIC     source.sku_subtotal_after_discount
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO workspace.gold.fact_order_lines_merge_sandbox AS target
# MAGIC
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         '585523356187723174' AS order_id,
# MAGIC         '1732064364367938743' AS sku_id,
# MAGIC         2 AS quantity,
# MAGIC         CAST(180.91 AS DECIMAL(18,2)) AS sku_subtotal_after_discount
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         '585522819869935571',
# MAGIC         '1732059819924817079',
# MAGIC         2,
# MAGIC         CAST(597.18 AS DECIMAL(18,2))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'NEW_585523356187723174',
# MAGIC         'NEW_1732064364367938743',
# MAGIC         1,
# MAGIC         CAST(170.91 AS DECIMAL(18,2))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'NEW_585522819869935571',
# MAGIC         'NEW_1732059819924817079',
# MAGIC         1,
# MAGIC         CAST(587.18 AS DECIMAL(18,2))
# MAGIC ) AS source
# MAGIC
# MAGIC ON target.order_id = source.order_id
# MAGIC AND target.sku_id = source.sku_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     target.quantity = source.quantity,
# MAGIC     target.sku_subtotal_after_discount = source.sku_subtotal_after_discount
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.order_id,
# MAGIC     source.sku_id,
# MAGIC     source.quantity,
# MAGIC     source.sku_subtotal_after_discount
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount
# MAGIC FROM workspace.gold.fact_order_lines_merge_sandbox
# MAGIC WHERE order_id IN (
# MAGIC     '585523356187723174',
# MAGIC     '585522819869935571',
# MAGIC     'NEW_585523356187723174',
# MAGIC     'NEW_585522819869935571'
# MAGIC )
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS sandbox_rows
# MAGIC FROM workspace.gold.fact_order_lines_merge_sandbox;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS sandbox_rows_after_rerun
# MAGIC FROM workspace.gold.fact_order_lines_merge_sandbox;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY workspace.gold.fact_order_lines_merge_sandbox;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY workspace.gold.fact_order_lines_merge_sandbox
# MAGIC )
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics['numTargetRowsInserted'] AS rows_inserted,
# MAGIC     operationMetrics['numTargetRowsUpdated'] AS rows_updated,
# MAGIC     operationMetrics['numTargetRowsDeleted'] AS rows_deleted,
# MAGIC     operationMetrics['numTargetRowsCopied'] AS rows_copied,
# MAGIC     operationMetrics['numSourceRows'] AS source_rows
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY workspace.gold.fact_order_lines_merge_sandbox
# MAGIC )
# MAGIC WHERE operation = 'MERGE'
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gold.pipeline_watermark (
# MAGIC     pipeline_name STRING,
# MAGIC     last_processed_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO workspace.gold.pipeline_watermark
# MAGIC SELECT
# MAGIC     'fact_order_lines_incremental',
# MAGIC     TIMESTAMP('2026-08-13 08:00:00')
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM workspace.gold.pipeline_watermark
# MAGIC     WHERE pipeline_name = 'fact_order_lines_incremental'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.pipeline_watermark;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     f.order_id,
# MAGIC     f.sku_id,
# MAGIC     f.created_time,
# MAGIC     f.quantity,
# MAGIC     f.sku_subtotal_after_discount
# MAGIC FROM workspace.gold.fact_order_lines AS f
# MAGIC CROSS JOIN workspace.gold.pipeline_watermark AS w
# MAGIC WHERE w.pipeline_name = 'fact_order_lines_incremental'
# MAGIC   AND f.created_time > w.last_processed_at
# MAGIC ORDER BY f.created_time;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS incremental_rows,
# MAGIC     MIN(f.created_time) AS first_incremental_time,
# MAGIC     MAX(f.created_time) AS last_incremental_time
# MAGIC FROM workspace.gold.fact_order_lines AS f
# MAGIC CROSS JOIN workspace.gold.pipeline_watermark AS w
# MAGIC WHERE w.pipeline_name = 'fact_order_lines_incremental'
# MAGIC   AND f.created_time > w.last_processed_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE workspace.gold.pipeline_watermark
# MAGIC SET last_processed_at = TIMESTAMP('2026-08-13 12:20:47')
# MAGIC WHERE pipeline_name = 'fact_order_lines_incremental';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.pipeline_watermark
# MAGIC WHERE pipeline_name = 'fact_order_lines_incremental';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS remaining_incremental_rows
# MAGIC FROM workspace.gold.fact_order_lines AS f
# MAGIC CROSS JOIN workspace.gold.pipeline_watermark AS w
# MAGIC WHERE w.pipeline_name = 'fact_order_lines_incremental'
# MAGIC   AND f.created_time > w.last_processed_at;

# COMMAND ----------

from pyspark.sql import functions as F

late_record = (
    spark.table("workspace.gold.fact_order_lines")
    .orderBy(F.col("created_time").desc())
    .limit(1)
    .withColumn(
        "order_id",
        F.concat(F.lit("LATE_"), F.col("order_id"))
    )
    .withColumn(
        "sku_id",
        F.concat(F.lit("LATE_"), F.col("sku_id"))
    )
    .withColumn(
        "created_time",
        F.to_timestamp(F.lit("2026-08-13 11:30:00"))
    )
)

late_record.select(
    "order_id",
    "sku_id",
    "created_time",
    "quantity",
    "sku_subtotal_after_discount"
).show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

watermark = (
    spark.table("workspace.gold.pipeline_watermark")
    .filter(F.col("pipeline_name") == "fact_order_lines_incremental")
    .select("last_processed_at")
    .collect()[0]["last_processed_at"]
)

print("Current watermark:", watermark)

late_record_simple_filter = (
    late_record
    .filter(F.col("created_time") > F.lit(watermark))
)

print(
    "Late record captured by simple watermark:",
    late_record_simple_filter.count()
)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import timedelta

watermark = (
    spark.table("workspace.gold.pipeline_watermark")
    .filter(F.col("pipeline_name") == "fact_order_lines_incremental")
    .select("last_processed_at")
    .collect()[0]["last_processed_at"]
)

lookback_hours = 2
lookback_start = watermark - timedelta(hours=lookback_hours)

print("Current watermark :", watermark)
print("Lookback start    :", lookback_start)

late_record_with_lookback = (
    late_record
    .filter(F.col("created_time") > F.lit(lookback_start))
)

print(
    "Late record captured with 2-hour lookback:",
    late_record_with_lookback.count()
)

late_record_with_lookback.select(
    "order_id",
    "sku_id",
    "created_time",
    "quantity"
).show(truncate=False)

# COMMAND ----------

late_record_with_lookback.createOrReplaceTempView("late_record_incremental_batch")

print("Created temp view: late_record_incremental_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO workspace.gold.fact_order_lines_merge_sandbox AS target
# MAGIC USING late_record_incremental_batch AS source
# MAGIC
# MAGIC ON target.order_id = source.order_id
# MAGIC AND target.sku_id = source.sku_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     target.quantity = source.quantity,
# MAGIC     target.sku_subtotal_after_discount = source.sku_subtotal_after_discount
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     product_id,
# MAGIC     created_time,
# MAGIC     order_status,
# MAGIC     quantity,
# MAGIC     sku_unit_original_price,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     order_amount,
# MAGIC     seller_sku,
# MAGIC     variation,
# MAGIC     product_category,
# MAGIC     order_channel,
# MAGIC     sku_product_name,
# MAGIC     sku_status,
# MAGIC     is_historical,
# MAGIC     product_name,
# MAGIC     product_status
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.order_id,
# MAGIC     source.sku_id,
# MAGIC     source.product_id,
# MAGIC     source.created_time,
# MAGIC     source.order_status,
# MAGIC     source.quantity,
# MAGIC     source.sku_unit_original_price,
# MAGIC     source.sku_subtotal_after_discount,
# MAGIC     source.order_amount,
# MAGIC     source.seller_sku,
# MAGIC     source.variation,
# MAGIC     source.product_category,
# MAGIC     source.order_channel,
# MAGIC     source.sku_product_name,
# MAGIC     source.sku_status,
# MAGIC     source.is_historical,
# MAGIC     source.product_name,
# MAGIC     source.product_status
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS sandbox_rows
# MAGIC FROM workspace.gold.fact_order_lines_merge_sandbox;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES workspace.gold.fact_order_lines_merge_sandbox;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fact_order_lines_merge_sandbox
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES workspace.gold.fact_order_lines_merge_sandbox;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE workspace.gold.fact_order_lines_merge_sandbox
# MAGIC SET quantity = quantity + 1
# MAGIC WHERE order_id = '585523356187723174'
# MAGIC   AND sku_id = '1732064364367938743';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO workspace.gold.fact_order_lines_merge_sandbox (
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount
# MAGIC )
# MAGIC VALUES (
# MAGIC     'CDF_TEST_ORDER_001',
# MAGIC     'CDF_TEST_SKU_001',
# MAGIC     1,
# MAGIC     99.99
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp
# MAGIC FROM table_changes(
# MAGIC     'workspace.gold.fact_order_lines_merge_sandbox',
# MAGIC     4
# MAGIC )
# MAGIC WHERE order_id IN (
# MAGIC     '585523356187723174',
# MAGIC     'CDF_TEST_ORDER_001'
# MAGIC )
# MAGIC ORDER BY
# MAGIC     _commit_version,
# MAGIC     order_id,
# MAGIC     _change_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationParameters
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY workspace.gold.fact_order_lines_merge_sandbox
# MAGIC )
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp
# MAGIC FROM table_changes(
# MAGIC     'workspace.gold.fact_order_lines_merge_sandbox',
# MAGIC     5,
# MAGIC     7
# MAGIC )
# MAGIC WHERE order_id IN (
# MAGIC     '585523356187723174',
# MAGIC     'CDF_TEST_ORDER_001'
# MAGIC )
# MAGIC ORDER BY
# MAGIC     _commit_version,
# MAGIC     order_id,
# MAGIC     _change_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW cdf_incremental_changes AS
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp
# MAGIC FROM table_changes(
# MAGIC     'workspace.gold.fact_order_lines_merge_sandbox',
# MAGIC     5,
# MAGIC     7
# MAGIC )
# MAGIC WHERE _change_type IN (
# MAGIC     'insert',
# MAGIC     'update_postimage'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM cdf_incremental_changes
# MAGIC ORDER BY _commit_version, order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gold.cdf_checkpoint (
# MAGIC     pipeline_name STRING,
# MAGIC     last_processed_version BIGINT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO workspace.gold.cdf_checkpoint
# MAGIC SELECT
# MAGIC     'fact_order_lines_cdf_downstream',
# MAGIC     7
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM workspace.gold.cdf_checkpoint
# MAGIC     WHERE pipeline_name = 'fact_order_lines_cdf_downstream'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.cdf_checkpoint;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE workspace.gold.fact_order_lines_merge_sandbox
# MAGIC SET quantity = quantity + 1
# MAGIC WHERE order_id = 'CDF_TEST_ORDER_001'
# MAGIC   AND sku_id = 'CDF_TEST_SKU_001';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY workspace.gold.fact_order_lines_merge_sandbox
# MAGIC )
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     sku_id,
# MAGIC     quantity,
# MAGIC     sku_subtotal_after_discount,
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp
# MAGIC FROM table_changes(
# MAGIC     'workspace.gold.fact_order_lines_merge_sandbox',
# MAGIC     8,
# MAGIC     8
# MAGIC )
# MAGIC WHERE _change_type IN (
# MAGIC     'insert',
# MAGIC     'update_postimage',
# MAGIC     'delete'
# MAGIC )
# MAGIC ORDER BY _commit_version, order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE workspace.gold.cdf_checkpoint
# MAGIC SET last_processed_version = 8
# MAGIC WHERE pipeline_name = 'fact_order_lines_cdf_downstream';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.cdf_checkpoint
# MAGIC WHERE pipeline_name = 'fact_order_lines_cdf_downstream';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS new_change_rows
# MAGIC FROM table_changes(
# MAGIC     'workspace.gold.fact_order_lines_merge_sandbox',
# MAGIC     9
# MAGIC )
# MAGIC WHERE _change_type IN (
# MAGIC     'insert',
# MAGIC     'update_postimage',
# MAGIC     'delete'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH latest_table AS (
# MAGIC     SELECT MAX(version) AS latest_version
# MAGIC     FROM (
# MAGIC         DESCRIBE HISTORY workspace.gold.fact_order_lines_merge_sandbox
# MAGIC     )
# MAGIC ),
# MAGIC checkpoint AS (
# MAGIC     SELECT last_processed_version
# MAGIC     FROM workspace.gold.cdf_checkpoint
# MAGIC     WHERE pipeline_name = 'fact_order_lines_cdf_downstream'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     c.last_processed_version,
# MAGIC     l.latest_version,
# MAGIC     CASE
# MAGIC         WHEN l.latest_version > c.last_processed_version
# MAGIC             THEN l.latest_version - c.last_processed_version
# MAGIC         ELSE 0
# MAGIC     END AS new_versions_available,
# MAGIC     CASE
# MAGIC         WHEN l.latest_version > c.last_processed_version
# MAGIC             THEN 'READ_CDF'
# MAGIC         ELSE 'NO_NEW_CHANGES'
# MAGIC     END AS pipeline_action
# MAGIC FROM checkpoint c
# MAGIC CROSS JOIN latest_table l;

# COMMAND ----------

dim_sku_df = spark.table("workspace.gold.dim_sku")

print("dim_sku row count:", dim_sku_df.count())

dim_sku_df.printSchema()

display(
    dim_sku_df
    .orderBy("sku_id")
)

# COMMAND ----------

from pyspark.sql import functions as F

source_dim_sku_df = spark.table("workspace.gold.dim_sku")

dim_sku_scd2_initial_df = (
    source_dim_sku_df
    .withColumn(
        "valid_from",
        F.current_timestamp()
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

(
    dim_sku_scd2_initial_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_sku_scd2_sandbox")
)

print(
    "sandbox row count:",
    spark.table("workspace.gold.dim_sku_scd2_sandbox").count()
)

spark.table(
    "workspace.gold.dim_sku_scd2_sandbox"
).printSchema()

display(
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .orderBy("sku_id")
)

# COMMAND ----------

target_sku_id = "1732698562646017207"

display(
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(F.col("sku_id") == target_sku_id)
)

# COMMAND ----------

from pyspark.sql import functions as F

target_sku_id = "1732698562646017207"

display(
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(F.col("sku_id") == target_sku_id)
)

# COMMAND ----------

from pyspark.sql import functions as F

target_sku_id = "1732698562646017207"

display(
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(F.col("sku_id") == target_sku_id)
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_sku_id = "1732698562646017207"

current_row_df = (
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(
        (F.col("sku_id") == target_sku_id)
        & (F.col("is_current") == True)
    )
)

scd2_change_event_df = (
    current_row_df
    .drop("valid_from", "valid_to", "is_current")
    .withColumn("status", F.lit("Inactive"))
    .withColumn("change_timestamp", F.current_timestamp())
)

display(
    scd2_change_event_df.select(
        "sku_id",
        "seller_sku",
        "status",
        "change_timestamp"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_df = (
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(F.col("is_current") == True)
    .select(
        "sku_id",
        F.col("status").alias("target_status")
    )
)

source_df = (
    scd2_change_event_df
    .select(
        "sku_id",
        F.col("status").alias("source_status"),
        "change_timestamp"
    )
)

change_detection_df = (
    source_df.alias("s")
    .join(
        target_df.alias("t"),
        on="sku_id",
        how="left"
    )
    .withColumn(
        "change_type",
        F.when(F.col("t.sku_id").isNull(), F.lit("NEW"))
         .when(F.col("source_status") != F.col("target_status"), F.lit("CHANGED"))
         .otherwise(F.lit("NO_CHANGE"))
    )
)

display(change_detection_df)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"

delta_target = DeltaTable.forName(spark, target_table)

changed_rows_df = (
    change_detection_df
    .filter(F.col("change_type") == "CHANGED")
    .select(
        "sku_id",
        "change_timestamp"
    )
)

(
    delta_target.alias("t")
    .merge(
        changed_rows_df.alias("s"),
        "t.sku_id = s.sku_id AND t.is_current = true"
    )
    .whenMatchedUpdate(
        set={
            "valid_to": "s.change_timestamp",
            "is_current": "false"
        }
    )
    .execute()
)

display(
    spark.table(target_table)
    .filter(F.col("sku_id") == target_sku_id)
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"

new_version_df = (
    scd2_change_event_df
    .drop("change_timestamp")
    .withColumn(
        "valid_from",
        F.current_timestamp()
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

(
    new_version_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

display(
    spark.table(target_table)
    .filter(F.col("sku_id") == target_sku_id)
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

print(
    "scd2_change_event_df rows:",
    scd2_change_event_df.count()
)

print(
    "current rows for target SKU:",
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(
        (F.col("sku_id") == target_sku_id)
        & (F.col("is_current") == True)
    )
    .count()
)

print(
    "all versions for target SKU:",
    spark.table("workspace.gold.dim_sku_scd2_sandbox")
    .filter(F.col("sku_id") == target_sku_id)
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

target_sku_id = "1732698562646017207"
target_table = "workspace.gold.dim_sku_scd2_sandbox"

expired_row_df = (
    spark.table(target_table)
    .filter(
        (F.col("sku_id") == target_sku_id)
        & (F.col("is_current") == False)
    )
    .orderBy(F.col("valid_to").desc())
    .limit(1)
)

recovered_change_event_df = (
    expired_row_df
    .drop("valid_from", "valid_to", "is_current")
    .withColumn("status", F.lit("Inactive"))
    .withColumn("change_timestamp", F.current_timestamp())
)

print(
    "recovered change event rows:",
    recovered_change_event_df.count()
)

display(
    recovered_change_event_df.select(
        "sku_id",
        "seller_sku",
        "status",
        "change_timestamp"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"

new_current_version_df = (
    recovered_change_event_df
    .drop("change_timestamp")
    .withColumn("valid_from", F.current_timestamp())
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

(
    new_current_version_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

display(
    spark.table(target_table)
    .filter(F.col("sku_id") == target_sku_id)
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"

scd2_df = spark.table(target_table)

# 1) ต้องมี current row ไม่เกิน 1 row ต่อ sku_id
current_count_check_df = (
    scd2_df
    .filter(F.col("is_current") == True)
    .groupBy("sku_id")
    .agg(F.count("*").alias("current_row_count"))
    .filter(F.col("current_row_count") > 1)
)

# 2) current row ต้องมี valid_to = NULL
invalid_current_valid_to_df = (
    scd2_df
    .filter(
        (F.col("is_current") == True)
        & (F.col("valid_to").isNotNull())
    )
)

# 3) expired row ต้องมี valid_to ไม่เป็น NULL
invalid_expired_valid_to_df = (
    scd2_df
    .filter(
        (F.col("is_current") == False)
        & (F.col("valid_to").isNull())
    )
)

print(
    "duplicate current SKU count:",
    current_count_check_df.count()
)

print(
    "invalid current valid_to count:",
    invalid_current_valid_to_df.count()
)

print(
    "invalid expired valid_to count:",
    invalid_expired_valid_to_df.count()
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

target_table = "workspace.gold.dim_sku_scd2_sandbox"

scd2_df = spark.table(target_table)

window_spec = (
    Window
    .partitionBy("sku_id")
    .orderBy("valid_from")
)

overlap_check_df = (
    scd2_df
    .withColumn(
        "next_valid_from",
        F.lead("valid_from").over(window_spec)
    )
    .filter(
        F.col("valid_to").isNotNull()
        & F.col("next_valid_from").isNotNull()
        & (F.col("valid_to") > F.col("next_valid_from"))
    )
)

print(
    "overlapping version count:",
    overlap_check_df.count()
)

display(
    overlap_check_df.select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "next_valid_from"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.dim_sku"
target_table = "workspace.gold.dim_sku_scd2_sandbox"

baseline_df = (
    spark.table(source_table)
    .withColumn("valid_from", F.current_timestamp())
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

(
    baseline_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(
    "sandbox rows:",
    spark.table(target_table).count()
)

print(
    "current rows:",
    spark.table(target_table)
    .filter(F.col("is_current") == True)
    .count()
)

print(
    "historical rows:",
    spark.table(target_table)
    .filter(F.col("is_current") == False)
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.dim_sku"
incoming_table = "workspace.gold.dim_sku_scd2_incoming_batch_sandbox"

target_sku_id = "1732698562646017207"

batch_id = "scd2_batch_001"
change_timestamp = "2026-08-17 01:30:00"

incoming_batch_df = (
    spark.table(source_table)
    .filter(F.col("sku_id") == target_sku_id)
    .withColumn("status", F.lit("Inactive"))
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn(
        "change_timestamp",
        F.to_timestamp(F.lit(change_timestamp))
    )
)

(
    incoming_batch_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(incoming_table)
)

persistent_batch_df = spark.table(incoming_table)

print(
    "incoming batch rows:",
    persistent_batch_df.count()
)

display(
    persistent_batch_df.select(
        "batch_id",
        "sku_id",
        "seller_sku",
        "status",
        "change_timestamp"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

incoming_table = "workspace.gold.dim_sku_scd2_incoming_batch_sandbox"
target_table = "workspace.gold.dim_sku_scd2_sandbox"

source_df = (
    spark.table(incoming_table)
    .alias("s")
)

current_target_df = (
    spark.table(target_table)
    .filter(F.col("is_current") == True)
    .alias("t")
)

classified_df = (
    source_df
    .join(
        current_target_df,
        F.col("s.sku_id") == F.col("t.sku_id"),
        "left"
    )
    .withColumn(
        "change_type",
        F.when(
            F.col("t.sku_id").isNull(),
            F.lit("NEW")
        )
        .when(
            ~(
                F.col("s.product_id").eqNullSafe(F.col("t.product_id"))
                & F.col("s.seller_sku").eqNullSafe(F.col("t.seller_sku"))
                & F.col("s.product_name").eqNullSafe(F.col("t.product_name"))
                & F.col("s.variation").eqNullSafe(F.col("t.variation"))
                & F.col("s.product_category").eqNullSafe(F.col("t.product_category"))
                & F.col("s.status").eqNullSafe(F.col("t.status"))
                & F.col("s.is_historical").eqNullSafe(F.col("t.is_historical"))
                & F.col("s.product_mapping_status").eqNullSafe(
                    F.col("t.product_mapping_status")
                )
            ),
            F.lit("CHANGED")
        )
        .otherwise(F.lit("NO_CHANGE"))
    )
)

display(
    classified_df.select(
        F.col("s.batch_id").alias("batch_id"),
        F.col("s.sku_id").alias("sku_id"),
        F.col("t.status").alias("target_status"),
        F.col("s.status").alias("source_status"),
        "change_type",
        F.col("s.change_timestamp").alias("change_timestamp")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

classified_table = "workspace.gold.dim_sku_scd2_classified_batch_sandbox"

classified_persist_df = (
    classified_df
    .select(
        F.col("s.batch_id").alias("batch_id"),
        F.col("s.sku_id").alias("sku_id"),
        F.col("s.product_id").alias("product_id"),
        F.col("s.seller_sku").alias("seller_sku"),
        F.col("s.product_name").alias("product_name"),
        F.col("s.variation").alias("variation"),
        F.col("s.product_category").alias("product_category"),
        F.col("s.status").alias("status"),
        F.col("s.is_historical").alias("is_historical"),
        F.col("s.product_mapping_status").alias("product_mapping_status"),
        F.col("s.change_timestamp").alias("change_timestamp"),
        F.col("change_type")
    )
)

(
    classified_persist_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(classified_table)
)

persistent_classified_df = spark.table(classified_table)

print(
    "classified rows:",
    persistent_classified_df.count()
)

print(
    "changed rows:",
    persistent_classified_df
    .filter(F.col("change_type") == "CHANGED")
    .count()
)

display(
    persistent_classified_df.select(
        "batch_id",
        "sku_id",
        "status",
        "change_type",
        "change_timestamp"
    )
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
classified_table = "workspace.gold.dim_sku_scd2_classified_batch_sandbox"

delta_target = DeltaTable.forName(spark, target_table)

changed_batch_df = (
    spark.table(classified_table)
    .filter(F.col("change_type") == "CHANGED")
    .select(
        "sku_id",
        "change_timestamp"
    )
)

(
    delta_target.alias("t")
    .merge(
        changed_batch_df.alias("s"),
        """
        t.sku_id = s.sku_id
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "valid_to": "s.change_timestamp",
            "is_current": "false"
        }
    )
    .execute()
)

display(
    spark.table(target_table)
    .filter(F.col("sku_id") == "1732698562646017207")
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
classified_table = "workspace.gold.dim_sku_scd2_classified_batch_sandbox"

new_current_df = (
    spark.table(classified_table)
    .filter(F.col("change_type") == "CHANGED")
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        F.col("change_timestamp").alias("valid_from")
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

(
    new_current_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

display(
    spark.table(target_table)
    .filter(F.col("sku_id") == "1732698562646017207")
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

incoming_table = "workspace.gold.dim_sku_scd2_incoming_batch_sandbox"
target_table = "workspace.gold.dim_sku_scd2_sandbox"

source_df = (
    spark.table(incoming_table)
    .alias("s")
)

current_target_df = (
    spark.table(target_table)
    .filter(F.col("is_current") == True)
    .alias("t")
)

rerun_classified_df = (
    source_df
    .join(
        current_target_df,
        F.col("s.sku_id") == F.col("t.sku_id"),
        "left"
    )
    .withColumn(
        "change_type",
        F.when(
            F.col("t.sku_id").isNull(),
            F.lit("NEW")
        )
        .when(
            ~(
                F.col("s.product_id").eqNullSafe(F.col("t.product_id"))
                & F.col("s.seller_sku").eqNullSafe(F.col("t.seller_sku"))
                & F.col("s.product_name").eqNullSafe(F.col("t.product_name"))
                & F.col("s.variation").eqNullSafe(F.col("t.variation"))
                & F.col("s.product_category").eqNullSafe(F.col("t.product_category"))
                & F.col("s.status").eqNullSafe(F.col("t.status"))
                & F.col("s.is_historical").eqNullSafe(F.col("t.is_historical"))
                & F.col("s.product_mapping_status").eqNullSafe(
                    F.col("t.product_mapping_status")
                )
            ),
            F.lit("CHANGED")
        )
        .otherwise(F.lit("NO_CHANGE"))
    )
)

display(
    rerun_classified_df.select(
        F.col("s.batch_id").alias("batch_id"),
        F.col("s.sku_id").alias("sku_id"),
        F.col("t.status").alias("target_status"),
        F.col("s.status").alias("source_status"),
        "change_type"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
target_sku_id = "1732698562646017207"

target_sku_df = (
    spark.table(target_table)
    .filter(F.col("sku_id") == target_sku_id)
)

print(
    "total versions:",
    target_sku_df.count()
)

print(
    "current versions:",
    target_sku_df
    .filter(F.col("is_current") == True)
    .count()
)

print(
    "historical versions:",
    target_sku_df
    .filter(F.col("is_current") == False)
    .count()
)

display(
    target_sku_df
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
previous_test_sku = "1732698562646017207"

candidate_skus_df = (
    spark.table(target_table)
    .filter(
        (F.col("is_current") == True)
        & (F.col("sku_id") != previous_test_sku)
    )
    .select(
        "sku_id",
        "seller_sku",
        "product_id",
        "status",
        "product_name"
    )
    .orderBy("sku_id")
    .limit(3)
)

display(candidate_skus_df)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.dim_sku"
mixed_incoming_table = "workspace.gold.dim_sku_scd2_mixed_incoming_sandbox"

batch_id = "scd2_batch_002"
change_timestamp = "2026-08-17 02:10:00"

# 1) NO_CHANGE
no_change_df = (
    spark.table(source_table)
    .filter(F.col("seller_sku") == "DO-0003")
)

# 2) CHANGED: Active -> Inactive
changed_df = (
    spark.table(source_table)
    .filter(F.col("seller_sku") == "CA-0001")
    .withColumn("status", F.lit("Inactive"))
)

# 3) NEW SKU
new_df = (
    spark.table(source_table)
    .filter(F.col("seller_sku") == "CA-0002")
    .withColumn("sku_id", F.lit("SCD2_TEST_NEW_001"))
    .withColumn("seller_sku", F.lit("SCD2-NEW-001"))
)

mixed_batch_df = (
    no_change_df
    .unionByName(changed_df)
    .unionByName(new_df)
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn(
        "change_timestamp",
        F.to_timestamp(F.lit(change_timestamp))
    )
)

(
    mixed_batch_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(mixed_incoming_table)
)

persistent_mixed_df = spark.table(mixed_incoming_table)

print(
    "mixed batch rows:",
    persistent_mixed_df.count()
)

display(
    persistent_mixed_df.select(
        "batch_id",
        "sku_id",
        "seller_sku",
        "status",
        "change_timestamp"
    )
    .orderBy("seller_sku")
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.dim_sku"

display(
    spark.table(source_table)
    .filter(F.col("seller_sku") == "DO-0003")
    .select(
        "sku_id",
        "seller_sku",
        "product_id",
        "status",
        "product_name",
        "is_historical",
        "product_mapping_status"
    )
    .orderBy("sku_id")
)

print(
    "DO-0003 row count:",
    spark.table(source_table)
    .filter(F.col("seller_sku") == "DO-0003")
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.dim_sku"
mixed_incoming_table = "workspace.gold.dim_sku_scd2_mixed_incoming_sandbox"

batch_id = "scd2_batch_002"
change_timestamp = "2026-08-17 02:10:00"

# 1) NO_CHANGE
# seller_sku = DO-0003 มีหลาย sku_id
# จึงเลือก 1 sku_id แบบ deterministic
no_change_df = (
    spark.table(source_table)
    .filter(F.col("seller_sku") == "DO-0003")
    .orderBy("sku_id")
    .limit(1)
)

# 2) CHANGED
changed_df = (
    spark.table(source_table)
    .filter(F.col("seller_sku") == "CA-0001")
    .orderBy("sku_id")
    .limit(1)
    .withColumn("status", F.lit("Inactive"))
)

# 3) NEW
new_df = (
    spark.table(source_table)
    .filter(F.col("seller_sku") == "CA-0002")
    .orderBy("sku_id")
    .limit(1)
    .withColumn("sku_id", F.lit("SCD2_TEST_NEW_001"))
    .withColumn("seller_sku", F.lit("SCD2-NEW-001"))
)

mixed_batch_df = (
    no_change_df
    .unionByName(changed_df)
    .unionByName(new_df)
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn(
        "change_timestamp",
        F.to_timestamp(F.lit(change_timestamp))
    )
)

(
    mixed_batch_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(mixed_incoming_table)
)

persistent_mixed_df = spark.table(mixed_incoming_table)

print(
    "mixed batch rows:",
    persistent_mixed_df.count()
)

display(
    persistent_mixed_df.select(
        "batch_id",
        "sku_id",
        "seller_sku",
        "status",
        "change_timestamp"
    )
    .orderBy("seller_sku")
)

# COMMAND ----------

from pyspark.sql import functions as F

mixed_incoming_table = "workspace.gold.dim_sku_scd2_mixed_incoming_sandbox"
target_table = "workspace.gold.dim_sku_scd2_sandbox"

source_df = (
    spark.table(mixed_incoming_table)
    .alias("s")
)

current_target_df = (
    spark.table(target_table)
    .filter(F.col("is_current") == True)
    .alias("t")
)

mixed_classified_df = (
    source_df
    .join(
        current_target_df,
        F.col("s.sku_id") == F.col("t.sku_id"),
        "left"
    )
    .withColumn(
        "change_type",
        F.when(
            F.col("t.sku_id").isNull(),
            F.lit("NEW")
        )
        .when(
            ~(
                F.col("s.product_id").eqNullSafe(F.col("t.product_id"))
                & F.col("s.seller_sku").eqNullSafe(F.col("t.seller_sku"))
                & F.col("s.product_name").eqNullSafe(F.col("t.product_name"))
                & F.col("s.variation").eqNullSafe(F.col("t.variation"))
                & F.col("s.product_category").eqNullSafe(F.col("t.product_category"))
                & F.col("s.status").eqNullSafe(F.col("t.status"))
                & F.col("s.is_historical").eqNullSafe(F.col("t.is_historical"))
                & F.col("s.product_mapping_status").eqNullSafe(
                    F.col("t.product_mapping_status")
                )
            ),
            F.lit("CHANGED")
        )
        .otherwise(F.lit("NO_CHANGE"))
    )
)

display(
    mixed_classified_df.select(
        F.col("s.batch_id").alias("batch_id"),
        F.col("s.sku_id").alias("sku_id"),
        F.col("s.seller_sku").alias("seller_sku"),
        F.col("t.status").alias("target_status"),
        F.col("s.status").alias("source_status"),
        "change_type",
        F.col("s.change_timestamp").alias("change_timestamp")
    )
    .orderBy("seller_sku")
)

# COMMAND ----------

from pyspark.sql import functions as F

mixed_classified_table = "workspace.gold.dim_sku_scd2_mixed_classified_sandbox"

mixed_classified_persist_df = (
    mixed_classified_df
    .select(
        F.col("s.batch_id").alias("batch_id"),
        F.col("s.sku_id").alias("sku_id"),
        F.col("s.product_id").alias("product_id"),
        F.col("s.seller_sku").alias("seller_sku"),
        F.col("s.product_name").alias("product_name"),
        F.col("s.variation").alias("variation"),
        F.col("s.product_category").alias("product_category"),
        F.col("s.status").alias("status"),
        F.col("s.is_historical").alias("is_historical"),
        F.col("s.product_mapping_status").alias("product_mapping_status"),
        F.col("s.change_timestamp").alias("change_timestamp"),
        F.col("change_type")
    )
)

(
    mixed_classified_persist_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(mixed_classified_table)
)

persistent_mixed_classified_df = spark.table(mixed_classified_table)

print("classified rows:", persistent_mixed_classified_df.count())

display(
    persistent_mixed_classified_df
    .groupBy("change_type")
    .count()
    .orderBy("change_type")
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
mixed_classified_table = "workspace.gold.dim_sku_scd2_mixed_classified_sandbox"

delta_target = DeltaTable.forName(spark, target_table)

changed_rows_df = (
    spark.table(mixed_classified_table)
    .filter(F.col("change_type") == "CHANGED")
    .select(
        "sku_id",
        "change_timestamp"
    )
)

(
    delta_target.alias("t")
    .merge(
        changed_rows_df.alias("s"),
        """
        t.sku_id = s.sku_id
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "valid_to": "s.change_timestamp",
            "is_current": "false"
        }
    )
    .execute()
)

display(
    spark.table(target_table)
    .filter(F.col("seller_sku") == "CA-0001")
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
mixed_classified_table = "workspace.gold.dim_sku_scd2_mixed_classified_sandbox"

rows_to_insert_df = (
    spark.table(mixed_classified_table)
    .filter(
        F.col("change_type").isin("CHANGED", "NEW")
    )
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        F.col("change_timestamp").alias("valid_from")
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

print(
    "rows to insert:",
    rows_to_insert_df.count()
)

(
    rows_to_insert_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

display(
    spark.table(target_table)
    .filter(
        F.col("seller_sku").isin(
            "CA-0001",
            "DO-0003",
            "SCD2-NEW-001"
        )
    )
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy(
        "seller_sku",
        "valid_from"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
classified_table = "workspace.gold.dim_sku_scd2_mixed_classified_sandbox"

batch_keys_df = (
    spark.table(classified_table)
    .select(
        "sku_id",
        "seller_sku",
        "change_type"
    )
)

target_versions_df = (
    spark.table(target_table)
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("total_versions"),

        F.sum(
            F.when(F.col("is_current") == True, 1).otherwise(0)
        ).alias("current_versions"),

        F.sum(
            F.when(F.col("is_current") == False, 1).otherwise(0)
        ).alias("historical_versions")
    )
)

validation_df = (
    batch_keys_df
    .join(
        target_versions_df,
        on="sku_id",
        how="left"
    )
    .withColumn(
        "validation_status",
        F.when(
            (F.col("change_type") == "CHANGED")
            & (F.col("total_versions") == 2)
            & (F.col("current_versions") == 1)
            & (F.col("historical_versions") == 1),
            F.lit("PASS")
        )
        .when(
            (F.col("change_type") == "NEW")
            & (F.col("total_versions") == 1)
            & (F.col("current_versions") == 1)
            & (F.col("historical_versions") == 0),
            F.lit("PASS")
        )
        .when(
            (F.col("change_type") == "NO_CHANGE")
            & (F.col("total_versions") == 1)
            & (F.col("current_versions") == 1)
            & (F.col("historical_versions") == 0),
            F.lit("PASS")
        )
        .otherwise(F.lit("FAIL"))
    )
)

display(
    validation_df
    .select(
        "sku_id",
        "seller_sku",
        "change_type",
        "total_versions",
        "current_versions",
        "historical_versions",
        "validation_status"
    )
    .orderBy("change_type")
)

print(
    "failed validations:",
    validation_df
    .filter(F.col("validation_status") == "FAIL")
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

mixed_incoming_table = "workspace.gold.dim_sku_scd2_mixed_incoming_sandbox"
target_table = "workspace.gold.dim_sku_scd2_sandbox"

source_df = (
    spark.table(mixed_incoming_table)
    .alias("s")
)

current_target_df = (
    spark.table(target_table)
    .filter(F.col("is_current") == True)
    .alias("t")
)

rerun_mixed_classified_df = (
    source_df
    .join(
        current_target_df,
        F.col("s.sku_id") == F.col("t.sku_id"),
        "left"
    )
    .withColumn(
        "change_type",
        F.when(
            F.col("t.sku_id").isNull(),
            F.lit("NEW")
        )
        .when(
            ~(
                F.col("s.product_id").eqNullSafe(F.col("t.product_id"))
                & F.col("s.seller_sku").eqNullSafe(F.col("t.seller_sku"))
                & F.col("s.product_name").eqNullSafe(F.col("t.product_name"))
                & F.col("s.variation").eqNullSafe(F.col("t.variation"))
                & F.col("s.product_category").eqNullSafe(F.col("t.product_category"))
                & F.col("s.status").eqNullSafe(F.col("t.status"))
                & F.col("s.is_historical").eqNullSafe(F.col("t.is_historical"))
                & F.col("s.product_mapping_status").eqNullSafe(
                    F.col("t.product_mapping_status")
                )
            ),
            F.lit("CHANGED")
        )
        .otherwise(F.lit("NO_CHANGE"))
    )
)

display(
    rerun_mixed_classified_df.select(
        F.col("s.batch_id").alias("batch_id"),
        F.col("s.sku_id").alias("sku_id"),
        F.col("s.seller_sku").alias("seller_sku"),
        F.col("t.status").alias("target_status"),
        F.col("s.status").alias("source_status"),
        "change_type"
    )
    .orderBy("seller_sku")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
classified_table = "workspace.gold.dim_sku_scd2_mixed_classified_sandbox"

batch_keys_df = (
    spark.table(classified_table)
    .select(
        "sku_id",
        "seller_sku",
        "change_type"
    )
)

target_versions_df = (
    spark.table(target_table)
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("total_versions"),
        F.sum(
            F.when(F.col("is_current") == True, 1).otherwise(0)
        ).alias("current_versions"),
        F.sum(
            F.when(F.col("is_current") == False, 1).otherwise(0)
        ).alias("historical_versions")
    )
)

rerun_validation_df = (
    batch_keys_df
    .join(
        target_versions_df,
        on="sku_id",
        how="left"
    )
)

display(
    rerun_validation_df
    .select(
        "sku_id",
        "seller_sku",
        "change_type",
        "total_versions",
        "current_versions",
        "historical_versions"
    )
    .orderBy("seller_sku")
)

print(
    "invalid current-key count:",
    rerun_validation_df
    .filter(F.col("current_versions") != 1)
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
late_event_table = "workspace.gold.dim_sku_scd2_late_event_sandbox"

late_event_sku = (
    spark.table(target_table)
    .filter(
        (F.col("seller_sku") == "CA-0001")
        & (F.col("is_current") == True)
    )
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status"
    )
    .limit(1)
)

late_event_df = (
    late_event_sku
    .withColumn("status", F.lit("Suspended"))
    .withColumn("batch_id", F.lit("scd2_late_batch_001"))
    .withColumn(
        "event_timestamp",
        F.to_timestamp(F.lit("2026-08-17 01:50:00"))
    )
    .withColumn(
        "ingestion_timestamp",
        F.to_timestamp(F.lit("2026-08-17 09:30:00"))
    )
)

(
    late_event_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(late_event_table)
)

persistent_late_event_df = spark.table(late_event_table)

print(
    "late event rows:",
    persistent_late_event_df.count()
)

display(
    persistent_late_event_df.select(
        "batch_id",
        "sku_id",
        "seller_sku",
        "status",
        "event_timestamp",
        "ingestion_timestamp"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
late_event_table = "workspace.gold.dim_sku_scd2_late_event_sandbox"

late_df = (
    spark.table(late_event_table)
    .alias("e")
)

history_df = (
    spark.table(target_table)
    .alias("d")
)

effective_match_df = (
    late_df
    .join(
        history_df,
        (
            (F.col("e.sku_id") == F.col("d.sku_id"))
            & (F.col("e.event_timestamp") >= F.col("d.valid_from"))
            & (
                F.col("d.valid_to").isNull()
                | (F.col("e.event_timestamp") < F.col("d.valid_to"))
            )
        ),
        "left"
    )
)

display(
    effective_match_df.select(
        F.col("e.sku_id").alias("sku_id"),
        F.col("e.status").alias("incoming_status"),
        F.col("e.event_timestamp").alias("event_timestamp"),
        F.col("d.status").alias("matched_status"),
        F.col("d.valid_from").alias("matched_valid_from"),
        F.col("d.valid_to").alias("matched_valid_to"),
        F.col("d.is_current").alias("matched_is_current")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

late_match_table = "workspace.gold.dim_sku_scd2_late_match_sandbox"

late_match_persist_df = (
    effective_match_df
    .select(
        F.col("e.batch_id").alias("batch_id"),
        F.col("e.sku_id").alias("sku_id"),

        F.col("e.product_id").alias("product_id"),
        F.col("e.seller_sku").alias("seller_sku"),
        F.col("e.product_name").alias("product_name"),
        F.col("e.variation").alias("variation"),
        F.col("e.product_category").alias("product_category"),
        F.col("e.status").alias("incoming_status"),
        F.col("e.is_historical").alias("is_historical"),
        F.col("e.product_mapping_status").alias("product_mapping_status"),

        F.col("e.event_timestamp").alias("event_timestamp"),
        F.col("e.ingestion_timestamp").alias("ingestion_timestamp"),

        F.col("d.status").alias("matched_status"),
        F.col("d.valid_from").alias("matched_valid_from"),
        F.col("d.valid_to").alias("matched_valid_to"),
        F.col("d.is_current").alias("matched_is_current")
    )
)

(
    late_match_persist_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(late_match_table)
)

persistent_late_match_df = spark.table(late_match_table)

print(
    "late match rows:",
    persistent_late_match_df.count()
)

display(
    persistent_late_match_df.select(
        "sku_id",
        "incoming_status",
        "event_timestamp",
        "matched_status",
        "matched_valid_from",
        "matched_valid_to",
        "matched_is_current"
    )
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
late_match_table = "workspace.gold.dim_sku_scd2_late_match_sandbox"

delta_target = DeltaTable.forName(spark, target_table)

interval_to_shorten_df = (
    spark.table(late_match_table)
    .select(
        "sku_id",
        "matched_valid_from",
        "event_timestamp"
    )
)

(
    delta_target.alias("t")
    .merge(
        interval_to_shorten_df.alias("s"),
        """
        t.sku_id = s.sku_id
        AND t.valid_from = s.matched_valid_from
        AND t.is_current = false
        """
    )
    .whenMatchedUpdate(
        set={
            "valid_to": "s.event_timestamp"
        }
    )
    .execute()
)

display(
    spark.table(target_table)
    .filter(F.col("seller_sku") == "CA-0001")
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
late_match_table = "workspace.gold.dim_sku_scd2_late_match_sandbox"

late_history_df = (
    spark.table(late_match_table)
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        F.col("incoming_status").alias("status"),
        "is_historical",
        "product_mapping_status",
        F.col("event_timestamp").alias("valid_from"),
        F.col("matched_valid_to").alias("valid_to")
    )
    .withColumn(
        "is_current",
        F.lit(False)
    )
)

print(
    "late history rows to insert:",
    late_history_df.count()
)

(
    late_history_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

display(
    spark.table(target_table)
    .filter(F.col("seller_sku") == "CA-0001")
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

target_table = "workspace.gold.dim_sku_scd2_sandbox"
target_seller_sku = "CA-0001"

sku_history_df = (
    spark.table(target_table)
    .filter(F.col("seller_sku") == target_seller_sku)
)

window_spec = (
    Window
    .partitionBy("sku_id")
    .orderBy("valid_from")
)

timeline_check_df = (
    sku_history_df
    .withColumn(
        "next_valid_from",
        F.lead("valid_from").over(window_spec)
    )
)

gap_df = (
    timeline_check_df
    .filter(
        F.col("valid_to").isNotNull()
        & F.col("next_valid_from").isNotNull()
        & (F.col("valid_to") < F.col("next_valid_from"))
    )
)

overlap_df = (
    timeline_check_df
    .filter(
        F.col("valid_to").isNotNull()
        & F.col("next_valid_from").isNotNull()
        & (F.col("valid_to") > F.col("next_valid_from"))
    )
)

current_count = (
    sku_history_df
    .filter(F.col("is_current") == True)
    .count()
)

print("gap count:", gap_df.count())
print("overlap count:", overlap_df.count())
print("current row count:", current_count)

display(
    timeline_check_df.select(
        "sku_id",
        "status",
        "valid_from",
        "valid_to",
        "next_valid_from",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"
late_event_table = "workspace.gold.dim_sku_scd2_late_event_sandbox"

late_df = (
    spark.table(late_event_table)
    .alias("e")
)

history_df = (
    spark.table(target_table)
    .alias("d")
)

rerun_late_match_df = (
    late_df
    .join(
        history_df,
        (
            (F.col("e.sku_id") == F.col("d.sku_id"))
            & (F.col("e.event_timestamp") >= F.col("d.valid_from"))
            & (
                F.col("d.valid_to").isNull()
                | (F.col("e.event_timestamp") < F.col("d.valid_to"))
            )
        ),
        "left"
    )
)

display(
    rerun_late_match_df.select(
        F.col("e.sku_id").alias("sku_id"),
        F.col("e.status").alias("incoming_status"),
        F.col("e.event_timestamp").alias("event_timestamp"),
        F.col("d.status").alias("matched_status"),
        F.col("d.valid_from").alias("matched_valid_from"),
        F.col("d.valid_to").alias("matched_valid_to"),
        F.col("d.is_current").alias("matched_is_current")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

late_rerun_classified_df = (
    rerun_late_match_df
    .withColumn(
        "late_event_action",
        F.when(
            F.col("e.status").eqNullSafe(F.col("d.status")),
            F.lit("NO_CHANGE")
        )
        .otherwise(
            F.lit("SPLIT_REQUIRED")
        )
    )
)

display(
    late_rerun_classified_df.select(
        F.col("e.batch_id").alias("batch_id"),
        F.col("e.sku_id").alias("sku_id"),
        F.col("e.status").alias("incoming_status"),
        F.col("d.status").alias("matched_status"),
        F.col("e.event_timestamp").alias("event_timestamp"),
        F.col("d.valid_from").alias("matched_valid_from"),
        F.col("d.valid_to").alias("matched_valid_to"),
        "late_event_action"
    )
)

# COMMAND ----------

display(
    late_rerun_classified_df.select(
        "late_event_action"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "workspace.gold.dim_sku_scd2_sandbox"

target_sku_id = (
    spark.table("workspace.gold.dim_sku_scd2_late_event_sandbox")
    .select("sku_id")
    .first()["sku_id"]
)

target_history_df = (
    spark.table(target_table)
    .filter(F.col("sku_id") == target_sku_id)
)

print(
    "total versions:",
    target_history_df.count()
)

print(
    "current versions:",
    target_history_df
    .filter(F.col("is_current") == True)
    .count()
)

print(
    "historical versions:",
    target_history_df
    .filter(F.col("is_current") == False)
    .count()
)

print(
    "suspended versions:",
    target_history_df
    .filter(F.col("status") == "Suspended")
    .count()
)

display(
    target_history_df
    .select(
        "sku_id",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.dim_sku_scd2_sandbox"
recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"

recovery_df = spark.table(source_table)

(
    recovery_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(recovery_table)
)

print(
    "recovery sandbox rows:",
    spark.table(recovery_table).count()
)

print(
    "recovery current rows:",
    spark.table(recovery_table)
    .filter(F.col("is_current") == True)
    .count()
)

print(
    "recovery historical rows:",
    spark.table(recovery_table)
    .filter(F.col("is_current") == False)
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
failure_test_sku = "SCD2_TEST_NEW_001"

display(
    spark.table(recovery_table)
    .filter(F.col("sku_id") == failure_test_sku)
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

print(
    "test SKU row count:",
    spark.table(recovery_table)
    .filter(F.col("sku_id") == failure_test_sku)
    .count()
)

print(
    "test SKU current rows:",
    spark.table(recovery_table)
    .filter(
        (F.col("sku_id") == failure_test_sku)
        & (F.col("is_current") == True)
    )
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

failure_test_sku = "SCD2_TEST_NEW_001"

recovery_batch_df = (
    spark.table(recovery_table)
    .filter(
        (F.col("sku_id") == failure_test_sku)
        & (F.col("is_current") == True)
    )
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status"
    )
    .withColumn("status", F.lit("Inactive"))
    .withColumn("batch_id", F.lit("recovery_batch_001"))
    .withColumn(
        "change_timestamp",
        F.to_timestamp(F.lit("2026-08-17 03:00:00"))
    )
)

(
    recovery_batch_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(recovery_batch_table)
)

persistent_recovery_batch_df = spark.table(recovery_batch_table)

print(
    "recovery batch rows:",
    persistent_recovery_batch_df.count()
)

display(
    persistent_recovery_batch_df.select(
        "batch_id",
        "sku_id",
        "seller_sku",
        "status",
        "change_timestamp"
    )
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"
failure_test_sku = "SCD2_TEST_NEW_001"

delta_recovery = DeltaTable.forName(spark, recovery_table)

expire_source_df = (
    spark.table(recovery_batch_table)
    .select(
        "sku_id",
        "change_timestamp"
    )
)

(
    delta_recovery.alias("t")
    .merge(
        expire_source_df.alias("s"),
        """
        t.sku_id = s.sku_id
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "valid_to": "s.change_timestamp",
            "is_current": "false"
        }
    )
    .execute()
)

print(
    "current rows after expire:",
    spark.table(recovery_table)
    .filter(
        (F.col("sku_id") == failure_test_sku)
        & (F.col("is_current") == True)
    )
    .count()
)

display(
    spark.table(recovery_table)
    .filter(F.col("sku_id") == failure_test_sku)
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

batch_keys_df = (
    spark.table(recovery_batch_table)
    .select("sku_id")
    .distinct()
)

current_counts_df = (
    spark.table(recovery_table)
    .groupBy("sku_id")
    .agg(
        F.sum(
            F.when(F.col("is_current") == True, 1).otherwise(0)
        ).alias("current_row_count")
    )
)

broken_keys_df = (
    batch_keys_df
    .join(
        current_counts_df,
        on="sku_id",
        how="left"
    )
    .fillna({"current_row_count": 0})
    .withColumn(
        "recovery_status",
        F.when(
            F.col("current_row_count") == 0,
            F.lit("MISSING_CURRENT_ROW")
        )
        .when(
            F.col("current_row_count") > 1,
            F.lit("MULTIPLE_CURRENT_ROWS")
        )
        .otherwise(
            F.lit("HEALTHY")
        )
    )
)

display(broken_keys_df)

print(
    "broken key count:",
    broken_keys_df
    .filter(F.col("recovery_status") != "HEALTHY")
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

replay_insert_df = (
    spark.table(recovery_batch_table)
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        F.col("change_timestamp").alias("valid_from")
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

print(
    "replay rows to insert:",
    replay_insert_df.count()
)

(
    replay_insert_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(recovery_table)
)

display(
    spark.table(recovery_table)
    .filter(F.col("sku_id") == "SCD2_TEST_NEW_001")
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
failure_test_sku = "SCD2_TEST_NEW_001"

recovered_df = (
    spark.table(recovery_table)
    .filter(F.col("sku_id") == failure_test_sku)
)

print(
    "total versions:",
    recovered_df.count()
)

print(
    "current versions:",
    recovered_df
    .filter(F.col("is_current") == True)
    .count()
)

print(
    "historical versions:",
    recovered_df
    .filter(F.col("is_current") == False)
    .count()
)

display(
    recovered_df
    .select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

naive_replay_df = (
    spark.table(recovery_batch_table)
    .select(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        F.col("change_timestamp").alias("valid_from")
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

(
    naive_replay_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(recovery_table)
)

target_df = (
    spark.table(recovery_table)
    .filter(F.col("sku_id") == "SCD2_TEST_NEW_001")
)

print("total versions after naive replay:", target_df.count())

print(
    "current versions after naive replay:",
    target_df
    .filter(F.col("is_current") == True)
    .count()
)

display(
    target_df
    .select(
        "sku_id",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

batch_df = spark.table(recovery_batch_table).alias("b")
target_df = spark.table(recovery_table).alias("t")

current_state_df = (
    target_df
    .filter(F.col("t.is_current") == True)
    .groupBy("t.sku_id")
    .agg(
        F.count("*").alias("current_row_count")
    )
)

applied_version_df = (
    target_df
    .groupBy("t.sku_id")
    .agg(
        F.sum(
            F.when(
                (F.col("t.status") == F.lit("Inactive"))
                & (F.col("t.valid_from") == F.to_timestamp(F.lit("2026-08-17 03:00:00")))
                & (F.col("t.is_current") == True),
                1
            ).otherwise(0)
        ).alias("matching_applied_versions")
    )
)

replay_guard_df = (
    batch_df
    .join(
        current_state_df,
        F.col("b.sku_id") == F.col("current_state_df.sku_id"),
        "left"
    )
    .drop(current_state_df.sku_id)
    .join(
        applied_version_df,
        F.col("b.sku_id") == F.col("applied_version_df.sku_id"),
        "left"
    )
    .drop(applied_version_df.sku_id)
    .fillna({
        "current_row_count": 0,
        "matching_applied_versions": 0
    })
    .withColumn(
        "replay_action",
        F.when(
            F.col("current_row_count") > 1,
            F.lit("BROKEN_DUPLICATE_CURRENT")
        )
        .when(
            F.col("matching_applied_versions") >= 1,
            F.lit("ALREADY_APPLIED")
        )
        .otherwise(
            F.lit("REPLAY_REQUIRED")
        )
    )
)

display(
    replay_guard_df.select(
        "batch_id",
        "sku_id",
        "status",
        "change_timestamp",
        "current_row_count",
        "matching_applied_versions",
        "replay_action"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

batch_df = (
    spark.table(recovery_batch_table)
    .select(
        "batch_id",
        "sku_id",
        "status",
        "change_timestamp"
    )
    .alias("b")
)

current_state_df = (
    spark.table(recovery_table)
    .filter(F.col("is_current") == True)
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("current_row_count")
    )
    .alias("c")
)

applied_version_df = (
    spark.table(recovery_table)
    .alias("t")
    .join(
        spark.table(recovery_batch_table)
        .select(
            "sku_id",
            "status",
            "change_timestamp"
        )
        .alias("rb"),
        (
            (F.col("t.sku_id") == F.col("rb.sku_id"))
            & F.col("t.status").eqNullSafe(F.col("rb.status"))
            & (F.col("t.valid_from") == F.col("rb.change_timestamp"))
            & (F.col("t.is_current") == True)
        ),
        "inner"
    )
    .groupBy(F.col("t.sku_id").alias("sku_id"))
    .agg(
        F.count("*").alias("matching_applied_versions")
    )
    .alias("a")
)

replay_guard_df = (
    batch_df
    .join(
        current_state_df,
        F.col("b.sku_id") == F.col("c.sku_id"),
        "left"
    )
    .join(
        applied_version_df,
        F.col("b.sku_id") == F.col("a.sku_id"),
        "left"
    )
    .select(
        F.col("b.batch_id").alias("batch_id"),
        F.col("b.sku_id").alias("sku_id"),
        F.col("b.status").alias("status"),
        F.col("b.change_timestamp").alias("change_timestamp"),
        F.coalesce(
            F.col("c.current_row_count"),
            F.lit(0)
        ).alias("current_row_count"),
        F.coalesce(
            F.col("a.matching_applied_versions"),
            F.lit(0)
        ).alias("matching_applied_versions")
    )
    .withColumn(
        "replay_action",
        F.when(
            F.col("current_row_count") > 1,
            F.lit("BROKEN_DUPLICATE_CURRENT")
        )
        .when(
            F.col("matching_applied_versions") >= 1,
            F.lit("ALREADY_APPLIED")
        )
        .otherwise(
            F.lit("REPLAY_REQUIRED")
        )
    )
)

display(replay_guard_df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
failure_test_sku = "SCD2_TEST_NEW_001"

current_dupes_df = (
    spark.table(recovery_table)
    .filter(
        (F.col("sku_id") == failure_test_sku)
        & (F.col("is_current") == True)
    )
)

window_spec = (
    Window
    .partitionBy("sku_id")
    .orderBy(
        F.col("valid_from").desc(),
        F.col("status").asc()
    )
)

ranked_current_df = (
    current_dupes_df
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
)

display(
    ranked_current_df.select(
        "sku_id",
        "status",
        "valid_from",
        "valid_to",
        "is_current",
        "rn"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
failure_test_sku = "SCD2_TEST_NEW_001"

duplicate_signature_df = (
    spark.table(recovery_table)
    .filter(F.col("sku_id") == failure_test_sku)
    .groupBy(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .agg(
        F.count("*").alias("duplicate_count")
    )
    .filter(F.col("duplicate_count") > 1)
)

print(
    "duplicate version groups:",
    duplicate_signature_df.count()
)

display(
    duplicate_signature_df.select(
        "sku_id",
        "seller_sku",
        "status",
        "valid_from",
        "valid_to",
        "is_current",
        "duplicate_count"
    )
)


# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"

before_count = spark.table(recovery_table).count()

deduplicated_df = (
    spark.table(recovery_table)
    .dropDuplicates([
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        "valid_from",
        "valid_to",
        "is_current"
    ])
)

after_dedup_count = deduplicated_df.count()

print("rows before repair:", before_count)
print("rows after dedup:", after_dedup_count)
print("duplicate rows removed:", before_count - after_dedup_count)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
staging_table = "workspace.gold.dim_sku_scd2_recovery_dedup_staging"

deduplicated_df = (
    spark.table(recovery_table)
    .dropDuplicates([
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        "valid_from",
        "valid_to",
        "is_current"
    ])
)

(
    deduplicated_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(staging_table)
)

print(
    "recovery rows:",
    spark.table(recovery_table).count()
)

print(
    "staging rows:",
    spark.table(staging_table).count()
)

print(
    "rows to remove:",
    spark.table(recovery_table).count()
    - spark.table(staging_table).count()
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
staging_table = "workspace.gold.dim_sku_scd2_recovery_dedup_staging"

staging_df = spark.table(staging_table)

(
    staging_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(recovery_table)
)

failure_test_sku = "SCD2_TEST_NEW_001"

repaired_df = (
    spark.table(recovery_table)
    .filter(F.col("sku_id") == failure_test_sku)
)

print(
    "total versions after repair:",
    repaired_df.count()
)

print(
    "current versions after repair:",
    repaired_df
    .filter(F.col("is_current") == True)
    .count()
)

print(
    "historical versions after repair:",
    repaired_df
    .filter(F.col("is_current") == False)
    .count()
)

display(
    repaired_df
    .select(
        "sku_id",
        "status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .orderBy("valid_from")
)

# COMMAND ----------

from pyspark.sql import functions as F

recovery_table = "workspace.gold.dim_sku_scd2_recovery_sandbox"
recovery_batch_table = "workspace.gold.dim_sku_scd2_recovery_batch_sandbox"

# -----------------------------------
# 1) ตรวจ exact duplicate หลัง repair
# -----------------------------------

duplicate_check_df = (
    spark.table(recovery_table)
    .groupBy(
        "sku_id",
        "product_id",
        "seller_sku",
        "product_name",
        "variation",
        "product_category",
        "status",
        "is_historical",
        "product_mapping_status",
        "valid_from",
        "valid_to",
        "is_current"
    )
    .agg(
        F.count("*").alias("duplicate_count")
    )
    .filter(F.col("duplicate_count") > 1)
)

print(
    "exact duplicate groups after repair:",
    duplicate_check_df.count()
)

# -----------------------------------
# 2) สร้าง replay guard ใหม่
# -----------------------------------

batch_df = (
    spark.table(recovery_batch_table)
    .select(
        "batch_id",
        "sku_id",
        "status",
        "change_timestamp"
    )
    .alias("b")
)

current_state_df = (
    spark.table(recovery_table)
    .filter(F.col("is_current") == True)
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("current_row_count")
    )
    .alias("c")
)

applied_version_df = (
    spark.table(recovery_table)
    .alias("t")
    .join(
        spark.table(recovery_batch_table)
        .select(
            "sku_id",
            "status",
            "change_timestamp"
        )
        .alias("rb"),
        (
            (F.col("t.sku_id") == F.col("rb.sku_id"))
            & F.col("t.status").eqNullSafe(F.col("rb.status"))
            & (F.col("t.valid_from") == F.col("rb.change_timestamp"))
            & (F.col("t.is_current") == True)
        ),
        "inner"
    )
    .groupBy(
        F.col("t.sku_id").alias("sku_id")
    )
    .agg(
        F.count("*").alias("matching_applied_versions")
    )
    .alias("a")
)

post_repair_guard_df = (
    batch_df
    .join(
        current_state_df,
        F.col("b.sku_id") == F.col("c.sku_id"),
        "left"
    )
    .join(
        applied_version_df,
        F.col("b.sku_id") == F.col("a.sku_id"),
        "left"
    )
    .select(
        F.col("b.batch_id").alias("batch_id"),
        F.col("b.sku_id").alias("sku_id"),
        F.col("b.status").alias("status"),
        F.col("b.change_timestamp").alias("change_timestamp"),
        F.coalesce(
            F.col("c.current_row_count"),
            F.lit(0)
        ).alias("current_row_count"),
        F.coalesce(
            F.col("a.matching_applied_versions"),
            F.lit(0)
        ).alias("matching_applied_versions")
    )
    .withColumn(
        "replay_action",
        F.when(
            F.col("current_row_count") > 1,
            F.lit("BROKEN_DUPLICATE_CURRENT")
        )
        .when(
            F.col("matching_applied_versions") >= 1,
            F.lit("ALREADY_APPLIED")
        )
        .otherwise(
            F.lit("REPLAY_REQUIRED")
        )
    )
)

display(post_repair_guard_df)

# COMMAND ----------

audit_table = "workspace.gold.pipeline_run_audit"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {audit_table} (
    run_id STRING,
    pipeline_name STRING,
    batch_id STRING,

    run_status STRING,
    failed_step STRING,
    error_message STRING,

    rows_read BIGINT,
    rows_written BIGINT,
    rows_rejected BIGINT,

    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds DOUBLE
)
USING DELTA
""")

print("audit table:", audit_table)

spark.table(audit_table).printSchema()

print(
    "current audit rows:",
    spark.table(audit_table).count()
)

# COMMAND ----------

import time
import uuid
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

audit_table = "workspace.gold.pipeline_run_audit"
source_table = "workspace.gold.fact_order_lines"

run_id = str(uuid.uuid4())
pipeline_name = "fact_order_lines_monitoring_demo"
batch_id = "monitoring_batch_001"

started_at = datetime.now()
start_perf = time.perf_counter()

# -----------------------------------
# 1) Insert RUNNING state
# -----------------------------------

running_row = Row(
    run_id=run_id,
    pipeline_name=pipeline_name,
    batch_id=batch_id,
    run_status="RUNNING",
    failed_step=None,
    error_message=None,
    rows_read=None,
    rows_written=None,
    rows_rejected=None,
    started_at=started_at,
    completed_at=None,
    duration_seconds=None
)

spark.createDataFrame(
    [running_row],
    schema=spark.table(audit_table).schema
).write.mode("append").saveAsTable(audit_table)

# -----------------------------------
# 2) Simulate pipeline workload
# -----------------------------------

rows_read = spark.table(source_table).count()

rows_written = rows_read
rows_rejected = 0

# -----------------------------------
# 3) Calculate completion metrics
# -----------------------------------

completed_at = datetime.now()
duration_seconds = time.perf_counter() - start_perf

# -----------------------------------
# 4) Update audit row to SUCCESS
# -----------------------------------

spark.sql(f"""
UPDATE {audit_table}
SET
    run_status = 'SUCCESS',
    rows_read = {rows_read},
    rows_written = {rows_written},
    rows_rejected = {rows_rejected},
    completed_at = TIMESTAMP('{completed_at.strftime("%Y-%m-%d %H:%M:%S.%f")}'),
    duration_seconds = {duration_seconds}
WHERE run_id = '{run_id}'
""")

print("run_id:", run_id)
print("rows_read:", rows_read)
print("rows_written:", rows_written)
print("rows_rejected:", rows_rejected)
print("duration_seconds:", round(duration_seconds, 3))

display(
    spark.table(audit_table)
    .filter(F.col("run_id") == run_id)
    .select(
        "run_id",
        "pipeline_name",
        "batch_id",
        "run_status",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "started_at",
        "completed_at",
        "duration_seconds"
    )
)

# COMMAND ----------

import time
import uuid
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

audit_table = "workspace.gold.pipeline_run_audit"

run_id = str(uuid.uuid4())
pipeline_name = "fact_order_lines_monitoring_demo"
batch_id = "monitoring_batch_002"

started_at = datetime.now()
start_perf = time.perf_counter()

running_row = Row(
    run_id=run_id,
    pipeline_name=pipeline_name,
    batch_id=batch_id,
    run_status="RUNNING",
    failed_step=None,
    error_message=None,
    rows_read=None,
    rows_written=None,
    rows_rejected=None,
    started_at=started_at,
    completed_at=None,
    duration_seconds=None
)

spark.createDataFrame(
    [running_row],
    schema=spark.table(audit_table).schema
).write.mode("append").saveAsTable(audit_table)

try:
    # ตั้งใจให้ fail
    rows_read = spark.table(
        "workspace.gold.table_that_does_not_exist"
    ).count()

except Exception as e:
    completed_at = datetime.now()
    duration_seconds = time.perf_counter() - start_perf

    error_message = str(e).replace("'", "''")[:2000]

    spark.sql(f"""
    UPDATE {audit_table}
    SET
        run_status = 'FAILED',
        failed_step = 'READ_SOURCE',
        error_message = '{error_message}',
        rows_read = 0,
        rows_written = 0,
        rows_rejected = 0,
        completed_at = TIMESTAMP('{completed_at.strftime("%Y-%m-%d %H:%M:%S.%f")}'),
        duration_seconds = {duration_seconds}
    WHERE run_id = '{run_id}'
    """)

print("run_id:", run_id)

display(
    spark.table(audit_table)
    .filter(F.col("run_id") == run_id)
    .select(
        "run_id",
        "batch_id",
        "run_status",
        "failed_step",
        "error_message",
        "rows_read",
        "rows_written",
        "completed_at",
        "duration_seconds"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit_table = "workspace.gold.pipeline_run_audit"
pipeline_name = "fact_order_lines_monitoring_demo"

audit_df = (
    spark.table(audit_table)
    .filter(F.col("pipeline_name") == pipeline_name)
)

latest_window = (
    Window
    .partitionBy("pipeline_name")
    .orderBy(F.col("started_at").desc())
)

latest_run_df = (
    audit_df
    .withColumn(
        "rn",
        F.row_number().over(latest_window)
    )
    .filter(F.col("rn") == 1)
)

summary_df = (
    audit_df
    .groupBy("pipeline_name")
    .agg(
        F.count("*").alias("total_runs"),

        F.sum(
            F.when(F.col("run_status") == "SUCCESS", 1).otherwise(0)
        ).alias("success_runs"),

        F.sum(
            F.when(F.col("run_status") == "FAILED", 1).otherwise(0)
        ).alias("failed_runs"),

        F.round(
            F.avg("duration_seconds"),
            3
        ).alias("avg_duration_seconds"),

        F.max("rows_read").alias("max_rows_read")
    )
)

print("LATEST RUN")

display(
    latest_run_df.select(
        "pipeline_name",
        "batch_id",
        "run_status",
        "failed_step",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "started_at",
        "completed_at",
        "duration_seconds"
    )
)

print("PIPELINE SUMMARY")

display(summary_df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit_table = "workspace.gold.pipeline_run_audit"
pipeline_name = "fact_order_lines_monitoring_demo"

runtime_threshold_seconds = 5.0

audit_df = (
    spark.table(audit_table)
    .filter(F.col("pipeline_name") == pipeline_name)
)

latest_window = (
    Window
    .partitionBy("pipeline_name")
    .orderBy(F.col("started_at").desc())
)

health_df = (
    audit_df
    .withColumn(
        "rn",
        F.row_number().over(latest_window)
    )
    .filter(F.col("rn") == 1)
    .withColumn(
        "health_status",
        F.when(
            F.col("run_status") == "FAILED",
            F.lit("ALERT_REQUIRED")
        )
        .when(
            (F.col("run_status") == "SUCCESS")
            & (F.col("duration_seconds") > F.lit(runtime_threshold_seconds)),
            F.lit("ALERT_REQUIRED")
        )
        .otherwise(
            F.lit("HEALTHY")
        )
    )
    .withColumn(
        "alert_reason",
        F.when(
            F.col("run_status") == "FAILED",
            F.concat(
                F.lit("PIPELINE_FAILED_AT_"),
                F.coalesce(F.col("failed_step"), F.lit("UNKNOWN_STEP"))
            )
        )
        .when(
            F.col("duration_seconds") > F.lit(runtime_threshold_seconds),
            F.lit("RUNTIME_THRESHOLD_EXCEEDED")
        )
        .otherwise(
            F.lit(None).cast("string")
        )
    )
)

display(
    health_df.select(
        "pipeline_name",
        "batch_id",
        "run_status",
        "failed_step",
        "duration_seconds",
        "health_status",
        "alert_reason"
    )
)

# COMMAND ----------

import time
import uuid
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

audit_table = "workspace.gold.pipeline_run_audit"
source_table = "workspace.gold.fact_order_lines"

run_id = str(uuid.uuid4())
pipeline_name = "fact_order_lines_monitoring_demo"
batch_id = "monitoring_batch_003"

started_at = datetime.now()
start_perf = time.perf_counter()

running_row = Row(
    run_id=run_id,
    pipeline_name=pipeline_name,
    batch_id=batch_id,
    run_status="RUNNING",
    failed_step=None,
    error_message=None,
    rows_read=None,
    rows_written=None,
    rows_rejected=None,
    started_at=started_at,
    completed_at=None,
    duration_seconds=None
)

spark.createDataFrame(
    [running_row],
    schema=spark.table(audit_table).schema
).write.mode("append").saveAsTable(audit_table)

try:
    rows_read = spark.table(source_table).count()
    rows_written = rows_read
    rows_rejected = 0

    completed_at = datetime.now()
    duration_seconds = time.perf_counter() - start_perf

    spark.sql(f"""
    UPDATE {audit_table}
    SET
        run_status = 'SUCCESS',
        rows_read = {rows_read},
        rows_written = {rows_written},
        rows_rejected = {rows_rejected},
        completed_at = TIMESTAMP('{completed_at.strftime("%Y-%m-%d %H:%M:%S.%f")}'),
        duration_seconds = {duration_seconds}
    WHERE run_id = '{run_id}'
    """)

except Exception as e:
    completed_at = datetime.now()
    duration_seconds = time.perf_counter() - start_perf
    error_message = str(e).replace("'", "''")[:2000]

    spark.sql(f"""
    UPDATE {audit_table}
    SET
        run_status = 'FAILED',
        failed_step = 'READ_SOURCE',
        error_message = '{error_message}',
        rows_read = 0,
        rows_written = 0,
        rows_rejected = 0,
        completed_at = TIMESTAMP('{completed_at.strftime("%Y-%m-%d %H:%M:%S.%f")}'),
        duration_seconds = {duration_seconds}
    WHERE run_id = '{run_id}'
    """)

display(
    spark.table(audit_table)
    .filter(F.col("run_id") == run_id)
    .select(
        "run_id",
        "batch_id",
        "run_status",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "duration_seconds"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit_table = "workspace.gold.pipeline_run_audit"
pipeline_name = "fact_order_lines_monitoring_demo"

runtime_threshold_seconds = 5.0

audit_df = (
    spark.table(audit_table)
    .filter(F.col("pipeline_name") == pipeline_name)
)

latest_window = (
    Window
    .partitionBy("pipeline_name")
    .orderBy(F.col("started_at").desc())
)

latest_health_df = (
    audit_df
    .withColumn(
        "rn",
        F.row_number().over(latest_window)
    )
    .filter(F.col("rn") == 1)
    .withColumn(
        "health_status",
        F.when(
            F.col("run_status") == "FAILED",
            F.lit("ALERT_REQUIRED")
        )
        .when(
            (F.col("run_status") == "SUCCESS")
            & (F.col("duration_seconds") > F.lit(runtime_threshold_seconds)),
            F.lit("ALERT_REQUIRED")
        )
        .otherwise(
            F.lit("HEALTHY")
        )
    )
    .withColumn(
        "alert_reason",
        F.when(
            F.col("run_status") == "FAILED",
            F.concat(
                F.lit("PIPELINE_FAILED_AT_"),
                F.coalesce(F.col("failed_step"), F.lit("UNKNOWN_STEP"))
            )
        )
        .when(
            F.col("duration_seconds") > F.lit(runtime_threshold_seconds),
            F.lit("RUNTIME_THRESHOLD_EXCEEDED")
        )
        .otherwise(
            F.lit(None).cast("string")
        )
    )
)

display(
    latest_health_df.select(
        "pipeline_name",
        "batch_id",
        "run_status",
        "duration_seconds",
        "health_status",
        "alert_reason"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit_table = "workspace.gold.pipeline_run_audit"
metrics_table = "workspace.gold.pipeline_monitoring_metrics"

audit_df = spark.table(audit_table)

latest_window = (
    Window
    .partitionBy("pipeline_name")
    .orderBy(F.col("started_at").desc())
)

metrics_df = (
    audit_df
    .withColumn(
        "rn",
        F.row_number().over(latest_window)
    )
    .withColumn(
        "success_flag",
        F.when(F.col("run_status") == "SUCCESS", 1).otherwise(0)
    )
    .withColumn(
        "failure_flag",
        F.when(F.col("run_status") == "FAILED", 1).otherwise(0)
    )
    .groupBy("pipeline_name")
    .agg(
        F.count("*").alias("total_runs"),
        F.sum("success_flag").alias("success_runs"),
        F.sum("failure_flag").alias("failed_runs"),
        F.round(F.avg("duration_seconds"), 3).alias("avg_duration_seconds"),
        F.max("duration_seconds").alias("max_duration_seconds"),
        F.max("rows_read").alias("max_rows_read"),
        F.max("rows_written").alias("max_rows_written"),
        F.max("rows_rejected").alias("max_rows_rejected")
    )
)

(
    metrics_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(metrics_table)
)

print(
    "monitoring metrics rows:",
    spark.table(metrics_table).count()
)

display(
    spark.table(metrics_table)
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType

source_table = "workspace.gold.fact_order_lines"

source_df = spark.table(source_table)

time_columns = [
    field.name
    for field in source_df.schema.fields
    if isinstance(field.dataType, (TimestampType, DateType))
]

print("time/date columns:", time_columns)

source_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

freshness_df = (
    spark.table(source_table)
    .agg(
        F.max("created_time").alias("latest_event_time"),
        F.count("*").alias("row_count")
    )
    .withColumn(
        "observed_at",
        F.current_timestamp()
    )
    .withColumn(
        "freshness_lag_seconds",
        F.unix_timestamp("observed_at")
        - F.unix_timestamp("latest_event_time")
    )
    .withColumn(
        "freshness_lag_hours",
        F.round(
            F.col("freshness_lag_seconds") / F.lit(3600.0),
            2
        )
    )
)

display(freshness_df)

# COMMAND ----------

from pyspark.sql import functions as F

freshness_threshold_hours = 24.0

freshness_status_df = (
    freshness_df
    .withColumn(
        "freshness_status",
        F.when(
            F.col("freshness_lag_hours") <= F.lit(freshness_threshold_hours),
            F.lit("FRESH")
        )
        .otherwise(
            F.lit("STALE")
        )
    )
    .withColumn(
        "alert_required",
        F.when(
            F.col("freshness_status") == "STALE",
            F.lit(True)
        )
        .otherwise(
            F.lit(False)
        )
    )
)

display(
    freshness_status_df.select(
        "latest_event_time",
        "observed_at",
        "row_count",
        "freshness_lag_hours",
        "freshness_status",
        "alert_required"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

expected_rows = 5249

completeness_df = (
    spark.table(source_table)
    .agg(
        F.count("*").alias("actual_rows")
    )
    .withColumn(
        "expected_rows",
        F.lit(expected_rows)
    )
    .withColumn(
        "missing_rows",
        F.col("expected_rows") - F.col("actual_rows")
    )
    .withColumn(
        "completeness_pct",
        F.round(
            (F.col("actual_rows") / F.col("expected_rows")) * 100.0,
            2
        )
    )
    .withColumn(
        "completeness_status",
        F.when(
            F.col("actual_rows") >= F.col("expected_rows"),
            F.lit("COMPLETE")
        )
        .otherwise(
            F.lit("INCOMPLETE")
        )
    )
    .withColumn(
        "alert_required",
        F.when(
            F.col("completeness_status") == "INCOMPLETE",
            F.lit(True)
        )
        .otherwise(
            F.lit(False)
        )
    )
)

display(completeness_df)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

expected_rows = 5249

incomplete_test_df = (
    spark.table(source_table)
    .limit(5000)
)

incomplete_check_df = (
    incomplete_test_df
    .agg(
        F.count("*").alias("actual_rows")
    )
    .withColumn(
        "expected_rows",
        F.lit(expected_rows)
    )
    .withColumn(
        "missing_rows",
        F.col("expected_rows") - F.col("actual_rows")
    )
    .withColumn(
        "completeness_pct",
        F.round(
            (F.col("actual_rows") / F.col("expected_rows")) * 100.0,
            2
        )
    )
    .withColumn(
        "completeness_status",
        F.when(
            F.col("actual_rows") >= F.col("expected_rows"),
            F.lit("COMPLETE")
        )
        .otherwise(
            F.lit("INCOMPLETE")
        )
    )
    .withColumn(
        "alert_required",
        F.when(
            F.col("completeness_status") == "INCOMPLETE",
            F.lit(True)
        )
        .otherwise(
            F.lit(False)
        )
    )
)

display(incomplete_check_df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit_table = "workspace.gold.pipeline_run_audit"
pipeline_name = "fact_order_lines_monitoring_demo"

# -----------------------------
# Latest pipeline status
# -----------------------------
latest_window = (
    Window
    .partitionBy("pipeline_name")
    .orderBy(F.col("started_at").desc())
)

latest_run_df = (
    spark.table(audit_table)
    .filter(F.col("pipeline_name") == pipeline_name)
    .withColumn(
        "rn",
        F.row_number().over(latest_window)
    )
    .filter(F.col("rn") == 1)
    .select(
        "pipeline_name",
        "batch_id",
        "run_status"
    )
)

# -----------------------------
# Freshness result
# -----------------------------
freshness_health_df = (
    freshness_status_df
    .select(
        "freshness_lag_hours",
        "freshness_status"
    )
)

# -----------------------------
# Completeness result
# ใช้ baseline จริง ไม่ใช่ failure simulation
# -----------------------------
completeness_health_df = (
    completeness_df
    .select(
        "actual_rows",
        "expected_rows",
        "completeness_pct",
        "completeness_status"
    )
)

# -----------------------------
# Combine
# -----------------------------
reliability_health_df = (
    latest_run_df
    .crossJoin(freshness_health_df)
    .crossJoin(completeness_health_df)
    .withColumn(
        "overall_health",
        F.when(
            F.col("run_status") != "SUCCESS",
            F.lit("CRITICAL")
        )
        .when(
            F.col("completeness_status") != "COMPLETE",
            F.lit("CRITICAL")
        )
        .when(
            F.col("freshness_status") != "FRESH",
            F.lit("WARNING")
        )
        .otherwise(
            F.lit("HEALTHY")
        )
    )
)

display(
    reliability_health_df.select(
        "pipeline_name",
        "batch_id",
        "run_status",
        "freshness_lag_hours",
        "freshness_status",
        "actual_rows",
        "expected_rows",
        "completeness_pct",
        "completeness_status",
        "overall_health"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

reliability_table = "workspace.gold.pipeline_reliability_snapshot"

snapshot_df = (
    reliability_health_df
    .withColumn(
        "observed_at",
        F.current_timestamp()
    )
)

(
    snapshot_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(reliability_table)
)

print(
    "reliability snapshot rows:",
    spark.table(reliability_table).count()
)

display(
    spark.table(reliability_table)
    .orderBy(F.col("observed_at").desc())
    .limit(5)
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

perf_df = (
    spark.table(source_table)
    .filter(F.col("order_status") != "Cancelled")
    .groupBy("order_channel")
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

print(
    "source partitions:",
    spark.table(source_table).rdd.getNumPartitions()
)

print(
    "result partitions:",
    perf_df.rdd.getNumPartitions()
)

print("EXECUTION PLAN")
perf_df.explain("formatted")

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

perf_df = (
    spark.table(source_table)
    .filter(F.col("order_status") != "Cancelled")
    .groupBy("order_channel")
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

print("SOURCE FILE COUNT")

source_file_count_df = (
    spark.sql(f"""
        SELECT COUNT(DISTINCT _metadata.file_path) AS source_file_count
        FROM {source_table}
    """)
)

display(source_file_count_df)

print("EXECUTION PLAN")
perf_df.explain("formatted")

# COMMAND ----------

print("EXTENDED EXECUTION PLAN")
perf_df.explain("extended")

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

base_df = spark.table(source_table)

perf_sandbox_df = (
    base_df
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
)

print(
    "synthetic row count:",
    perf_sandbox_df.count()
)

# COMMAND ----------

from pyspark.sql import functions as F

synthetic_agg_df = (
    perf_sandbox_df
    .filter(F.col("order_status") != "Cancelled")
    .groupBy(
        "order_channel",
        "synthetic_batch_id"
    )
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

print("SYNTHETIC EXECUTION PLAN")
synthetic_agg_df.explain("extended")

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

perf_sandbox_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
)

synthetic_agg_df = (
    perf_sandbox_df
    .filter(F.col("order_status") != "Cancelled")
    .groupBy(
        "order_channel",
        "synthetic_batch_id"
    )
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

print("SYNTHETIC EXECUTION PLAN")
synthetic_agg_df.explain("extended")

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

perf_sandbox_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
)

baseline_agg_df = (
    perf_sandbox_df
    .filter(F.col("order_status") != "Cancelled")
    .groupBy(
        "order_channel",
        "synthetic_batch_id"
    )
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

start_time = time.perf_counter()

result_rows = baseline_agg_df.count()

baseline_seconds = time.perf_counter() - start_time

print("result rows:", result_rows)
print("baseline runtime seconds:", round(baseline_seconds, 3))

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

perf_sandbox_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
)

repartitioned_df = (
    perf_sandbox_df
    .repartition(
        16,
        "order_channel",
        "synthetic_batch_id"
    )
)

optimized_agg_df = (
    repartitioned_df
    .filter(F.col("order_status") != "Cancelled")
    .groupBy(
        "order_channel",
        "synthetic_batch_id"
    )
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

start_time = time.perf_counter()

result_rows = optimized_agg_df.count()

optimized_seconds = time.perf_counter() - start_time

print("result rows:", result_rows)
print("repartition runtime seconds:", round(optimized_seconds, 3))

# COMMAND ----------

print("REPARTITION EXECUTION PLAN")
optimized_agg_df.explain("extended")

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

def build_base_df():
    return (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
    )

def run_baseline():
    df = (
        build_base_df()
        .filter(F.col("order_status") != "Cancelled")
        .groupBy(
            "order_channel",
            "synthetic_batch_id"
        )
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


def run_repartition():
    df = (
        build_base_df()
        .repartition(
            16,
            "order_channel",
            "synthetic_batch_id"
        )
        .filter(F.col("order_status") != "Cancelled")
        .groupBy(
            "order_channel",
            "synthetic_batch_id"
        )
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


tests = [
    ("baseline_1", run_baseline),
    ("repartition_1", run_repartition),
    ("baseline_2", run_baseline),
    ("repartition_2", run_repartition),
    ("baseline_3", run_baseline),
    ("repartition_3", run_repartition),
]

results = []

for test_name, test_fn in tests:
    rows, seconds = test_fn()

    results.append(
        (test_name, rows, round(seconds, 3))
    )

for result in results:
    print(result)

# COMMAND ----------

shuffle_partitions = spark.conf.get(
    "spark.sql.shuffle.partitions"
)

print(
    "spark.sql.shuffle.partitions:",
    shuffle_partitions
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

aqe_test_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
    .filter(F.col("order_status") != "Cancelled")
    .groupBy(
        "order_channel",
        "synthetic_batch_id"
    )
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("sku_subtotal_after_discount").alias("revenue")
    )
)

# Trigger execution
result_rows = aqe_test_df.count()

print("result rows:", result_rows)

print("FINAL / ADAPTIVE PLAN")
aqe_test_df.explain("extended")

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

def run_auto_shuffle_test():
    df = (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
        .filter(F.col("order_status") != "Cancelled")
        .groupBy(
            "order_channel",
            "synthetic_batch_id"
        )
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


for i in range(1, 4):
    rows, seconds = run_auto_shuffle_test()

    print(
        f"auto_{i}: rows={rows}, runtime={seconds:.3f}s"
    )

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

original_shuffle_setting = spark.conf.get(
    "spark.sql.shuffle.partitions"
)

def run_shuffle_test():
    df = (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
        .filter(F.col("order_status") != "Cancelled")
        .groupBy(
            "order_channel",
            "synthetic_batch_id"
        )
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


try:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        "4"
    )

    print(
        "test shuffle partitions:",
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    for i in range(1, 4):
        rows, seconds = run_shuffle_test()

        print(
            f"manual_4_{i}: rows={rows}, runtime={seconds:.3f}s"
        )

finally:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        original_shuffle_setting
    )

    print(
        "restored shuffle partitions:",
        spark.conf.get("spark.sql.shuffle.partitions")
    )

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

original_shuffle_setting = spark.conf.get(
    "spark.sql.shuffle.partitions"
)

def run_shuffle_test():
    df = (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
        .filter(F.col("order_status") != "Cancelled")
        .groupBy(
            "order_channel",
            "synthetic_batch_id"
        )
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


try:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        "16"
    )

    print(
        "test shuffle partitions:",
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    for i in range(1, 4):
        rows, seconds = run_shuffle_test()

        print(
            f"manual_16_{i}: rows={rows}, runtime={seconds:.3f}s"
        )

finally:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        original_shuffle_setting
    )

    print(
        "restored shuffle partitions:",
        spark.conf.get("spark.sql.shuffle.partitions")
    )

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

original_shuffle_setting = spark.conf.get(
    "spark.sql.shuffle.partitions"
)

def run_shuffle_test():
    df = (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
        .filter(F.col("order_status") != "Cancelled")
        .groupBy(
            "order_channel",
            "synthetic_batch_id"
        )
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


try:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        "32"
    )

    print(
        "test shuffle partitions:",
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    for i in range(1, 4):
        rows, seconds = run_shuffle_test()

        print(
            f"manual_32_{i}: rows={rows}, runtime={seconds:.3f}s"
        )

finally:
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        original_shuffle_setting
    )

    print(
        "restored shuffle partitions:",
        spark.conf.get("spark.sql.shuffle.partitions")
    )

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

benchmark_table = "workspace.gold.spark_performance_benchmark"

benchmark_rows = [
    Row(
        experiment_name="shuffle_partitions",
        configuration="auto",
        median_runtime_seconds=0.785,
        result_rows=600,
        observed_at=datetime.now()
    ),
    Row(
        experiment_name="shuffle_partitions",
        configuration="4",
        median_runtime_seconds=0.722,
        result_rows=600,
        observed_at=datetime.now()
    ),
    Row(
        experiment_name="shuffle_partitions",
        configuration="16",
        median_runtime_seconds=0.622,
        result_rows=600,
        observed_at=datetime.now()
    ),
    Row(
        experiment_name="shuffle_partitions",
        configuration="32",
        median_runtime_seconds=0.709,
        result_rows=600,
        observed_at=datetime.now()
    )
]

benchmark_df = spark.createDataFrame(benchmark_rows)

(
    benchmark_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(benchmark_table)
)

print(
    "benchmark rows:",
    spark.table(benchmark_table).count()
)

display(
    spark.table(benchmark_table)
    .orderBy("median_runtime_seconds")
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

base_df = spark.table(source_table)

skew_df = (
    base_df
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
    .withColumn(
        "skew_key",
        F.when(
            F.col("synthetic_batch_id") < 180,
            F.lit("HOT_KEY")
        )
        .otherwise(
            F.concat(
                F.lit("KEY_"),
                F.col("synthetic_batch_id").cast("string")
            )
        )
    )
)

skew_profile_df = (
    skew_df
    .groupBy("skew_key")
    .count()
    .orderBy(F.col("count").desc())
)

display(skew_profile_df.limit(10))

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

def build_synthetic_df():
    return (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
    )


def run_skewed():
    df = (
        build_synthetic_df()
        .withColumn(
            "group_key",
            F.when(
                F.col("synthetic_batch_id") < 180,
                F.lit("HOT_KEY")
            )
            .otherwise(
                F.concat(
                    F.lit("KEY_"),
                    F.col("synthetic_batch_id").cast("string")
                )
            )
        )
        .groupBy("group_key")
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


def run_balanced():
    df = (
        build_synthetic_df()
        .withColumn(
            "group_key",
            F.concat(
                F.lit("KEY_"),
                F.col("synthetic_batch_id").cast("string")
            )
        )
        .groupBy("group_key")
        .agg(
            F.sum("quantity").alias("total_units"),
            F.sum("sku_subtotal_after_discount").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


tests = [
    ("skewed_1", run_skewed),
    ("balanced_1", run_balanced),
    ("skewed_2", run_skewed),
    ("balanced_2", run_balanced),
    ("skewed_3", run_skewed),
    ("balanced_3", run_balanced),
]

for test_name, test_fn in tests:
    rows, seconds = test_fn()

    print(
        f"{test_name}: rows={rows}, runtime={seconds:.3f}s"
    )

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
salt_buckets = 8

def build_skewed_df():
    return (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
        .withColumn(
            "group_key",
            F.when(
                F.col("synthetic_batch_id") < 180,
                F.lit("HOT_KEY")
            )
            .otherwise(
                F.concat(
                    F.lit("KEY_"),
                    F.col("synthetic_batch_id").cast("string")
                )
            )
        )
    )


def run_salted():
    salted_df = (
        build_skewed_df()
        .withColumn(
            "salt",
            F.when(
                F.col("group_key") == "HOT_KEY",
                F.pmod(
                    F.hash(
                        F.col("order_id"),
                        F.col("synthetic_batch_id")
                    ),
                    F.lit(salt_buckets)
                )
            )
            .otherwise(F.lit(0))
        )
    )

    first_stage_df = (
        salted_df
        .groupBy(
            "group_key",
            "salt"
        )
        .agg(
            F.sum("quantity").alias("partial_units"),
            F.sum("sku_subtotal_after_discount").alias("partial_revenue")
        )
    )

    final_df = (
        first_stage_df
        .groupBy("group_key")
        .agg(
            F.sum("partial_units").alias("total_units"),
            F.sum("partial_revenue").alias("revenue")
        )
    )

    start = time.perf_counter()
    rows = final_df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


for i in range(1, 4):
    rows, seconds = run_salted()

    print(
        f"salted_{i}: rows={rows}, runtime={seconds:.3f}s"
    )

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

benchmark_table = "workspace.gold.spark_performance_benchmark"

skew_benchmark_rows = [
    Row(
        experiment_name="data_skew",
        configuration="skewed",
        median_runtime_seconds=0.711,
        result_rows=21,
        observed_at=datetime.now()
    ),
    Row(
        experiment_name="data_skew",
        configuration="salted_8_buckets",
        median_runtime_seconds=0.642,
        result_rows=21,
        observed_at=datetime.now()
    ),
    Row(
        experiment_name="data_skew",
        configuration="balanced",
        median_runtime_seconds=0.637,
        result_rows=200,
        observed_at=datetime.now()
    )
]

skew_benchmark_df = spark.createDataFrame(skew_benchmark_rows)

(
    skew_benchmark_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'data_skew'")
    .orderBy("median_runtime_seconds")
)

# COMMAND ----------

target_table = "workspace.gold.fact_order_lines"

detail_df = spark.sql(f"DESCRIBE DETAIL {target_table}")

display(
    detail_df.select(
        "format",
        "numFiles",
        "sizeInBytes"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
small_files_table = "workspace.gold.fact_order_lines_small_files_sandbox"

source_df = spark.table(source_table)

# reset sandbox
spark.sql(f"DROP TABLE IF EXISTS {small_files_table}")

# สร้างหลายไฟล์ด้วยการ append หลายรอบ
for i in range(10):
    (
        source_df
        .withColumn("load_batch", F.lit(i))
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(small_files_table)
    )

detail_df = spark.sql(
    f"DESCRIBE DETAIL {small_files_table}"
)

display(
    detail_df.select(
        "numFiles",
        "sizeInBytes"
    )
)

# COMMAND ----------

small_files_table = "workspace.gold.fact_order_lines_small_files_sandbox"

before_detail = (
    spark.sql(f"DESCRIBE DETAIL {small_files_table}")
    .select("numFiles", "sizeInBytes")
)

print("BEFORE OPTIMIZE")
display(before_detail)

print("RUN OPTIMIZE")

spark.sql(f"""
OPTIMIZE {small_files_table}
""")

after_detail = (
    spark.sql(f"DESCRIBE DETAIL {small_files_table}")
    .select("numFiles", "sizeInBytes")
)

print("AFTER OPTIMIZE")
display(after_detail)

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

benchmark_table = "workspace.gold.spark_performance_benchmark"

small_files_benchmark_rows = [
    Row(
        experiment_name="small_files",
        configuration="before_optimize_10_files",
        median_runtime_seconds=None,
        result_rows=10,
        observed_at=datetime.now()
    ),
    Row(
        experiment_name="small_files",
        configuration="after_optimize_1_file",
        median_runtime_seconds=None,
        result_rows=1,
        observed_at=datetime.now()
    )
]

small_files_benchmark_df = spark.createDataFrame(
    small_files_benchmark_rows
)

(
    small_files_benchmark_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'small_files'")
)

# COMMAND ----------

from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)

benchmark_table = "workspace.gold.spark_performance_benchmark"

benchmark_schema = StructType([
    StructField("experiment_name", StringType(), True),
    StructField("configuration", StringType(), True),
    StructField("median_runtime_seconds", DoubleType(), True),
    StructField("result_rows", LongType(), True),
    StructField("observed_at", TimestampType(), True)
])

small_files_benchmark_data = [
    (
        "small_files",
        "before_optimize_10_files",
        None,
        10,
        datetime.now()
    ),
    (
        "small_files",
        "after_optimize_1_file",
        None,
        1,
        datetime.now()
    )
]

small_files_benchmark_df = spark.createDataFrame(
    small_files_benchmark_data,
    schema=benchmark_schema
)

(
    small_files_benchmark_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'small_files'")
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

predicate_df = (
    spark.table(source_table)
    .filter(
        (F.col("order_status") == "Completed")
        & (F.col("quantity") >= 1)
    )
    .select(
        "order_id",
        "order_status",
        "quantity",
        "order_channel"
    )
)

predicate_df.explain("extended")

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

display(
    spark.table(source_table)
    .groupBy("order_status")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

predicate_df = (
    spark.table(source_table)
    .filter(
        (F.col("order_status") == "สำเร็จสมบูรณ์")
        & (F.col("quantity") >= 1)
    )
    .select(
        "order_id",
        "order_status",
        "quantity",
        "order_channel"
    )
)

predicate_df.explain("extended")

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

spark.sql(f"DROP TABLE IF EXISTS {skipping_table}")

(
    spark.table(source_table)
    .orderBy("order_status")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(skipping_table)
)

detail_df = spark.sql(
    f"DESCRIBE DETAIL {skipping_table}"
)

display(
    detail_df.select(
        "numFiles",
        "sizeInBytes"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

spark.sql(f"DROP TABLE IF EXISTS {skipping_table}")

status_values = [
    row["order_status"]
    for row in (
        spark.table(source_table)
        .select("order_status")
        .distinct()
        .collect()
    )
]

for status_value in status_values:
    (
        spark.table(source_table)
        .filter(F.col("order_status") == status_value)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(skipping_table)
    )

detail_df = spark.sql(
    f"DESCRIBE DETAIL {skipping_table}"
)

display(
    detail_df.select(
        "numFiles",
        "sizeInBytes"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

skipping_test_df = (
    spark.table(skipping_table)
    .filter(
        F.col("order_status") == "สำเร็จสมบูรณ์"
    )
    .select(
        "order_id",
        "order_status",
        "quantity",
        "order_channel"
    )
)

print("DATA SKIPPING PLAN")
skipping_test_df.explain("extended")

# COMMAND ----------

from pyspark.sql import functions as F

skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

display(
    spark.table(skipping_table)
    .groupBy("order_status")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------


skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

skipping_test_df = spark.sql(f"""
SELECT
    order_id,
    order_status,
    quantity,
    order_channel
FROM {skipping_table}
WHERE order_status = 'สำเร็จสมบูรณ์'
""")

print("row count:", skipping_test_df.count())

print("DATA SKIPPING PLAN")
skipping_test_df.explain("extended")



# COMMAND ----------


from pyspark.sql import functions as F

skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

status_debug_df = (
    spark.table(skipping_table)
    .select("order_status")
    .distinct()
    .withColumn(
        "char_length",
        F.length("order_status")
    )
    .withColumn(
        "byte_length",
        F.length(F.encode("order_status", "UTF-8"))
    )
    .withColumn(
        "hex_value",
        F.hex(F.encode("order_status", "UTF-8"))
    )
    .orderBy("order_status")
)

display(status_debug_df)


# COMMAND ----------


skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

skipping_test_df = spark.sql(f"""
SELECT
    order_id,
    order_status,
    quantity,
    order_channel
FROM {skipping_table}
WHERE order_status = 'เสร็จสมบูรณ์'
""")

print(
    "row count:",
    skipping_test_df.count()
)

print("DATA SKIPPING PLAN")
skipping_test_df.explain("extended")


# COMMAND ----------

from pyspark.sql import functions as F

skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

matched_files_df = (
    spark.table(skipping_table)
    .filter(F.col("order_status") == "เสร็จสมบูรณ์")
    .select(
        F.input_file_name().alias("file_name")
    )
    .distinct()
)

print(
    "matched file count:",
    matched_files_df.count()
)

display(matched_files_df)

# COMMAND ----------

from pyspark.sql import functions as F

skipping_table = "workspace.gold.fact_order_lines_skipping_sandbox"

matched_files_df = (
    spark.table(skipping_table)
    .filter(F.col("order_status") == "เสร็จสมบูรณ์")
    .select(
        F.col("_metadata.file_path").alias("file_path")
    )
    .distinct()
)

print(
    "matched file count:",
    matched_files_df.count()
)

display(matched_files_df)

# COMMAND ----------

from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)

benchmark_table = "workspace.gold.spark_performance_benchmark"

benchmark_schema = StructType([
    StructField("experiment_name", StringType(), True),
    StructField("configuration", StringType(), True),
    StructField("median_runtime_seconds", DoubleType(), True),
    StructField("result_rows", LongType(), True),
    StructField("observed_at", TimestampType(), True)
])

data_skipping_data = [
    (
        "data_skipping",
        "total_files",
        None,
        5,
        datetime.now()
    ),
    (
        "data_skipping",
        "matched_files_for_completed_status",
        None,
        1,
        datetime.now()
    )
]

data_skipping_df = spark.createDataFrame(
    data_skipping_data,
    schema=benchmark_schema
)

(
    data_skipping_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'data_skipping'")
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

benchmark_table = "workspace.gold.spark_performance_benchmark"

benchmark_df = spark.table(benchmark_table)

window_spec = (
    Window
    .partitionBy(
        "experiment_name",
        "configuration",
        "median_runtime_seconds",
        "result_rows"
    )
    .orderBy(F.col("observed_at").desc())
)

clean_benchmark_df = (
    benchmark_df
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)

(
    clean_benchmark_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'data_skipping'")
)import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
materialized_table = "workspace.gold.fact_order_lines_materialized_sandbox"

base_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
    .filter(F.col("quantity") >= 1)
)

start = time.perf_counter()

baseline_rows = base_df.count()

baseline_seconds = time.perf_counter() - start

print("baseline rows:", baseline_rows)
print("baseline runtime seconds:", round(baseline_seconds, 3))import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
materialized_table = "workspace.gold.fact_order_lines_materialized_sandbox"

base_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
    .filter(F.col("quantity") >= 1)
)

start = time.perf_counter()

baseline_rows = base_df.count()

baseline_seconds = time.perf_counter() - start

print("baseline rows:", baseline_rows)
print("baseline runtime seconds:", round(baseline_seconds, 3))

# COMMAND ----------

import time

materialized_table = "workspace.gold.fact_order_lines_materialized_sandbox"

# สร้าง/เขียน intermediate result ลง Delta
start = time.perf_counter()

(
    base_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(materialized_table)
)

write_seconds = time.perf_counter() - start

# วัดเวลาอ่านจาก materialized Delta
start = time.perf_counter()

materialized_rows = spark.table(materialized_table).count()

read_seconds = time.perf_counter() - start

print("materialized rows:", materialized_rows)
print("materialization write seconds:", round(write_seconds, 3))
print("materialized read seconds:", round(read_seconds, 3))

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
materialized_table = "workspace.gold.fact_order_lines_materialized_sandbox"

base_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
    .filter(F.col("quantity") >= 1)
)

start = time.perf_counter()

baseline_rows = base_df.count()

baseline_seconds = time.perf_counter() - start

print("baseline rows:", baseline_rows)
print("baseline runtime seconds:", round(baseline_seconds, 3))

# COMMAND ----------

import time
from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"
materialized_table = "workspace.gold.fact_order_lines_materialized_sandbox"


def run_recompute():
    df = (
        spark.table(source_table)
        .crossJoin(
            spark.range(200)
            .withColumnRenamed("id", "synthetic_batch_id")
        )
        .filter(F.col("quantity") >= 1)
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


def run_materialized():
    start = time.perf_counter()

    rows = spark.table(materialized_table).count()

    seconds = time.perf_counter() - start

    return rows, seconds


tests = [
    ("recompute_1", run_recompute),
    ("materialized_1", run_materialized),
    ("recompute_2", run_recompute),
    ("materialized_2", run_materialized),
    ("recompute_3", run_recompute),
    ("materialized_3", run_materialized),
]

for test_name, test_fn in tests:
    rows, seconds = test_fn()

    print(
        f"{test_name}: rows={rows}, runtime={seconds:.3f}s"
    )

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F

source_table = "workspace.gold.fact_order_lines"

fact_df = spark.table(source_table)

small_dimension_df = (
    fact_df
    .select(
        "seller_sku",
        "product_name",
        "product_status"
    )
    .filter(F.col("seller_sku").isNotNull())
    .dropDuplicates(["seller_sku"])
)

join_test_df = (
    fact_df.alias("f")
    .join(
        small_dimension_df.alias("d"),
        F.col("f.seller_sku") == F.col("d.seller_sku"),
        "left"
    )
    .select(
        F.col("f.order_id"),
        F.col("f.seller_sku"),
        F.col("f.quantity"),
        F.col("d.product_name"),
        F.col("d.product_status")
    )
)

print("dimension rows:", small_dimension_df.count())
print("joined rows:", join_test_df.count())

print("JOIN EXECUTION PLAN")
join_test_df.explain("extended")

# COMMAND ----------

print("FORMATTED JOIN PLAN")
join_test_df.explain("formatted")

# COMMAND ----------

import time
from pyspark.sql import functions as F

def run_auto_broadcast():
    auto_join_df = (
        fact_df.alias("f")
        .join(
            small_dimension_df.alias("d"),
            F.col("f.seller_sku") == F.col("d.seller_sku"),
            "left"
        )
        .select(
            F.col("f.order_id"),
            F.col("f.seller_sku"),
            F.col("f.quantity"),
            F.col("d.product_name"),
            F.col("d.product_status")
        )
    )

    start = time.perf_counter()
    rows = auto_join_df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


def run_explicit_broadcast():
    explicit_join_df = (
        fact_df.alias("f")
        .join(
            F.broadcast(small_dimension_df).alias("d"),
            F.col("f.seller_sku") == F.col("d.seller_sku"),
            "left"
        )
        .select(
            F.col("f.order_id"),
            F.col("f.seller_sku"),
            F.col("f.quantity"),
            F.col("d.product_name"),
            F.col("d.product_status")
        )
    )

    start = time.perf_counter()
    rows = explicit_join_df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


tests = [
    ("auto_1", run_auto_broadcast),
    ("explicit_1", run_explicit_broadcast),
    ("auto_2", run_auto_broadcast),
    ("explicit_2", run_explicit_broadcast),
    ("auto_3", run_auto_broadcast),
    ("explicit_3", run_explicit_broadcast),
]

for test_name, test_fn in tests:
    rows, seconds = test_fn()

    print(
        f"{test_name}: rows={rows}, runtime={seconds:.3f}s"
    )

# COMMAND ----------

from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)

benchmark_table = "workspace.gold.spark_performance_benchmark"

benchmark_schema = StructType([
    StructField("experiment_name", StringType(), True),
    StructField("configuration", StringType(), True),
    StructField("median_runtime_seconds", DoubleType(), True),
    StructField("result_rows", LongType(), True),
    StructField("observed_at", TimestampType(), True)
])

join_strategy_data = [
    (
        "join_strategy",
        "optimizer_auto_broadcast",
        0.332,
        5249,
        datetime.now()
    ),
    (
        "join_strategy",
        "explicit_broadcast_hint",
        0.510,
        5249,
        datetime.now()
    )
]

join_strategy_df = spark.createDataFrame(
    join_strategy_data,
    schema=benchmark_schema
)

(
    join_strategy_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'join_strategy'")
)

# COMMAND ----------

source_table = "workspace.gold.fact_order_lines"

partition_test_df = (
    spark.table(source_table)
    .crossJoin(
        spark.range(200)
        .withColumnRenamed("id", "synthetic_batch_id")
    )
)

print("PARTITION TEST PLAN")
partition_test_df.explain("formatted")

# COMMAND ----------

repartition_df = partition_test_df.repartition(
    16,
    "order_channel"
)

coalesce_df = partition_test_df.coalesce(2)

print("REPARTITION PLAN")
repartition_df.explain("formatted")

print("COALESCE PLAN")
coalesce_df.explain("formatted")

# COMMAND ----------

import time


def run_repartition():
    df = partition_test_df.repartition(
        16,
        "order_channel"
    )

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


def run_coalesce():
    df = partition_test_df.coalesce(2)

    start = time.perf_counter()
    rows = df.count()
    seconds = time.perf_counter() - start

    return rows, seconds


tests = [
    ("repartition_1", run_repartition),
    ("coalesce_1", run_coalesce),
    ("repartition_2", run_repartition),
    ("coalesce_2", run_coalesce),
    ("repartition_3", run_repartition),
    ("coalesce_3", run_coalesce),
]

for test_name, test_fn in tests:
    rows, seconds = test_fn()

    print(
        f"{test_name}: rows={rows}, runtime={seconds:.3f}s"
    )

# COMMAND ----------

from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)

benchmark_table = "workspace.gold.spark_performance_benchmark"

benchmark_schema = StructType([
    StructField("experiment_name", StringType(), True),
    StructField("configuration", StringType(), True),
    StructField("median_runtime_seconds", DoubleType(), True),
    StructField("result_rows", LongType(), True),
    StructField("observed_at", TimestampType(), True)
])

partitioning_data = [
    (
        "partitioning_strategy",
        "repartition_16_order_channel",
        0.714,
        1049800,
        datetime.now()
    ),
    (
        "partitioning_strategy",
        "coalesce_2",
        0.784,
        1049800,
        datetime.now()
    )
]

partitioning_df = spark.createDataFrame(
    partitioning_data,
    schema=benchmark_schema
)

(
    partitioning_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(benchmark_table)
)

display(
    spark.table(benchmark_table)
    .filter("experiment_name = 'partitioning_strategy'")
)

# COMMAND ----------

from pyspark.sql import functions as F

benchmark_table = "workspace.gold.spark_performance_benchmark"

performance_summary_df = (
    spark.table(benchmark_table)
    .select(
        "experiment_name",
        "configuration",
        "median_runtime_seconds",
        "result_rows",
        "observed_at"
    )
    .orderBy(
        "experiment_name",
        F.col("median_runtime_seconds").asc_nulls_last()
    )
)

print(
    "benchmark row count:",
    performance_summary_df.count()
)

display(performance_summary_df)

# COMMAND ----------

from pyspark.sql import functions as F

benchmark_table = "workspace.gold.spark_performance_benchmark"

portfolio_performance_df = (
    spark.table(benchmark_table)
    .filter(
        F.col("experiment_name").isin(
            "shuffle_partitions",
            "data_skew",
            "materialization",
            "join_strategy",
            "partitioning_strategy"
        )
    )
    .select(
        "experiment_name",
        "configuration",
        "median_runtime_seconds",
        "result_rows"
    )
    .orderBy(
        "experiment_name",
        F.col("median_runtime_seconds").asc_nulls_last()
    )
)

display(portfolio_performance_df)

# COMMAND ----------

from pyspark.sql import functions as F

benchmark_table = "workspace.gold.spark_performance_benchmark"

improvement_df = (
    spark.createDataFrame(
        [
            ("shuffle_partitions", "auto", "16"),
            ("data_skew", "skewed", "salted_8_buckets"),
            ("materialization", "recompute", "materialized_delta_read"),
            ("join_strategy", "explicit_broadcast_hint", "optimizer_auto_broadcast"),
            ("partitioning_strategy", "coalesce_2", "repartition_16_order_channel"),
        ],
        [
            "experiment_name",
            "baseline_configuration",
            "optimized_configuration"
        ]
    )
    .alias("m")
    .join(
        spark.table(benchmark_table).alias("b"),
        (
            (F.col("m.experiment_name") == F.col("b.experiment_name"))
            & (F.col("m.baseline_configuration") == F.col("b.configuration"))
        ),
        "left"
    )
    .select(
        F.col("m.experiment_name"),
        F.col("m.baseline_configuration"),
        F.col("m.optimized_configuration"),
        F.col("b.median_runtime_seconds").alias("baseline_seconds")
    )
    .alias("x")
    .join(
        spark.table(benchmark_table).alias("o"),
        (
            (F.col("x.experiment_name") == F.col("o.experiment_name"))
            & (F.col("x.optimized_configuration") == F.col("o.configuration"))
        ),
        "left"
    )
    .select(
        F.col("x.experiment_name"),
        F.col("x.baseline_configuration"),
        F.col("x.optimized_configuration"),
        F.col("x.baseline_seconds"),
        F.col("o.median_runtime_seconds").alias("optimized_seconds")
    )
    .withColumn(
        "improvement_pct",
        F.round(
            (
                (F.col("baseline_seconds") - F.col("optimized_seconds"))
                / F.col("baseline_seconds")
            ) * 100,
            2
        )
    )
    .orderBy(F.col("improvement_pct").desc())
)

display(improvement_df)

# COMMAND ----------

from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

summary_table = "workspace.gold.spark_performance_improvement_summary"

summary_schema = StructType([
    StructField("experiment_name", StringType(), True),
    StructField("baseline_configuration", StringType(), True),
    StructField("optimized_configuration", StringType(), True),
    StructField("baseline_seconds", DoubleType(), True),
    StructField("optimized_seconds", DoubleType(), True),
    StructField("improvement_pct", DoubleType(), True),
    StructField("observed_at", TimestampType(), True)
])

summary_data = [
    (
        "join_strategy",
        "explicit_broadcast_hint",
        "optimizer_auto_broadcast",
        0.510,
        0.332,
        34.90,
        datetime.now()
    ),
    (
        "shuffle_partitions",
        "auto",
        "16",
        0.785,
        0.622,
        20.76,
        datetime.now()
    ),
    (
        "materialization",
        "recompute",
        "materialized_delta_read",
        0.541,
        0.480,
        11.28,
        datetime.now()
    ),
    (
        "data_skew",
        "skewed",
        "salted_8_buckets",
        0.711,
        0.642,
        9.70,
        datetime.now()
    ),
    (
        "partitioning_strategy",
        "coalesce_2",
        "repartition_16_order_channel",
        0.784,
        0.714,
        8.93,
        datetime.now()
    )
]

summary_df = spark.createDataFrame(
    summary_data,
    schema=summary_schema
)

(
    summary_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(summary_table)
)

display(
    spark.table(summary_table)
    .orderBy("improvement_pct", ascending=False)
)

# COMMAND ----------

from pyspark.sql import functions as F

benchmark_table = "workspace.gold.spark_performance_benchmark"
summary_table = "workspace.gold.spark_performance_improvement_summary"

print("=== PERFORMANCE BENCHMARK ===")

benchmark_check_df = (
    spark.table(benchmark_table)
    .groupBy("experiment_name")
    .agg(
        F.count("*").alias("evidence_rows")
    )
    .orderBy("experiment_name")
)

display(benchmark_check_df)


print("=== PERFORMANCE IMPROVEMENT SUMMARY ===")

summary_check_df = (
    spark.table(summary_table)
    .select(
        "experiment_name",
        "baseline_configuration",
        "optimized_configuration",
        "baseline_seconds",
        "optimized_seconds",
        "improvement_pct"
    )
    .orderBy(
        F.col("improvement_pct").desc()
    )
)

display(summary_check_df)

# COMMAND ----------

from pyspark.sql import functions as F

tables_to_check = [
    "workspace.gold.fact_order_lines",
    "workspace.gold.pipeline_run_audit",
    "workspace.gold.spark_performance_benchmark",
    "workspace.gold.spark_performance_improvement_summary",
    "workspace.gold.dim_sku_scd2_recovery_sandbox"
]

inventory_rows = []

for table_name in tables_to_check:
    exists = spark.catalog.tableExists(table_name)

    if exists:
        row_count = spark.table(table_name).count()
        status = "FOUND"
    else:
        row_count = None
        status = "MISSING"

    inventory_rows.append(
        (
            table_name,
            status,
            row_count
        )
    )

inventory_df = spark.createDataFrame(
    inventory_rows,
    [
        "table_name",
        "status",
        "row_count"
    ]
)

display(inventory_df)

# COMMAND ----------

tables_df = spark.sql("""
SHOW TABLES IN workspace.bronze
""")

print("=== BRONZE TABLES ===")
display(tables_df)

silver_tables_df = spark.sql("""
SHOW TABLES IN workspace.silver
""")

print("=== SILVER TABLES ===")
display(silver_tables_df)

gold_tables_df = spark.sql("""
SHOW TABLES IN workspace.gold
""")

print("=== GOLD TABLES ===")
display(gold_tables_df)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_tables = (
    spark.sql("SHOW TABLES IN workspace.bronze")
    .select(
        F.lit("BRONZE").alias("layer"),
        F.col("tableName").alias("table_name")
    )
)

silver_tables = (
    spark.sql("SHOW TABLES IN workspace.silver")
    .select(
        F.lit("SILVER").alias("layer"),
        F.col("tableName").alias("table_name")
    )
)

gold_tables = (
    spark.sql("SHOW TABLES IN workspace.gold")
    .select(
        F.lit("GOLD").alias("layer"),
        F.col("tableName").alias("table_name")
    )
)

all_project_tables_df = (
    bronze_tables
    .unionByName(silver_tables)
    .unionByName(gold_tables)
    .orderBy("layer", "table_name")
)

display(all_project_tables_df)

# COMMAND ----------

from pyspark.sql import Row

evidence_rows = [
    Row(
        capability="Bronze Ingestion",
        evidence_table="workspace.bronze.orders_raw",
        evidence_status="FOUND"
    ),
    Row(
        capability="Silver Transformation",
        evidence_table="workspace.silver.orders_clean",
        evidence_status="FOUND"
    ),
    Row(
        capability="Gold Fact",
        evidence_table="workspace.gold.fact_order_lines",
        evidence_status="FOUND"
    ),
    Row(
        capability="Gold Aggregation",
        evidence_table="workspace.gold.daily_sales",
        evidence_status="FOUND"
    ),
    Row(
        capability="Gold Dimension",
        evidence_table="workspace.gold.dim_product",
        evidence_status="FOUND"
    ),
    Row(
        capability="Incremental / CDC",
        evidence_table="workspace.gold.cdf_checkpoint",
        evidence_status="FOUND"
    ),
    Row(
        capability="SCD2 / Recovery",
        evidence_table="workspace.gold.dim_sku_scd2_recovery_sandbox",
        evidence_status="FOUND"
    ),
    Row(
        capability="Monitoring / Audit",
        evidence_table="workspace.gold.pipeline_run_audit",
        evidence_status="FOUND"
    ),
    Row(
        capability="Performance Benchmark",
        evidence_table="workspace.gold.spark_performance_benchmark",
        evidence_status="FOUND"
    ),
    Row(
        capability="Performance Improvement",
        evidence_table="workspace.gold.spark_performance_improvement_summary",
        evidence_status="FOUND"
    )
]

final_evidence_df = spark.createDataFrame(evidence_rows)

display(final_evidence_df)

# COMMAND ----------

from pyspark.sql import functions as F

gold_tables_df = spark.sql("""
SHOW TABLES IN workspace.gold
""")

dq_candidate_df = (
    gold_tables_df
    .filter(
        F.lower(F.col("tableName")).contains("dq")
        | F.lower(F.col("tableName")).contains("quality")
        | F.lower(F.col("tableName")).contains("reject")
        | F.lower(F.col("tableName")).contains("error")
        | F.lower(F.col("tableName")).contains("validation")
    )
    .select(
        F.col("tableName").alias("table_name")
    )
    .orderBy("table_name")
)

print(
    "DQ candidate tables:",
    dq_candidate_df.count()
)

display(dq_candidate_df)

# COMMAND ----------

from pyspark.sql import functions as F

silver_table = "workspace.silver.orders_clean"

silver_df = spark.table(silver_table)

total_rows = silver_df.count()

null_order_id = silver_df.filter(
    F.col("order_id").isNull()
).count()

null_sku_id = silver_df.filter(
    F.col("sku_id").isNull()
).count()

duplicate_order_sku = (
    silver_df
    .groupBy(
        "order_id",
        "sku_id"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_quantity = silver_df.filter(
    F.col("quantity").isNull()
    | (F.col("quantity") <= 0)
).count()

completeness_pct = round(
    ((total_rows - null_order_id) / total_rows) * 100,
    2
) if total_rows > 0 else 0.0

dq_summary_df = spark.createDataFrame(
    [
        ("NULL_ORDER_ID", null_order_id, 0),
        ("NULL_SKU_ID", null_sku_id, 0),
        ("DUPLICATE_ORDER_SKU", duplicate_order_sku, 0),
        ("INVALID_QUANTITY", invalid_quantity, 0),
    ],
    [
        "dq_check",
        "failed_rows",
        "allowed_failed_rows"
    ]
)

dq_summary_df = (
    dq_summary_df
    .withColumn(
        "dq_status",
        F.when(
            F.col("failed_rows") <= F.col("allowed_failed_rows"),
            F.lit("PASS")
        ).otherwise(
            F.lit("FAIL")
        )
    )
)

print("total rows:", total_rows)
print("completeness pct:", completeness_pct)

display(dq_summary_df)

# COMMAND ----------

from pyspark.sql import functions as F

dq_table = "workspace.gold.data_quality_summary"

final_dq_df = (
    dq_summary_df
    .withColumn("total_rows", F.lit(total_rows))
    .withColumn("completeness_pct", F.lit(completeness_pct))
    .withColumn("checked_at", F.current_timestamp())
)

(
    final_dq_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(dq_table)
)

display(
    spark.table(dq_table)
    .orderBy("dq_check")
)

# COMMAND ----------

from pyspark.sql import functions as F

audit_table = "workspace.gold.pipeline_run_audit"

failure_evidence_df = (
    spark.table(audit_table)
    .filter(F.col("run_status") == "FAILED")
    .select(
        "run_id",
        "batch_id",
        "run_status",
        "failed_step",
        "error_message",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "duration_seconds"
    )
    .orderBy(F.col("completed_at").desc())
)

print(
    "failed run count:",
    failure_evidence_df.count()
)

display(failure_evidence_df)

# COMMAND ----------

from pyspark.sql import Row

final_evidence_v2_rows = [
    Row(
        capability="Bronze Ingestion",
        evidence_table="workspace.bronze.orders_raw",
        evidence_status="FOUND"
    ),
    Row(
        capability="Silver Transformation",
        evidence_table="workspace.silver.orders_clean",
        evidence_status="FOUND"
    ),
    Row(
        capability="Gold Fact",
        evidence_table="workspace.gold.fact_order_lines",
        evidence_status="FOUND"
    ),
    Row(
        capability="Gold Aggregation",
        evidence_table="workspace.gold.daily_sales",
        evidence_status="FOUND"
    ),
    Row(
        capability="Gold Dimension",
        evidence_table="workspace.gold.dim_product",
        evidence_status="FOUND"
    ),
    Row(
        capability="Incremental / CDC",
        evidence_table="workspace.gold.cdf_checkpoint",
        evidence_status="FOUND"
    ),
    Row(
        capability="SCD2 / Recovery",
        evidence_table="workspace.gold.dim_sku_scd2_recovery_sandbox",
        evidence_status="FOUND"
    ),
    Row(
        capability="Data Quality",
        evidence_table="workspace.gold.data_quality_summary",
        evidence_status="FOUND"
    ),
    Row(
        capability="Monitoring / Audit",
        evidence_table="workspace.gold.pipeline_run_audit",
        evidence_status="FOUND"
    ),
    Row(
        capability="Troubleshooting / Failure",
        evidence_table="workspace.gold.pipeline_run_audit",
        evidence_status="FOUND"
    ),
    Row(
        capability="Performance Benchmark",
        evidence_table="workspace.gold.spark_performance_benchmark",
        evidence_status="FOUND"
    ),
    Row(
        capability="Performance Improvement",
        evidence_table="workspace.gold.spark_performance_improvement_summary",
        evidence_status="FOUND"
    )
]

final_evidence_v2_df = spark.createDataFrame(
    final_evidence_v2_rows
)

display(final_evidence_v2_df)

# COMMAND ----------

from pyspark.sql import Row

completion_rows = [
    Row(area="Bronze Ingestion", status="DONE"),
    Row(area="Silver Transformation", status="DONE"),
    Row(area="Gold Modeling", status="DONE"),
    Row(area="Incremental / CDC", status="DONE"),
    Row(area="SCD2 / Recovery", status="DONE"),
    Row(area="Data Quality", status="DONE"),
    Row(area="Monitoring / Audit", status="DONE"),
    Row(area="Troubleshooting", status="DONE"),
    Row(area="Performance Tuning", status="DONE"),
    Row(area="Portfolio Documentation", status="TODO"),
    Row(area="Architecture Diagram", status="TODO"),
    Row(area="GitHub Finalization", status="TODO"),
]

completion_df = spark.createDataFrame(completion_rows)

display(completion_df)

# COMMAND ----------

from pyspark.sql import Row

portfolio_evidence_rows = [
    Row(
        category="Architecture",
        evidence="Bronze -> Silver -> Gold flow",
        status="TODO"
    ),
    Row(
        category="Bronze",
        evidence="orders_raw table + ingestion output",
        status="FOUND"
    ),
    Row(
        category="Silver",
        evidence="orders_clean transformation output",
        status="FOUND"
    ),
    Row(
        category="Gold",
        evidence="fact_order_lines / dimensions / aggregations",
        status="FOUND"
    ),
    Row(
        category="Incremental",
        evidence="CDF checkpoint / incremental processing",
        status="FOUND"
    ),
    Row(
        category="SCD2",
        evidence="SCD2 recovery + duplicate repair",
        status="FOUND"
    ),
    Row(
        category="Data Quality",
        evidence="data_quality_summary",
        status="FOUND"
    ),
    Row(
        category="Monitoring",
        evidence="pipeline_run_audit success and failure",
        status="FOUND"
    ),
    Row(
        category="Troubleshooting",
        evidence="READ_SOURCE failure + error message",
        status="FOUND"
    ),
    Row(
        category="Performance",
        evidence="benchmark + improvement summary",
        status="FOUND"
    ),
    Row(
        category="Performance",
        evidence="execution plans / Photon / shuffle / broadcast",
        status="FOUND"
    ),
    Row(
        category="GitHub",
        evidence="README + screenshots + project structure",
        status="TODO"
    )
]

portfolio_evidence_df = spark.createDataFrame(
    portfolio_evidence_rows
)

display(portfolio_evidence_df)

# COMMAND ----------

from pyspark.sql import Row

screenshot_rows = [
    Row(
        evidence="Bronze ingestion",
        source="orders_raw table / ingestion output",
        capture="TODO"
    ),
    Row(
        evidence="Silver transformation",
        source="orders_clean output",
        capture="TODO"
    ),
    Row(
        evidence="Gold fact",
        source="fact_order_lines output",
        capture="TODO"
    ),
    Row(
        evidence="Incremental / CDC",
        source="cdf_checkpoint / incremental result",
        capture="TODO"
    ),
    Row(
        evidence="SCD2 recovery",
        source="duplicate repair + recovered current/history rows",
        capture="TODO"
    ),
    Row(
        evidence="Data Quality",
        source="Cell 305 - data_quality_summary",
        capture="TODO"
    ),
    Row(
        evidence="Monitoring success",
        source="pipeline_run_audit SUCCESS",
        capture="TODO"
    ),
    Row(
        evidence="Monitoring failure",
        source="Cell 306 - FAILED READ_SOURCE",
        capture="TODO"
    ),
    Row(
        evidence="Performance benchmark",
        source="Cell 298 - benchmark evidence",
        capture="TODO"
    ),
    Row(
        evidence="Performance improvement",
        source="Cell 297 / 298 - improvement %",
        capture="TODO"
    ),
    Row(
        evidence="Execution plan",
        source="Broadcast / Shuffle / Photon plan",
        capture="TODO"
    ),
    Row(
        evidence="Architecture",
        source="Bronze -> Silver -> Gold diagram",
        capture="TODO"
    )
]

screenshot_checklist_df = spark.createDataFrame(
    screenshot_rows
)

display(screenshot_checklist_df)

# COMMAND ----------

from pyspark.sql import Row

readiness_rows = [
    Row(area="Bronze Ingestion", status="DONE", weight=10, score=10),
    Row(area="Silver Transformation", status="DONE", weight=10, score=10),
    Row(area="Gold Modeling", status="DONE", weight=15, score=15),
    Row(area="Incremental / CDC", status="DONE", weight=10, score=10),
    Row(area="SCD2 / Recovery", status="DONE", weight=10, score=10),
    Row(area="Data Quality", status="DONE", weight=10, score=10),
    Row(area="Monitoring / Audit", status="DONE", weight=10, score=10),
    Row(area="Troubleshooting / Failure", status="DONE", weight=5, score=5),
    Row(area="Performance Tuning", status="DONE", weight=10, score=10),
    Row(area="Portfolio Documentation", status="TODO", weight=5, score=0),
    Row(area="Architecture Diagram", status="TODO", weight=5, score=0),
    Row(area="GitHub Finalization", status="TODO", weight=5, score=0),
]

readiness_df = spark.createDataFrame(readiness_rows)

display(readiness_df)

total_weight = sum(r.weight for r in readiness_rows)
total_score = sum(r.score for r in readiness_rows)

completion_pct = round(
    (total_score / total_weight) * 100,
    2
)

print("total weight:", total_weight)
print("total score:", total_score)
print("completion %:", completion_pct)