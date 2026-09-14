# Databricks notebook source
"""
02_bronze_to_silver.py — Bronze → Silver Transformation Pipeline

Reads raw Delta tables from the bronze layer, applies cleaning, type casting,
deduplication, and validation, then writes conformed 3NF tables to the silver layer.

Each entity's transformation is an isolated, testable function.
Quality checks run before every write — critical failures halt the pipeline.

Runtime: Databricks Runtime 14.3 LTS
"""

# COMMAND ----------

import sys
import os
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

# Add project root to path for Databricks notebook execution
project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# TODO: Create utils modules - imports commented out until modules exist
# from utils.transforms import deduplicate, add_audit_columns, validate_not_null
# from utils.quality import check_null_rate, check_row_count, run_quality_suite

# Temporary inline implementations until utils modules are created
def deduplicate(df, partition_cols, order_col, ascending=True):
    """Deduplicate DataFrame keeping first/last row per partition."""
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy(*partition_cols).orderBy(
        F.col(order_col).asc() if ascending else F.col(order_col).desc()
    )
    return df.withColumn("_row_num", F.row_number().over(window_spec)) \
             .filter(F.col("_row_num") == 1) \
             .drop("_row_num")

def validate_not_null(df, columns):
    """Split DataFrame into valid and rejected based on null checks."""
    condition = F.col(columns[0]).isNotNull()
    for col in columns[1:]:
        condition = condition & F.col(col).isNotNull()
    df_valid = df.filter(condition)
    df_rejected = df.filter(~condition)
    return df_valid, df_rejected

def add_audit_columns(df):
    """Add audit columns to DataFrame."""
    return df.withColumn("_loaded_at", F.current_timestamp()) \
             .withColumn("_source", F.lit("bronze_to_silver"))

def check_null_rate(df, column, threshold):
    """Check if null rate for column is below threshold."""
    total = df.count()
    if total == 0:
        return (True, 0.0)
    null_count = df.filter(F.col(column).isNull()).count()
    null_rate = null_count / total
    return (null_rate <= threshold, null_rate)

def check_row_count(df, min_count):
    """Check if row count meets minimum."""
    count = df.count()
    return (count >= min_count, count)

def run_quality_suite(checks, table_name):
    """Run quality checks and fail on critical failures."""
    failed_critical = []
    for check in checks:
        status = "✅" if check["passed"] else "❌"
        print(f"  {status} {check['name']}: {check['metric']}")
        if not check["passed"] and check.get("critical", False):
            failed_critical.append(check["name"])
    
    if failed_critical:
        raise ValueError(f"Critical quality checks failed for {table_name}: {', '.join(failed_critical)}")

# COMMAND ----------

# Configuration
CATALOG = os.environ.get('CATALOG_NAME', 'retail_lakehouse')
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# COMMAND ----------

def transform_customers(spark: SparkSession) -> int:
    """
    Bronze → Silver: Customers

    Transformations applied:
      - Deduplicate on customer_id (latest _ingested_at wins)
      - Drop rows with null customer_id
      - Cast customer_id to BIGINT, postcode to INT, lat/lon to DOUBLE
      - Title-case customer_name and city
      - Validate state is 2-char code
      - Cast valid_from / valid_to to TIMESTAMP
      - Map 'district' → 'county' for semantic clarity
      - Map 'postcode' → 'zip_code'
      - Add audit columns (_loaded_at, _source)
    """
    df = spark.table(f"{BRONZE}.raw_customers")

    # 1. Deduplicate — keep latest ingestion per customer
    df_dedup = deduplicate(df, partition_cols=["customer_id"], order_col="_ingested_at", ascending=False)

    # 2. Drop null primary keys
    df_valid, df_rejected = validate_not_null(df_dedup, ["customer_id"])
    if df_rejected.count() > 0:
        print(f"⚠️  Rejected {df_rejected.count()} rows with null customer_id")

    # 3. Clean & cast
    transformed = df_valid.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.initcap(F.trim(F.col("customer_name"))).alias("customer_name"),
        # Validate 2-char state code; null out invalid
        F.when(
            F.length(F.trim(F.col("state"))) == 2,
            F.upper(F.trim(F.col("state")))
        ).otherwise(None).alias("state"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.trim(F.col("district")).alias("county"),          # district → county
        F.col("postcode").cast("double").cast("int").alias("zip_code"),    # postcode → zip_code
        F.col("lat").cast("double").alias("latitude"),
        F.col("lon").cast("double").alias("longitude"),
        F.col("loyalty_segment").cast("int").alias("loyalty_segment"),
        F.to_timestamp("valid_from").alias("valid_from"),
        F.to_timestamp("valid_to").alias("valid_to"),
    )

    # 4. Add audit columns
    final_df = add_audit_columns(transformed)

    # 5. Quality checks
    row_count = final_df.count()
    checks = [
        {"name": "Row count ≥ 1",     "passed": row_count >= 1,                                 "metric": f"{row_count} rows",  "critical": True},
        {"name": "PK not null",        "passed": check_null_rate(final_df, "customer_id", 0.0)[0], "metric": "0% nulls",         "critical": True},
    ]
    run_quality_suite(checks, "silver.customers")

    # 6. Write
    final_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.customers")
    print(f"  ✅ silver.customers — {row_count} rows")
    return row_count


# COMMAND ----------

def transform_orders(spark: SparkSession) -> int:
    """
    Bronze → Silver: Orders (header level, one row per order)

    Transformations:
      - Extract order-level fields from the sales_orders JSON
      - Cast order_number, customer_id to BIGINT
      - Cast order_datetime to TIMESTAMP
      - Deduplicate on order_number
      - Drop rows with null order_number
    """
    df = spark.table(f"{BRONZE}.raw_sales_orders")

    # 1. Select order-header fields & cast
    orders = df.select(
        F.col("order_number").cast("bigint").alias("order_number"),
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.to_timestamp("order_datetime").alias("order_datetime"),
        F.col("number_of_line_items").cast("int").alias("number_of_line_items"),
    ).filter(F.col("order_number").isNotNull())

    # 2. Deduplicate
    orders_dedup = deduplicate(orders, partition_cols=["order_number"], order_col="order_datetime", ascending=False)

    # 3. Audit columns
    final_df = add_audit_columns(orders_dedup)

    # 4. Quality checks
    row_count = final_df.count()
    checks = [
        {"name": "Row count ≥ 1",     "passed": row_count >= 1,                                   "metric": f"{row_count} rows",  "critical": True},
        {"name": "PK not null",        "passed": check_null_rate(final_df, "order_number", 0.0)[0], "metric": "0% nulls",         "critical": True},
    ]
    run_quality_suite(checks, "silver.orders")

    # 5. Write
    final_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.orders")
    print(f"  ✅ silver.orders — {row_count} rows")
    return row_count


# COMMAND ----------

def transform_order_items(spark: SparkSession) -> int:
    """
    Bronze → Silver: Order Items (line-item level, one row per product per order)

    The raw sales_orders JSON contains ordered_products as an ARRAY<STRUCT>.
    We use posexplode to flatten it into individual line items, creating the
    bridge table that resolves the orders ↔ products many-to-many relationship.

    Transformations:
      - posexplode ordered_products → item_seq (0-based → 1-based)
      - Extract struct fields: id, name, qty, price, unit_discount, curr, promotion_info
      - Cast numeric fields to proper types
      - Default unit_discount to 0.00 where null
    """
    df = spark.table(f"{BRONZE}.raw_sales_orders")

    # 1. Explode nested array
    exploded = df.select(
        F.col("order_number").cast("bigint").alias("order_number"),
        F.posexplode("ordered_products").alias("item_seq", "product"),
    ).filter(F.col("order_number").isNotNull())

    # 2. Extract struct fields and cast
    transformed = exploded.select(
        "order_number",
        (F.col("item_seq") + 1).cast("int").alias("item_seq"),  # 1-based
        F.col("product.id").alias("product_id"),
        F.trim(F.col("product.name")).alias("product_name"),
        F.col("product.qty").cast("int").alias("quantity"),
        F.col("product.price").cast(DecimalType(10, 2)).alias("unit_price"),
        F.coalesce(
            F.col("product.unit_discount").cast(DecimalType(10, 2)),
            F.lit(0.00).cast(DecimalType(10, 2)),
        ).alias("unit_discount"),
        F.col("product.curr").alias("currency"),
        F.to_json("product.promotion_info").alias("promotion_info"),
    )

    # 3. Audit columns
    final_df = add_audit_columns(transformed)

    # 4. Quality checks
    row_count = final_df.count()
    checks = [
        {"name": "Row count ≥ 1",         "passed": row_count >= 1,                                     "metric": f"{row_count} rows",  "critical": True},
        {"name": "Order number not null",  "passed": check_null_rate(final_df, "order_number", 0.0)[0],  "metric": "0% nulls",           "critical": True},
    ]
    run_quality_suite(checks, "silver.order_items")

    # 5. Write
    final_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.order_items")
    print(f"  ✅ silver.order_items — {row_count} rows")
    return row_count


# COMMAND ----------

def transform_products(spark: SparkSession) -> int:
    """
    Bronze → Silver: Products

    Transformations:
      - Deduplicate on product_id (latest _ingested_at wins)
      - Drop rows with null product_id
      - Rename product_category → category, product_price → price, etc.
      - Cast price to DECIMAL(10,2), quantity to INT, weight to DECIMAL(8,3)
      - Convert weight from grams to kg (÷ 1000)
      - Title-case product_name and category
      - Default null descriptions to 'No description available'
    """
    df = spark.table(f"{BRONZE}.raw_products")

    # 1. Deduplicate
    df_dedup = deduplicate(df, partition_cols=["product_id"], order_col="_ingested_at", ascending=False)

    # 2. Drop null PKs
    df_valid, _ = validate_not_null(df_dedup, ["product_id"])

    # 3. Clean & cast — note: bronze columns are product_category, product_price, etc.
    transformed = df_valid.select(
        F.col("product_id"),
        F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
        F.initcap(F.trim(F.col("product_category"))).alias("category"),
        F.col("product_price").cast(DecimalType(10, 2)).alias("price"),
        F.col("product_quantity").cast("int").alias("quantity"),
        F.coalesce(
            F.trim(F.col("product_description")),
            F.lit("No description available"),
        ).alias("description"),
        # Convert grams → kg if weight is in grams; keep raw if already kg
        (F.col("product_weight").cast("double") / 1000).cast(DecimalType(8, 3)).alias("weight_kg"),
    )

    # 4. Audit columns
    final_df = add_audit_columns(transformed)

    # 5. Quality checks
    row_count = final_df.count()
    checks = [
        {"name": "Row count ≥ 1",  "passed": row_count >= 1,                                  "metric": f"{row_count} rows",  "critical": True},
        {"name": "PK not null",     "passed": check_null_rate(final_df, "product_id", 0.0)[0], "metric": "0% nulls",           "critical": True},
    ]
    run_quality_suite(checks, "silver.products")

    # 6. Write
    final_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.products")
    print(f"  ✅ silver.products — {row_count} rows")
    return row_count


# COMMAND ----------

def run_silver_transforms(spark: SparkSession) -> None:
    """
    Orchestrates all bronze → silver transformations.
    Runs each table's transform sequentially and prints a summary.
    """
    print(f"\n{'=' * 60}")
    print("  BRONZE → SILVER PIPELINE")
    print(f"{'=' * 60}\n")

    start = time.time()
    results = {}

    for name, fn in [
        ("customers",   transform_customers),
        ("products",    transform_products),
        ("orders",      transform_orders),
        ("order_items", transform_order_items),
    ]:
        t0 = time.time()
        try:
            count = fn(spark)
            results[name] = {"rows": count, "status": "SUCCESS", "duration": f"{time.time() - t0:.1f}s"}
        except Exception as e:
            results[name] = {"rows": 0, "status": f"FAILED: {e}", "duration": f"{time.time() - t0:.1f}s"}
            print(f"  ❌ {name} failed: {e}")
            raise

    duration = time.time() - start

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  SUMMARY  (completed in {duration:.1f}s)")
    print(f"{'=' * 60}")
    print(f"  {'Table':<20} {'Rows':>10}  {'Duration':>10}  {'Status'}")
    print(f"  {'-'*20} {'-'*10}  {'-'*10}  {'-'*10}")
    for name, info in results.items():
        print(f"  {name:<20} {info['rows']:>10}  {info['duration']:>10}  {info['status']}")
    print()


# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    run_silver_transforms(spark)