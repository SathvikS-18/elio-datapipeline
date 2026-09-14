# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""
03_silver_to_gold.py — Silver → Gold Transformation Pipeline

WHY NORMALISE IN SILVER AND DENORMALISE IN GOLD?
  Silver is normalised (3NF) to serve as a single source of truth — correct,
  non-redundant, entity-aligned. Any downstream consumer can rely on it.

  Gold denormalises into a star schema because downstream analytics (BI tools,
  SQL queries, dashboards) need fast aggregations without join sprawl.
  Pre-computing dim_customer with lifetime metrics avoids repeated expensive
  joins across orders + order_items for every dashboard query. The star schema
  also makes the data self-describing for analysts unfamiliar with the
  normalised model.

Execution order: dim_date → dim_customer → dim_product → fact_sales
(Dimensions MUST be built before the fact table for surrogate key lookups.)

Runtime: Databricks Runtime 14.3 LTS
"""

# COMMAND ----------

import sys
import os
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, os.path.dirname(__file__))
except NameError:
    pass

from utils.quality import check_referential_integrity, check_row_count, run_quality_suite

# COMMAND ----------

# Configuration
CATALOG = os.environ.get('CATALOG_NAME', 'retail_lakehouse')
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# COMMAND ----------

def build_dim_date(spark: SparkSession) -> int:
    """
    Generate a comprehensive date dimension covering 2019-01-01 to 2025-12-31.

    Date key format: yyyyMMdd as INT (e.g. 20240115).
    Includes year, quarter, month, week, day, day-of-week, weekend flag.
    """
    # Generate date range using Spark SQL sequence
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW _date_range AS
        SELECT explode(
            sequence(to_date('2019-01-01'), to_date('2025-12-31'), interval 1 day)
        ) AS full_date
    """)

    dim_date = spark.table("_date_range").select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        F.current_timestamp().alias("_loaded_at"),
    )

    dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_date")
    count = dim_date.count()
    print(f"  ✅ gold.dim_date — {count} rows")
    return count


# COMMAND ----------

def build_dim_customer(spark: SparkSession) -> int:
    """
    Build customer dimension with pre-aggregated lifetime metrics.

    Joins silver.customers with silver.orders and silver.order_items to compute:
      - first_order_date, last_order_date
      - total_orders (count distinct)
      - total_items_purchased (sum quantity)
      - total_spend (sum line_total)
      - avg_order_value

    Left join ensures customers with zero orders are retained (metrics default to 0).
    NOTE: customer_sk is generated via monotonically_increasing_id() at write time.
    """
    customers = spark.table(f"{SILVER}.customers")
    orders = spark.table(f"{SILVER}.orders")
    items = spark.table(f"{SILVER}.order_items")

    # Compute line-level totals
    items_with_total = items.withColumn(
        "line_total",
        (F.col("quantity") * F.col("unit_price")) - F.col("unit_discount"),
    )

    # Aggregate per customer via orders ← items join
    customer_metrics = (
        orders.join(items_with_total, "order_number", "inner")
        .groupBy("customer_id")
        .agg(
            F.min(F.to_date("order_datetime")).alias("first_order_date"),
            F.max(F.to_date("order_datetime")).alias("last_order_date"),
            F.countDistinct("order_number").cast("int").alias("total_orders"),
            F.sum("quantity").cast("int").alias("total_items_purchased"),
            F.round(F.sum("line_total"), 2).cast(DecimalType(12, 2)).alias("total_spend"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_spend") / F.col("total_orders"), 2).cast(DecimalType(10, 2)),
        )
    )

    # Left join to keep all customers (even those with no orders)
    dim_customer = (
        customers.join(customer_metrics, "customer_id", "left")
        .select(
            F.monotonically_increasing_id().alias("customer_sk"),
            "customer_id",
            "customer_name",
            "state",
            "city",
            "county",
            "zip_code",
            "latitude",
            "longitude",
            "loyalty_segment",
            "first_order_date",
            "last_order_date",
            F.coalesce("total_orders", F.lit(0)).alias("total_orders"),
            F.coalesce("total_items_purchased", F.lit(0)).alias("total_items_purchased"),
            F.coalesce("total_spend", F.lit(0.00).cast(DecimalType(12, 2))).alias("total_spend"),
            F.coalesce("avg_order_value", F.lit(0.00).cast(DecimalType(10, 2))).alias("avg_order_value"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )

    dim_customer.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_customer")
    count = dim_customer.count()
    print(f"  ✅ gold.dim_customer — {count} rows")
    return count


# COMMAND ----------

def build_dim_product(spark: SparkSession) -> int:
    """
    Build product dimension with aggregate sales metrics.

    Joins silver.products with silver.order_items to compute:
      - total_units_sold
      - total_revenue
      - avg_discount

    Left join ensures unsold products are retained (metrics default to 0).
    """
    products = spark.table(f"{SILVER}.products")
    items = spark.table(f"{SILVER}.order_items")

    product_metrics = (
        items.groupBy("product_id")
        .agg(
            F.sum("quantity").cast("int").alias("total_units_sold"),
            F.round(
                F.sum((F.col("quantity") * F.col("unit_price")) - F.col("unit_discount")), 2
            ).cast(DecimalType(12, 2)).alias("total_revenue"),
            F.round(F.avg("unit_discount"), 2).cast(DecimalType(10, 2)).alias("avg_discount"),
        )
    )

    dim_product = (
        products.join(product_metrics, "product_id", "left")
        .select(
            F.monotonically_increasing_id().alias("product_sk"),
            "product_id",
            "product_name",
            "category",
            F.col("price").alias("list_price"),
            "weight_kg",
            F.coalesce("total_units_sold", F.lit(0)).alias("total_units_sold"),
            F.coalesce("total_revenue", F.lit(0.00).cast(DecimalType(12, 2))).alias("total_revenue"),
            F.coalesce("avg_discount", F.lit(0.00).cast(DecimalType(10, 2))).alias("avg_discount"),
            F.current_timestamp().alias("_loaded_at"),
        )
    )

    dim_product.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_product")
    count = dim_product.count()
    print(f"  ✅ gold.dim_product — {count} rows")
    return count


# COMMAND ----------

def build_fact_sales(spark: SparkSession) -> int:
    """
    Build the sales fact table at line-item grain.

    Joins silver.order_items × silver.orders to get the order timestamp,
    then looks up surrogate keys from the gold dimensions.

    Fact columns:
      - order_number, item_seq (degenerate dimensions / grain)
      - customer_sk, product_sk, date_key (foreign keys to dims)
      - quantity, unit_price, unit_discount (measures)
      - line_total = (quantity × unit_price) − unit_discount
    """
    orders = spark.table(f"{SILVER}.orders")
    items = spark.table(f"{SILVER}.order_items")
    dim_cust = spark.table(f"{GOLD}.dim_customer").select("customer_id", "customer_sk")
    dim_prod = spark.table(f"{GOLD}.dim_product").select("product_id", "product_sk")

    # Base: join items with orders to get customer_id + order_datetime
    base = items.join(
        orders.select("order_number", "customer_id", "order_datetime"),
        "order_number",
        "inner",
    )

    # Derive date_key and line_total
    enriched = (
        base
        .withColumn("date_key", F.date_format("order_datetime", "yyyyMMdd").cast("int"))
        .withColumn(
            "line_total",
            F.round(
                (F.col("quantity") * F.col("unit_price")) - F.col("unit_discount"), 2
            ).cast(DecimalType(12, 2)),
        )
    )

    # Surrogate key lookups
    fact_sales = (
        enriched
        .join(dim_cust, "customer_id", "left")
        .join(dim_prod, "product_id", "left")
        .select(
            "order_number",
            "item_seq",
            "customer_sk",
            "product_sk",
            "date_key",
            "quantity",
            "unit_price",
            "unit_discount",
            "line_total",
            F.current_timestamp().alias("_loaded_at"),
        )
    )

    # Referential integrity checks
    ri_checks = [
        {
            "name": "Customer RI",
            "passed": check_referential_integrity(fact_sales, dim_cust, "customer_sk")[0],
            "metric": "All customer_sk found in dim_customer",
            "critical": False,  # warn, don't halt — orphans possible for late-arriving dims
        },
        {
            "name": "Product RI",
            "passed": check_referential_integrity(fact_sales, dim_prod, "product_sk")[0],
            "metric": "All product_sk found in dim_product",
            "critical": False,
        },
        {
            "name": "Row count ≥ 1",
            "passed": check_row_count(fact_sales)[0],
            "metric": f"{fact_sales.count()} rows",
            "critical": True,
        },
    ]
    run_quality_suite(ri_checks, "gold.fact_sales")

    fact_sales.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.fact_sales")
    count = fact_sales.count()
    print(f"  ✅ gold.fact_sales — {count} rows")
    return count


# COMMAND ----------

def run_gold_transforms(spark: SparkSession) -> None:
    """
    Orchestrates silver → gold transformation pipeline.

    Execution order matters:
      1. dim_date     (no dependencies)
      2. dim_customer (reads silver.customers + orders + order_items)
      3. dim_product  (reads silver.products + order_items)
      4. fact_sales   (reads silver + looks up dim surrogate keys)
    """
    print(f"\n{'=' * 60}")
    print("  SILVER → GOLD PIPELINE")
    print(f"{'=' * 60}\n")

    start = time.time()
    results = {}

    for name, fn in [
        ("dim_date",     build_dim_date),
        ("dim_customer", build_dim_customer),
        ("dim_product",  build_dim_product),
        ("fact_sales",   build_fact_sales),
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
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    run_gold_transforms(spark)