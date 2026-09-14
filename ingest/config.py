"""
config.py — Centralised configuration for the retail lakehouse pipeline.

All source paths, schema definitions, and catalog/schema names are defined here.
Override CATALOG_NAME via environment variable for different environments.

Runtime: Databricks Runtime 14.3 LTS (Spark 3.5.1, Delta 3.2.0)
"""

import os
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

# ---------------------------------------------------------------------------
# Catalog & Schema Configuration
# ---------------------------------------------------------------------------
CATALOG = os.environ.get('CATALOG_NAME', 'retail_lakehouse')

BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'

# ---------------------------------------------------------------------------
# Source File Paths (pre-loaded in every Databricks workspace)
# ---------------------------------------------------------------------------
SOURCE_PATHS = {
    'customers': '/databricks-datasets/retail-org/customers/customers.csv',
    'sales_orders': '/databricks-datasets/retail-org/sales_orders/',
    'products': '/databricks-datasets/retail-org/products/products.csv',
}


def full_table_name(schema: str, table: str) -> str:
    """Returns fully qualified table name: catalog.schema.table."""
    return f"{CATALOG}.{schema}.{table}"


# ---------------------------------------------------------------------------
# Explicit Schemas
# ---------------------------------------------------------------------------
# NOTE: The retail-org customers CSV has a wide schema. We read ALL columns
# as strings in bronze (raw-land, no cleaning). Silver will cast & rename.
# Using StringType for all bronze columns ensures no data loss from inference.

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("tax_id", StringType(), True),
    StructField("tax_code", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("street", StringType(), True),
    StructField("number", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("region", StringType(), True),
    StructField("district", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("ship_to_address", StringType(), True),
    StructField("valid_from", StringType(), True),
    StructField("valid_to", StringType(), True),
    StructField("units_purchased", StringType(), True),
    StructField("loyalty_segment", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# Sales orders — schema inferred from JSON (nested ordered_products array).
# ordered_products contains structs with: id, name, price, qty, curr,
# promotion_info (nested struct), unit_discount.
SALES_ORDERS_SCHEMA = None

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", StringType(), True),
    StructField("product_quantity", StringType(), True),
    StructField("product_description", StringType(), True),
    StructField("product_weight", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])
