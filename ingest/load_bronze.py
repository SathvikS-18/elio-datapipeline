# Databricks notebook source
"""
load_bronze.py
Author: Data Engineer
Purpose: PySpark script to ingest raw sources into bronze Delta tables.
Runtime Requirements: Databricks Runtime 14.3 LTS (Spark 3.5.1, Delta 3.2.0)

Assumptions:
- Raw data is loaded AS-IS with no cleaning.
- _corrupt_record column captures malformed CSV rows.
- JSON schema is inferred (ordered_products is a nested array).
- All timestamps are preserved as strings in bronze.
- Overwrite mode used for idempotent re-runs.
"""

# COMMAND ----------

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, sha2, concat_ws, col

from config import (
    CATALOG, BRONZE_SCHEMA, SOURCE_PATHS, full_table_name,
    CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA
)

# COMMAND ----------

def create_bronze_schemas(spark: SparkSession):
    """Creates the catalog and bronze schema if they don't exist."""
    print(f"Ensuring catalog '{CATALOG}' exists...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    
    print(f"Ensuring schema '{CATALOG}.{BRONZE_SCHEMA}' exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

def add_metadata(df, source_name: str):
    """Adds auditing metadata to the dataframe."""
    return df \
        .withColumn("_source_file", lit(source_name)) \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_raw_row_hash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256))

# COMMAND ----------

def load_customers_bronze(spark: SparkSession) -> int:
    """Ingests customers data into bronze."""
    print("Loading customers bronze table...")
    path = SOURCE_PATHS['customers']
    table_name = full_table_name(BRONZE_SCHEMA, 'raw_customers')
    
    df = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(CUSTOMERS_SCHEMA) \
        .csv(path)
    
    df = add_metadata(df, path)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    return spark.table(table_name).count()

# COMMAND ----------

def load_sales_orders_bronze(spark: SparkSession) -> int:
    """Ingests sales_orders data into bronze."""
    print("Loading sales_orders bronze table...")
    path = SOURCE_PATHS['sales_orders']
    table_name = full_table_name(BRONZE_SCHEMA, 'raw_sales_orders')
    
    # JSON schema inferred, multiLine=False by default for standard JSON lines
    df = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(path)
    
    df = add_metadata(df, path)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    return spark.table(table_name).count()

# COMMAND ----------

def load_products_bronze(spark: SparkSession) -> int:
    """Ingests products data into bronze."""
    print("Loading products bronze table...")
    path = SOURCE_PATHS['products']
    table_name = full_table_name(BRONZE_SCHEMA, 'raw_products')
    
    df = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(PRODUCTS_SCHEMA) \
        .csv(path)
    
    df = add_metadata(df, path)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    return spark.table(table_name).count()

# COMMAND ----------

def run_bronze_ingestion(spark: SparkSession):
    """Main orchestrator function to load all bronze tables."""
    print(f"{'='*50}\nStarting Bronze Ingestion\n{'='*50}")
    
    try:
        create_bronze_schemas(spark)
    except Exception as e:
        print(f"Error creating schemas: {e}")
        return
        
    summary = []
    
    loaders = [
        ('customers', load_customers_bronze),
        ('sales_orders', load_sales_orders_bronze),
        ('products', load_products_bronze)
    ]
    
    for source, loader_func in loaders:
        start_time = time.time()
        try:
            row_count = loader_func(spark)
            duration = time.time() - start_time
            summary.append((source, row_count, "SUCCESS", f"{duration:.2f}s"))
            print(f"-> Successfully loaded {source} ({row_count} rows in {duration:.2f}s)")
        except Exception as e:
            duration = time.time() - start_time
            summary.append((source, 0, "FAILED", f"{duration:.2f}s"))
            print(f"-> Failed to load {source}: {e}")
            
    print(f"\n{'='*50}\nIngestion Summary\n{'='*50}")
    print(f"{'Source':<15} | {'Rows Loaded':<12} | {'Status':<10} | {'Duration':<10}")
    print("-" * 55)
    for row in summary:
        print(f"{row[0]:<15} | {row[1]:<12} | {row[2]:<10} | {row[3]:<10}")
    print(f"{'='*50}")

# COMMAND ----------

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .getOrCreate()
    run_bronze_ingestion(spark)
