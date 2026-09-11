-- Create schemas
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold;

-- BRONZE LAYER TABLES
CREATE TABLE IF NOT EXISTS retail_lakehouse.bronze.raw_customers (
    customer_id STRING,
    tax_id STRING,
    tax_code STRING,
    customer_name STRING,
    state STRING,
    city STRING,
    postcode STRING,
    street STRING,
    number STRING,
    unit STRING,
    region STRING,
    district STRING,
    lon STRING,
    lat STRING,
    ship_to_address STRING,
    valid_from STRING,
    valid_to STRING,
    units_purchased STRING,
    loyalty_segment STRING,
    _corrupt_record STRING,
    _source_file STRING,
    _ingested_at TIMESTAMP,
    _raw_row_hash STRING
)
USING DELTA
COMMENT 'Raw customer data ingested from CSV'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.bronze.raw_sales_orders (
    customer_id STRING,
    customer_name STRING,
    number_of_line_items STRING,
    order_datetime STRING,
    order_number STRING,
    ordered_products ARRAY<STRUCT<
        curr: STRING,
        id: STRING,
        name: STRING,
        price: STRING,
        promotion_info: STRUCT<promotion_id: STRING, promotion_name: STRING>,
        qty: STRING,
        unit_discount: STRING
    >>,
    _source_file STRING,
    _ingested_at TIMESTAMP,
    _raw_row_hash STRING
)
USING DELTA
COMMENT 'Raw sales orders data ingested from JSON'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.bronze.raw_products (
    product_id STRING,
    product_name STRING,
    product_category STRING,
    product_price STRING,
    product_quantity STRING,
    product_description STRING,
    product_weight STRING,
    _corrupt_record STRING,
    _source_file STRING,
    _ingested_at TIMESTAMP,
    _raw_row_hash STRING
)
USING DELTA
COMMENT 'Raw product data ingested from CSV'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');


-- SILVER LAYER TABLES
CREATE TABLE IF NOT EXISTS retail_lakehouse.silver.customers (
    customer_id BIGINT COMMENT 'Primary Key',
    customer_name STRING,
    state STRING,
    city STRING,
    county STRING,
    zip_code INT,
    latitude DOUBLE,
    longitude DOUBLE,
    loyalty_segment INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    _loaded_at TIMESTAMP,
    _source STRING
)
USING DELTA
COMMENT 'Cleaned and typed customer data (3NF)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.silver.orders (
    order_number BIGINT COMMENT 'Primary Key',
    customer_id BIGINT COMMENT 'Foreign Key to customers',
    order_datetime TIMESTAMP,
    number_of_line_items INT,
    _loaded_at TIMESTAMP,
    _source STRING
)
USING DELTA
COMMENT 'Cleaned order header data (3NF)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.silver.order_items (
    order_number BIGINT COMMENT 'Composite PK part 1',
    item_seq INT COMMENT 'Composite PK part 2',
    product_id STRING COMMENT 'Foreign Key to products',
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    unit_discount DECIMAL(10,2),
    currency STRING,
    promotion_info STRING,
    _loaded_at TIMESTAMP,
    _source STRING
)
USING DELTA
COMMENT 'Cleaned order line items data (3NF)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.silver.products (
    product_id STRING COMMENT 'Primary Key',
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    quantity INT,
    description STRING,
    weight_kg DECIMAL(8,3),
    _loaded_at TIMESTAMP,
    _source STRING
)
USING DELTA
COMMENT 'Cleaned product catalog data (3NF)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');


-- GOLD LAYER TABLES
CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.dim_customer (
    customer_sk BIGINT COMMENT 'Surrogate Key',
    customer_id BIGINT,
    customer_name STRING,
    state STRING,
    city STRING,
    county STRING,
    zip_code INT,
    latitude DOUBLE,
    longitude DOUBLE,
    loyalty_segment INT,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INT,
    total_items_purchased INT,
    total_spend DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    _loaded_at TIMESTAMP
)
USING DELTA
COMMENT 'Customer dimension table with lifetime metrics'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.dim_product (
    product_sk BIGINT COMMENT 'Surrogate Key',
    product_id STRING,
    product_name STRING,
    category STRING,
    list_price DECIMAL(10,2),
    weight_kg DECIMAL(8,3),
    total_units_sold INT,
    total_revenue DECIMAL(12,2),
    avg_discount DECIMAL(10,2),
    _loaded_at TIMESTAMP
)
USING DELTA
COMMENT 'Product dimension table with aggregate sales metrics'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.dim_date (
    date_key INT COMMENT 'Surrogate Key (yyyyMMdd)',
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN,
    _loaded_at TIMESTAMP
)
USING DELTA
COMMENT 'Date dimension table'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.fact_sales (
    order_number BIGINT,
    item_seq INT,
    customer_sk BIGINT COMMENT 'Foreign Key to dim_customer',
    product_sk BIGINT COMMENT 'Foreign Key to dim_product',
    date_key INT COMMENT 'Foreign Key to dim_date',
    quantity INT,
    unit_price DECIMAL(10,2),
    unit_discount DECIMAL(10,2),
    line_total DECIMAL(12,2),
    _loaded_at TIMESTAMP
)
USING DELTA
COMMENT 'Sales fact table'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');
