# Data Model Documentation

## Section 1: Entity-Relationship Diagram (Silver — 3NF)

```mermaid
erDiagram
    customers ||--o{ orders : "places"
    orders ||--|{ order_items : "contains"
    products ||--o{ order_items : "appears in"

    customers {
        string customer_id PK
        string customer_name
        string state
        string city
        string county
        string zip_code
        float latitude
        float longitude
        int loyalty_segment
    }
    
    orders {
        string order_number PK
        string customer_id FK
        timestamp order_timestamp
    }
    
    products {
        string product_id PK
        string product_name
        string category
        float list_price
        float weight_kg
    }
    
    order_items {
        string order_number PK, FK
        int item_seq PK
        string product_id FK
        int quantity
        float unit_price
        float unit_discount
    }
```

## Section 2: Star Schema (Gold — Dimensional)

```mermaid
erDiagram
    dim_customer ||--o{ fact_sales : "has"
    dim_product ||--o{ fact_sales : "sold in"
    dim_date ||--o{ fact_sales : "occurred on"

    dim_customer {
        int customer_sk PK
        string customer_id
        string customer_name
        string state
        string city
        float total_spend
        int total_orders
    }

    dim_product {
        int product_sk PK
        string product_id
        string product_name
        string category
        float list_price
    }

    dim_date {
        int date_key PK
        date full_date
        int year
        int month
    }

    fact_sales {
        int fact_id PK
        string order_number
        int item_seq
        int customer_sk FK
        int product_sk FK
        int date_key FK
        int quantity
        float unit_price
        float unit_discount
        float line_total
    }
```

## Section 3: Normalisation Rationale

- **1NF:** All columns are atomic. The nested ordered_products array in the raw JSON is exploded into the separate order_items entity with one row per line item. No repeating groups.
- **2NF:** All non-key attributes depend on the full primary key. In order_items (composite PK: order_number + item_seq), product_id, quantity, unit_price etc. all depend on the specific line item within a specific order — not on order_number alone.
- **3NF:** No transitive dependencies. Customer city/state could depend on zip_code, but since US zip codes don't map 1:1 to cities (P.O. boxes, multi-city zips), we keep them as direct attributes. Product details (name, category, price) are stored in the products entity, not repeated in order_items.
- **Many-to-many resolution:** The orders↔products M:N relationship is resolved through the order_items bridge table.
- **Deliberate denormalisation in Gold:** The dim_customer table pre-aggregates lifetime metrics (total_orders, total_spend, avg_order_value) that would require joining orders + order_items for every query. This trades storage for query performance and analyst convenience.

## Section 4: Key Strategy

- **Bronze**: Original keys preserved as-is, `_raw_row_hash` added for dedup detection.
- **Silver**: Natural keys (`customer_id`, `order_number`, `product_id`) cleaned and validated.
- **Gold**: Surrogate keys (`customer_sk`, `product_sk`) generated via `monotonically_increasing_id()` on dimensions; `date_key` is an integer (yyyyMMdd format); fact table references surrogate keys.
