-- =============================================================================
-- analytics_queries.sql — Gold Layer Analytics Queries
-- =============================================================================
-- Databricks SQL queries against the retail_lakehouse.gold star schema.
-- Each query answers a distinct business question and is ready for
-- dashboarding or ad-hoc stakeholder requests.
--
-- Tables used:
--   retail_lakehouse.gold.fact_sales
--   retail_lakehouse.gold.dim_customer
--   retail_lakehouse.gold.dim_product
--   retail_lakehouse.gold.dim_date
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Query 1: Top 10 Customers by Lifetime Spend
-- ---------------------------------------------------------------------------
-- Business Question:
--   Who are our most valuable customers, and what does their purchasing
--   profile look like? This powers VIP retention and white-glove programs.
-- ---------------------------------------------------------------------------
SELECT
    c.customer_name,
    c.state,
    c.city,
    c.loyalty_segment,
    c.total_orders,
    c.total_spend,
    c.avg_order_value,
    c.first_order_date,
    c.last_order_date
FROM retail_lakehouse.gold.dim_customer c
WHERE c.total_orders > 0
ORDER BY c.total_spend DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- Query 2: Monthly Revenue Trend with Month-over-Month Growth
-- ---------------------------------------------------------------------------
-- Business Question:
--   How is our revenue trending month over month? Where are the inflection
--   points? This is the first chart a CFO/COO checks on Monday morning.
-- ---------------------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        ROUND(SUM(f.line_total), 2) AS monthly_revenue
    FROM retail_lakehouse.gold.fact_sales f
    JOIN retail_lakehouse.gold.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
)
SELECT
    year,
    month,
    month_name,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month), 2
    ) AS mom_change,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month))
        / NULLIF(LAG(monthly_revenue) OVER (ORDER BY year, month), 0) * 100,
        2
    ) AS mom_pct_change
FROM monthly_revenue
ORDER BY year, month;


-- ---------------------------------------------------------------------------
-- Query 3: Outlier Detection — High-Value Orders (Z-Score)
-- ---------------------------------------------------------------------------
-- Business Question:
--   Which orders are statistically unusual? Flags potential wholesale
--   purchases, pricing errors, or fraudulent transactions for review.
-- ---------------------------------------------------------------------------
WITH order_totals AS (
    SELECT
        f.order_number,
        MAX(c.customer_name)        AS customer_name,
        ROUND(SUM(f.line_total), 2) AS order_total
    FROM retail_lakehouse.gold.fact_sales f
    JOIN retail_lakehouse.gold.dim_customer c ON f.customer_sk = c.customer_sk
    GROUP BY f.order_number
),
stats AS (
    SELECT
        AVG(order_total)    AS mean_total,
        STDDEV(order_total) AS stddev_total
    FROM order_totals
),
z_scores AS (
    SELECT
        o.order_number,
        o.customer_name,
        o.order_total,
        ROUND((o.order_total - s.mean_total) / NULLIF(s.stddev_total, 0), 3) AS z_score
    FROM order_totals o
    CROSS JOIN stats s
)
SELECT
    order_number,
    customer_name,
    order_total,
    z_score,
    CASE WHEN ABS(z_score) > 2 THEN 'OUTLIER' ELSE 'NORMAL' END AS outlier_flag
FROM z_scores
WHERE ABS(z_score) > 2
ORDER BY z_score DESC;


-- ---------------------------------------------------------------------------
-- Query 4: Revenue by State (Geographic Breakdown)
-- ---------------------------------------------------------------------------
-- Business Question:
--   How does revenue distribute geographically? Which states are over- or
--   under-penetrated relative to their customer base? Guides regional
--   marketing spend and supply chain decisions.
-- ---------------------------------------------------------------------------
WITH state_totals AS (
    SELECT
        c.state,
        ROUND(SUM(f.line_total), 2)           AS state_revenue,
        COUNT(DISTINCT f.order_number)         AS total_orders,
        COUNT(DISTINCT f.customer_sk)          AS unique_customers,
        ROUND(AVG(f.line_total), 2)            AS avg_line_value
    FROM retail_lakehouse.gold.fact_sales f
    JOIN retail_lakehouse.gold.dim_customer c ON f.customer_sk = c.customer_sk
    GROUP BY c.state
),
total_revenue AS (
    SELECT SUM(state_revenue) AS overall_revenue FROM state_totals
)
SELECT
    st.state,
    st.state_revenue,
    st.total_orders,
    st.unique_customers,
    st.avg_line_value,
    ROUND(st.state_revenue / tr.overall_revenue * 100, 2) AS revenue_pct
FROM state_totals st
CROSS JOIN total_revenue tr
ORDER BY st.state_revenue DESC;


-- ---------------------------------------------------------------------------
-- Query 5: Product Category Performance with Discount Impact
-- ---------------------------------------------------------------------------
-- Business Question:
--   How do our product categories perform, and is discounting driving
--   volume or simply eroding margins? Used to optimise pricing strategy
--   and promotional spend by category.
-- ---------------------------------------------------------------------------
SELECT
    p.category,
    SUM(f.quantity)                                                AS units_sold,
    ROUND(SUM(f.line_total), 2)                                   AS category_revenue,
    ROUND(SUM(f.unit_discount * f.quantity), 2)                    AS total_discounts,
    ROUND(AVG(f.unit_discount / NULLIF(f.unit_price, 0)) * 100, 2) AS avg_discount_rate_pct,
    ROUND(SUM(f.line_total) / NULLIF(SUM(f.quantity), 0), 2)      AS revenue_per_unit
FROM retail_lakehouse.gold.fact_sales f
JOIN retail_lakehouse.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.category
ORDER BY category_revenue DESC;


-- ---------------------------------------------------------------------------
-- Query 6 (Stakeholder Ad-Hoc):
--   "Which loyalty segments are most profitable and are any declining?"
-- ---------------------------------------------------------------------------
-- Business Question:
--   How does profitability and engagement vary across loyalty tiers?
--   Are any segments shrinking quarter-over-quarter? Guides loyalty
--   programme investment and retention resource allocation.
-- ---------------------------------------------------------------------------
WITH segment_stats AS (
    SELECT
        c.loyalty_segment,
        COUNT(DISTINCT c.customer_sk)  AS customer_count,
        ROUND(AVG(c.total_spend), 2)   AS avg_spend,
        ROUND(AVG(c.total_orders), 1)  AS avg_orders,
        ROUND(SUM(f.line_total), 2)    AS segment_revenue
    FROM retail_lakehouse.gold.dim_customer c
    JOIN retail_lakehouse.gold.fact_sales f ON c.customer_sk = f.customer_sk
    GROUP BY c.loyalty_segment
),
total_rev AS (
    SELECT SUM(segment_revenue) AS all_rev FROM segment_stats
),
quarterly_trend AS (
    SELECT
        c.loyalty_segment,
        d.year,
        d.quarter,
        COUNT(DISTINCT f.order_number) AS quarterly_orders,
        ROUND(SUM(f.line_total), 2)    AS quarterly_revenue
    FROM retail_lakehouse.gold.fact_sales f
    JOIN retail_lakehouse.gold.dim_customer c ON f.customer_sk = c.customer_sk
    JOIN retail_lakehouse.gold.dim_date d ON f.date_key = d.date_key
    GROUP BY c.loyalty_segment, d.year, d.quarter
)
SELECT
    s.loyalty_segment,
    s.customer_count,
    s.avg_spend,
    s.avg_orders,
    ROUND(s.segment_revenue / tr.all_rev * 100, 2) AS revenue_share_pct,
    qt.year,
    qt.quarter,
    qt.quarterly_orders,
    qt.quarterly_revenue
FROM segment_stats s
CROSS JOIN total_rev tr
LEFT JOIN quarterly_trend qt ON s.loyalty_segment = qt.loyalty_segment
ORDER BY s.loyalty_segment, qt.year, qt.quarter;
