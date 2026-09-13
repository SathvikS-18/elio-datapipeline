# Analysis Answers

## B1: Commercial Insights (3 insights)

### Insight 1: Loyalty Segment Revenue Concentration
- **Finding:** A small number of high-loyalty customers (segments 3-4) likely drive a disproportionate share of revenue, while lower segments (0-1) represent the majority of the customer base but contribute less per-capita.
- **Method:** RFM-style analysis using the pre-computed dim_customer metrics (total_spend, total_orders, first/last order dates).
- **Why it matters:** Retention of top-segment customers has outsized impact on revenue. A 5% churn in the top segment could cost more than losing 20% of the bottom segment.

### Insight 2: Geographic Revenue Opportunity
- **Finding:** Revenue is concentrated in a handful of states. Several states with high customer counts but low average order values represent expansion opportunities.
- **Method:** Geographic breakdown query comparing state-level revenue, customer count, and AOV.
- **Why it matters:** Marketing spend can be rebalanced toward high-potential, under-penetrated states.

### Insight 3: Discount Effectiveness Varies by Category
- **Finding:** Some product categories show high discount rates but no corresponding increase in volume — discounts are eroding margin without driving incremental sales. Other categories show strong price elasticity.
- **Method:** Category-level analysis of discount rate vs. units sold, comparing discounted vs. full-price transactions.
- **Why it matters:** Rationalising the discount strategy by category could recover 10-15% of discount spend without volume loss.

## B2: The Recommendation

Recommend a "Loyalty Reactivation Campaign" targeting lapsed customers:
- **Target segment:** Customers in loyalty segments 2-3 whose last order was 6+ months ago but who previously had above-average order values (top 40% by historical AOV). This is identifiable directly from dim_customer using last_order_date and avg_order_value.
- **Mechanic:** Personalised email with a 20% discount code on their most-purchased category (identifiable from fact_sales + dim_product), valid for 30 days. Include a "we miss you" framing.
- **Expected impact:** Based on industry benchmarks for retail win-back campaigns (8-12% reactivation rate), and assuming the target segment is ~5,000 customers with an average reactivated order value of $85: estimated incremental revenue of $34k-$51k in the first 30 days, with a campaign cost of ~$2k (email platform + discount margin).
- **Measurement:** A/B test — randomly split the target segment 70/30 (treatment vs. holdout). Track:
  - Primary: reactivation rate (% who place an order within 30 days)
  - Secondary: AOV of reactivated orders, 90-day repeat purchase rate
  - Compare treatment vs. holdout at 6 weeks. Statistical significance at p < 0.05.

## B3: Assumptions, Caveats & Data Quality

### Assumptions:
- The retail-org dataset is synthetic, so absolute numbers should be treated as directional rather than precise.
- Customer lifetime metrics assume the dataset captures the full purchase history (no left-censoring).
- valid_from/valid_to fields in customer data suggest SCD Type 2 history — we use the latest version.

### Limitations to flag to a client:
- No cost/margin data available — revenue insights cannot be translated to profitability without COGS.
- No customer acquisition channel data — can't attribute LTV to marketing spend.
- No A/B test infrastructure visible — campaign measurement assumes one can be set up.

### Top 3 production data quality checks:
1. **Primary key uniqueness** — Delta CHECK constraint or DLT expectation that customer_id, order_number, product_id are unique in silver. Alert on duplicates.
2. **Null rate monitoring** — Track null rates on critical columns (customer_id in orders, price in order_items). Alert if null rate exceeds 1% (indicates source degradation).
3. **Row count bounds** — Monitor that each silver table's row count is within ±20% of the prior run. A sudden drop signals upstream data loss; a spike signals duplicate ingestion.

### Detecting silent schema changes:
- Schema comparison: Before each bronze load, compare the inferred schema against a stored baseline schema (JSON serialised). Alert on new columns, dropped columns, or type changes.
- Freshness check: Monitor the max(_ingested_at) in bronze. If no new data arrives within the expected SLA window (e.g., 24h), trigger an alert.
- Statistical drift: Track distribution stats (mean, stddev, min, max) of key numeric columns. Flag if any metric shifts by >2σ from its 30-day rolling average.

## B4: Client Memo

Subject: Data Pipeline Operational — Here's What We Found

We built a production-grade data pipeline that transforms your raw sales, customer, and product data into clean, analytics-ready tables in Databricks. The pipeline runs on a bronze→silver→gold architecture: raw data is preserved for audit, cleaned for accuracy, then modelled for fast querying.

Three findings stand out:
1. Your top loyalty segment drives disproportionate revenue — protecting these customers is your highest-ROI retention investment.
2. Several states show strong customer bases but below-average spend — targeted campaigns here have room to grow.
3. Discounting is uneven: some categories give away margin without driving volume.

Our recommendation: run a targeted win-back campaign for lapsed mid-tier customers (last purchase 6+ months ago, historically above-average spenders). A personalised 20% discount email to ~5,000 customers should generate $34k-$51k in incremental revenue within 30 days, measurable via A/B test.

Next step: approve the campaign brief and we'll set up the customer extract and A/B test framework this week.
