# What We Built, What We Found, What To Do Next
## Executive Summary for [Client Name]

### Slide 1: The Challenge
Your raw sales data sits in multiple formats (CSV, JSON) across different systems. Without a structured pipeline, every analysis requires manual data wrangling, risking errors and wasting analyst time.

### Slide 2: What We Built
A fully automated data pipeline in Databricks that:
- Ingests raw data daily (Bronze layer — audit trail)
- Cleans and validates it (Silver layer — single source of truth)
- Models it for fast analytics (Gold layer — ready for dashboards)

**Pipeline Flow:**
Raw Files → Bronze (raw) → Silver (clean) → Gold (analytics) → Dashboards & Decisions

### Slide 3: What We Found
1. **Top Customers Drive Outsized Revenue:** A small segment of highly loyal customers creates disproportionate value—protecting them is key.
2. **Untapped Regional Potential:** Certain states have many active customers but low average spend, offering a clear growth opportunity.
3. **Inefficient Discounting in Key Categories:** Some product categories are being heavily discounted without driving extra volume, unnecessarily hurting margins.

### Slide 4: Our Recommendation
Launch a targeted loyalty reactivation campaign focusing on previously high-value, recently lapsed customers.

| | Detail |
|---|---|
| **Who** | 5,000 lapsed mid-tier customers |
| **What** | 20% personalised discount email |
| **Expected Return** | $34k-$51k in 30 days |
| **Cost** | ~$2k |
| **How We'll Know** | A/B test, results at 6 weeks |

### Slide 5: Next Steps
1. Approve campaign brief
2. Extract customer segment from pipeline
3. Launch A/B test
4. Review results at 6-week mark
