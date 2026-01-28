-- FK indexes
CREATE INDEX idx_fact_sales_date_key ON dwh.fact_sales (date_key);
CREATE INDEX idx_fact_sales_customer_key ON dwh.fact_sales (customer_key);
CREATE INDEX idx_fact_sales_store_key ON dwh.fact_sales (store_key);
CREATE INDEX idx_fact_sales_product_key ON dwh.fact_sales (product_key);

-- this index would be helpful for multidim. queries
-- with date and product dimensions
-- (for ex. compute sales sum for a specified period AND a given category)
CREATE INDEX idx_fact_sales_date_product ON dwh.fact_sales (date_key, product_key);

-- can be used when group by category, subcategory is made
-- 10 most sold products query for instance
-- as well as with cube extension of group by
CREATE INDEX idx_dim_product_category_subcategory ON dwh.dim_product (category, subcategory);

-- material view as optimization of the revenue per year/quarter/month query (#1)
-- would be more accurate if there's a daily need for this info
-- (if there's a weekly param in the table, like week #1, #2)

-- however, even with the monthly compute
-- it could still come in hand

CREATE MATERIALIZED VIEW dwh.mv_sales_time_agg AS
SELECT
    d.year_number,
    d.quarter_number,
    d.month_number,
    SUM(f.total_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.year_number, d.quarter_number, d.month_number;

CREATE INDEX idx_mv_sales_time_agg_year_month
ON dwh.mv_sales_time_agg (year_number, quarter_number, month_number);