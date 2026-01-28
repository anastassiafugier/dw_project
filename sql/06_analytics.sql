-- 1. RetailChain revenue per year/quarter/month
SELECT
    d.year_number,
    d.quarter_number,
    d.month_number,
    SUM(f.total_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY ROLLUP (d.year_number, d.quarter_number, d.month_number)
ORDER BY d.year_number, d.quarter_number, d.month_number;

-- 2. Store ranking by revenue within countries
SELECT
    s.store_name,
    s.city,
    s.country,
    SUM(f.total_amount) AS total_sales,
    RANK() OVER (PARTITION BY s.country ORDER BY SUM(f.total_amount) DESC) AS rank_in_country
FROM dwh.fact_sales f
JOIN dwh.dim_store s ON f.store_key = s.store_key
GROUP BY s.store_name, s.city, s.country
ORDER BY s.country, rank_in_country;

-- 3. Revenue per country/city/store

-- I left the null values detection with GROUPING as a comment
-- since I am not convinced putting multicity is a better solution when visualising the query result
-- the blank space seems more suitable
SELECT
    s.country,
    --CASE WHEN GROUPING(s.city) = 1 THEN 'multicity' ELSE s.city END AS city,
    s.city,
    SUM(f.total_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_store s ON f.store_key = s.store_key
GROUP BY GROUPING SETS (
    (s.country, s.city),
    (s.country)
)
ORDER BY s.country, s.city;

-- 4. Revenue per day of the week (weekly buying patterns)
SELECT
    d.day_of_week AS day_of_week_num,
    CASE d.day_of_week
	    WHEN 0 THEN 'Sunday'
	    WHEN 1 THEN 'Monday'
	    WHEN 2 THEN 'Tuesday'
	    WHEN 3 THEN 'Wednesday'
	    WHEN 4 THEN 'Thursday'
	    WHEN 5 THEN 'Friday'
	    WHEN 6 THEN 'Saturday'
    END AS day_of_week,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.total_amount) AS total_sales
FROM dwh.fact_sales f
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week, day_type
ORDER BY total_sales DESC;

-- 5. 10 RetailChain most sold products
SELECT
    p.product_name,
    p.category,
    p.subcategory,
    SUM(f.total_amount) AS total_sales,
    RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS sales_rank
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.subcategory
ORDER BY sales_rank
LIMIT 10;

-- 6. Monthly revenue fluctuations by category
SELECT
    p.category,
    d.month_name,
    ROUND(AVG(f.total_amount), 2) AS avg_monthly_sales
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY p.category, d.month_name
ORDER BY p.category, d.month_name;

-- 7. Client segmentation based on their spending
-- used the ntile() ranking function here
WITH customer_spend AS (
    SELECT
        c.customer_business_key,
        SUM(f.total_amount) AS total_spent
    FROM dwh.fact_sales f
    JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_business_key
)
SELECT
    customer_business_key,
    total_spent,
    NTILE(4) OVER (ORDER BY total_spent DESC) AS spend_quartile,
    CASE
        WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 1 THEN 'Platinum'
        WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 2 THEN 'Gold'
        WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 3 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_segment
FROM customer_spend
ORDER BY total_spent DESC;

-- 8. Sales per category/subcategory

-- the average unit price can be insightful
-- if the price changes over the period taken into account
SELECT
    p.category,
    p.subcategory,
    SUM(f.total_amount) AS total_sales,
    AVG(f.unit_price) AS avg_price
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY CUBE (p.category, p.subcategory)
ORDER BY p.category, p.subcategory;

-- 9. Annual sales growth

-- handled the 0 as total sales for the preceding year
-- using nullif()
WITH yearly_sales AS (
    SELECT
        d.year_number,
        SUM(f.total_amount) AS total_sales
    FROM dwh.fact_sales f
    JOIN dwh.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year_number
)
SELECT
    year_number,
    total_sales,
    LAG(total_sales) OVER (ORDER BY year_number) AS previous_year,
    ROUND(100.0 * (total_sales - LAG(total_sales) OVER (ORDER BY year_number)) / 
          NULLIF(LAG(total_sales) OVER (ORDER BY year_number), 0), 2) AS growth_percent
FROM yearly_sales;
