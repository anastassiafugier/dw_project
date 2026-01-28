-- stored functions to extract data from the staging schema tables;

CREATE OR REPLACE FUNCTION dwh.populate_test_product()
RETURNS VOID AS $$
BEGIN
	--TRUNCATE TABLE dwh.dim_product RESTART IDENTITY CASCADE;

	-- precaution if there's duplicates among business keys
	-- (randomly generated in the script)
	CREATE TEMP TABLE tmp_product AS
    SELECT DISTINCT ON (product_business_key)
        product_business_key,
        product_name,
        category,
        subcategory,
        load_timestamp
    FROM staging.stg_product
    ORDER BY product_business_key, load_timestamp DESC;

	-- Set the end date && the current bool field to False for tuples
	-- where changes occured
	UPDATE dwh.dim_product AS dim
	SET
	    valid_to = NOW(),
	    is_current = FALSE
	FROM tmp_product AS stg
	WHERE dim.product_business_key = stg.product_business_key
	  AND dim.is_current = TRUE
	  AND (
	        dim.product_name IS DISTINCT FROM initcap(stg.product_name)
	        OR dim.category IS DISTINCT FROM stg.category
	        OR dim.subcategory IS DISTINCT FROM stg.subcategory
	      );

	INSERT INTO dwh.dim_product (
	    product_business_key,
	    product_name,
	    category,
	    subcategory,
	    valid_from,
	    valid_to,
	    is_current,
	    version
	)
	SELECT DISTINCT ON (stg.product_name, stg.category, stg.subcategory)
    	stg.product_business_key,
    	initcap(stg.product_name),
    	stg.category,
    	stg.subcategory,
    	NOW() AS valid_from,
	    NULL AS valid_to,
	    TRUE AS is_current,
	    COALESCE(dim.version, 0) + 1
    FROM tmp_product stg
	LEFT JOIN dwh.dim_product AS dim
	  ON stg.product_business_key = dim.product_business_key
	  AND dim.is_current = FALSE
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM dwh.dim_product AS d
	    WHERE d.product_business_key = stg.product_business_key
	      AND d.is_current = TRUE
	      AND d.product_name IS NOT DISTINCT FROM initcap(stg.product_name)
	      AND d.category IS NOT DISTINCT FROM stg.category
	      AND d.subcategory IS NOT DISTINCT FROM stg.subcategory
	)
	ORDER BY stg.product_name, stg.category, stg.subcategory, load_timestamp DESC;

	DROP TABLE tmp_product;
END;
$$ LANGUAGE plpgsql;

SELECT dwh.populate_test_product();

CREATE OR REPLACE FUNCTION dwh.populate_test_customer()
RETURNS VOID AS $$
BEGIN
	--TRUNCATE TABLE dwh.dim_customer RESTART IDENTITY CASCADE;

	-- precaution if there's duplicates among business keys
	-- (randomly generated in the script)
	CREATE TEMP TABLE tmp_customer AS
    SELECT DISTINCT ON (customer_business_key)
        customer_business_key,
        first_name,
        last_name,
        country,
        load_timestamp
    FROM staging.stg_customer
    ORDER BY customer_business_key, load_timestamp DESC;

	UPDATE dwh.dim_customer AS dim
	SET
	    valid_to = NOW(),
	    is_current = FALSE
	FROM tmp_customer AS stg
	WHERE dim.customer_business_key = stg.customer_business_key
	  AND dim.is_current = TRUE
	  AND (
	        dim.first_name IS DISTINCT FROM initcap(stg.first_name)
	        OR dim.last_name IS DISTINCT FROM upper(stg.last_name)
	        OR dim.country IS DISTINCT FROM stg.country
	      );

	INSERT INTO dwh.dim_customer (
	    customer_business_key,
	    first_name,
	    last_name,
	    country,
	    valid_from,
	    valid_to,
	    is_current,
	    version
	)
	SELECT DISTINCT ON (stg.first_name, stg.last_name, stg.country)
    	stg.customer_business_key,
    	initcap(stg.first_name),
    	upper(stg.last_name),
    	stg.country,
    	NOW() AS valid_from,
	    NULL AS valid_to,
	    TRUE AS is_current,
	    COALESCE(dim.version, 0) + 1
    FROM tmp_customer stg
	LEFT JOIN dwh.dim_customer AS dim
	  ON stg.customer_business_key = dim.customer_business_key
	  AND dim.is_current = FALSE
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM dwh.dim_customer AS d
	    WHERE d.customer_business_key = stg.customer_business_key
	      AND d.is_current = TRUE
	      AND d.first_name IS NOT DISTINCT FROM initcap(stg.first_name)
	      AND d.last_name IS NOT DISTINCT FROM upper(stg.last_name)
	      AND d.country IS NOT DISTINCT FROM stg.country
	)
	ORDER BY stg.first_name, stg.last_name, stg.country, load_timestamp DESC;

	DROP TABLE tmp_customer;
END;
$$ LANGUAGE plpgsql;

SELECT dwh.populate_test_customer();

CREATE OR REPLACE FUNCTION dwh.populate_test_sales()
RETURNS VOID AS $$
BEGIN
	TRUNCATE TABLE dwh.fact_sales RESTART IDENTITY CASCADE;
    INSERT INTO dwh.fact_sales (
        date_key, customer_key, store_key, product_key, transaction_id, quantity, unit_price, total_amount
    )
    SELECT
        s.date_key,
        c.customer_key,
        st.store_key,
        p.product_key,
        s.transaction_id,
        s.quantity,
        s.unit_price,
        s.quantity * s.unit_price
    FROM staging.stg_sales s
    JOIN dwh.dim_customer c ON s.customer_business_key = c.customer_business_key
    JOIN dwh.dim_product p ON s.product_business_key = p.product_business_key
    JOIN dwh.dim_store st ON s.store_business_key = st.store_business_key;
END;
$$ LANGUAGE plpgsql;

SELECT dwh.populate_test_sales();
