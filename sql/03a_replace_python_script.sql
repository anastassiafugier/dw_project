INSERT INTO staging.stg_product (product_business_key, product_name, category, subcategory) VALUES
(183920, 'SMARTWATCH', 'Electronics', 'Premium'),
(495621, 'hiking boots', 'Hiking', 'Standard'),
(292134, 'Jeans', 'Clothing', 'Budget'),
(714839, 'VACUUM cleaner', 'Home', 'Premium'),
(653287, 'face wash', 'Beauty', 'Standard'),
(918374, 'Laptop', 'Electronics', 'Budget'),
(537910, 'multi-tool knife', 'Hiking', 'Premium'),
(829105, 'DRESS shirt', 'Clothing', 'Standard'),
(376429, 'table lamp', 'Home', 'Budget'),
(604892, 'Moisturizer', 'Beauty', 'Premium');

INSERT INTO staging.stg_customer (customer_business_key, first_name, last_name, country) VALUES
(102938, 'marie', 'Dubois', 'France'),
(847362, 'THOMAS', 'müller', 'Germany'),
(561290, 'luca', 'Rossi', 'Italy'),
(394820, 'SOPHIE', 'bernard', 'Switzerland'),
(758291, 'alex', 'Kowalski', 'Poland'),
(629185, 'JULIA', 'smith', 'USA'),
(980372, 'kevin', 'Ng', 'Hong Kong'),
(572031, 'ANNA', 'Boucher', 'Canada'),
(438201, 'marc', 'Silva', 'Luxembourg'),
(120957, 'SARAH', 'de Vries', 'South Africa'),
(791045, 'pierre', 'Durand', 'France'),
(214598, 'LAURA', 'Dupont', 'Switzerland'),
(860392, 'michael', 'Nowak', 'Germany'),
(495028, 'ISABELLE', 'Fontana', 'Italy'),
(374610, 'john', 'Brown', 'USA');

CREATE OR REPLACE FUNCTION staging.generate_stg_sales(n_records INTEGER DEFAULT 10000)
RETURNS VOID AS $$
DECLARE
    v_date_key INTEGER;
    v_customer_key INTEGER;
    v_store_key INTEGER;
    v_product_key INTEGER;
    v_transaction_id INTEGER;
    v_quantity INTEGER;
    v_unit_price DECIMAL(10,2);
BEGIN

    TRUNCATE TABLE staging.stg_sales RESTART IDENTITY;

    FOR i IN 1..n_records LOOP
        -- get an existing value from the date dimension table
        SELECT date_key INTO v_date_key
        FROM dwh.dim_date
        ORDER BY random()
        LIMIT 1;

        SELECT customer_business_key INTO v_customer_key
        FROM staging.stg_customer
        ORDER BY random()
        LIMIT 1;

        SELECT store_business_key INTO v_store_key
        FROM staging.stg_store
        ORDER BY random()
        LIMIT 1;

        SELECT product_business_key INTO v_product_key
        FROM staging.stg_product
        ORDER BY random()
        LIMIT 1;

        v_transaction_id := floor(random() * 999999999) + 1000000000;
        v_quantity := floor(random() * 1000 + 1);
        v_unit_price := 5 + random() * 695;

        INSERT INTO staging.stg_sales (
            date_key,
            customer_business_key,
            store_business_key,
            product_business_key,
            transaction_id,
            quantity,
            unit_price
        ) VALUES (
            v_date_key,
            v_customer_key,
            v_store_key,
            v_product_key,
            v_transaction_id,
            v_quantity,
            v_unit_price
        );
    END LOOP;

END;
$$ LANGUAGE plpgsql;

SELECT staging.generate_stg_sales(500);
