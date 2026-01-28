CREATE OR REPLACE FUNCTION dwh.populate_test_date(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE;
BEGIN
	-- added this line to be able to exec the function several times
	-- (for testing purposes mainly && debug)
	TRUNCATE TABLE dwh.dim_date RESTART IDENTITY CASCADE;
    curr_date := start_date;
    WHILE curr_date <= end_date LOOP
        INSERT INTO dwh.dim_date (
            date_key, full_date, year_number, month_number, 
            month_name, quarter_number, day_of_week, is_weekend
        ) VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER,
            curr_date,
            EXTRACT(YEAR FROM curr_date),
            EXTRACT(MONTH FROM curr_date),
            TRIM(TO_CHAR(curr_date, 'Month')),
            EXTRACT(QUARTER FROM curr_date),
            EXTRACT(DOW FROM curr_date),
            EXTRACT(DOW FROM curr_date) IN (0,6)
        );
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT dwh.populate_test_date('2023-11-30', '2025-11-30');
