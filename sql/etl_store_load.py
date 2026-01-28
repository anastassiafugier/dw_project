import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "",
    "user": "",
    "port": 5432
}

def load_dim_store():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # here population data inserted is : null OR numeric value casted to int
    cur.execute("""
        INSERT INTO dwh.dim_store (
            store_business_key,
            store_name,
            city,
            country,
            latitude,
            longitude,
            country_code,
            admin_name,
            population
        )
        SELECT
            FLOOR(100000 + random() * 900000)::INT AS store_business_key,
            'RetailChain ' || city_ascii AS store_name,
            city_ascii AS city,
            country,
            lat,
            lng,
            LEFT(iso2, 2) AS country_code,
            admin_name,
            CASE
                WHEN population ~ '^[0-9]+(\.[0-9]+)?$'
                THEN CAST(population AS NUMERIC)::BIGINT
                ELSE NULL
            END
        FROM staging.stg_store
        ON CONFLICT (store_business_key) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("ETL completed successfully")

if __name__ == "__main__":
    load_dim_store()
