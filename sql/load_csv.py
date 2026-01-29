import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "localhost",
    "dbname": "",
    "user": "",
    "port": 5432
}

CSV_PATH = "worldcities.csv"

def load_cities():
    df = pd.read_csv(CSV_PATH)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE staging.stg_store RESTART IDENTITY CASCADE;")

    insert_sql = """
        INSERT INTO staging.stg_store (
            city,
            city_ascii,
            lat,
            lng,
            country,
            iso2,
            iso3,
            admin_name,
            capital,
            population,
            id
        )
        VALUES %s
    """

    execute_values(
        cur,
        insert_sql,
        df.values.tolist(),
        page_size=2000
    )
    print("CSV loaded into the staging store table.")

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
            'Drug store ' || city_ascii AS store_name,
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
    print("ETL for dim_store completed successfully.")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_cities()
