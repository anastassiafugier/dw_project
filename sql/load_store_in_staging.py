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

def load_csv_to_staging():
    df = pd.read_csv(CSV_PATH)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE staging.stg_store;")

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

    conn.commit()
    cur.close()
    conn.close()

    print("CSV loaded into staging.stg_store")

if __name__ == "__main__":
    load_csv_to_staging()
