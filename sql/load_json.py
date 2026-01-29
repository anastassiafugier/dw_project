import json
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "anastassiafugier",
    "port": 5432
}

JSON_PATH = "reviews.json"

def load_reviews():
    with open(JSON_PATH, "r") as f:
        reviews = json.load(f)

    rows = [
        (
            r["review_id"],
            r["review_date"],
            r["customer_business_key"],
            r["product_business_key"],
            r["rating"],
            r["review_text"],
            r["sentiment_score"],
            json.dumps(r)
        )
        for r in reviews
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE staging.stg_reviews RESTART IDENTITY CASCADE;")

    sql_staging = """
        INSERT INTO staging.stg_reviews (
            review_id,
            review_date,
            customer_business_key,
            product_business_key,
            rating,
            review_text,
            sentiment_score,
            raw_source
        ) VALUES %s
    """

    execute_values(cur, sql_staging, rows, page_size=2000)
    print("JSON loaded into the staging reviews table.")

    sql_dwh = """
        INSERT INTO dwh.fact_reviews (
            date_key,
            customer_key,
            product_key,
            rating,
            review_text,
            sentiment_score
        )
        SELECT
            d.date_key,
            c.customer_key,
            p.product_key,
            s.rating,
            s.review_text,
            s.sentiment_score
        FROM staging.stg_reviews s
        JOIN dwh.dim_date d
            ON d.full_date = s.review_date
        JOIN dwh.dim_customer c
            ON c.customer_business_key = s.customer_business_key
        JOIN dwh.dim_product p
            ON p.product_business_key = s.product_business_key;
    """

    cur.execute(sql_dwh)
    print("Reviews loaded into dwh.")
    conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    load_reviews()