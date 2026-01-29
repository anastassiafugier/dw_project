import json
import random
import uuid
from datetime import date, timedelta
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "anastassiafugier",
    "port": 5432
}
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# handle foreign key constraints;
cursor.execute("SELECT product_business_key FROM dwh.dim_product;")
product_business_keys = [p[0] for p in cursor.fetchall()]

cursor.execute("SELECT customer_business_key FROM dwh.dim_customer;")
customer_business_keys = [c[0] for c in cursor.fetchall()]

cursor.close()
conn.close()

NUM_REVIEWS = 10_000

review_texts = {
    5: [
        "Excellent product, highly recommended",
        "Amazing quality, exceeded expectations",
        "Perfect, would buy again"
    ],
    4: [
        "Good quality and fast delivery",
        "Very satisfied overall",
        "Good value for the price"
    ],
    3: [
        "Average product, does the job",
        "Nothing special but acceptable",
        "Okay for the price"
    ],
    2: [
        "Not great, quality could be better",
        "Somewhat disappointed",
        "Below expectations"
    ],
    1: [
        "Terrible experience, do not buy",
        "Very poor quality",
        "Completely disappointed"
    ]
}

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

reviews = []

for _ in range(NUM_REVIEWS):
    rating = random.randint(1, 5)
    review = {
        "review_id": f"REV-{uuid.uuid4().hex[:8]}",
        "review_date": random_date(date(2023, 11, 30), date(2024, 12, 31)).isoformat(),
        "customer_business_key": random.choice(customer_business_keys),
        "product_business_key": random.choice(product_business_keys),
        "rating": rating,
        "review_text": random.choice(review_texts[rating]),
        "sentiment_score": round(random.uniform(-1, 1), 2)
    }
    reviews.append(review)

with open("reviews.json", "w", encoding="utf-8") as f:
    json.dump(reviews, f, indent=2)

print(f"Generated {NUM_REVIEWS} reviews in reviews.json")
