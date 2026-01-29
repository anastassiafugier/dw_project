import psycopg2
import random
from faker import Faker

conn = psycopg2.connect(
    host="localhost",
    dbname="postgres",
    user="anastassiafugier",
    port=5432
)
cursor = conn.cursor()
fake = Faker()

truncate_queries = [
    "TRUNCATE TABLE staging.stg_sales RESTART IDENTITY CASCADE;",
    "TRUNCATE TABLE staging.stg_product RESTART IDENTITY CASCADE;",
    "TRUNCATE TABLE staging.stg_customer RESTART IDENTITY CASCADE;"
]

for query in truncate_queries:
    cursor.execute(query)

# this 2 f applies 1.upper/lower case 2.capitalize/lower case randomly
# in order to apply an additional step in etl later
def random_case_1(text):
    return text.upper() if random.randint(0,1) == 1 else text.lower()

def random_case_2(text):
    return text.lower() if random.randint(0,1) == 1 else text

# product data generation
def generate_stg_product(n=200):
    products = []
    categories = ["Electronics", "Hiking", "Clothing", "Home", "Beauty"]
    names = {
        "Electronics": [
            "Wireless Earbuds",
            "Smartphone",
            "Bluetooth Speaker",
            "Laptop",
            "Tablet",
            "Smartwatch",
            "Portable Charger",
            "Noise Cancelling Headphones",
            "Gaming Mouse",
            "Mechanical Keyboard",
            "External Hard Drive",
            "Smart TV",
            "USB-C Hub",
            "Drone",
            "Action Camera"
        ],
        "Hiking": [
            "Hiking Boots",
            "Backpack",
            "Headlamp",
            "Sleeping Bag",
            "Trekking Poles",
            "Tent",
            "Water Filter",
            "Camping Stove",
            "Compass",
            "First Aid Kit",
            "Hiking Socks",
            "Insulated Jacket",
            "Hydration Bladder",
            "Portable Lantern",
            "Multi-tool Knife"
        ],
        "Clothing": [
            "Jeans",
            "T-Shirt",
            "Sweater",
            "Jacket",
            "Sneakers",
            "Dress Shirt",
            "Coat",
            "Shorts",
            "Scarf",
            "Socks",
            "Hat",
            "Skirt",
            "Belt",
            "Blazer",
            "Raincoat"
        ],
        "Home": [
            "Coffee Maker",
            "Vacuum Cleaner",
            "Desk Lamp",
            "Microwave Oven",
            "Toaster",
            "Blender",
            "Air Purifier",
            "Electric Kettle",
            "Cookware Set",
            "Curtains",
            "Rug",
            "Bookshelf",
            "Wall Clock",
            "Table Lamp",
            "Storage Basket"
        ],
        "Beauty": [
            "Moisturizer",
            "Shampoo",
            "Conditioner",
            "Perfume",
            "Face Wash",
            "Lip Balm",
            "Body Lotion",
            "Foundation",
            "Mascara",
            "Nail Polish",
            "Face Mask",
            "Hair Dryer",
            "Makeup Brush Set",
            "Sunscreen",
            "Hand Cream"
        ]
    }
    for _ in range(n):
        category = random.choice(categories)
        subcategory = random.choice(["Premium", "Standard", "Budget"])
        product = (
            random.randint(100000, 999999),
            random_case_1(random.choice(names[category])),
            category,
            subcategory
        )
        products.append(product)
    return products

products = generate_stg_product()
cursor.executemany("""
    INSERT INTO staging.stg_product
    (product_business_key, product_name, category, subcategory)
    VALUES (%s, %s, %s, %s);
""", products)
print(f"Inserted {len(products)} products into staging.")

# customer data generation
def generate_stg_customer(n=2000):
    customers = []
    countries = ["Canada", "Switzerland", "USA", "Germany", "South Africa", "Luxembourg", "Poland", "Italy", "Latvia", "Hong Kong"]
    for _ in range(n):
        customers.append((
            random.randint(100000, 999999),
            random_case_1(fake.first_name()),
            random_case_2(fake.last_name()),
            random.choice(countries)
        ))
    return customers

customers = generate_stg_customer()
cursor.executemany("""
    INSERT INTO staging.stg_customer
    (customer_business_key, first_name, last_name, country)
    VALUES (%s, %s, %s, %s);
""", customers)
print(f"Inserted {len(customers)} customers into staging.")

# handle foreign key constraints;
cursor.execute("SELECT date_key FROM dwh.dim_date;")
date_keys = [r[0] for r in cursor.fetchall()]

cursor.execute("SELECT store_business_key FROM dwh.dim_store;")
store_business_keys = [r[0] for r in cursor.fetchall()]

product_business_keys = [p[0] for p in products]
customer_business_keys = [c[0] for c in customers]

# sales data generation
def generate_stg_sales(n=170000):
    fact_data = []
    for _ in range(n):
        date_key = random.choice(date_keys)
        customer_business_key = random.choice(customer_business_keys)
        store_business_key = random.choice(store_business_keys)
        product_business_key = random.choice(product_business_keys)
        # searched on the internet => transaction id ~12-18 char
        # since int type => upper limit 2^31
        transaction_id = random.randint(1_000_000_000, 1_999_999_999)
        quantity = random.randint(1, 1000)
        unit_price = round(random.uniform(5.0, 700.0), 2)

        fact_data.append((
            date_key,
            customer_business_key,
            store_business_key,
            product_business_key,
            transaction_id,
            quantity,
            unit_price
        ))
    return fact_data

facts = generate_stg_sales()
cursor.executemany("""
    INSERT INTO staging.stg_sales (
        date_key, customer_business_key, store_business_key, product_business_key,
        transaction_id, quantity, unit_price
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
""", facts)
print(f"Inserted {len(facts)} sales records into staging.")

conn.commit()
cursor.close()
conn.close()

