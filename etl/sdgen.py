import json
import random
import time
from utils.logger import get_logger
from datetime import datetime, timedelta

logger = get_logger(__name__)

def generate_record():
    def random_date():
        # Generate a random date in last 5 years
        start_date = datetime.now() - timedelta(days=5*365)
        random_days = random.randint(0, 5*365)
        date = start_date + timedelta(days=random_days)
        return date.strftime("%m/%d/%Y")
    
    def maybe_null(value):
        return value if random.random() > 0.1 else None  # 10% chance of null

    def maybe_dirty_string(value):
        if random.random() < 0.15:  # 15% chance of dirty/mismatched type
            return str(value) + random.choice(["_x", "", "!", "@#"])
        return value

    return {
        "order_id": maybe_null(maybe_dirty_string(random.randint(1000, 9999))),
        "order_quantity": maybe_null(random.choice([random.randint(1, 20), "ten", None])),
        "price": maybe_null(round(random.uniform(10, 500), 2)),
        "line_item": maybe_null(random.choice([random.randint(1, 5), "5a", "two", None])),
        "sales": maybe_null(random.choice([round(random.uniform(100, 10000), 2), "N/A", None])),
        "order_date": maybe_null(random_date()),
        "order_status": maybe_null(random.choice(["Shipped", "Cancelled", "In Process", "Resolved", "", None])),
        "order_quarter": maybe_null(random.choice(["Q1", "Q2", "Q3", "Q4", "Quarter 1", None])),
        "order_month": maybe_null(random.choice(list(range(1,13)) + ["January", None])),
        "order_year": maybe_null(random.choice([2020, 2021, 2022, "Twenty Twenty", None])),
        "product_line": maybe_null(random.choice(["Motorcycles", "Classic Cars", "Planes", "Trains", "", None])),
        "msrp": maybe_null(random.choice([round(random.uniform(50, 1000), 2), "one hundred", None])),
        "product_id": maybe_null(random.choice(["S10_1678", "S12_1099", 12345, None])),
        "customer_name": maybe_null(random.choice(["John Doe", "Jane Smith", "A. B. Customer", "", None])),
        "phone": maybe_null(random.choice(["555-1234", "1234567890", None, 9876543210])),
        "address": maybe_null(random.choice(["123 Elm St", "456 Oak Ave", None, 12345])),
        "city": maybe_null(random.choice(["New York", "Paris", "London", "123City", None])),
        "country": maybe_null(random.choice(["USA", "France", "UK", "", "123", None])),
        "last_name": maybe_null(random.choice(["Smith", "Johnson", "Doe", "", None])),
        "first_name": maybe_null(random.choice(["John", "Jane", "Alex", 42, None])),
        "deal_size": maybe_null(random.choice(["Small", "Medium", "Large", "Extra Large", "", None]))
    }


def stream_records(file_path = "./data/processed/sales_stream.txt", interval = 2):
    with open(file_path, 'a') as f:
        while True:
            f.write(json.dump(generate_record()) + "\n")
            f.flush()
            logger.info(f"New record generated and added to {file_path}.")
            time.sleep(interval)
