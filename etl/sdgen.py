import os
import boto3
import json
import random
import time
from dotenv import load_dotenv
from utils.logger2 import get_logger
from datetime import datetime, timedelta

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
STREAM_NAME = os.getenv("STREAM_NAME")
REGION = os.getenv("REGION")
INTERVAL = 2

kinesis_client = boto3.client(
    "kinesis",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

logger = get_logger(__name__)

def generate_record(oid):
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
        "order_id": oid,
        "order_quantity": maybe_null(random.choice([random.randint(1, 20), "ten", None])),
        "price": maybe_null(round(random.uniform(10, 500), 2)),
        "sales": maybe_null(random.choice([round(random.uniform(100, 10000), 2), "N/A", None])),
        "order_date": maybe_null(random_date()),
        "order_status": maybe_null(random.choice(["Shipped", "Cancelled", "In Process", "Resolved", None])),
        "product_id": maybe_null(random.choice(["S10_1678", "S12_1099", 12345])),
        "customer_name": maybe_null(random.choice(["John Doe", "Jane Smith", "A. B. Customer", None])),
        "phone": maybe_null(random.choice(["555-1234", "1234567890", None, 9876543210])),#
        "pincode": None,#
        "address": maybe_null(random.choice(["123 Elm St", "456 Oak Ave", None, 12345])),#
        "city": maybe_null(random.choice(["New York", "Paris", "London", "123City", None])),#
        "state": None,#
        "country": maybe_null(random.choice(["USA", "France", "UK"])),
        "last_name": maybe_null(random.choice(["Smith", "Johnson", "Doe", None])),#
        "first_name": maybe_null(random.choice(["John", "Jane", "Alex", 42, None])),#
    }


def send_to_kinesis(record, partition_key = "order_id"):
    try:
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=str(record[partition_key])
        )
        logger.info(f"Record sent to Kinesis (ShardId={response['ShardId']})")
    except Exception as e:
        logger.error(f"Failed to send record to Kinesis: {e}")

def stream_records(interval = INTERVAL):
    oid = 1000
    while True:
        # f.write(json.dumps(generate_record(x)) + "\n")
        # f.flush()
        record = generate_record(oid)
        send_to_kinesis(record, partition_key="order_id")
        logger.info(f"New record generated.")
        time.sleep(interval)
        oid += 1

if __name__ == '__main__':
    stream_records()
