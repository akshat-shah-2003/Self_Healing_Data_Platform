import time
import random
import json

def generate_record():
    return {
        "order_id":random.randint(1000,9999),
        "quantity":random.randint(1,10),
        "unit__price":round(random.uniform(10,500),2),
        "timestamp":time.time()
    }

def stream_records(file_path = "./data/processed/sales_stream.json", interval = 2):
    with open(file_path, 'a') as f:
        while True:
            f.write(json.dump(generate_record()) + "\n")
            f.flush()
            print('Record generated.')
            time.sleep(interval)
