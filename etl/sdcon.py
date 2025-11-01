import re
import json
import time
from datetime import datetime
import pandas as pd
from utils.logger2 import get_logger

logger = get_logger(__name__)

def transform_record(rec):
    rec.pop('city')
    rec.pop('phone')
    rec.pop('address')
    rec.pop('pincode')
    rec.pop('state')
    rec.pop('first_name')
    rec.pop('last_name')
    #print(rec)
    return rec


def validate_record(rec):
    is_valid = True
    no_error = True

    oi_pattern = r'^[1-9][0-9]{3}$'
    name_pattern = r'^[A-Za-z .]+$'
    city_country_pattern = r'^[A-Za-z ]+$'

    def safe_float(val):
        try:
            return float(val)
        except:
            return None

    def safe_int(val):
        try:
            return int(float(val))
        except:
            return None

    rec["sales"] = safe_float(rec["sales"])
    rec["price"] = safe_float(rec["price"])
    rec["order_quantity"] = safe_int(rec["order_quantity"])

    if not rec["order_id"] or not re.match(oi_pattern, str(rec["order_id"])):
        is_valid = False
        no_error = False

    if not rec["order_quantity"]:
        if rec["price"] and rec["sales"]:
            try:
                rec["order_quantity"] = int(rec["sales"] // rec["price"])
                logger.info("Fixed order quantity.")
            except:
                is_valid = False
                no_error = False
        else:
            is_valid = False
            no_error = False

    if not rec["price"]:
        if rec["order_quantity"] and rec["sales"]:
            try:
                rec["price"] = round(rec["sales"] / rec["order_quantity"], 2)
                logger.info("Fixed order price.")
            except:
                is_valid = False
                no_error = False
        else:
            is_valid = False
            no_error = False

    if not rec["sales"]:
        if rec["order_quantity"] and rec["price"]:
            rec["sales"] = round(rec["price"] * rec["order_quantity"], 2)
            logger.info("Fixed total sales.")
        else:
            is_valid = False
            no_error = False

    if not rec["order_status"]:
        is_valid = False
        no_error = False

    if not rec["product_id"]:
        is_valid = False
        no_error = False

    if not rec["customer_name"] or not re.match(name_pattern, str(rec["customer_name"])):
        no_error = False
        rec["customer_name"] = "Unknown Customer"
        logger.info("Fixed customer name.")

    return is_valid or no_error
    

def process_stream(s_path = './data/processed/sales_stream.txt', o_path = './data/processed/sales_stream.csv'):
    #log
    seen = 0
    while True:
        with open(s_path, 'r') as f:
            lines = f.readlines()
            new_lines = lines[seen:]
        if not new_lines:
            logger.info("Waiting ...")
            time.sleep(1)
            continue
        for line in new_lines:
            reco = json.loads(line.strip())
            reco = transform_record(reco)
            logger.info("Validating new record ...")
            if validate_record(reco):
                df = pd.DataFrame([reco])
                df.to_csv(o_path, mode = 'a', index = False, header = False)
                logger.info("Record Valid: Processing ...")
            else:
                logger.info("Record Invalid: Skipping ...")
                pass
            print(reco)
        seen = len(lines)

if __name__ == '__main__':
    process_stream()



