import json
import time
import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

def transform_record(rec):
    pass

def validate_record(rec):
    pass

def process_stream(s_path = './data/processed/sales_stream.txt', o_path = './data/processed/sales_stream.csv'):
    #log
    seen = 0
    while True:
        with open(s_path, 'r') as f:
            lines = f.readlines()
            new_lines = lines[seen:]
        if not new_lines:
            time.sleep(1)
            continue
        for line in new_lines:
            reco = json.loads(line.strip())
            reco = transform_record(reco)
            logger.info("Validating new record ...")
            if validate_record(reco):
                df = pd.DataFrame(reco)
                df.to_csv(o_path, mode = 'a', index = False, header = False)
                logger.info("Record Valid: Processing ...")
            else:
                logger.info("Record Invalid: Skipping ...")
                pass
        seen = len(lines)




