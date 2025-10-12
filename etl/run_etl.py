import pandas as pd
from utils.logger import get_logger
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from utils.metrics import Timer

logger = get_logger(__name__)

def main():
    timer = Timer()
    timer.start()
    logger.info("ETL process started.")
    try:
        df = extract_data()
        df_transformed = transform_data(df)
        load_data(df_transformed)
        duration = timer.stop()
        logger.info(f"ETL process completed in {duration} seconds.")
    
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()