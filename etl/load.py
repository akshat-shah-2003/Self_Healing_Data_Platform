import os
from utils.logger import get_logger

logger = get_logger(__name__)

def load_data(df, l_path = './data/processed/'):
    logger.info("Loading data...")
    try:
        if not os.path.exists(l_path):
            os.makedirs(l_path)
        df.to_csv(f"{l_path}/sales_data.csv", index=False)
        logger.info(f"Data loaded to {l_path}/sales_data.csv")
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise