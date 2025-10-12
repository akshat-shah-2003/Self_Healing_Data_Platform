import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)
load_dotenv()

def extract_data(output_dir = "./data/raw"):
    try:
        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        logger.info("Snowflake Connection Success.")
    except Exception as e:
        logger.error(f"Snowflake connection failed: {e}")
        raise

    query = "SELECT * FROM SALES_DATA;"

    try:
        df = pd.read_sql(query, connection)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "sales_data.csv")
        df.to_csv(output_path, index=False)
        logger.info(f"Raw data extracted and saved to {output_path}")
        return df
    
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise

    finally:
        connection.close()
        logger.info("Snowflake connection closed.")

if __name__ == "__main__":
    extract_data()
