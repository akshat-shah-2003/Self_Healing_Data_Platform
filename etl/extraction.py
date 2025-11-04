import os
import json
import snowflake.connector
from dotenv import load_dotenv
from utils.logger import get_logger
from datetime import datetime

# Initialize logger
logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()


def infer_schema():
    """Connect to Snowflake, infer schema metadata, and save it as a JSON file."""
    try:
        # Establish Snowflake connection
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        logger.info("Snowflake connection successful.")
    except Exception as e:
        logger.error(f"Snowflake connection failed: {e}")
        raise

    try:
        logger.info("Starting schema inference...")

        query = f"""
            SELECT 
                table_name,
                column_name, 
                data_type, 
                is_nullable, 
                column_default 
            FROM {os.getenv("SNOWFLAKE_DATABASE")}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{os.getenv("SNOWFLAKE_SCHEMA")}';
        """
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        user_id = os.getenv("SNOWFLAKE_USER")
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        metadata = {
            "user_id": os.getenv("SNOWFLAKE_USER"),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schemas": {
                schema: {"tables": {}}
            },
        }

        cur = conn.cursor()
        cur.execute(query)
        logger.info("Fetching metadata...")

        schema_dict = metadata["schemas"][schema]["tables"]

        for table_name, column_name, data_type, is_nullable, column_default in cur.fetchall():
            schema_dict.setdefault(table_name,[]).append({
                "column_name": column_name,
                "data_type": data_type,
                "is_nullable": is_nullable,
                "default": column_default,
            })
        
        #print(metadata)

        # Ensure directory exists
        output_dir = "data/db_metadata"
        os.makedirs(output_dir, exist_ok=True)

        # Define file path
        file_path = f"{output_dir}/{os.getenv('SNOWFLAKE_DATABASE')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_metadata_snapshot.json"

        # Save schema to JSON
        with open(file_path, "w") as f:
            json.dump(metadata, f, indent=4)

        logger.info(f"Schema inference completed and saved to: {file_path}")

    except Exception as e:
        logger.error(f"Schema inference failed: {e}")
        raise


if __name__ == "__main__":
    infer_schema()
