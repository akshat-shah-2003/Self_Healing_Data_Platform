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
output_dir = "data/db_metadata"
os.makedirs(output_dir, exist_ok=True)

def load_snapshot():
    """Load the most recent snapshot file (if any)."""
    files = sorted(
        [f for f in os.listdir(output_dir) if f.endswith("_snapshot.json")]
    )
    if not files:
        return None
    latest_file = os.path.join(output_dir, files[-1])
    with open(latest_file, "r") as f:
        return json.load(f)

def save_snapshot(snap):
    """Save the snapshot to a JSON file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"{timestamp}_snapshot.json")
    with open(file_path, "w") as f:
        json.dump(snap, f, indent=4)
    logger.info(f"Snapshot saved to {file_path}")

def to_map(snap):
    if not snap:
        return {}
    flat_map = {}
    for schema_name, schema_data in snap.get("schemas", {}).items():
        for table_name, columns in schema_data.get("tables", {}).items():
            for col in columns:
                kkey = f"{schema_name}.{table_name}.{col['column_name']}"
                flat_map[kkey] = col
    return flat_map

def compare_snapshots(old_snap, new_snap):
    old_map = to_map(old_snap)
    new_map = to_map(new_snap)
    pass


def infer_schema():
    ss = load_snapshot()
    logger.info(f"Loaded latest snapshot.")
    
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
        schema = os.getenv("SNOWFLAKE_SCHEMA")

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

        # Define file path
        file_path = f"{output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_snapshot.json"

        # Save schema to JSON
        with open(file_path, "w") as f:
            json.dump(metadata, f, indent=4)

        logger.info(f"Schema inference completed and saved to: {file_path}")

        snapshot = json.load(open(os.path.join(output_dir,ss), "r"))

        print(type(snapshot))
        #ss = file_path

    except Exception as e:
        logger.error(f"Schema inference failed: {e}")
        raise


if __name__ == "__main__":
    infer_schema()
