import os
import json
import snowflake.connector
from dotenv import load_dotenv
from utils.logger import get_logger
from datetime import datetime, timezone
from difflib import SequenceMatcher

# Initialize logger
logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()
output_dir = "data/db_metadata"
os.makedirs(output_dir, exist_ok=True)

def is_probable_rename(old, new):
    return SequenceMatcher(None, old.lower(), new.lower()).ratio() > 0.5

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
    # {PUBLIC.SALES_DATA.CONTACTLASTNAME: {"column_name": "CONTACTLASTNAME","data_type": "TEXT","is_nullable": "YES","default": null},...}
    return flat_map

def to_snap(flat_map, user_id="AKSHATSHAH", database="SALES_DB", timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")):

    if not flat_map:
        return {
            "user_id": user_id,
            "timestamp": timestamp,
            "database": database,
            "schemas": {}
        }

    snap = {
        "user_id": user_id,
        "timestamp": timestamp,
        "database": database,
        "schemas": {}
    }

    for key, col_data in flat_map.items():
        parts = key.split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid key format: {key}")
        schema_name, table_name, column_name = parts

        schemas = snap.setdefault("schemas", {})
        schema_obj = schemas.setdefault(schema_name, {})
        tables = schema_obj.setdefault("tables", {})
        table_cols = tables.setdefault(table_name, [])

        table_cols.append(col_data)


    return snap


def compare_schema(old_snap, new_snap):
    if not old_snap:
        logger.info("No previous snapshot found. Skipping comparison.")
        return
    old_snap_d = to_map(old_snap)
    new_snap_d = to_map(new_snap)
    old_cols = old_snap_d.keys()
    new_cols = new_snap_d.keys()

    added = new_cols - old_cols
    removed = old_cols - new_cols
    common = old_cols & new_cols

    if len(common) == len(old_cols) and len(added) == 0 and len(removed) == 0:
        logger.info("No changes detected in the schema.")
        return
    
    logger.warning("Schema changes detected:")
    logger.warning(f"New columns detected: {added}")
    logger.warning(f"Deleted columns detected: {removed}")
    # print("************************************")
    # print(old_snap)
    # print("************************************")
    renamed = {}
    for rem in removed:
        for add in added:
            if is_probable_rename(old_snap_d[rem]["column_name"], new_snap_d[add]["column_name"]) and old_snap_d[rem]["data_type"] == new_snap_d[add]["data_type"]:
                logger.warning(f"Column rename detected: {rem} => {add}")
                renamed[rem] = add
    
    return {"added": added, "removed": removed, "common": common, "renamed": renamed}

def heal_schema(diff_status, old_snap, new_snap):
    if not diff_status or not old_snap:
        return
    old_snap_d = to_map(old_snap)
    new_snap_d = to_map(new_snap)
    logger.info(f"Newly added columns:, {diff_status['added']}")
    for i in diff_status["added"]:
        if i not in diff_status["renamed"].values():
            uinp1 = input(f"Do you want to remove added column '{i}'?(Y/N): ").strip().upper()
            if uinp1 == "Y":
                uinp2 = input("Changes will be permanent! Are you sure?(Y/N): ").strip().upper()
                if uinp2 == "Y":
                    new_snap_d.pop(i)
            logger.info("Column not removed.")

    logger.info(f"Removed columns: {diff_status['removed']}")
    for j in diff_status["removed"]:
        if j not in diff_status["renamed"].keys():
            uinp3 = input(f"Do you want to restore deleted column '{j}'?(Y/N): ").strip().upper()
            if uinp3 == "Y":
                new_snap_d[j] = old_snap_d[j]

    return to_snap(new_snap_d)


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
            WHERE table_schema = '{os.getenv("SNOWFLAKE_SCHEMA")}'
            AND table_name = 'SALES_DATA'
            ORDER BY ordinal_position;
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
                "default": column_default
            })
        logger.info("Saving snapshot before healing...")
        save_snapshot(metadata)
        #print(metadata)
        diff_stat = compare_schema(ss, metadata)
        #print(diff_stat)
        fns = heal_schema(diff_stat, ss, metadata)
        if not fns:
            logger.info("No healing performed. Snapshot already saved.")
        else:
            logger.info("Saving snapshot after healing...")
            save_snapshot(fns)
        
        #ss = file_path

    except Exception as e:
        logger.error(f"Schema inference failed: {e}")
        raise


if __name__ == "__main__":
    infer_schema()
