from pathlib import Path
import os

files_list = [
    "data/raw/readme.txt",
    "data/processed/readme.txt",
    "logs/readme.txt",
    "etl/extract.py",
    "etl/transform.py",
    "etl/load.py",
    "etl/run_etl.py",
    "utils/logger.py",
    "utils/metrics.py"
]

for eachfile in files_list:
    filepath = Path(eachfile)
    filedir, filename = os.path.split(filepath)
    if filedir != "":
        os.makedirs(filedir, exist_ok = True)
    if not filepath.exists():
        with open(filepath, "w") as f:
            pass
        print(f"Created file: {eachfile}")
    else:
        print(f"File already exists: {eachfile}")
