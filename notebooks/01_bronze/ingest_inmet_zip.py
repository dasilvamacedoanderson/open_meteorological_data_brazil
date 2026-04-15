# ==============================================================================
# Notebook: 01_bronze/ingest_inmet_zip.py
# Purpose: Read the queue, download a pending ZIP, hash it, and update the table.
# ==============================================================================

import requests
import hashlib
from datetime import datetime

# 1. Configuration
CONTROL_TABLE_NAME = "open_meteorological_data_brazil.admin.control_table"
VOLUME_BASE_PATH = "/Volumes/open_meteorological_data_brazil/bronze/zipfileraw/"

print("Checking the queue for pending downloads...")

# 2. Get the next pending file from the queue
# We use LIMIT 1 so the notebook processes one file per run (perfect for scheduled jobs)
pending_task_df = spark.sql(f"""
    SELECT IDFILE, SOURCE_ADDRESS, FILE_NAME 
    FROM {CONTROL_TABLE_NAME}
    WHERE LOAD_TIMESTAMP IS NULL AND FILE_TYPE = 'ZIP'
    ORDER BY FILE_NAME ASC
    LIMIT 1
""")

# Check if the queue is empty
if pending_task_df.count() == 0:
    print("🎉 The queue is empty! All files have been successfully downloaded.")
    dbutils.notebook.exit("Queue empty")

# 3. Extract task details
task_row = pending_task_df.collect()[0]
id_file = task_row["IDFILE"]
source_url = task_row["SOURCE_ADDRESS"]
file_name = task_row["FILE_NAME"]

print(f"Task Found! ID: {id_file} | File: {file_name}")
print(f"Downloading from: {source_url} ...")

# 4. Download the file
response = requests.get(source_url)

if response.status_code == 200:
    zip_bytes = response.content
    zip_size = len(zip_bytes)
    
    # Calculate SHA-256 Hash
    file_hash = hashlib.sha256(zip_bytes).hexdigest()
    print(f"Download complete. Size: {zip_size} bytes | Hash: {file_hash}")
    
    # 5. Save to Unity Catalog Volume
    save_path = VOLUME_BASE_PATH + file_name
    with open(save_path, "wb") as f:
        f.write(zip_bytes)
    print(f"Saved securely to Volume: {save_path}")
    
    # 6. Update the Control Table to mark it as DONE
    # Because Delta tables support standard SQL, we can just run an UPDATE statement!
    update_sql = f"""
        UPDATE {CONTROL_TABLE_NAME}
        SET 
            SIZE_BYTES = {zip_size},
            FILE_HASH = '{file_hash}',
            LOAD_TIMESTAMP = current_timestamp()
        WHERE IDFILE = {id_file}
    """
    spark.sql(update_sql)
    
    print(f"✅ Control table updated. Task {id_file} marked as complete!")

else:
    print(f"❌ Failed to download {file_name}. HTTP Status: {response.status_code}")
    # We leave LOAD_TIMESTAMP as NULL so it can be retried on the next run.