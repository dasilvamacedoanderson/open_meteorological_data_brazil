# ==============================================================================
# Script: 02_silver/01_get_control_register.py
# Purpose: Pull next pending file from DB queue and push ID to downstream tasks.
# ==============================================================================

import sys
from databricks.sdk.runtime import dbutils, spark

CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"

print("Checking the control table for the next pending CSV file...")

# 1. Get EXACTLY ONE pending file from the queue
pending_csv_df = spark.sql(f"""
    SELECT IDFILE, FILE_NAME 
    FROM {CONTROL_TABLE}
    WHERE FILE_TYPE = 'CSV' 
      AND PROCESSED_TIMESTAMP IS NULL
    ORDER BY LOAD_TIMESTAMP ASC
    LIMIT 1
""")

pending_files = pending_csv_df.collect()

if not pending_files:
    print("No pending CSV files in the queue. Pipeline sleeping. 💤")
    # Gracefully exit the job if the queue is empty
    dbutils.notebook.exit("No files to process")

# 2. Extract the file details
row = pending_files[0]
id_file = row["IDFILE"]
file_name = row["FILE_NAME"]

print(f"✅ Found pending file! Locking IDFILE: {id_file} ({file_name})")

# 3. PUSH the values to the next step using Task Values
dbutils.jobs.taskValues.set(key="current_id_file", value=id_file)
dbutils.jobs.taskValues.set(key="current_file_name", value=file_name)

print("🚀 Baton passed successfully to the Dimension task!")