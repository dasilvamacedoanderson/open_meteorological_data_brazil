import json
import sys
from databricks.sdk.runtime import dbutils, spark

CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"

print("Scanning for pending CSV files...")

pending_csv_df = spark.sql(f"""
    SELECT IDFILE, FILE_NAME 
    FROM {CONTROL_TABLE}
    WHERE FILE_TYPE = 'CSV' 
      AND PROCESSED_TIMESTAMP IS NULL
""")

pending_files_raw = pending_csv_df.collect()

if not pending_files_raw:
    print("No pending files. 💤")
    dbutils.notebook.exit("No files to process")

# Create the list of dictionaries
file_list = [{"id": row["IDFILE"], "file": row["FILE_NAME"]} for row in pending_files_raw]

# Push the JSON array into the Databricks Workflow memory
dbutils.jobs.taskValues.set(key="file_array", value=json.dumps(file_list))

print(f"📦 Array of {len(file_list)} files passed to the For Each loop!")