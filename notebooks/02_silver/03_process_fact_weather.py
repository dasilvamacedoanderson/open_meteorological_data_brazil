# ==============================================================================
# Script: 02_silver/03_process_fact_weather.py
# Purpose: Loop through batch for Fact data, and bulk close the DB Queue.
# ==============================================================================

import os
import sys
import json
from databricks.sdk.runtime import dbutils, spark

CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"

# 1. PULL the JSON payload and convert back to Python list
json_payload = dbutils.jobs.taskValues.get(taskKey="get_register_task", key="pending_files_json", default="[]")
file_list = json.loads(json_payload)

if not file_list:
    print("❌ Error: Received empty payload from previous task.")
    sys.exit(1)

print(f"🚀 Fact Task started for batch of {len(file_list)} files.")

# ==========================================
# PHASE 2: FACT EXTRACTION (PLACEHOLDER)
# ==========================================
for item in file_list:
    id_file = item["id"]
    file_name = item["file"]
    # [Future logic to read each file, clean, and append to hourly_weather goes here]
    # print(f"Processing facts for {file_name}...")

print("🚧 Fact processing placeholder completed.")

# ==========================================
# PHASE 3: BULK UPDATE CONTROL TABLE
# ==========================================
# Extract just the IDs from our list to build a SQL IN clause (e.g., "1, 2, 3, 4")
id_strings = [str(item["id"]) for item in file_list]
sql_in_clause = ", ".join(id_strings)

print(f"Closing control records for IDs: {sql_in_clause}")

spark.sql(f"""
    UPDATE {CONTROL_TABLE}
    SET PROCESSED_TIMESTAMP = current_timestamp()
    WHERE IDFILE IN ({sql_in_clause})
""")

print(f"🏁 SUCCESS! Batch orchestration complete. {len(file_list)} files marked as processed.")