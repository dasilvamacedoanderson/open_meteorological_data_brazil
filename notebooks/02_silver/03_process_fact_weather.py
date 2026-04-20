# ==============================================================================
# Script: 02_silver/03_process_fact_weather.py
# Purpose: SKELETON - Catch values from Task 1 and close the DB Queue.
# ==============================================================================

import sys
from databricks.sdk.runtime import dbutils, spark

CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"

# 1. PULL the values from the Router Task
# Ensure "get_register_task" matches the exact name of Task 1 in the UI
id_file = dbutils.jobs.taskValues.get(taskKey="get_register_task", key="current_id_file", default=-1)
file_name = dbutils.jobs.taskValues.get(taskKey="get_register_task", key="current_file_name", default="")

if id_file == -1 or not file_name:
    print("❌ Error: Failed to receive variables from the previous task.")
    sys.exit(1)

print(f"🚀 Fact Task reached for ID {id_file} ({file_name})")

# ==========================================
# PHASE 2: FACT EXTRACTION & LOAD (PLACEHOLDER)
# ==========================================
print("🚧 Fact processing logic will be built here later...")
# [Future logic to clean and load hourly_weather goes here]

# ==========================================
# PHASE 3: UPDATE CONTROL TABLE
# ==========================================
spark.sql(f"""
    UPDATE {CONTROL_TABLE}
    SET PROCESSED_TIMESTAMP = current_timestamp()
    WHERE IDFILE = {id_file}
""")

print(f"🏁 SUCCESS! Orchestration complete. IDFILE {id_file} marked as processed.")