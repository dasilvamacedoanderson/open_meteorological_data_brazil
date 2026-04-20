# ==============================================================================
# Script: 02_silver/process_dim_station.py
# Purpose: Extract dimensions triggered by Job-level Key-Value parameters.
# ==============================================================================

import os
import argparse
import chardet
from datetime import datetime
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, TimestampType
from delta.tables import DeltaTable

# 1. Configuration 
CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"
DIMENSION_TABLE = "open_meteorological_data_brazil.silver.station_dimension"

# 2. Catch the Key-Value Job Parameter using argparse
parser = argparse.ArgumentParser(description="Process INMET Station Dimensions")
# We define the key we expect to receive from the Databricks Job
parser.add_argument('--file_path', type=str, required=True, help='Path to the arrived CSV file')

# Parse the arguments
args = parser.parse_args()
trigger_file_path = args.file_path

file_name = os.path.basename(trigger_file_path)
print(f"🔔 Job Parameter received! Processing file: {file_name}")

# Convert path for native Python operations (in case Databricks passes a dbfs: prefix)
local_path = trigger_file_path.replace("dbfs:", "") if trigger_file_path.startswith("dbfs:") else trigger_file_path

if not os.path.exists(local_path):
    print(f"⚠️ Error: File {file_name} triggered the job, but cannot be found at {local_path}!")
    import sys
    sys.exit(1)

# 3. Retrieve the IDFILE from the Control Table
id_lookup_df = spark.sql(f"""
    SELECT IDFILE 
    FROM {CONTROL_TABLE}
    WHERE FILE_NAME = '{file_name}'
""")

id_records = id_lookup_df.collect()

if not id_records:
    print(f"⚠️ Error: {file_name} arrived, but is not registered in the control table!")
    import sys
    sys.exit(1)

id_file = id_records[0]["IDFILE"]
print(f" -> Lineage Confirmed: File is registered as Task ID {id_file}")

# ==========================================
# PHASE 1: DIMENSION EXTRACTION & UPSERT
# (The rest of your script remains identical from this point down)
# ==========================================