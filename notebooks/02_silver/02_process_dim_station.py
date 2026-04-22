# ==============================================================================
# Script: 02_silver/02_process_dim_station.py
# Purpose: Extract metadata and UPSERT Dimension (Triggered by Databricks For Each)
# ==============================================================================

import os
import sys
import argparse
import chardet
from datetime import datetime
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, TimestampType
from delta.tables import DeltaTable
from databricks.sdk.runtime import spark

BRONZE_CSV_VOLUME = "/Volumes/open_meteorological_data_brazil/bronze/csvfileraw/"
DIMENSION_TABLE = "open_meteorological_data_brazil.silver.station_dimension"

# 1. Catch the exact item from the Databricks For Each loop using argparse
parser = argparse.ArgumentParser()
parser.add_argument('--id', type=str, required=True)
parser.add_argument('--file', type=str, required=True)
args, unknown = parser.parse_known_args()

id_file = args.id
file_name = args.file

local_path = os.path.join(BRONZE_CSV_VOLUME, file_name)

print(f"🚀 Iteration started for ID {id_file}: {file_name}")

if not os.path.exists(local_path):
    print(f"❌ Error: File {file_name} missing from volume!")
    sys.exit(1)

# ==========================================
# PHASE 1: DIMENSION EXTRACTION & UPSERT
# ... (The rest of your extraction logic remains exactly the same) ...