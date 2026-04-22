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
# Changed required=False to prevent crashes during manual interactive testing
parser.add_argument('--id', type=str, required=False)
parser.add_argument('--file', type=str, required=False)

# Use parse_known_args() to ignore any hidden IPython arguments Databricks injects
args, unknown = parser.parse_known_args()

# 2. Determine if this is a Workflow Run or a Manual Test
if args.id and args.file:
    id_file = args.id
    file_name = args.file
    print("🔔 Job Parameters received from Workflow Loop!")
else:
    print("⚠️ No arguments provided. Entering Manual Testing Mode...")
    # HARDCODE values here that you know exist so you can test the script manually
    id_file = "999" 
    file_name = "2023.csv" 

local_path = os.path.join(BRONZE_CSV_VOLUME, file_name)

print(f"🚀 Iteration started for ID {id_file}: {file_name}")

if not os.path.exists(local_path):
    print(f"❌ Error: File {file_name} missing from volume at {local_path}!")
    sys.exit(1)

# ==========================================
# PHASE 1: DIMENSION EXTRACTION & UPSERT
# ==========================================
with open(local_path, 'rb') as f:
    raw_head_bytes = f.read(2000)

detected_encoding = chardet.detect(raw_head_bytes)['encoding'] or "UTF-8"
head_text = raw_head_bytes.decode(detected_encoding).split('\n')

station_meta = {}
for i in range(8): 
    if len(head_text) > i and ';' in head_text[i]:
        key, value = head_text[i].split(';', 1)
        clean_key = key.replace(':', '').strip()
        station_meta[clean_key] = value.strip()
        
station_code = station_meta.get('CODIGO (WMO)')

if station_code:
    lat_str = station_meta.get('LATITUDE', '').replace(',', '.')
    lon_str = station_meta.get('LONGITUDE', '').replace(',', '.')
    alt_str = station_meta.get('ALTITUDE', '').replace(',', '.')

    founding_date = None
    raw_date = station_meta.get('DATA DE FUNDACAO', '')
    if '/' in raw_date:
        try:
            founding_date = datetime.strptime(raw_date, '%d/%m/%Y').date()
        except ValueError:
            try:
                founding_date = datetime.strptime(raw_date, '%d/%m/%y').date()
            except ValueError:
                pass

    dim_record = [{
        "station_code": station_code,
        "region": station_meta.get('REGIAO', 'UNKNOWN'),
        "state": station_meta.get('UF', 'UNKNOWN'),
        "city": station_meta.get('ESTACAO', 'UNKNOWN'),
        "latitude": float(lat_str) if lat_str.replace('.','',1).lstrip('-').isdigit() else None,
        "longitude": float(lon_str) if lon_str.replace('.','',1).lstrip('-').isdigit() else None,
        "altitude": float(alt_str) if alt_str.replace('.','',1).lstrip('-').isdigit() else None,
        "founding_date": founding_date,
        "last_updated": datetime.now()
    }]

    dim_schema = StructType([
        StructField("station_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("founding_date", DateType(), True),
        StructField("last_updated", TimestampType(), True)
    ])

    dim_df = spark.createDataFrame(dim_record, schema=dim_schema)

    # Execute the MERGE for this single file
    target_dim = DeltaTable.forName(spark, DIMENSION_TABLE)
    target_dim.alias("target").merge(
        dim_df.alias("source"),
        "target.station_code = source.station_code"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

    print(f"✅ Dimension updated for {file_name}.")
else:
    print(f"⚠️ Warning: No valid Station Code found in {file_name}. Skipping Dimension Upsert.")