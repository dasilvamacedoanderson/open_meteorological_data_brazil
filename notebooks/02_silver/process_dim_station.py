# ==============================================================================
# Script: 02_silver/process_dim_station.py
# Purpose: Extract station metadata from the next pending CSV and update Dimensions.
# ==============================================================================

import os
import chardet
from datetime import datetime
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, TimestampType
from delta.tables import DeltaTable

# 1. Configuration 
BRONZE_CSV_VOLUME = "/Volumes/open_meteorological_data_brazil/bronze/csvfileraw/"
CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"
DIMENSION_TABLE = "open_meteorological_data_brazil.silver.station_dimension"

print("Checking the control table for the next pending CSV file...")

# 2. Get EXACTLY ONE pending file from the queue
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
    print("No pending CSV files in the queue. Dimension extraction skipping. 💤")
    dbutils.notebook.exit("No files to process")

# 3. Extract file details
row = pending_files[0]
id_file = row["IDFILE"]
file_name = row["FILE_NAME"]
local_path = os.path.join(BRONZE_CSV_VOLUME, file_name)

print(f"\n🚀 Extracting Dimensions for ID {id_file}: {file_name}")

if not os.path.exists(local_path):
    print(f"⚠️ Error: File {file_name} is in the queue but missing from the volume!")
    dbutils.notebook.exit("File missing")

# ==========================================
# PHASE 1: DIMENSION EXTRACTION & UPSERT
# ==========================================

# Open file and detect encoding using the first 2000 bytes
with open(local_path, 'rb') as f:
    raw_head_bytes = f.read(2000)

detected_encoding = chardet.detect(raw_head_bytes)['encoding'] or "UTF-8"
head_text = raw_head_bytes.decode(detected_encoding).split('\n')

# Parse the 8 lines of INMET metadata
station_meta = {}
for i in range(8): 
    if len(head_text) > i and ';' in head_text[i]:
        key, value = head_text[i].split(';', 1)
        clean_key = key.replace(':', '').strip()
        station_meta[clean_key] = value.strip()
        
station_code = station_meta.get('CODIGO (WMO)', 'UNKNOWN')

# Clean Brazilian coordinate formats (comma to dot)
lat_str = station_meta.get('LATITUDE', '').replace(',', '.')
lon_str = station_meta.get('LONGITUDE', '').replace(',', '.')
alt_str = station_meta.get('ALTITUDE', '').replace(',', '.')

# Parse the founding date safely
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

# Construct the single-row record
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

# Create DataFrame
dim_df = spark.createDataFrame(dim_record, schema=dim_schema)

# MERGE into Dimension Table (Upsert)
target_dim = DeltaTable.forName(spark, DIMENSION_TABLE)
target_dim.alias("target").merge(
    dim_df.alias("source"),
    "target.station_code = source.station_code"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

print(f" -> ✅ Dimension updated successfully for Station: {station_code}")
print(f" -> ⏭️  Passing file {file_name} to the Fact processor...")