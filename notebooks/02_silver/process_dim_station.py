# ==============================================================================
# Script: 02_silver/process_dim_station.py
# Purpose: Extract dimensions triggered directly by File Arrival events.
# ==============================================================================

import os
import sys
import chardet
from datetime import datetime
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, TimestampType
from delta.tables import DeltaTable

# 1. Configuration 
CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"
DIMENSION_TABLE = "open_meteorological_data_brazil.silver.station_dimension"

# 2. Catch the File Path from the Databricks Job Trigger
if len(sys.argv) < 2:
    print("❌ Error: No file location provided by the trigger.")
    sys.exit(1)

# The trigger passes the full path (e.g., /Volumes/.../file.CSV)
trigger_file_path = sys.argv[1]
file_name = os.path.basename(trigger_file_path)

print(f"🔔 Trigger received! Processing newly arrived file: {file_name}")

# Convert path for native Python operations (in case Databricks passes a dbfs: prefix)
local_path = trigger_file_path.replace("dbfs:", "") if trigger_file_path.startswith("dbfs:") else trigger_file_path

if not os.path.exists(local_path):
    print(f"⚠️ Error: File {file_name} triggered the job, but cannot be found at {local_path}!")
    sys.exit(1)

# 3. Retrieve the IDFILE from the Control Table
# We still link back to the queue to maintain our data lineage
id_lookup_df = spark.sql(f"""
    SELECT IDFILE 
    FROM {CONTROL_TABLE}
    WHERE FILE_NAME = '{file_name}'
""")

id_records = id_lookup_df.collect()

if not id_records:
    print(f"⚠️ Error: {file_name} arrived, but is not registered in the control table!")
    sys.exit(1)

id_file = id_records[0]["IDFILE"]
print(f" -> Lineage Confirmed: File is registered as Task ID {id_file}")

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