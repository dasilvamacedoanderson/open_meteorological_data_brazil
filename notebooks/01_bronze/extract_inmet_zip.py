# ==============================================================================
# Notebook: 01_bronze/extract_inmet_zip.py
# Purpose: Unzip raw INMET data, save CSVs to Volume, and log lineage in control table.
# ==============================================================================

import zipfile
import io
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# 1. Configuration
CONTROL_TABLE_NAME = "open_meteorological_data_brazil.admin.control_table"
ZIP_VOLUME_PATH = "/Volumes/open_meteorological_data_brazil/bronze/zipfileraw/"
CSV_VOLUME_PATH = "/Volumes/open_meteorological_data_brazil/bronze/csvfileraw/"

print("Checking for ZIP files that need extraction...")

# 2. Find a ZIP file that hasn't been extracted yet
# We check if the ZIP's IDFILE exists as a PARENT_FILE for any CSVs.
pending_zip_df = spark.sql(f"""
    SELECT IDFILE, FILE_NAME 
    FROM {CONTROL_TABLE_NAME} z
    WHERE FILE_TYPE = 'ZIP' 
      AND LOAD_TIMESTAMP IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM {CONTROL_TABLE_NAME} c 
          WHERE c.PARENT_FILE = z.IDFILE 
            AND c.FILE_TYPE = 'CSV'
      )
    ORDER BY FILE_NAME ASC
    LIMIT 1
""")

if pending_zip_df.count() == 0:
    print("🎉 All ZIP files have been extracted!")
    dbutils.notebook.exit("Extraction complete")

# 3. Get ZIP details
zip_row = pending_zip_df.collect()[0]
parent_id = zip_row["IDFILE"]
zip_name = zip_row["FILE_NAME"]

print(f"Extracting: {zip_name} (ID: {parent_id})")
full_zip_path = ZIP_VOLUME_PATH + zip_name

# 4. Read the ZIP from the Volume
with open(full_zip_path, "rb") as f:
    zip_bytes = f.read()

audit_records = []
current_time = datetime.now()

# 5. Extract the contents
with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
    for file_info in z.infolist():
        if file_info.filename.upper().endswith('.CSV'):
            
            # Read CSV and handle Portuguese characters (latin1)
            csv_bytes = z.read(file_info.filename)
            csv_text = csv_bytes.decode('latin1')
            
            # Clean filename (removes any internal folders from the zip structure)
            clean_csv_name = file_info.filename.split('/')[-1]
            csv_save_path = CSV_VOLUME_PATH + clean_csv_name
            
            # Save the raw CSV text to the new volume
            # We use 'w' and specify latin1 to preserve the characters
            with open(csv_save_path, "w", encoding="latin1") as out_f:
                out_f.write(csv_text)
            
            # Prepare the audit record for this specific CSV
            audit_records.append({
                "SOURCE": "INMET",
                "SOURCE_ADDRESS": f"Extracted from {zip_name}",
                "FILE_NAME": clean_csv_name,
                "FILE_TYPE": "CSV",
                "SIZE_BYTES": file_info.file_size,
                "FILE_HASH": None, # We skip hashing the CSVs to save compute time
                "PARENT_FILE": parent_id, # Links directly to the ZIP!
                "LOAD_TIMESTAMP": current_time
            })

print(f"Extracted {len(audit_records)} CSV files. Updating control table...")

# 6. Save the CSV records to the control table
audit_schema = StructType([
    StructField("SOURCE", StringType(), True),
    StructField("SOURCE_ADDRESS", StringType(), True),
    StructField("FILE_NAME", StringType(), True),
    StructField("FILE_TYPE", StringType(), True),
    StructField("SIZE_BYTES", LongType(), True),
    StructField("FILE_HASH", StringType(), True),
    StructField("PARENT_FILE", LongType(), True),
    StructField("LOAD_TIMESTAMP", TimestampType(), True)
])

audit_df = spark.createDataFrame(audit_records, schema=audit_schema)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(CONTROL_TABLE_NAME)

print(f"✅ Extraction complete for {zip_name}!")