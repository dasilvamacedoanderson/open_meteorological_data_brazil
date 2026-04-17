# ==============================================================================
# Script: 02_silver/process_arriving_csv.py
# Purpose: Read queue, process new CSVs to Dim/Fact, and mark as processed.
# ==============================================================================

import os
import chardet
from datetime import datetime
from pyspark.sql.functions import col, regexp_replace, to_date, substring, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, DateType, TimestampType
from delta.tables import DeltaTable

# 1. Configuration 
BRONZE_CSV_VOLUME = "/Volumes/open_meteorological_data_brazil/bronze/csvfileraw/"
CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"
DIMENSION_TABLE = "open_meteorological_data_brazil.silver.station_dimension"
FACT_TABLE = "open_meteorological_data_brazil.silver.hourly_weather"

print("Checking the control table for pending CSV files...")

# 2. Get pending files from the Control Table Queue
# We limit to 5 per run to keep the micro-batch fast, but you can adjust this.
pending_csv_df = spark.sql(f"""
    SELECT IDFILE, FILE_NAME 
    FROM {CONTROL_TABLE}
    WHERE FILE_TYPE = 'CSV' 
      AND PROCESSED_TIMESTAMP IS NULL
    ORDER BY LOAD_TIMESTAMP ASC
    LIMIT 5
""")

pending_files = pending_csv_df.collect()

if not pending_files:
    print("No pending CSV files in the queue. Going back to sleep. 💤")
    dbutils.notebook.exit("No files to process")

for row in pending_files:
    id_file = row["IDFILE"]
    file_name = row["FILE_NAME"]
    local_path = os.path.join(BRONZE_CSV_VOLUME, file_name)
    
    print(f"\n🚀 Processing ID {id_file}: {file_name}")
    
    # Check if the file physically exists before trying to open it
    if not os.path.exists(local_path):
        print(f"⚠️ Warning: File {file_name} is in the queue but missing from the volume! Skipping.")
        continue

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
            
    station_code = station_meta.get('CODIGO (WMO)', 'UNKNOWN')
    
    # Parse Coordinates
    lat_str = station_meta.get('LATITUDE', '').replace(',', '.')
    lon_str = station_meta.get('LONGITUDE', '').replace(',', '.')
    alt_str = station_meta.get('ALTITUDE', '').replace(',', '.')
    
    # Parse Founding Date
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
    
    # MERGE into Dimension Table
    target_dim = DeltaTable.forName(spark, DIMENSION_TABLE)
    target_dim.alias("target").merge(
        dim_df.alias("source"),
        "target.station_code = source.station_code"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()
    
    print(f" -> ✅ Dimension updated for Station: {station_code}")

    # ==========================================
    # PHASE 2: FACT EXTRACTION & LOAD
    # ==========================================
    raw_df = spark.read \
        .option("delimiter", ";") \
        .option("encoding", detected_encoding) \
        .option("header", "false") \
        .csv(local_path)
    
    filtered_df = raw_df.filter(col("_c0").rlike(r"^\d{4}[-/]\d{2}[-/]\d{2}"))
    
    def clean_numeric(column_name):
        return regexp_replace(col(column_name), ",", ".").cast(DoubleType())

    clean_df = filtered_df \
        .withColumn("station_code", lit(station_code)) \
        .withColumn("observation_date", to_date(regexp_replace(col("_c0"), "/", "-"))) \
        .withColumn("observation_hour", substring(col("_c1"), 1, 2).cast(IntegerType())) \
        .withColumn("precipitation_mm", clean_numeric("_c2")) \
        .withColumn("temp_dry_bulb_c", clean_numeric("_c3")) \
        .withColumn("temp_dew_point_c", clean_numeric("_c4")) \
        .withColumn("humidity_pct", col("_c5").cast(IntegerType())) \
        .withColumn("wind_speed_ms", clean_numeric("_c6")) \
        .withColumn("load_timestamp", current_timestamp())

    final_fact_df = clean_df.select(
        "station_code", "observation_date", "observation_hour", 
        "precipitation_mm", "temp_dry_bulb_c", "temp_dew_point_c", 
        "humidity_pct", "wind_speed_ms", "load_timestamp"
    )

    final_fact_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(FACT_TABLE)
        
    print(f" -> ✅ Loaded Fact data into hourly_weather.")

    # ==========================================
    # PHASE 3: UPDATE CONTROL QUEUE
    # ==========================================
    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET PROCESSED_TIMESTAMP = current_timestamp()
        WHERE IDFILE = {id_file}
    """)
    print(f" -> 📝 Task {id_file} marked as processed in control table.")

print("\n🏁 Event processing complete. Awaiting next batch...")