# ==============================================================================
# Script: 02_silver/03_process_fact_weather.py
# Purpose: Pull values from Task 1, load Fact data, and close the DB Queue.
# ==============================================================================

import os
import sys
import chardet
from pyspark.sql.functions import col, regexp_replace, to_date, substring, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType
from databricks.sdk.runtime import dbutils, spark

BRONZE_CSV_VOLUME = "/Volumes/open_meteorological_data_brazil/bronze/csvfileraw/"
FACT_TABLE = "open_meteorological_data_brazil.silver.hourly_weather"
CONTROL_TABLE = "open_meteorological_data_brazil.admin.control_table"

# 1. PULL the values from the Router Task
id_file = dbutils.jobs.taskValues.get(taskKey="get_register_task", key="current_id_file", default=-1)
file_name = dbutils.jobs.taskValues.get(taskKey="get_register_task", key="current_file_name", default="")

if id_file == -1 or not file_name:
    print("❌ Error: Failed to receive variables from the previous task.")
    sys.exit(1)

local_path = os.path.join(BRONZE_CSV_VOLUME, file_name)
print(f"🚀 Fact Task started for ID {id_file} ({file_name})")

if not os.path.exists(local_path):
    print(f"⚠️ Error: File {file_name} is missing from the volume!")
    sys.exit(1)

# ==========================================
# PHASE 2: FACT EXTRACTION & LOAD
# ==========================================
with open(local_path, 'rb') as f:
    raw_head_bytes = f.read(2000)

detected_encoding = chardet.detect(raw_head_bytes)['encoding'] or "UTF-8"
head_text = raw_head_bytes.decode(detected_encoding).split('\n')

station_code = 'UNKNOWN'
for i in range(8): 
    if len(head_text) > i and ';' in head_text[i]:
        key, value = head_text[i].split(';', 1)
        if 'CODIGO' in key.upper():
            station_code = value.strip()
            break

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
    
print(f"✅ Fact data loaded for Station {station_code}.")

# ==========================================
# PHASE 3: UPDATE CONTROL TABLE
# ==========================================
spark.sql(f"""
    UPDATE {CONTROL_TABLE}
    SET PROCESSED_TIMESTAMP = current_timestamp()
    WHERE IDFILE = {id_file}
""")

print(f"🏁 SUCCESS! IDFILE {id_file} marked as completely processed.")