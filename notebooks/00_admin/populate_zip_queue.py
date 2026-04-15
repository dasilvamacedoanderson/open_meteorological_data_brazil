# ==============================================================================
# Notebook: 00_admin/populate_zip_queue.py
# Purpose: Pre-register INMET ZIP files into the control table without duplicates.
# ==============================================================================

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import Row
from delta.tables import DeltaTable  # <-- Important: Import the DeltaTable API

CONTROL_TABLE_NAME = "open_meteorological_data_brazil.admin.control_table"

# 1. Define the years you want to ingest
years_to_ingest = [2021, 2022, 2023]
pending_records = []

for year in years_to_ingest:
    pending_records.append(Row(
        SOURCE="INMET",
        SOURCE_ADDRESS=f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip",
        FILE_NAME=f"{year}.zip",
        FILE_TYPE="ZIP",
        SIZE_BYTES=None, 
        FILE_HASH=None,
        PARENT_FILE=None,
        LOAD_TIMESTAMP=None  
    ))

queue_schema = StructType([
    StructField("SOURCE", StringType(), True),
    StructField("SOURCE_ADDRESS", StringType(), True),
    StructField("FILE_NAME", StringType(), True),
    StructField("FILE_TYPE", StringType(), True),
    StructField("SIZE_BYTES", LongType(), True),
    StructField("FILE_HASH", StringType(), True),
    StructField("PARENT_FILE", LongType(), True),
    StructField("LOAD_TIMESTAMP", TimestampType(), True)
])

# Create DataFrame of the files we WANT to be in the queue
source_df = spark.createDataFrame(pending_records, schema=queue_schema)

print(f"Checking {len(years_to_ingest)} files against the control queue...")

# 2. Perform the MERGE operation
# First, we connect to the existing Delta table in your catalog
target_table = DeltaTable.forName(spark, CONTROL_TABLE_NAME)



# Now we run the merge logic
target_table.alias("target") \
    .merge(
        source_df.alias("source"),
        "target.SOURCE_ADDRESS = source.SOURCE_ADDRESS"  
    ) \
    .whenNotMatchedInsert(values={
        "SOURCE": "source.SOURCE",
        "SOURCE_ADDRESS": "source.SOURCE_ADDRESS",
        "FILE_NAME": "source.FILE_NAME",
        "FILE_TYPE": "source.FILE_TYPE",
        "SIZE_BYTES": "source.SIZE_BYTES",
        "FILE_HASH": "source.FILE_HASH",
        "PARENT_FILE": "source.PARENT_FILE",
        "LOAD_TIMESTAMP": "source.LOAD_TIMESTAMP"
    }) \
    .execute()

print("Queue synchronization complete! No duplicates were added. 📝")