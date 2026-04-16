import os
import glob
import chardet
# ... other imports ...

BRONZE_VOLUME_PATH = "/Volumes/open_meteorological_data_brazil/bronze/csvfileraw/"

print("Scanning Bronze volume for station metadata...")

# Use standard Python to find all CSV files in the directory
# This automatically returns standard local paths, so you don't even need to strip "dbfs:" anymore!
file_paths = glob.glob(os.path.join(BRONZE_VOLUME_PATH, "*.csv"))

station_records = []

for local_path in file_paths:
    # 1. Open file natively (local_path is already perfect)
    with open(local_path, 'rb') as f:
        raw_head_bytes = f.read(2000)
    
    detected_encoding = chardet.detect(raw_head_bytes)['encoding'] or "UTF-8"
        
    # 2. Decode and parse the first 8 lines
    head_text = raw_head_bytes.decode(detected_encoding).split('\n')
    station_meta = {}
    
    for i in range(8): 
        if len(head_text) > i and ';' in head_text[i]:
            key, value = head_text[i].split(';', 1)
            clean_key = key.replace(':', '').strip()
            station_meta[clean_key] = value.strip()
    
    # 3. Extract and clean values
    station_code = station_meta.get('CODIGO (WMO)')
    
    if station_code:
        # Convert comma decimals to dots for coordinates
        lat_str = station_meta.get('LATITUDE', '').replace(',', '.')
        lon_str = station_meta.get('LONGITUDE', '').replace(',', '.')
        alt_str = station_meta.get('ALTITUDE', '').replace(',', '.')
        
        # Parse founding date (INMET formats can vary, usually DD/MM/YY or DD/MM/YYYY)
        founding_date = None
        raw_date = station_meta.get('DATA DE FUNDACAO', '')
        if '/' in raw_date:
            try:
                # Try 4-digit year first, then 2-digit year
                founding_date = datetime.strptime(raw_date, '%d/%m/%Y').date()
            except ValueError:
                try:
                    founding_date = datetime.strptime(raw_date, '%d/%m/%y').date()
                except ValueError:
                    pass

        # Build the record dictionary
        station_records.append({
            "station_code": station_code,
            "region": station_meta.get('REGIAO', 'UNKNOWN'),
            "state": station_meta.get('UF', 'UNKNOWN'),
            "city": station_meta.get('ESTACAO', 'UNKNOWN'),
            "latitude": float(lat_str) if lat_str.replace('.','',1).lstrip('-').isdigit() else None,
            "longitude": float(lon_str) if lon_str.replace('.','',1).lstrip('-').isdigit() else None,
            "altitude": float(alt_str) if alt_str.replace('.','',1).lstrip('-').isdigit() else None,
            "founding_date": founding_date,
            "last_updated": datetime.now()
        })

print(f"Extracted metadata from {len(files)} files.")

# 4. Build DataFrame and Deduplicate
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

# Create DF and keep only one row per station (the most recent one processed)
dim_df = spark.createDataFrame(station_records, schema=dim_schema)
unique_dim_df = dim_df.dropDuplicates(["station_code"])

print(f"Found {unique_dim_df.count()} unique weather stations. Merging into Silver layer...")

# 5. Bulk MERGE into Dimension Table
target_dim = DeltaTable.forName(spark, DIMENSION_TABLE)

target_dim.alias("target").merge(
    unique_dim_df.alias("source"),
    "target.station_code = source.station_code"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

print("✅ Station Dimension table successfully updated!")