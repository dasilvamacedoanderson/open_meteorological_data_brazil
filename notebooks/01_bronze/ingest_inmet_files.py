import requests
import zipfile
import io
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# 1. Configuration
YEAR_TO_DOWNLOAD = "2023"
INMET_URL = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{YEAR_TO_DOWNLOAD}.zip"
BRONZE_MOUNT_PATH = f"dbfs:/FileStore/data/weather/bronze/inmet/{YEAR_TO_DOWNLOAD}/"

# List to hold our metadata records before saving to the Control Table
audit_log = []
current_time = datetime.now()