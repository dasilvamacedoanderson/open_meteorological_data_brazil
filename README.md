# Historical Weather Datalake
## 🌦️ Project Overview
This project is a data engineering pipeline built on **Databricks (Free Edition)** and **PySpark**. The goal is to ingest historical, hourly weather data and process it to identify long-term climate patterns. Though that data is possible to produce insights to understand the weather behavior on each year period.

🏗️ Architecture
The pipeline follows the **Medallion Architecture** to ensure data quality, reliability, and maintainability:

* **🥉 Bronze Layer:** Raw, hourly weather data ingested directly from FTP' National Institute of Meteorology avaliable by Brazilian Ministry of Agriculture and Livestock. Data is stored in its original CSV format to preserve an untouched historical record.
* **🥈 Silver Layer:** Cleaned and transformed data. In this layer, missing values are handled, timestamps are standardized to the local timezone, and data types are cast appropriately. Data is saved as structured Parquet/Delta files.
* **🥇 Gold Layer:** Business-level aggregations. This layer summarizes annual rainfall and temperature averages, containing the logic to match the current year against historical data for pattern recognition.

## 🛠️ Technology Stack
* **Platform:** Databricks (Free Edition)
* **Data Processing:** PySpark, Python
* **Data Source:** FTP' National Institute of Meteorology avaliable by Brazilian Ministry of Agriculture
* **Version Control:** Git & GitHub (integrated via Databricks Repos)

## 📁 Repository Structure
```text
weather-datalake-project/
│
├── .gitignore               # Excludes sensitive info (API keys, passwords)
├── README.md                # Project documentation
│
├── config/                  
│   └── project_config.py    # Global variables: API endpoints, coordinates, dates
│
├── utils/                   
│   └── spark_helpers.py     # Reusable PySpark data cleaning functions
│
├── notebooks/               
│   ├── 01_bronze/           
│   │   └── ingest_inmet_files.py   # Fetches FTP data and saves to DBFS
│   ├── 02_silver/           
│   │   └── clean_weather_data.py   # Cleans Bronze data and structures it
│   └── 03_gold/             
│       ├── aggregate_yearly.py     # Summarizes key metrics (e.g., total rainfall)
│       └── compare_historical.py   # Logic to compare current vs. past years