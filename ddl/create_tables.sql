-- ==============================================================================
-- Script: ddl/create_unity_catalog_objects.sql
-- Purpose: Define the Medallion architecture using Databricks Unity Catalog
-- Namespace: catalog.schema.table_or_volume
-- ==============================================================================

-- ==========================================
-- 0. CATALOG SETUP
-- ==========================================
-- Ensure your catalog exists
CREATE CATALOG IF NOT EXISTS open_meteorological_data_brazil;

-- Set the active catalog so we don't have to type it every time
USE CATALOG open_meteorological_data_brazil;


-- ==========================================
-- 1. BRONZE LAYER (Raw Data & Volumes)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create a Managed Volume to hold the raw INMET CSV files
-- Unity Catalog will automatically manage the underlying cloud storage for this volume
CREATE VOLUME IF NOT EXISTS bronze.csvfileraw;


-- ==========================================
-- 2. SILVER LAYER (Cleaned & Standardized)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS silver;

-- Create the managed Delta table for cleaned hourly data
CREATE TABLE IF NOT EXISTS silver.hourly_weather (
    station_code STRING,
    region STRING,
    state STRING,
    city STRING,
    observation_date DATE,
    observation_hour INT,
    precipitation_mm DOUBLE,
    temp_dry_bulb_c DOUBLE,
    temp_dew_point_c DOUBLE,
    humidity_pct INT,
    wind_speed_ms DOUBLE,
    load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (observation_date);


-- ==========================================
-- 3. GOLD LAYER (Business Aggregations)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS gold;

-- Table for summarizing annual metrics
CREATE TABLE IF NOT EXISTS gold.yearly_summary (
    city STRING,
    observation_year INT,
    total_precipitation_mm DOUBLE,
    avg_temperature_c DOUBLE,
    max_temperature_c DOUBLE,
    days_with_rain INT,
    last_updated TIMESTAMP
)
USING DELTA;

-- Table for comparing a current year against historical baselines
CREATE TABLE IF NOT EXISTS gold.historical_comparisons (
    city STRING,
    comparison_year INT,
    baseline_decade STRING,
    rainfall_variance_pct DOUBLE,
    temp_variance_c DOUBLE,
    weather_category STRING
)
USING DELTA;


-- ==========================================
-- 4. ADMIN LAYER (Job Control & Metadata)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS admin;

-- Audit table to track file ingestion
CREATE TABLE IF NOT EXISTS admin.control_table (
    SOURCE STRING,
    SOURCE_ADDRESS STRING,
    FILE_NAME STRING,
    FILE_TYPE STRING,
    SIZE_BYTES BIGINT,
    PARENT_FILE STRING,
    LOAD_TIMESTAMP TIMESTAMP
)
USING DELTA;