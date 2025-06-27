-- Crea el esquema silver_layer si no existe (ya lo hace el script python, pero es buena práctica aquí también)
CREATE SCHEMA IF NOT EXISTS silver_layer;

-- Crea dim_person
DROP TABLE IF EXISTS silver_layer.dim_person CASCADE;
CREATE TABLE silver_layer.dim_person AS
SELECT DISTINCT
    unique_id,
    person_name,
    user_name,
    email,
    personal_number,
    birth_date,
    phone
FROM silver_layer.staging_silver;

-- Crea dim_address
DROP TABLE IF EXISTS silver_layer.dim_address CASCADE;
CREATE TABLE silver_layer.dim_address AS
SELECT DISTINCT
    unique_id,
    address,
    mac_address,
    ip_address
FROM silver_layer.staging_silver;

-- Crea dim_date
DROP TABLE IF EXISTS silver_layer.dim_date CASCADE;
CREATE TABLE silver_layer.dim_date AS
SELECT DISTINCT
    unique_id,
    accessed_at
FROM silver_layer.staging_silver;

-- Crea dim_finance
DROP TABLE IF EXISTS silver_layer.dim_finance CASCADE;
CREATE TABLE silver_layer.dim_finance AS
SELECT DISTINCT
    unique_id,
    iban
FROM silver_layer.staging_silver;

-- Crea fact_network_usage
DROP TABLE IF EXISTS silver_layer.fact_network_usage CASCADE;
CREATE TABLE silver_layer.fact_network_usage AS
SELECT
    unique_id,
    session_duration,
    download_speed,
    upload_speed,
    consumed_traffic
FROM silver_layer.staging_silver;