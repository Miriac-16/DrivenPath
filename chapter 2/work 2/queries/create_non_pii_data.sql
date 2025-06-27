-- Crea el esquema golden_layer si no existe (ya lo hace el script python, pero es buena práctica aquí también)
CREATE SCHEMA IF NOT EXISTS golden_layer;

DROP TABLE IF EXISTS golden_layer.non_pii_data CASCADE;
CREATE TABLE golden_layer.non_pii_data AS
SELECT
    fnu.session_duration,
    fnu.download_speed,
    fnu.upload_speed,
    fnu.consumed_traffic,
    da.address,
    da.mac_address,
    da.ip_address,
    dd.accessed_at,
    dp.user_name,
    fnu.unique_id
FROM
    silver_layer.fact_network_usage fnu
INNER JOIN
    silver_layer.dim_address da ON fnu.unique_id = da.unique_id
INNER JOIN
    silver_layer.dim_date dd ON da.unique_id = dd.unique_id
INNER JOIN
    silver_layer.dim_finance df ON dd.unique_id = df.unique_id
INNER JOIN
    silver_layer.dim_person dp ON df.unique_id = dp.unique_id;