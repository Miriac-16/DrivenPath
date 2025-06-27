-- Crea el esquema golden_layer si no existe (ya lo hace el script python, pero es buena práctica aquí también)
CREATE SCHEMA IF NOT EXISTS golden_layer;

DROP TABLE IF EXISTS golden_layer.support_data CASCADE;
CREATE TABLE golden_layer.support_data AS
SELECT
    dp.unique_id,
    dp.user_name,
    dp.email,
    da.mac_address,
    da.ip_address,
    fnu.session_duration,
    fnu.download_speed,
    fnu.upload_speed,
    fnu.consumed_traffic,
    dd.accessed_at
FROM
    silver_layer.dim_person dp
INNER JOIN
    silver_layer.dim_address da ON dp.unique_id = da.unique_id
INNER JOIN
    silver_layer.fact_network_usage fnu ON dp.unique_id = fnu.unique_id
INNER JOIN
    silver_layer.dim_date dd ON dp.unique_id = dd.unique_id;