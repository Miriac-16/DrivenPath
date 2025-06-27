-- Crea el esquema golden_layer si no existe (ya lo hace el script python, pero es buena práctica aquí también)
CREATE SCHEMA IF NOT EXISTS golden_layer;

DROP TABLE IF EXISTS golden_layer.financial_data CASCADE;
CREATE TABLE golden_layer.financial_data AS
SELECT
    dp.unique_id,
    dp.person_name,
    dp.email,
    df.iban,
    dd.accessed_at
FROM
    silver_layer.dim_person dp
INNER JOIN
    silver_layer.dim_finance df ON dp.unique_id = df.unique_id
INNER JOIN
    silver_layer.dim_date dd ON dp.unique_id = dd.unique_id;