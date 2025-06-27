import polars as pl
import os
from datetime import datetime # Asegúrate de importar datetime si no lo está en la versión original

# Rutas de entrada y salida
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(CURRENT_DIR, '..') # Sube de 'scripts' a 'work 2'

BRONZE_INPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'bronze', 'raw_data.csv')
SILVER_OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'silver')
SILVER_OUTPUT_PATH = os.path.join(SILVER_OUTPUT_DIR, 'transformed_data_silver.parquet')

def transform_data_to_silver():
    """
    Lee datos de la capa Bronze, aplica transformaciones y guarda en la capa Silver.
    """
    print(f"Iniciando transformación de datos de Bronze a Silver...")
    print(f"Leyendo datos desde: {BRONZE_INPUT_PATH}")

    try:
        df_bronze = pl.read_csv(BRONZE_INPUT_PATH)
        print(f"Datos de Bronze cargados. Dimensiones: {df_bronze.shape}")
    except Exception as e:
        print(f"Error al leer el archivo Bronze: {e}")
        print("Asegúrate de que 'raw_data.csv' existe en 'data/bronze/'.")
        return

    # Paso 2: Aplicar transformaciones
    # Corregir el formato de 'accessed_at' para que coincida con la salida de Faker
    # Cambiamos '%Y-%m-%d %H:%M:%S%.f' por '%Y-%m-%dT%H:%M:%S%.f'
    df_silver = df_bronze.with_columns(
        pl.col("accessed_at").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f"), # <--- ¡Aquí está el cambio!
        pl.col("birth_date").str.to_date("%Y-%m-%d"), 
        pl.col("session_duration").cast(pl.UInt32),
        pl.col("download_speed").cast(pl.Float32),
        pl.col("upload_speed").cast(pl.Float32),
        pl.col("consumed_traffic").cast(pl.Float32),
        pl.col("personal_number").cast(pl.String)
    )

    print("Primeros 5 registros del DataFrame Silver (después de transformaciones básicas):")
    print(df_silver.head())
    print("\nTipos de datos del DataFrame Silver:")
    print(df_silver.dtypes)

    os.makedirs(SILVER_OUTPUT_DIR, exist_ok=True)
    df_silver.write_parquet(SILVER_OUTPUT_PATH)
    print(f"Datos transformados y guardados en la capa Silver: {SILVER_OUTPUT_PATH}")

if __name__ == "__main__":
    transform_data_to_silver()