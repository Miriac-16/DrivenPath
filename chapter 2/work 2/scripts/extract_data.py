import polars as pl
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Determina la ruta del directorio actual (donde está extract_data.py)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Sube un nivel de 'scripts' para llegar a 'work 2'
PROJECT_ROOT = os.path.join(CURRENT_DIR, '..') 
# Construye la ruta completa a la carpeta 'data/bronze' desde 'work 2'
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'bronze')
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, "raw_data.csv")

def generate_synthetic_data(num_records: int = 1000) -> pl.DataFrame:
    """
    Genera un DataFrame de Polars con datos sintéticos basados en las 16 columnas requeridas.
    """
    fake = Faker('es_MX') # Puedes cambiar 'es_MX' a tu localidad preferida (ej. 'en_US', 'es_ES')

    data = []
    unique_ids = set() # Para asegurar que los unique_id sean únicos

    for _ in range(num_records):
        unique_id = fake.uuid4()
        while unique_id in unique_ids: # Regenerar si el ID ya existe
            unique_id = fake.uuid4()
        unique_ids.add(unique_id)

        accessed_at = fake.date_time_between(start_date="-1y", end_date="now")

        record = {
            "unique_id": unique_id,
            "person_name": fake.name(),
            "user_name": fake.user_name(),
            "email": fake.email(),
            "personal_number": fake.ssn(), # Número de seguridad social o similar
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90),
            "address": fake.address(),
            "phone": fake.phone_number(),
            "mac_address": fake.mac_address(),
            "ip_address": fake.ipv4_public(),
            "iban": fake.iban(),
            "accessed_at": accessed_at,
            "session_duration": random.randint(300, 3600), # Segundos (5 min a 1 hora)
            "download_speed": round(random.uniform(10.0, 500.0), 2), # Mbps
            "upload_speed": round(random.uniform(5.0, 200.0), 2), # Mbps
            "consumed_traffic": round(random.uniform(100.0, 5000.0), 2) # MB
        }
        data.append(record)

    df = pl.DataFrame(data)
    return df

def extract_data_to_bronze(num_records: int = 1000):
    """
    Función principal para generar datos sintéticos y guardarlos en la capa Bronze.
    """
    print(f"Iniciando extracción de datos a la capa Bronze con {num_records} registros...")

    # Generar los datos
    synthetic_df = generate_synthetic_data(num_records)

    # Asegurarse de que el directorio de salida exista
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Guardar los datos en formato CSV en la capa Bronze
    synthetic_df.write_csv(OUTPUT_FILE_PATH)
    print(f"Datos extraídos y guardados en: {OUTPUT_FILE_PATH}")
    print(f"Dimensiones de los datos generados: {synthetic_df.shape}")

if __name__ == "__main__":
    # --- Instalar dependencias si aún no lo has hecho ---
    # Si aún no lo has hecho, asegúrate de que las librerías 'faker' y 'polars' estén instaladas.
    # Puedes hacerlo ejecutando en tu terminal:
    # pip install faker polars

    # --- Ejecución del generador de datos ---
    # Para la tarea, el documento menciona 100,372 registros históricos y 1,000 diarios.
    # Para empezar, puedes generar 1000 registros para probar el flujo.
    # Más adelante, podrías ajustar este número para generar los 100,372 históricos.
    extract_data_to_bronze(num_records=1000)