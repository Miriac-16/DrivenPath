import polars as pl
import os
import psycopg2 # Para PostgreSQL
from sqlalchemy import create_engine, text # Para Polars .to_sql y ejecutar SQL con SQLAlchemy

# --- Configuración de la base de datos PostgreSQL ---
# ¡Asegúrate de que estas credenciales coincidan con las que usaste al iniciar el contenedor Docker!
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "drivenpath_db"
DB_USER = "user"
DB_PASSWORD = "password" 

# Rutas de entrada y salida
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(CURRENT_DIR, '..') # Sube de 'scripts' a 'work 2'

SILVER_INPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'silver', 'transformed_data_silver.parquet')
QUERIES_DIR = os.path.join(PROJECT_ROOT, 'queries')

def run_sql_query(cursor, query_path: str, conn=None):
    """Ejecuta una consulta SQL desde un archivo usando psycopg2 o SQLAlchemy."""
    with open(query_path, 'r') as f:
        sql_query = f.read()
    try:
        # Usamos cursor.execute para DDL (CREATE SCHEMA, CREATE TABLE) y DML que no son Polars to_sql
        cursor.execute(sql_query)
        print(f"Consulta ejecutada exitosamente: {os.path.basename(query_path)}")
    except Exception as e:
        print(f"Error al ejecutar la consulta {os.path.basename(query_path)}: {e}")
        # print(f"Query: \n{sql_query}") # Descomentar para depuración
        raise # Vuelve a lanzar la excepción para que el programa falle si hay un error crítico

def load_data_to_golden():
    """
    Carga datos transformados de la capa Silver a la capa Golden (PostgreSQL).
    """
    print("Iniciando carga de datos a la capa Golden (PostgreSQL)...")

    # Paso 1: Leer los datos de la capa Silver
    try:
        df_silver = pl.read_parquet(SILVER_INPUT_PATH)
        print(f"Datos de Silver cargados. Dimensiones: {df_silver.shape}")
    except Exception as e:
        print(f"Error al leer el archivo Parquet de Silver: {e}")
        print("Asegúrate de que 'transformed_data_silver.parquet' existe en 'data/silver/'.")
        return

    # Paso 2: Conectar a la base de datos PostgreSQL
    conn_pg = None
    engine = None
    try:
        # Conexión con psycopg2 para ejecutar DDL (CREATE TABLE)
        conn_pg = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor_pg = conn_pg.cursor()
        print(f"Conectado a la base de datos PostgreSQL: {DB_NAME}")

        # Conexión con SQLAlchemy para Polars .to_sql (maneja la inserción de datos de DataFrame)
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)

        # Crear esquema silver_layer si no existe
        cursor_pg.execute("CREATE SCHEMA IF NOT EXISTS silver_layer;")
        conn_pg.commit()
        print("Esquema 'silver_layer' verificado/creado.")

        # =====================================================================
        # ¡¡¡ESTE ES EL BLOQUE QUE HA SIDO MOVIDO PARA EJECUTARSE PRIMERO!!!
        # Paso: Cargar datos del DataFrame Silver a una tabla de staging en la capa Silver de la BD
        # Esto es crucial para que tus SQLs de la capa Silver puedan seleccionar de aquí
        print("\nCargando datos del DataFrame Silver a la tabla 'staging_silver' en PostgreSQL...")
        
        # Eliminar la tabla si existe para asegurar una carga limpia en cada ejecución
        cursor_pg.execute("DROP TABLE IF EXISTS silver_layer.staging_silver CASCADE;") # CASCADE para eliminar dependencias si las hubiera
        conn_pg.commit()
        
        # Convertir nombres de columnas a minúsculas para PostgreSQL
        df_silver_renamed = df_silver.rename({col: col.lower() for col in df_silver.columns})

        # Utilizar el método .write_database de Polars para insertar el DataFrame directamente
        # Esto creará la tabla staging_silver en el esquema silver_layer y cargará los datos
        df_silver_renamed.write_database(
            table_name="silver_layer.staging_silver",
            connection=engine,
            #if_exists="replace", # 'replace' borra y crea la tabla, 'append' añade, 'fail' da error si existe
            #schema="silver_layer"
        )
        print("Datos insertados en 'silver_layer.staging_silver'.")
        conn_pg.commit() # Confirmar la inserción de datos
        # =====================================================================

        # =====================================================================
        # ¡¡¡ESTE BLOQUE AHORA SE EJECUTA DESPUÉS DE CARGAR staging_silver!!!
        # Paso: Crear las tablas de la capa Silver (dimensiones y hechos)
        # Esto usará tu archivo queries/create_silver_layer_tables.sql
        print("\nCreando tablas de la capa Silver en la base de datos PostgreSQL...")
        run_sql_query(cursor_pg, os.path.join(QUERIES_DIR, 'create_silver_layer_tables.sql'))
        conn_pg.commit() # Commit después de crear tablas
        # =====================================================================

        # Crear esquema golden_layer si no existe
        cursor_pg.execute("CREATE SCHEMA IF NOT EXISTS golden_layer;")
        conn_pg.commit()
        print("Esquema 'golden_layer' verificado/creado.")

        # Paso 5: Crear y poblar las tablas finales de la capa Golden
        print("\nCreando y poblando tablas finales de la capa Golden (PostgreSQL)...")
        
        # Queries para crear y poblar tablas de la capa Golden
        # Estos SQLs DEBEN contener la lógica INSERT INTO ... SELECT ... de las tablas Silver
        golden_create_and_load_queries = [
            'create_financial_data.sql',
            'create_support_data.sql',
            'create_non_pii_data.sql',
            'create_pii_data.sql'
        ]

        for query_file in golden_create_and_load_queries:
            # Aquí es donde es crítico que tus SQLs hagan el SELECT desde silver_layer.staging_silver
            # o desde las dimensiones/hechos que `create_silver_layer_tables.sql` haya creado.
            run_sql_query(cursor_pg, os.path.join(QUERIES_DIR, query_file))
            conn_pg.commit() # Commit después de cada operación DDL/DML

        # Paso 6: Verificar el contenido de las tablas Golden (primeras 5 filas)
        print("\nVerificando el contenido de las tablas Golden (primeras 5 filas):")
        tables_to_check = ['financial_data', 'support_data', 'non_pii_data', 'pii_data']
        for table in tables_to_check:
            try:
                check_query = f"SELECT * FROM golden_layer.{table} LIMIT 5;" 
                cursor_pg.execute(check_query)
                rows = cursor_pg.fetchall()
                if rows:
                    print(f"\n--- Tabla: golden_layer.{table} ---")
                    cols = [description[0] for description in cursor_pg.description]
                    print(cols)
                    for row in rows:
                        print(row)
                else:
                    print(f"\n--- Tabla: golden_layer.{table} ---")
                    print("No se encontraron registros o la tabla está vacía.")
            except psycopg2.Error as e:
                print(f"Error al verificar la tabla golden_layer.{table}: {e}")

    except psycopg2.Error as e:
        print(f"Error de base de datos PostgreSQL: {e}")
        print("Asegúrate de que el contenedor de Docker PostgreSQL esté corriendo y las credenciales sean correctas.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
    finally:
        if conn_pg:
            conn_pg.close()
            print("Conexión a la base de datos PostgreSQL cerrada.")
        if engine:
            engine.dispose() # Cierra el pool de conexiones de SQLAlchemy

if __name__ == "__main__":
    load_data_to_golden()