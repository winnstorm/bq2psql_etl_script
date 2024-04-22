import argparse
import time
import os
from tqdm import tqdm
import psycopg2
from google.cloud import bigquery
from datetime import datetime, timedelta

default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def parse_args():
    parser = argparse.ArgumentParser(description="ETL script")

    parser.add_argument("--fecha", required=False, default=default_date, nargs='?', const=default_date, help="Fecha a cargar (por defecto: fecha de ayer)")

    parser.add_argument("-sql_file", required=True, nargs='?', help="Ruta al archivo SQL (por defecto: query.sql)")
    parser.add_argument("-db_name", required=False, const="base", nargs='?', help="Nombre de la base de datos PostgreSQL")
    parser.add_argument("-db_user", required=False, const="postgres", nargs='?', help="Usuario de la base de datos PostgreSQL")
    parser.add_argument("-db_password", required=False, const="1234", nargs='?', help="Contraseña de la base de datos PostgreSQL")
    parser.add_argument("-db_host", required=False, const="192.168.1.1", nargs='?', help="Host de la base de datos PostgreSQL")
    parser.add_argument("-db_port", required=False, const="5432", nargs='?', help="Puerto de la base de datos PostgreSQL")
    parser.add_argument("-table_name", required=True, nargs='?', help="Nombre de la tabla de destino en PostgreSQL")
    parser.add_argument("-date_column_destino", required=False, const="fecha", nargs='?', help="Nombre de la columna de fecha en la tabla de Origen")
    parser.add_argument("-date_column_origen", required=False, const="fecha", nargs='?', help="Nombre de la columna de fecha en la tabla de destino en PostgreSQL")
    parser.add_argument("-truncate", const=False, nargs='?', help="Truncar la tabla destino o eliminar registros por fecha (Default False)")
    
    parser.add_argument("-project_id", required=True, nargs='?', help="ID del proyecto en BigQuery")
    parser.add_argument("-dataset_id", required=True, nargs='?', help="ID del dataset en BigQuery")
    parser.add_argument("-table_id", required=True, nargs='?', help="ID de la tabla en BigQuery")

    parser.add_argument("-project_id_run", required=False, const="gcp-project", nargs='?', help="ID del proyecto en BigQuery que ejecutara")
    parser.add_argument("-gcp_location", required=False, const="us-east4", nargs='?', help="ID de la region que ejecutara este proceso en BQ")
    return parser.parse_args()

args = parse_args()

sql_file_name = os.path.basename(args.sql_file)
sql_file_name = os.path.splitext(sql_file_name)[0]

#########################################################
# Log Module v2                                         #
#########################################################
import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
import time
PATH_LOGS = 'G:\PY_SCRIPTS\SyBaseIQ\Logs' + '\\'

logging.basicConfig(level=logging.INFO, 
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    handlers=[RotatingFileHandler(PATH_LOGS + sql_file_name + '.log', 
                    maxBytes=100000, backupCount=3),
                              logging.StreamHandler()]
                    )

#########################################################

logging.info(f"GabETL Big PSQL Script v0.2")

logging.info(f"Script -> {sql_file_name}")

##
# vars
##

chunk_size = 100000  # Tamaño del chunk

global client
client = bigquery.Client(project=args.project_id_run, location=args.gcp_location)

logging.info(f"Cliente GCP -> {args.project_id_run} - {args.gcp_location}")

logging.info(f"Fecha seleccionada... {args.fecha}")

def check_source_table(date, project_id, dataset_id, table_id):

    logging.info(f"Verificando los datos de origen en {project_id}.{dataset_id}.{table_id}")

    # Construir la consulta para verificar la existencia de datos
    query = f"""
        SELECT COUNT(*) as count
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE {args.date_column_origen} = '{date}'
    """

    # Ejecutar la consulta
    query_job = client.query(query)
    results = query_job.result()

    # Verificar si hay datos
    for row in results:
        count = row.count

    formatted_count = "{:,}".format(count)

    logging.info(f"Se encontraron {formatted_count} registros")

    return count > 0

def execute_sql_with_date(sql_file, date, project_id, location):
    logging.info(f"Ejecutando script... {sql_file}")

    # Leer el archivo SQL
    with open(sql_file, "r") as file:
        sql_query = file.read()

    # Reemplazar la variable de fecha con la fecha proporcionada
    sql_query = sql_query.replace("$fecha", date)

    # Ejecutar la consulta
    query_job = client.query(sql_query)
    results = query_job.result()

    # Obtener los datos de la consulta en chunks
    data_chunks = []
    for chunk in tqdm(results, desc="Obteniendo datos en chunks"):
        data_chunks.append(chunk)
        if len(data_chunks) >= chunk_size:
            yield data_chunks
            data_chunks = []

    # Si quedan datos en el último chunk
    if data_chunks:
        yield data_chunks

def clean_destination_table(date, db_name, db_user, db_password, db_host, db_port, table_name, date_column_destino):
    # Conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()

    logging.info(f"Eliminando los datos de la tabla destino {table_name}")

    if args.truncate == True:
        query = f"TRUNCATE TABLE {table_name}"
    else:
        query = f"DELETE FROM {table_name} WHERE {date_column_destino} = %s"
    
    cursor.execute(query, (date,))
    
    # Confirmar la transacción y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Registros eliminados correctamente.")

def load_data_to_postgresql(data, date, db_name, db_user, db_password, db_host, db_port, table_name):
    from psycopg2.extras import execute_values

    try:
        # Conexión a la base de datos PostgreSQL
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()

        # Consulta SQL para inserción de datos
        insert_query = f"INSERT INTO {table_name} VALUES %s"

        # Contador para el total de filas
        total_rows = 0

        # Iterar sobre los resultados y cargarlos en PostgreSQL en chunks
        with tqdm(desc="Cargando datos en PostgreSQL") as pbar:
            for chunk_start in range(0, len(data), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(data))
                chunk_data = data[chunk_start:chunk_end]

                psycopg2.extras.execute_values(cursor, insert_query, chunk_data)

                # Confirmar la transacción después de cada chunk
                conn.commit()

                total_rows += len(chunk_data)
                pbar.update(len(chunk_data))

        # Cerrar la conexión
        cursor.close()
        conn.close()

        logging.info("Datos cargados")
    except psycopg2.Error as e:
        logging.info("Error al cargar los datos:", e)

def main():
    fecha = args.fecha
    sql_file = args.sql_file

    # Intentar ejecutar la ETL varias veces en caso de errores
    retry_count = 5
    for attempt in range(retry_count):
        try:
            # Verificar la existencia de datos en la tabla de origen
            if not check_source_table(fecha, args.project_id, args.dataset_id, args.table_id):
                logging.info("No hay datos disponibles para la fecha proporcionada en la tabla de origen.")
                return

            # Limpiar la tabla de destino
            clean_destination_table(fecha, args.db_name, args.db_user, args.db_password, args.db_host, args.db_port, args.table_name, args.date_column_destino)

            logging.info(f"Insertando registros en destino... {args.table_name}")
            # Ejecutar SQL con la fecha
            for data_chunk in execute_sql_with_date(sql_file, fecha, args.project_id, args.gcp_location):
                # Cargar datos en PostgreSQL
                load_data_to_postgresql(data_chunk, fecha, args.db_name, args.db_user, args.db_password, args.db_host, args.db_port, args.table_name)

            # Salir del bucle de reintento si la ETL se ejecuta con éxito
            logging.info(f"Done")
            break
        except Exception as e:
            logging.info(f"Error en el intento {attempt + 1} al ejecutar la ETL:", e)
            if attempt < retry_count - 1:
                logging.info("Reintentando...")
                time.sleep(5)  # Esperar 5 segundos antes de reintentar
            else:
                logging.info("No se pudo ejecutar la ETL después de varios intentos.")

if __name__ == "__main__":
    main()
