"""Main ETL orchestrator for Excel-to-PostgreSQL sync."""

import logging
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Modulos propios
from utils.send_email import send_email_report
from utils.xlsx_extractor import xlsx_to_df
from utils.table_state import get_last_transfer_id

# from upsert.tracker_changes import track_changes
from db.insertion_upsert import insert_new_modified_records


def setup_logging(log_dir="logs", log_file="etl.log"):
    """Configure logging to file and console.

    Args:
        log_dir (str): Directory where the log file will be created.
        log_file (str): Log filename.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    logging.info(f"Log file: {log_path}")


def export_excel_to_postgres(
    sheet_name,
    excel_path,
):
    """Export data from an Excel sheet into PostgreSQL.

    This process only inserts new `no_de_transferencia` values. Change tracking
    and updates are currently disabled.

    Args:
        sheet_name (str): Excel sheet name to process.
        excel_path (str): Full path to the Excel file.
    """
    ejecucion_exitosa = False
    error_message = None
    rows_inserted = 0
    status_message = None
    start_time = datetime.now()
    logging.info(f"Inicio proceso hoja: {sheet_name} | {start_time}")

    try:
        # Asignar tabla según la hoja
        if sheet_name == "COMISIONES":
            table_name = os.getenv("SCHEMA_TABLE_COMISIONES")
        elif sheet_name == "COBRANZA":
            table_name = os.getenv("SCHEMA_TABLE_COBRANZA")
        else:
            error_message = (
                f"Hoja '{sheet_name}' no reconocida. No se realizará ninguna operación."
            )
            logging.error(error_message)
            return

        id_column = "no de transferencia"
        id_column_db = "no de transferencia"  # Nombre en la BD (con espacios)

        logging.info(f"Procesando hoja '{sheet_name}' para la tabla '{table_name}'.")

        # 1. Obtener el último no de transferencia de la BD
        last_transfer_id = get_last_transfer_id(table_name, id_column_db)
        logging.info(f"Último no de transferencia en BD: {last_transfer_id}")

        # 2. Lectura del archivo Excel local
        df_excel = xlsx_to_df(excel_path, sheet_name)
        if df_excel is None or df_excel.empty:
            logging.warning("El DataFrame leído del Excel está vacío o es None.")
            raise ValueError("El DataFrame leído del Excel está vacío o es None.")

        # 3. Validar que el id_column existe en el Excel
        if id_column not in df_excel.columns:
            raise ValueError(f"La columna '{id_column}' no existe en el Excel.")

        # 4. Filtrar registros nuevos (mayores al último ID en BD)
        if last_transfer_id is not None:
            # Convertir la columna a numérico para comparación
            df_excel[id_column] = pd.to_numeric(df_excel[id_column], errors="coerce")
            df_new_records = df_excel[df_excel[id_column] > last_transfer_id]
            logging.info(
                f"Registros en Excel: {len(df_excel)} | "
                f"Registros nuevos (> {last_transfer_id}): {len(df_new_records)}"
            )
        else:
            # Si la tabla está vacía, insertar todos los registros
            df_new_records = df_excel
            logging.info(
                f"Tabla vacía. Se insertarán todos los registros: {len(df_new_records)}"
            )

        # 5. Verificar si hay datos para insertar
        if df_new_records.empty:
            logging.info(
                "No hay inserciones. No existen registros nuevos posteriores al último ID en BD."
            )
            ejecucion_exitosa = True
            status_message = f"No hay datos nuevos para insertar. Último ID en BD: {last_transfer_id}"
            return

        # 6. Ejecutar inserción (sin modificaciones, solo nuevos)
        df_empty = df_excel.iloc[0:0]  # DataFrame vacío para modificados
        insert_new_modified_records(df_new_records, df_empty, table_name, id_column)

        logging.info(f"Inserciones completadas. Filas nuevas: {len(df_new_records)}")
        rows_inserted = len(df_new_records)
        status_message = f"Inserciones completadas. Último ID insertado: {df_new_records[id_column].max()}"
        ejecucion_exitosa = True

    except Exception as e:
        logging.exception(f"Error en el proceso ETL: {e}")
        error_message = str(e)

    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(
            "Fin proceso hoja: %s | %s | duracion: %s",
            sheet_name,
            end_time,
            duration,
        )

        # Enviar correo de reporte (SIEMPRE SE EJECUTA)
        if ejecucion_exitosa:
            subject = "✅ ETL-Excel a PostgreSQL - ÉXITO"
            body = (
                f"proceso ETL para la hoja '{sheet_name}' se completó exitosamente.\n"
                f"Duración: {duration},\nFilas nuevas insertadas: {rows_inserted}\n"
                f"Estado: {status_message or 'OK'}"
            )
        else:
            subject = "❌ ETL-Excel a PostgreSQL - FALLÓ"
            # Aquí vendrá el mensaje de 'API Key inválida'
            body = (
                "Error:\n"
                f"{error_message or 'Fallo no especificado.'}\n\n"
                "Revisar el log adjunto para más detalles."
            )

        log_path = log_file
        if log_file and not os.path.isabs(log_file):
            log_path = os.path.join("logs", log_file)

        # Obtener email del destinatario desde variable de entorno
        recipient_email = os.getenv("RECIPIENT_EMAIL", "default@company.com")
        send_email_report(
            subject=subject,
            body=body,
            recipient=recipient_email,
            attachment_path=log_path,
        )


# RUTA DEL ARCHIVO EXCEL LOCAL (configurable vía variable de entorno)
EXCEL_PATH = os.getenv(
    "EXCEL_FILE_PATH", r"C:\Users\Administrador\OneDrive - LaZarza\ETL_Cobranza_Comisiones\ETL_Excel_Postgres\excel\PAPEL DE TRABAJO MEDIOS DE COBRO.xlsx"
)
# # TABLE_NAME = "DataMart.presupuestos_planeacion" - teoricamente esto ya se maneja dentro de la funcion
# TABLE_NAME = "excel_etl_testing.test_data_insertions_cobranza"  # pruebas en local
sheets_name = ["COBRANZA", "COMISIONES"]
if __name__ == "__main__":
    log_file = f"etl.{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    setup_logging(log_file=log_file)
    process_start = datetime.now()
    logging.info(f"Inicio proceso ETL: {process_start}")

    for sheet in sheets_name:
        if sheet:  # Solo procesar si el nombre de la hoja no está vacío
            logging.info(f"Procesando hoja: {sheet}")
            export_excel_to_postgres(sheet, EXCEL_PATH)

    process_end = datetime.now()
    logging.info(f"Fin proceso ETL: {process_end}")
    logging.info(f"Duracion total ETL: {process_end - process_start}")
