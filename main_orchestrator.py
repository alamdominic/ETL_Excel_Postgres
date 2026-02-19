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
from db.insertion_upsert import insert_new_modified_records


def setup_logging(log_dir="logs", log_file="etl.log"):
    """Configura el sistema de logging para archivos y consola.

    Crea el directorio de logs si no existe y establece un formato estándar
    para todos los mensajes de log. Los logs se escriben tanto a archivo
    como a consola para facilitar el monitoreo.

    Consumers:
        - __main__ (script principal)

    Dependencies:
        - logging (configuración básica, handlers, formatters)
        - os.makedirs
        - os.path.join

    Args:
        log_dir (str): Directorio donde se creará el archivo de log.
            Default: "logs"
        log_file (str): Nombre del archivo de log.
            Default: "etl.log"

    Returns:
        None: Configura el logging globalmente, no retorna valor.

    Side Effects:
        - Crea directorio log_dir si no existe
        - Configura logging a nivel INFO
        - Establece formato de mensaje con timestamp y nivel
        - Añade handlers para archivo y consola
    """
    import sys

    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    # Limpiar handlers existentes para evitar duplicación
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configurar formato
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Handler para archivo
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Handler para consola (terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Configurar logger root
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Mensaje de confirmación que debería aparecer en terminal
    print(f"🔧 Sistema de logging configurado - Archivo: {log_path}")
    logging.info(f"Log file: {log_path}")
    logging.info("Sistema de logging iniciado correctamente")


def validate_and_clean_data(df, sheet_name):
    """Valida y limpia datos problemáticos (NaN, null, strings vacíos).

    Identifica registros con valores problemáticos y los separa del conjunto
    de datos a insertar. Retorna el DataFrame limpio y una lista de registros
    problemáticos para reporte.

    Args:
        df (pandas.DataFrame): DataFrame a validar y limpiar
        sheet_name (str): Nombre de la hoja para contexto en el reporte

    Returns:
        tuple: (df_clean, problematic_records)
            - df_clean: DataFrame sin registros problemáticos
            - problematic_records: Lista de diccionarios con registros problemáticos
    """
    import numpy as np

    problematic_records = []
    valid_indices = []

    logging.info(f"Iniciando validación de datos para hoja '{sheet_name}'...")

    for index, row in df.iterrows():
        row_issues = []

        # Verificar cada columna del registro
        for col_name, value in row.items():
            # Detectar valores problemáticos
            if pd.isna(value) or value is None:
                row_issues.append(f"{col_name}: NaN/None")
            elif isinstance(value, str) and value.strip() == "":
                row_issues.append(f"{col_name}: string vacío")
            elif isinstance(value, (int, float)) and np.isnan(float(value)):
                row_issues.append(f"{col_name}: NaN numérico")

        if row_issues:
            # Registro problemático - agregar a lista de problemas
            problematic_record = {
                "index": index,
                "no_transferencia": row.get("no de transferencia", "N/A"),
                "issues": row_issues,
                "sheet": sheet_name,
            }
            problematic_records.append(problematic_record)
            logging.warning(
                f"Registro problemático encontrado - Índice: {index}, Problemas: {', '.join(row_issues)}"
            )
        else:
            # Registro válido - mantener índice
            valid_indices.append(index)

    # Crear DataFrame limpio solo con registros válidos
    df_clean = df.loc[valid_indices].copy()

    logging.info(
        f"Validación completada - Registros válidos: {len(df_clean)}, Problemáticos: {len(problematic_records)}"
    )

    return df_clean, problematic_records


def export_excel_to_postgres(
    sheet_name,
    excel_path,
):
    """Exporta datos desde una hoja de Excel específica hacia PostgreSQL.

    Proceso ETL completo que lee datos de Excel, identifica registros nuevos
    basándose en el último 'no de transferencia' existente en BD, y ejecuta
    inserción de datos nuevos solamente. El tracking de cambios y updates
    están deshabilitados por diseño.

    El proceso incluye:
    - Mapeo automático de hoja a tabla destino
    - Validación de existencia de columna ID
    - Filtrado de registros nuevos vs existentes
    - Confirmación interactiva del usuario
    - Inserción controlada con manejo de errores
    - Envío automático de reporte por correo

    Consumers:
        - __main__ (loop principal del script)

    Dependencies:
        - utils.xlsx_extractor.xlsx_to_df
        - utils.table_state.get_last_transfer_id
        - db.insertion_upsert.insert_new_modified_records
        - utils.send_email.send_email_report
        - pandas para manipulación de datos
        - logging para trazabilidad
        - datetime para medición de duración
        - os.getenv para configuración

    Args:
        sheet_name (str): Nombre de la hoja de Excel a procesar.
            Valores soportados: "COMISIONES", "COBRANZA"
        excel_path (str): Ruta completa del archivo Excel a leer.

    Returns:
        None: Ejecuta el proceso ETL completo pero no retorna valores.
            Los resultados se comunican vía logs y email.

    Side Effects:
        - Inserta registros nuevos en PostgreSQL
        - Genera logs detallados del proceso
        - Envía reporte de resultado por correo electrónico
        - Solicita confirmación interactiva al usuario

    Environment Variables Required:
        - SCHEMA_TABLE_COMISIONES: Tabla destino para hoja COMISIONES
        - SCHEMA_TABLE_COBRANZA: Tabla destino para hoja COBRANZA
        - RECIPIENT_EMAIL: Destinatario del reporte por correo

    Raises:
        ValueError: Si la hoja no es reconocida o datos son inválidos
        Exception: Errores de BD, lectura de archivo, o configuración
    """
    ejecucion_exitosa = False
    error_message = None
    rows_inserted = 0
    status_message = None
    start_time = datetime.now()
    logging.info(f"Inicio proceso hoja: {sheet_name} | {start_time}")

    try:
        # Construir nombres de tablas con esquema entre comillas
        table_name_comisiones = (
            f'"{os.getenv("schema_tables")}".{os.getenv("table_comisiones")}'
        )
        table_name_cobranza = (
            f'"{os.getenv("schema_tables")}".{os.getenv("table_cobranza")}'
        )

        # Log de debug para verificar construcción de nombres
        logging.info(
            f"Tabla cobranza: {table_name_cobranza}, Tabla comisiones: {table_name_comisiones}"
        )

        # Asignar tabla según la hoja
        if sheet_name == "COMISIONES":
            table_name = table_name_comisiones
            logging.info(f'Excel hoja: "{sheet_name.lower()}" | table db: {table_name}')
        elif sheet_name == "COBRANZA":
            table_name = table_name_cobranza
            logging.info(f'Excel hoja: "{sheet_name.lower()}" | table db: {table_name}')
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
        last_transfer_id, db_status = get_last_transfer_id(table_name, id_column_db)

        # Validar estado de la consulta a DB
        if db_status == "error":
            raise ValueError(
                f"Error de conexión a la base de datos. No se puede continuar."
            )
        elif db_status == "not_found":
            raise ValueError(
                f"La tabla '{table_name}' no existe en la base de datos. Crear la tabla primero."
            )

        logging.info(
            f"Último no de transferencia en BD: {last_transfer_id} (estado: {db_status})"
        )

        # 2. Lectura del archivo Excel local
        df_excel = xlsx_to_df(excel_path, sheet_name)
        if df_excel is None or df_excel.empty:
            logging.warning("El DataFrame leído del Excel está vacío o es None.")
            raise ValueError("El DataFrame leído del Excel está vacío o es None.")

        # 3. Validar que el id_column existe en el Excel
        if id_column not in df_excel.columns:
            raise ValueError(f"La columna '{id_column}' no existe en el Excel.")

        # Convertir la columna a numérico para comparaciones
        df_excel[id_column] = pd.to_numeric(df_excel[id_column], errors="coerce")

        # 4. Filtrar registros nuevos basándose en la posición del último ID de BD en Excel
        # 4. Filtrar registros nuevos basándose en la posición del último ID de BD en Excel
        if last_transfer_id is not None and db_status == "ok":
            # Convertir la columna a numérico para asegurar coincidencia de tipos
            df_excel[id_column] = pd.to_numeric(df_excel[id_column], errors="coerce")

            # Resetear índice para asegurar orden secuencial 0..N
            # IMPORTANTE: Trabajamos sobre el df_excel reseteado
            df_reset = df_excel.reset_index(drop=True)

            # Buscar índices donde coincide el último ID de la BD
            matching_indices = df_reset.index[
                df_reset[id_column] == last_transfer_id
            ].tolist()

            if not matching_indices:
                # El último ID de la BD no está en el Excel
                error_msg = (
                    f"El último 'no de transferencia' en BD ({last_transfer_id}) "
                    f"NO se encontró en el archivo Excel. No es posible determinar el límite para nuevos registros."
                )
                logging.error(error_msg)
                raise ValueError(error_msg)

            # Si hay duplicados del mismo ID, tomamos el último
            last_match_index = matching_indices[-1]

            logging.info(
                f"Punto de sincronización encontrado: ID {last_transfer_id} en fila {last_match_index} del Excel."
            )

            # Seleccionar todo lo que está POR DEBAJO de ese índice (+1 hasta el final)
            # Aseguramos que seleccionamos todas las filas restantes
            df_new_records = df_reset.iloc[last_match_index + 1 :].copy()

            logging.info(
                f"Registros en Excel: {len(df_excel)} | "
                f"Corte en fila (0-based): {last_match_index} | "
                f"Total filas disponibles: {len(df_reset)} | "
                f"Registros nuevos identificados: {len(df_new_records)}"
            )

            # Debug adicional si parece que no hay nuevos registros pero debería haberlos
            if df_new_records.empty and last_match_index < len(df_reset) - 1:
                logging.warning(
                    f"¡Extraño! El corte fue en {last_match_index} y el total es {len(df_reset)}, "
                    f"debería haber {len(df_reset) - 1 - last_match_index} registros, pero df_new_records está vacío."
                )

            if not df_new_records.empty:
                ids_preview = df_new_records[id_column].head(10).tolist()
                logging.info(f"Primeros IDs nuevos a insertar: {ids_preview}")

        elif db_status == "empty":
            # Si la tabla está vacía, insertar todos los registros
            df_new_records = df_excel
            logging.info(
                f"Tabla vacía. Se insertarán todos los registros: {len(df_new_records)}"
            )
        else:
            # Estado inesperado
            raise ValueError(f"Estado inesperado de la base de datos: {db_status}")

        # 5. Verificar si hay datos para insertar
        if df_new_records.empty:
            logging.info(
                "No hay inserciones. No existen registros nuevos posteriores al último ID en BD."
            )
            ejecucion_exitosa = True
            status_message = f"No hay datos nuevos para insertar. Último ID en BD: {last_transfer_id}"
            return

        # 6. Validar y limpiar datos problemáticos
        df_clean_records, problematic_records = validate_and_clean_data(
            df_new_records, sheet_name
        )

        # Verificar si quedan registros válidos después de la limpieza
        if df_clean_records.empty:
            logging.warning(
                "No hay registros válidos para insertar después de la limpieza de datos."
            )
            ejecucion_exitosa = True
            status_message = f"Todos los registros nuevos tienen datos problemáticos. Se encontraron {len(problematic_records)} registros con errores."
            # Almacenar registros problemáticos para el email
            globals()["problematic_records_global"] = problematic_records
            return

        # usado para verificar que se están filtrando correctamente los registros nuevos
        confirmation_message = f"Se encontraron {len(df_new_records)} registros nuevos."
        if problematic_records:
            confirmation_message += (
                f"\n- Registros válidos a insertar: {len(df_clean_records)}"
            )
            confirmation_message += f"\n- Registros con problemas (se excluirán): {len(problematic_records)}"
        else:
            confirmation_message += f" Todos son válidos para insertar."

        confirmation_message += "\n¿Desea continuar? (s/n): "

        response_user = input(confirmation_message)
        if response_user.lower() != "s":
            logging.info("Proceso cancelado por el usuario.")
            return

        # Almacenar registros problemáticos para el email (variable global temporal)
        globals()["problematic_records_global"] = problematic_records

        # 7. Ejecutar inserción (sin modificaciones, solo nuevos y válidos)
        df_empty = df_excel.iloc[0:0]  # DataFrame vacío para modificados
        insert_new_modified_records(df_clean_records, df_empty, table_name, id_column)

        logging.info(f"Inserciones completadas. Filas nuevas: {len(df_clean_records)}")
        rows_inserted = len(df_clean_records)
        status_message = f"Inserciones completadas. Último ID insertado: {df_clean_records[id_column].max()}"
        if problematic_records:
            status_message += (
                f" | {len(problematic_records)} registros con problemas excluidos"
            )
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
        # Obtener registros problemáticos de variable global temporal
        problematic_records_email = globals().get("problematic_records_global", [])

        if ejecucion_exitosa:
            subject = "✅ ETL-Excel a PostgreSQL - ÉXITO"
            body = (
                f"proceso ETL para la hoja '{sheet_name}' se completó exitosamente.\n"
                f"Duración: {duration},\nFilas nuevas insertadas: {rows_inserted}\n"
                f"Estado: {status_message or 'OK'}"
            )

            # Añadir información sobre registros problemáticos si los hay
            if problematic_records_email:
                body += "\n\n⚠️ REGISTROS CON DATOS PROBLEMÁTICOS EXCLUIDOS:\n"
                body += f"Total de registros problemáticos: {len(problematic_records_email)}\n\n"
                for i, record in enumerate(
                    problematic_records_email[:10], 1
                ):  # Mostrar máximo 10
                    body += f"{i}. No. Transferencia: {record['no_transferencia']} | Problemas: {', '.join(record['issues'])}\n"

                if len(problematic_records_email) > 10:
                    body += f"... y {len(problematic_records_email) - 10} registros más con problemas.\n"

                body += "\nEstos registros NO fueron insertados en la base de datos y requieren corrección manual en el archivo Excel."
        else:
            subject = "❌ ETL-Excel a PostgreSQL - FALLÓ"
            # Aquí vendrá el mensaje de 'API Key inválida'
            body = (
                "Error:\n"
                f"{error_message or 'Fallo no especificado.'}\n\n"
                "Revisar el log adjunto para más detalles."
            )

            # Añadir información sobre registros problemáticos incluso en caso de error
            if problematic_records_email:
                body += "\n\n⚠️ REGISTROS CON DATOS PROBLEMÁTICOS ENCONTRADOS (antes del error):\n"
                body += f"Total: {len(problematic_records_email)}\n"
                for i, record in enumerate(
                    problematic_records_email[:5], 1
                ):  # Menos registros en caso de error
                    body += f"{i}. No. Transferencia: {record['no_transferencia']} | Problemas: {', '.join(record['issues'])}\n"

        log_path = log_file
        if log_file and not os.path.isabs(log_file):
            log_path = os.path.join("logs", log_file)

        # Obtener emails de destinatarios (principal + adicional)
        primary_email = os.getenv("RECIPIENT_EMAIL")
        if not primary_email:
            logging.error("Variable de entorno RECIPIENT_EMAIL no está configurada")
            primary_email = "becario.bi@lazarza.com.mx"  # Email de respaldo en caso de falta de configuración

        secondary_email = "esp.bi02@lazarza.com.mx"

        # Combinar destinatarios
        recipients = f"{primary_email},{secondary_email}"

        send_email_report(
            subject=subject,
            body=body,
            recipient=recipients,
            attachment_path=log_path,
        )


# RUTA DEL ARCHIVO EXCEL LOCAL (configurable vía variable de entorno)
EXCEL_PATH = os.getenv("EXCEL_FILE_PATH")
logging.info(f"Ruta del archivo Excel configurada: {EXCEL_PATH}")
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
