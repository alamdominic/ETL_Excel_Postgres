"""Database table state inspection helpers."""

import logging
from sqlalchemy import text
from config.db_config import configPostgre

logger = logging.getLogger(__name__)


def get_last_transfer_id(table_name, id_column="no de transferencia"):
    """Obtiene el último valor del id_column en la tabla ordenado de forma descendente.

    Ejecuta consulta SQL para encontrar el valor máximo de una columna específica,
    típicamente usada para determinar desde dónde continuar procesamiento incremental.

    Consumers:
        - main_orchestrator.export_excel_to_postgres

    Dependencies:
        - config.db_config.configPostgre
        - sqlalchemy.text
        - logging

    Args:
        table_name (str): Nombre completo de la tabla incluyendo schema (ej. "schema.tabla").
        id_column (str): Nombre de la columna a consultar. Default: "no de transferencia".

    Returns:
        tuple: (valor, estado)
            - valor (int | None): El último (mayor) valor encontrado, o None si tabla vacía
            - estado (str): "ok", "empty", "not_found", "error"

    Raises:
        Exception: Errores de conexión o ejecución SQL se capturan y registran en log.
    """
    try:
        engine = configPostgre()
        if engine is None:
            logger.error("No se pudo crear el engine de PostgreSQL.")
            return None, "error"

        with engine.connect() as conn:
            # Primero verificar si la tabla existe
            check_query = text(f"SELECT to_regclass(:table)")
            table_exists = conn.execute(check_query, {"table": table_name}).scalar()

            if not table_exists:
                logger.error(f'La tabla "{table_name}" no existe en la base de datos.')
                return None, "not_found"

            # Tabla existe, obtener último ID
            query = text(
                f"""
                SELECT "{id_column}"
                FROM {table_name}
                ORDER BY "{id_column}" DESC
                LIMIT 1
            """
            )
            result = conn.execute(query).scalar()

            if result is not None:
                logger.info(f'Último "{id_column}" en "{table_name}": {result}')
                return result, "ok"
            else:
                logger.info(f'La tabla "{table_name}" existe pero está vacía.')
                return None, "empty"

    except Exception as e:
        logger.error(f'Error obteniendo último "{id_column}" de "{table_name}": {e}')
        return None, "error"


def get_table_db_state(table_name, id_column):
    """Obtiene información completa del estado de una tabla en la base de datos.

    Consulta metadatos y datos de la tabla para determinar su existencia,
    cantidad de registros y crear un diccionario indexado por ID para
    operaciones de comparación y UPSERT.

    Consumers:
        - upsert.tracker_changes.track_changes (indirectamente)
        - Funciones que requieren estado completo de tabla para comparaciones

    Dependencies:
        - config.db_config.configPostgre
        - sqlalchemy.text
        - logging

    Args:
        table_name (str): Nombre completo de la tabla incluyendo schema.
        id_column (str): Nombre de la columna que sirve como clave para indexar.

    Returns:
        dict: Diccionario con estructura:
            {
                "exists": bool,  # True si la tabla existe
                "count": int,    # Número total de registros (no usado actualmente)
                "records_dict": dict  # {id_value: {col: val, ...}}
            }

    Note:
        - Las claves del records_dict mantienen el tipo original de la BD
        - Solo incluye registros donde id_column no es NULL
        - Si la tabla no existe o hay error, retorna estado "vacío"
        - El campo "count" está implementado pero no se usa actualmente

    Raises:
        Exception: Errores de BD se capturan, registran en log y retornan estado vacío.
    """
    state = {"exists": False, "count": 0, "records_dict": {}}

    try:
        engine = configPostgre()

        with engine.connect() as conn:
            # 1. Check existence (using schema if included in table_name)
            # Note: SQL Alchemy requires text() for raw queries

            # Si la tabla existe, la función devuelve el nombre de la tabla.
            check_query = text(f"SELECT to_regclass(:table)")
            result = conn.execute(check_query, {"table": table_name}).scalar()

            if not result:
                logger.error(f"La tabla {table_name} no existe.")
                return state

            state["exists"] = True

            select_all = text(
                f'SELECT * FROM {table_name} WHERE "{id_column}" IS NOT NULL'
            )
            rows = conn.execute(select_all)
            # Convertimos a diccionario: {id: {col: val}}
            state["records_dict"] = {
                row._asdict()[id_column]: row._asdict() for row in rows
            }
        return state

    except Exception as e:
        logger.error(f"Error inspeccionando la tabla {table_name}: {e}")
        return state
