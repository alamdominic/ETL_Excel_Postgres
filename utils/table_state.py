"""Database table state inspection helpers."""

import logging
from sqlalchemy import text
from config.db_config import configPostgre

logger = logging.getLogger(__name__)


def get_last_transfer_id(table_name, id_column="no de transferencia"):
    """
    Obtiene el último valor del id_column en la tabla ordenado de forma descendente.

    Args:
        table_name (str): Nombre completo de la tabla (schema.tabla).
        id_column (str): Nombre de la columna a buscar (default: "no de transferencia").

    Returns:
        int | None: El último valor del id_column, o None si la tabla está vacía o hay error.
    """
    try:
        engine = configPostgre()
        if engine is None:
            logger.error("No se pudo crear el engine de PostgreSQL.")
            return None

        with engine.connect() as conn:
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
                return result
            else:
                logger.info(f'La tabla "{table_name}" está vacía.')
                return None

    except Exception as e:
        logger.error(f'Error obteniendo último "{id_column}" de "{table_name}": {e}')
        return None


def get_table_db_state(table_name, id_column):
    """
    Returns information about the table:
    1. Whether it exists.
    2. The total row count.
    3. A dictionary of records if an id_column is provided (for UPSERT).

    Note:
        The record keys keep the original database type for `id_column`.
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
