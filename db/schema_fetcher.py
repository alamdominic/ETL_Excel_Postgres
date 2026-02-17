"""Fetches table schema information from the database."""

import logging
from contextlib import closing
import psycopg2
from config.db_config import get_db_config


def get_table_schema(table_name):
    """Obtiene nombres de columnas y tipos de datos para una tabla específica.

    Consulta el catálogo de sistema de PostgreSQL (information_schema.columns)
    para extraer metadatos de esquema de una tabla, útil para validaciones
    de estructura y alineación de tipos de datos.

    Consumers:
        - utils.data_aligner.align_df_to_schema (potencial)
        - Funciones que requieran validación de esquema dinámico

    Dependencies:
        - psycopg2 para conexión directa a PostgreSQL
        - config.db_config.get_db_config
        - contextlib.closing para manejo seguro de conexiones
        - logging para registro de eventos

    Args:
        table_name (str): Nombre de la tabla en formato '"Schema"."Table"'
            o 'schema.table'. Las comillas se manejan automáticamente.

    Returns:
        dict: Diccionario mapeando nombres de columna a sus tipos de datos.
            Ejemplo: {'id': 'integer', 'nombre': 'varchar', 'fecha': 'date'}
            Retorna diccionario vacío si la tabla no existe o hay errores.

    Error Handling:
        - Captura errores de psycopg2 y los registra como errores de BD
        - Captura excepciones generales y las registra
        - Retorna diccionario vacío en caso de cualquier error
        - Registra advertencia si no se encuentran columnas

    Note:
        - Asume formato de table_name como 'schema.table'
        - Remueve comillas del nombre antes de hacer split
        - Usa parámetros de consulta para prevenir SQL injection
    """
    schema, table = table_name.replace('"', "").split(".")
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """
    schema_info = {}
    try:
        config = get_db_config()
        with closing(psycopg2.connect(**config)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(query, (schema, table))
                columns = cursor.fetchall()
                if not columns:
                    logging.warning(
                        f"No columns found for table '{table_name}'. The table might not exist or is empty."
                    )
                    return {}
                for col_name, data_type in columns:
                    schema_info[col_name] = data_type
                logging.info(f"Schema retrieved for table '{table_name}'.")
    except psycopg2.Error as e:
        logging.error(f"Database error while fetching schema for '{table_name}': {e}")
        return {}
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while fetching schema for '{table_name}': {e}"
        )
        return {}

    return schema_info
