"""Fetches table schema information from the database."""

import logging
from contextlib import closing
import psycopg2
from config.db_config import get_db_config

def get_table_schema(table_name):
    """
    Retrieves column names and data types for a given table.

    Args:
        table_name (str): The name of the table (e.g., '"Schema"."Table"').

    Returns:
        dict: A dictionary mapping column names to their data types.
              Returns an empty dictionary if the table is not found or an error occurs.
    """
    schema, table = table_name.replace('"', '').split('.')
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
                    logging.warning(f"No columns found for table '{table_name}'. The table might not exist or is empty.")
                    return {}
                for col_name, data_type in columns:
                    schema_info[col_name] = data_type
                logging.info(f"Schema retrieved for table '{table_name}'.")
    except psycopg2.Error as e:
        logging.error(f"Database error while fetching schema for '{table_name}': {e}")
        return {}
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching schema for '{table_name}': {e}")
        return {}

    return schema_info
