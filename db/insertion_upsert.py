"""Database insert/update helpers for ETL sync."""

import logging
import unicodedata
import pandas as pd

# Modulos propios
from config.db_config import configPostgre

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS = {"importe", "año", "no_de_transferencia"}


def _normalize_text(value):
    """Normalize text for consistent comparisons and storage."""
    if value is None or not isinstance(value, str):
        return value

    cleaned = " ".join(value.strip().split())
    cleaned = "".join(
        char
        # NFKD significa Normal Form Compatibility Decomposition. Su trabajo es "descomponer" caracteres compuestos en sus partes individuales.
        for char in unicodedata.normalize("NFKD", cleaned)
        if not unicodedata.combining(char)
    )
    return cleaned.lower()


def _normalize_dataframe(df):
    """Normalize all object columns in a DataFrame."""
    normalized = df.copy()
    text_columns = normalized.select_dtypes(include=["object"]).columns
    for col in text_columns:
        normalized[col] = normalized[col].apply(_normalize_text)
    return normalized


def _clean_numeric_columns(df, numeric_columns):
    """Coerce numeric columns to numbers, invalid values become NaN."""
    cleaned = df.copy()
    for col in numeric_columns:
        if col in cleaned.columns:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
    return cleaned


def _convert_timestamp_to_date(df):
    """Convert Timestamp columns to date format (YYYY-MM-DD) for PostgreSQL."""
    converted = df.copy()
    # Buscar columnas de tipo datetime/timestamp
    datetime_columns = converted.select_dtypes(include=['datetime64']).columns
    for col in datetime_columns:
        # Convertir Timestamp a solo fecha (sin hora)
        converted[col] = pd.to_datetime(converted[col], errors='coerce').dt.date
    return converted


def insert_new_modified_records(df_new, df_modified, table_name, id_column):
    """Inserta nuevos y actualiza registros modificados en la base de datos.

    Consumers:
        - main_orchestrator.export_excel_to_postgres

    Dependencies:
        - psycopg2.connect
        - psycopg2.Error
        - config.db_config.configPostgre
        - pandas
        - logging

    Args:
        df_new (pandas.DataFrame): Registros nuevos a insertar.
        df_modified (pandas.DataFrame): Registros existentes con cambios.
        table_name (str): Nombre de la tabla destino.
        id_column (str): Nombre de la columna ID para el WHERE del UPDATE.

    Returns:
        None: Ejecuta inserciones y actualizaciones en la base de datos.

    Raises:
        psycopg2.Error: Propaga errores de base de datos luego de rollback.
    """
    engine = None

    try:
        engine = configPostgre()
        if engine is None:
            raise ValueError("No se pudo crear el engine de PostgreSQL.")

        # --- INSERT nuevos ---
        if not df_new.empty:
            df_numeric_clean = _clean_numeric_columns(df_new, NUMERIC_COLUMNS)
            df_timestamp_clean = _convert_timestamp_to_date(df_numeric_clean)
            df_normalized = _normalize_dataframe(df_timestamp_clean)
            df_clean = df_normalized.where(pd.notna(df_normalized), None)
            data = [tuple(row) for row in df_clean.values]

            columns_str = ", ".join([f'"{col}"' for col in df_new.columns])
            placeholders = ", ".join(["%s"] * len(df_new.columns))

            query_insert = (
                f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            )
            with engine.begin() as conn:
                conn.exec_driver_sql(query_insert, data)

            logger.info(f"INSERT: {len(data)} registros nuevos")

        # --- UPDATE modificados ---
        if not df_modified.empty:
            df_numeric_clean = _clean_numeric_columns(df_modified, NUMERIC_COLUMNS)
            df_timestamp_clean = _convert_timestamp_to_date(df_numeric_clean)
            df_normalized = _normalize_dataframe(df_timestamp_clean)
            df_clean = df_normalized.where(pd.notna(df_normalized), None)

            update_columns = [col for col in df_modified.columns if col != id_column]
            set_clause = ", ".join([f'"{col}" = %s' for col in update_columns])

            query_update = (
                f'UPDATE {table_name} SET {set_clause} WHERE "{id_column}" = %s'
            )

            # Preparar datos: (col1, col2, ..., id)
            data = []
            for _, row in df_clean.iterrows():
                values = [row[col] for col in update_columns]
                values.append(row[id_column])  # ID al final para el WHERE
                data.append(tuple(values))

            with engine.begin() as conn:
                conn.exec_driver_sql(query_update, data)

            logger.info(f"UPDATE: {len(data)} registros modificados")

        logger.info("Sincronizacion completada")

    except Exception as e:
        logger.error(f"Error en UPSERT: {e}")
        raise
