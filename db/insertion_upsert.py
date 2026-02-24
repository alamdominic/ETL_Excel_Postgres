"""Database insert/update helpers for ETL sync."""

import logging
import unicodedata
import pandas as pd

# Modulos propios
from config.db_config import configPostgre

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS = {"importe", "año", "no_de_transferencia"}


def _normalize_text(value, to_upper=True):
    """Normaliza texto para comparaciones consistentes y almacenamiento.

    Limpia y estandariza cadenas de texto removiendo espacios extras,
    normalizando caracteres Unicode y convirtiendo a mayúsculas (por defecto)
    o minúsculas para garantizar comparaciones consistentes entre datos de Excel y BD.

    Internal function consumed by:
        - _normalize_dataframe

    Dependencies:
        - unicodedata.normalize (NFKD)
        - unicodedata.combining

    Args:
        value: Valor a normalizar. Puede ser str, None, o cualquier tipo.
        to_upper (bool): Si es True convierte a mayúsculas, False a minúsculas. Default: True.

    Returns:
        str | Any: Cadena normalizada si es texto, valor original si no es str.

    Normalization Process:
        1. Retorna valor original si es None o no es string
        2. Trim y colapsa espacios múltiples a uno solo
        3. Descomposición NFKD para separar caracteres compuestos
        4. Remueve marcas diacríticas (acentos, tildes)
        5. Convierte a mayúsculas o minúsculas según parametro

    Example:
        Ángel Máximo -> ANGEL MAXIMO
        \"  Texto   con espacios  \" -> \"TEXTO CON ESPACIOS\"
    """
    if value is None or not isinstance(value, str):
        return value

    cleaned = " ".join(value.strip().split())
    cleaned = "".join(
        char
        # NFKD significa Normal Form Compatibility Decomposition. Su trabajo es "descomponer" caracteres compuestos en sus partes individuales.
        for char in unicodedata.normalize("NFKD", cleaned)
        if not unicodedata.combining(char)
    )
    return cleaned.upper() if to_upper else cleaned.lower()


def _normalize_dataframe(df):
    """Normaliza todas las columnas de texto (object) en un DataFrame.

    Aplica normalización de texto a todas las columnas de tipo 'object'
    para estandarizar datos antes de inserción o comparación con BD.
    TODAS las columnas de texto se normalizan a MAYÚSCULAS para evitar
    duplicados por inconsistencias de mayúsculas/minúsculas.

    Internal function consumed by:
        - insert_new_modified_records

    Dependencies:
        - _normalize_text
        - pandas DataFrame operations

    Args:
        df (pandas.DataFrame): DataFrame a normalizar.

    Returns:
        pandas.DataFrame: Copia del DataFrame con columnas de texto normalizadas.
        Columnas numéricas y otros tipos permanecen sin cambios.

    Processing:
        - Identifica columnas tipo 'object' (texto)
        - Aplica _normalize_text con to_upper=True a cada valor
        - Preserva estructura y tipos de otras columnas
        - Crea copia para evitar modificar el DataFrame original
    """
    normalized = df.copy()
    text_columns = normalized.select_dtypes(include=["object"]).columns
    for col in text_columns:
        # Normalizar todas las columnas de texto a MAYÚSCULAS para consistencia
        normalized[col] = normalized[col].apply(
            lambda x: _normalize_text(x, to_upper=True)
        )
    return normalized


def _clean_numeric_columns(df, numeric_columns):
    """Convierte columnas numéricas a números, valores inválidos se vuelven NaN.

    Aplica coerción numérica a columnas específicas para garantizar
    tipos consistentes antes de inserción en BD, evitando errores
    de tipo de dato.

    Internal function consumed by:
        - insert_new_modified_records

    Dependencies:
        - pandas.to_numeric

    Args:
        df (pandas.DataFrame): DataFrame a procesar.
        numeric_columns (set | list): Conjunto de nombres de columnas
            que deben ser numéricas.

    Returns:
        pandas.DataFrame: Copia del DataFrame con columnas numéricas convertidas.
        Valores no convertibles se transforman a NaN.

    Processing:
        - Itera sobre numeric_columns que existan en el DataFrame
        - Aplica pd.to_numeric con errors='coerce'
        - Valores como texto, fechas mal formateadas -> NaN
        - Números válidos se mantienen como float/int
        - Columnas no especificadas permanecen sin cambios
    """
    cleaned = df.copy()
    for col in numeric_columns:
        if col in cleaned.columns:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
    return cleaned


def _convert_timestamp_to_date(df):
    """Convierte columnas Timestamp a formato date (YYYY-MM-DD) para PostgreSQL.

    Transforma columnas datetime64 a formato date puro, removiendo
    información de hora para compatibilidad con campos DATE en PostgreSQL
    y evitar problemas de zona horaria.

    Internal function consumed by:
        - insert_new_modified_records

    Dependencies:
        - pandas.to_datetime
        - pandas DataFrame operations

    Args:
        df (pandas.DataFrame): DataFrame con posibles columnas datetime.

    Returns:
        pandas.DataFrame: Copia del DataFrame con columnas datetime
        convertidas a date. Otras columnas permanecen sin cambios.

    Processing:
        - Identifica columnas de tipo 'datetime64'
        - Convierte a datetime con errors='coerce' (inválidos -> NaT)
        - Extrae solo la parte de fecha (.dt.date)
        - Resultado compatible con campos PostgreSQL DATE
        - Preserva NaT como None para valores NULL en BD

    Note:
        - Remueve información de hora permanentemente
        - Útil para campos que solo requieren fecha (sin timestamp)
    """
    converted = df.copy()
    # Buscar columnas de tipo datetime/timestamp
    datetime_columns = converted.select_dtypes(include=["datetime64"]).columns
    for col in datetime_columns:
        # Convertir Timestamp a solo fecha (sin hora)
        converted[col] = pd.to_datetime(converted[col], errors="coerce").dt.date
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

            batch_size = 1000
            total_inserted = 0

            with engine.begin() as conn:
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
                    conn.exec_driver_sql(query_insert, batch)
                    total_inserted += len(batch)
                    logger.info(
                        f"Insercion por lotes: {total_inserted}/{len(data)} registros procesados"
                    )

            logger.info(f"INSERT: {len(data)} registros nuevos completado exitosamente")

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

            batch_size = 1000
            total_updated = 0

            with engine.begin() as conn:
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
                    conn.exec_driver_sql(query_update, batch)
                    total_updated += len(batch)
                    logger.info(
                        f"Actualizacion por lotes: {total_updated}/{len(data)} registros procesados"
                    )

            logger.info(
                f"UPDATE: {len(data)} registros modificados completado exitosamente"
            )

        logger.info("Sincronizacion completada")

    except Exception as e:
        logger.error(f"Error en UPSERT: {e}")
        raise
