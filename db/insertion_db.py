"""Bulk insert helpers for PostgreSQL."""

import logging

import pandas as pd

from config.db_config import configPostgre


def insert_to_database(df_missing, table_name):
    """
    Inserta filas faltantes en PostgreSQL usando inserción masiva.

    Args:
        df_missing (pandas.DataFrame): Filas a insertar.
        table_name (str): Nombre de la tabla destino.

    Returns:
        None: Ejecuta la insercion en la base de datos.
    """
    engine = None

    try:
        # 1. Preparar datos: DataFrame → Lista de tuplas
        # Reemplazar NaN por None (NULL en PostgreSQL)
        df_missing = df_missing.where(pd.notna(df_missing), None)

        # Convertir a lista de tuplas
        data = [tuple(row) for row in df_missing.values]

        logging.info(f"Preparadas {len(data)} filas para inserción.")

        # 2. Construir la query dinámicamente
        columns = df_missing.columns.tolist()
        columns_str = ", ".join([f'"{col}"' for col in columns])

        # Crear placeholders: (%s, %s, %s, ...)
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
        """

        logging.info(f"Query generada: {query}")

        # 3. Conectar a PostgreSQL
        engine = configPostgre()
        if engine is None:
            raise ValueError("No se pudo crear el engine de PostgreSQL.")

        # 4. Insercion masiva (exec_driver_sql soporta executemany con lista de tuplas)
        with engine.begin() as conn:
            result = conn.exec_driver_sql(query, data)

        logging.info(
            f"✅ Se insertaron {result.rowcount if result else len(data)} filas exitosamente en {table_name}"
        )

    except Exception as e:
        logging.error(f"❌ Error en la insercion a PostgreSQL: {e}")
        raise
