"""Bulk insert helpers for PostgreSQL."""

import logging

import pandas as pd

from config.db_config import configPostgre


def insert_to_database(df_missing, table_name):
    """Inserta filas faltantes en PostgreSQL usando inserción masiva optimizada.

    Convierte un DataFrame a formato de tuplas y ejecuta inserción masiva
    usando exec_driver_sql con executemany, proporcionando mejor rendimiento
    que inserciones individuales.

    Consumers:
        - Funciones ETL que requieran inserción de datos nuevos únicamente
        - Procesos que trabajen con diferencias entre datasets

    Dependencies:
        - config.db_config.configPostgre
        - pandas para manipulación de datos
        - logging para trazabilidad

    Args:
        df_missing (pandas.DataFrame): DataFrame con las filas que deben insertarse.
            Debe tener columnas que correspondan exactamente con la tabla destino.
        table_name (str): Nombre completo de la tabla destino (ej. "schema.tabla").

    Returns:
        None: Ejecuta la inserción pero no retorna valores.
        El éxito se confirma via logs.

    Side Effects:
        - Inserta registros en la base de datos PostgreSQL
        - Genera logs informativos del proceso
        - Modifica el estado de la tabla destino

    Data Processing:
        - Reemplaza NaN con None (NULL en PostgreSQL)
        - Convierte DataFrame a lista de tuplas para inserción masiva
        - Construye query dinámicamente basada en columnas del DataFrame
        - Usa placeholders %s para prevenir SQL injection

    Error Handling:
        - Captura y registra cualquier excepción durante inserción
        - Hace re-raise de excepciones para manejo en nivel superior
        - Usa transacciones para garantizar consistencia

    Performance Note:
        - Usa exec_driver_sql para inserción masiva optimizada
        - Maneja automáticamente transacciones con engine.begin()
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
            f"Se insertaron {result.rowcount if result else len(data)} filas exitosamente en {table_name}"
        )

    except Exception as e:
        logging.error(f"Error en la insercion a PostgreSQL: {e}")
        raise
