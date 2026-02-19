"""PostgreSQL configuration and engine creation."""

import logging
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Cargar las variables de entorno desde el archivo .env
load_dotenv()


def configPostgre():
    """Crea y devuelve un motor de conexión (engine) SQLAlchemy para PostgreSQL.

    Establece conexión utilizando psycopg2 como driver PostgreSQL con codificación
    segura de credenciales. Maneja automáticamente caracteres especiales en contraseñas.

    Consumers:
        - utils.table_state.get_last_transfer_id
        - utils.table_state.get_table_db_state
        - db.insertion_upsert.insert_new_modified_records
        - db.insertion_db.insert_to_database

    Dependencies:
        - sqlalchemy.create_engine
        - urllib.parse.quote_plus
        - os.getenv
        - dotenv.load_dotenv

    Variables de entorno requeridas:
        - DB_HOST_LOCAL_PG: Dirección del servidor PostgreSQL
        - DB_USER_LOCAL_PG: Usuario de base de datos
        - DB_PASSWORD_LOCAL_PG: Contraseña (se codifica automáticamente)
        - DB_NAME_LOCAL_PG: Nombre de la base de datos
        - DB_PORT_LOCAL_PG: Puerto (opcional, default 5432)

    Returns:
        sqlalchemy.engine.Engine | None: Engine configurado y listo para usar,
        o None si faltan variables de entorno requeridas.

    Raises:
        Exception: Si hay error en la creación del engine (conexión, credenciales, etc.)
    """

    # Cambiado a la variable de entorno local para pruebas en localhost DB_HOST --> DB_HOST_LOCAL_PG
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    DB_PORT = os.getenv("DB_PORT", "5432")

    # Verificación básica de que las variables existen
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        return None

    try:
        # Codificamos el password solo si existe
        safe_password = quote_plus(DB_PASSWORD)
        connection_str = f"postgresql+psycopg2://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        engine = create_engine(connection_str)
        return engine

    except Exception as e:
        return None


if __name__ == "__main__":
    engine = configPostgre()
    if engine:
        logging.info("Conexión a PostgreSQL exitosa.")
