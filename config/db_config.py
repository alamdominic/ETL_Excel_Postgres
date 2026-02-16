"""PostgreSQL configuration and engine creation."""

import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Cargar las variables de entorno desde el archivo .env
load_dotenv()


def configPostgre():
    """
    Crea y devuelve un motor de conexión (engine) SQLAlchemy para PostgreSQL.

    Variables de entorno requeridas:
        - DB_HOST_LOCAL_PG
        - DB_USER_LOCAL_PG
        - DB_PASSWORD_LOCAL_PG
        - DB_NAME_LOCAL_PG
        - DB_PORT_LOCAL_PG (opcional, default 5432)

    Returns:
        sqlalchemy.engine.Engine | None: Engine listo para usar, o None si falta
        configuracion.
    """

    # Cambiado a la variable de entorno local para pruebas en localhost DB_HOST --> DB_HOST_LOCAL_PG
    DB_HOST = os.getenv("DB_HOST_LOCAL_PG")
    DB_USER = os.getenv("DB_USER_LOCAL_PG")
    DB_PASSWORD = os.getenv("DB_PASSWORD_LOCAL_PG")
    DB_NAME = os.getenv("DB_NAME_LOCAL_PG")
    DB_PORT = os.getenv("DB_PORT_LOCAL_PG", "5432")
    print(
        f"Intentando conectar a DB con host: {DB_HOST}, user: {DB_USER}, port: {DB_PORT}"
    )
    # Verificación básica de que las variables existen
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        print("Error: Faltan variables de entorno para la conexión a la DB.")
        return None

    try:
        # Codificamos el password solo si existe
        safe_password = quote_plus(DB_PASSWORD)
        connection_str = f"postgresql+psycopg2://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        engine = create_engine(connection_str)
        return engine

    except Exception as e:
        print(f"Error al crear conexion con la DB: {e}")
        return None


if __name__ == "__main__":
    engine = configPostgre()
    if engine:
        print("Conexión a PostgreSQL exitosa.")
