import pandas as pd
import logging


def align_df_to_schema(df, schema):
    """Alinea y convierte las columnas de un DataFrame para coincidir con el esquema de BD.

    Procesa cada columna del DataFrame aplicando conversiones de tipo basadas
    en el esquema SQL proporcionado. Maneja tipos numéricos, fechas, booleanos
    y texto con coerción de errores para evitar fallos por datos inconsistentes.

    Consumers:
        - Funciones de inserción que requieran alineación de esquema
        - Procesos ETL que trabajen con esquemas dinámicos

    Dependencies:
        - pandas (to_numeric, to_datetime, astype)
        - logging para advertencias de conversión

    Args:
        df (pandas.DataFrame): DataFrame a alinear con el esquema.
        schema (dict): Diccionario con nombres de columna como claves y
            tipos SQL como valores (ej. {'id': 'integer', 'name': 'varchar'}).

    Returns:
        pandas.DataFrame: DataFrame con columnas convertidas a los tipos
        apropiados según el esquema. Las columnas que no estén en el esquema
        permanecen sin cambios.

    Type Mappings:
        - int/serial -> Int64 (nullable integer)
        - numeric/decimal/double -> float
        - date/timestamp -> datetime
        - boolean -> bool
        - otros (varchar/text) -> string

    Note:
        - Usa 'errors=coerce' para convertir valores inválidos a NaN/NaT
        - Registra advertencias para columnas que no se pudieron convertir
        - Preserva columnas no especificadas en el esquema
    """
    df_aligned = df.copy()

    for col_name, sql_type in schema.items():
        if col_name in df_aligned.columns:
            try:
                if "int" in sql_type or "serial" in sql_type:
                    # Convert to numeric, coercing errors, then to nullable integer
                    df_aligned[col_name] = pd.to_numeric(
                        df_aligned[col_name], errors="coerce"
                    ).astype("Int64")
                elif (
                    "numeric" in sql_type
                    or "decimal" in sql_type
                    or "double" in sql_type
                ):
                    df_aligned[col_name] = pd.to_numeric(
                        df_aligned[col_name], errors="coerce"
                    )
                elif "date" in sql_type or "timestamp" in sql_type:
                    df_aligned[col_name] = pd.to_datetime(
                        df_aligned[col_name], errors="coerce"
                    )
                elif "boolean" in sql_type:
                    df_aligned[col_name] = df_aligned[col_name].astype(bool)
                else:  # Varchar, text, etc.
                    df_aligned[col_name] = df_aligned[col_name].astype(str)
            except Exception as e:
                logging.warning(
                    f"Could not cast column '{col_name}' to {sql_type}. Error: {e}"
                )

    return df_aligned
