"""Excel extraction utilities."""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def xlsx_to_df(excel_path, sheet_name):
    """Lee una hoja de Excel y la devuelve como DataFrame.

    Consumers:
        - main_orchestrator.export_excel_to_postgres

    Dependencies:
        - pandas.ExcelFile
        - pandas.read_excel
        - logging

    Args:
        excel_path (str): Ruta completa del archivo Excel.
        sheet_name (str): Nombre de la hoja a leer.

    Returns:
        pandas.DataFrame | None: DataFrame con los datos leidos. Devuelve un
        DataFrame vacio si la hoja esta vacia, o None si ocurre un error.
        Column names are normalized to lowercase and empty/unnamed columns
        are removed.
    """
    logger.info(f"Iniciando lectura de Excel: {excel_path} (Hoja: {sheet_name})")

    try:
        sheet_name = (
            sheet_name.upper()
        )  # Convertir a mayúsculas para evitar problemas de coincidencia
        # El uso de 'with' asegura que el archivo se cierre correctamente tras la lectura
        with pd.ExcelFile(excel_path) as xls:
            df = pd.read_excel(xls, sheet_name=sheet_name)

        # Normalizar nombres de columnas: trim + minusculas y remover columnas vacias
        df.columns = [str(col).strip().lower() for col in df.columns]
        df = df.loc[:, [col for col in df.columns if col and col != "unnamed: 0"]]

        if df.empty:
            logger.warning(f"El archivo o la hoja '{sheet_name}' está vacía.")
            return (
                pd.DataFrame()
            )  # Retornamos DF vacío para mantener consistencia de tipo

        logger.info(f"Lectura exitosa. Filas procesadas: {len(df)}")
        return df

    except FileNotFoundError:
        logger.error(f"Archivo no encontrado en la ruta: {excel_path}")
    except ValueError as e:
        logger.error(f"Error en la hoja '{sheet_name}': {e}")
    except Exception as e:
        logger.error(f"Error inesperado al extraer datos: {e}")

    return None
