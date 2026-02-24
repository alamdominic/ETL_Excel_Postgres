"""Excel extraction utilities."""

import logging
import os
from datetime import datetime
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
        # Leer sin asumir headers en primera fila para inspeccionar
        df = pd.read_excel(excel_path, sheet_name=sheet_name)

        # Normalizar nombres de columnas: trim + minusculas y remover columnas vacias
        df.columns = [str(col).strip().lower() for col in df.columns]

        # Eliminar filas completamente vacías
        df.dropna(how="all", inplace=True)

        df = df.loc[:, [col for col in df.columns if col and "unnamed" not in col]]

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


def export_debug_excel(df, sheet_name, output_dir="excel"):
    """Exporta DataFrame procesado a Excel para análisis de debugging.

    Genera un archivo Excel con los datos ya procesados y normalizados
    para permitir inspección antes de la carga a base de datos.
    El archivo incluye timestamp en el nombre para versionar cada ejecución.

    Consumers:
        - main_orchestrator.export_excel_to_postgres

    Dependencies:
        - pandas.DataFrame.to_excel
        - os.makedirs
        - datetime
        - logging

    Args:
        df (pandas.DataFrame): DataFrame con datos procesados a exportar.
        sheet_name (str): Nombre de la hoja/tabla para incluir en el nombre del archivo.
        output_dir (str): Directorio donde se guardará el archivo. Default: "excel"

    Returns:
        str | None: Ruta completa del archivo generado si fue exitoso, None si hubo error.

    Side Effects:
        - Crea directorio output_dir si no existe
        - Genera archivo Excel con timestamp en el nombre
        - Registra información del proceso en logs

    Filename Format:
        debug_{sheet_name}_{YYYYMMDD_HHMMSS}.xlsx

    Example:
        debug_COBRANZA_20260224_153045.xlsx
    """
    try:
        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)

        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"debug_{sheet_name}_{timestamp}.xlsx"
        output_path = os.path.join(output_dir, filename)

        # Exportar a Excel
        df.to_excel(output_path, index=False, sheet_name=sheet_name)

        logger.info(
            f"✅ Archivo de debugging generado: {output_path} ({len(df)} registros)"
        )
        print(f"\n📊 Archivo de debugging generado: {output_path}")
        print(f"   Total de registros: {len(df)}")

        return output_path

    except PermissionError:
        logger.error(
            f"❌ Error de permisos al escribir en: {output_path}. El archivo puede estar abierto."
        )
        print(f"\n⚠️ No se pudo generar archivo de debugging: archivo en uso")
    except Exception as e:
        logger.error(f"❌ Error al exportar archivo de debugging: {e}")
        print(f"\n⚠️ Error al generar archivo de debugging: {e}")

    return None
