"""Helpers to compare Excel data against DB records."""

import pandas as pd


def track_changes(df_existing, records_dict, id_column):
    """Compara registros existentes en BD y en Excel.

    Su objetivo es identificar cuales registros realmente cambiaron y devolver
    solo los que tienen diferencias.

    Consumers:
        - main_orchestrator.export_excel_to_postgres

    Dependencies:
        - pandas

    Args:
        df_existing (pandas.DataFrame): Filas de Excel con IDs existentes en BD.
        records_dict (dict): Registros de BD indexados por el id_column.
        id_column (str): Nombre de la columna ID para comparar.

    Returns:
        pandas.DataFrame: Subconjunto de df_existing con registros modificados.
    """
    modified_ids = []

    # Recorre las filas del DataFrame una a una, desechando el indice
    for _, row in df_existing.iterrows():
        record_id = row[id_column]
        registro_bd = records_dict[record_id]

        # Comparar cada columna
        for col in df_existing.columns:
            valor_excel = None if pd.isna(row[col]) else row[col]
            valor_bd = None if pd.isna(registro_bd.get(col)) else registro_bd.get(col)

            if valor_excel != valor_bd:
                modified_ids.append(record_id)
                break  # Ya encontramos un cambio, no seguir comparando

    return df_existing[df_existing[id_column].isin(modified_ids)]
