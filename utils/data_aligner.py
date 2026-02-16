import pandas as pd
import logging

def align_df_to_schema(df, schema):
    """
    Aligns and casts DataFrame columns to match the database schema.

    Args:
        df (pd.DataFrame): The DataFrame to align.
        schema (dict): A dictionary with column names as keys and SQL data types as values.

    Returns:
        pd.DataFrame: The aligned and casted DataFrame.
    """
    df_aligned = df.copy()
    
    for col_name, sql_type in schema.items():
        if col_name in df_aligned.columns:
            try:
                if 'int' in sql_type or 'serial' in sql_type:
                    # Convert to numeric, coercing errors, then to nullable integer
                    df_aligned[col_name] = pd.to_numeric(df_aligned[col_name], errors='coerce').astype('Int64')
                elif 'numeric' in sql_type or 'decimal' in sql_type or 'double' in sql_type:
                    df_aligned[col_name] = pd.to_numeric(df_aligned[col_name], errors='coerce')
                elif 'date' in sql_type or 'timestamp' in sql_type:
                    df_aligned[col_name] = pd.to_datetime(df_aligned[col_name], errors='coerce')
                elif 'boolean' in sql_type:
                    df_aligned[col_name] = df_aligned[col_name].astype(bool)
                else: # Varchar, text, etc.
                    df_aligned[col_name] = df_aligned[col_name].astype(str)
            except Exception as e:
                logging.warning(f"Could not cast column '{col_name}' to {sql_type}. Error: {e}")
    
    return df_aligned
