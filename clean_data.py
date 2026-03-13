import pandas as pd

def remove_columns(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    df = df.drop(columns=column_names)
    return df

def cast_numbers(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    for column in column_names:
        df[column] = pd.to_numeric(df[column], errors="coerce", downcast='integer')
    return df

def remove_null_rows(df: pd.DataFrame, column_names:list) -> pd.DataFrame:
    df = df.dropna(subset=column_names)
    return df