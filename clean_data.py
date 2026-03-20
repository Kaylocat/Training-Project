import pandas as pd

def remove_columns(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Remove columns with given column names from the given dataframe.\n
    Return resulting dataframe.
    """
    df = df.drop(columns=column_names)
    return df

def cast_numbers(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Cast values of given columns in the given dataframe.\n
    Return resulting dataframe.
    """
    for column in column_names:
        df[column] = pd.to_numeric(df[column], errors="coerce", downcast='integer')
    return df

def remove_null_rows(df: pd.DataFrame, column_names:list) -> pd.DataFrame:
    """
    Remove rows with null values in given column names from the given dataframe.\n
    Return resulting dataframe.
    """
    df = df.dropna(subset=column_names)
    return df