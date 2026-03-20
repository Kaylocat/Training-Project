import pandas as pd
import re

def get_all_genres(df: pd.DataFrame, column_name) -> list:
    """
    Given a dataframe, return all genres that appear in the given column name
    """
    genres = []
    regex = re.compile(r"[\[\]\"\']")

    for ls in df[column_name]:
        ls = regex.sub("", ls).split(",")
        for word in ls:
            word = re.sub(regex, "", word).strip()
            if word not in genres and word.strip():
                genres.append(word)

    return genres

def get_genre_columns(df: pd.DataFrame, genres: list) -> dict:
    """
    Given a dataframe and list of genres, return a dictionary with each row id and its list of genres as ids
    """
    dt = dict()
    regex = re.compile(r"[\[\]\"\']")

    for i in range(df.shape[0]):
        row = []
        ls = regex.sub("", df.iloc[i]["genres"]).split(",")
        for word in ls:
            if word.strip():
                row.append(int(genres.index(word.strip())))
        
        dt[int(df.iloc[i]["animeID"])] = row

    return dt