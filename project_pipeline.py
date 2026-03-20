import pandas as pd
from pathlib import Path
import logging

import clean_data as cd
import genres as genre
import mal_database as db
import analysis as an
import graphing as graph

# ------ Paths and Logging Config ------
BASEPATH = Path(__file__).resolve().parent
RAWDATA = BASEPATH / "dataset.csv"
OUTDIR = BASEPATH / "out"
OUTDIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Project Logger")

COLUMNS_TO_REMOVE = ["alternative_title", "mal_url", "image_url", "genres_detailed"]
NUMERIC_COLUMNS = ["year", "episodes", "score"]
NON_NULLABLE_COLUMNS = ["title", "type", "year", "score"]

# ------ Load Data -------
def load_data(path: Path) -> pd.DataFrame:
    """
    Load csv file from the given path.\n
    Returns resulting dataframe.
    """
    df = pd.read_csv(path)
    logger.info("\nRead " + str(df.shape[0]) + " rows from " + str(path) + "\n")
    return df

def create_tables():
    """Create tables for raw data, genres, and relationship table."""
    logger.info("\n" + str(db.create_raw_table()) + "\n")
    logger.info("\n" + str(db.create_genres_table()) + "\n")
    logger.info("\n" + str(db.create_anime_genres_table()) + "\n")
    logger.info("\n" + str(db.create_type_tables()) + "\n")

def input_data(df: pd.DataFrame, genres: list, genres_by_row: dict):
    """Insert data into raw data, genres, and relationship table."""
    logger.info("\nBegin insert into ANIME_RAW.\n")
    logger.info("\n" + str(db.input_raw_data(df)) + "\n")

    logger.info("\nBegin insert into GENRES.")
    logger.info("\n" + str(db.input_genres(genres)) + "\n")

    logger.info("\nBegin insert into GENRES_BY_ANIME.\n")
    logger.info("\n" + str(db.input_genres_by_anime(genres_by_row)) + "\n")
    db.input_type_tables()
    logger.info("\n" + str(db.input_type_tables()) + "\n")

# ------ Transformations ------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform necessary cleanup on the dataframe.\n
    Return resulting dataframe.
    """
    initial_rows = df.shape[0]

    df = cd.remove_columns(df, COLUMNS_TO_REMOVE)
    df = cd.cast_numbers(df, NUMERIC_COLUMNS)
    df = cd.remove_null_rows(df, NON_NULLABLE_COLUMNS)

    logger.info(f"\nRemoved {initial_rows - df.shape[0]} invalid rows from Dataframe.\n")
    return df

# ------ Analysis -------
def create_graphs():
    genres_table = an.get_score_by_genre()
    graph.plot_genres_bar(genres_table, OUTDIR / "Score_by_genre")
    year_table = an.get_score_by_year()
    graph.plot_year_bar(year_table, OUTDIR / "Score_by_year")
    #print(year_table)

# ------ Orchestration ------
def main():
    df = load_data(RAWDATA)

    df = clean_dataframe(df)

    genres = genre.get_all_genres(df, "genres")
    genres_by_row = genre.get_genre_columns(df, genres)
    df = cd.remove_columns(df, list(["genres"]))
    
    create_tables()
    input_data(df, genres, genres_by_row)

    #print(df.head())
    create_graphs()

if __name__ == "__main__":
    main()