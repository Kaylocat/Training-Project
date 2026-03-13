import pandas as pd
from pathlib import Path
import logging

import clean_data as cd
import genres as genre
import mal_database as db

# ------ Paths and Logging Config ------
BASEPATH = Path(__file__).resolve().parent
RAWDATA = BASEPATH / "dataset.csv"
OUTDIR = BASEPATH / "out"
OUTDIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Project Logger")

COLUMNS_TO_REMOVE = ["alternative_title", "mal_url", "image_url", "genres", "genres_detailed"]
NUMERIC_COLUMNS = ["year", "episodes", "score"]
NON_NULLABLE_COLUMNS = ["title", "type", "year", "score"]

# ------ Load Data -------
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    logger.info("\nRead " + str(df.shape[0]) + " rows from " + str(path) + "\n")
    return df

# ------ Transformations ------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    initial_rows = df.shape[0]

    df = cd.remove_columns(df, COLUMNS_TO_REMOVE)
    df = cd.cast_numbers(df, NUMERIC_COLUMNS)
    df = cd.remove_null_rows(df, NON_NULLABLE_COLUMNS)

    logger.info(f"\nRemoved {initial_rows - df.shape[0]} invalid rows from Dataframe.\n")
    return df

# ------ Orchestration ------
def main():
    df = load_data(RAWDATA)

    genres = genre.get_all_genres(df, "genres")
    genre_columns = genre.get_genre_columns(df, genres)

    df = clean_dataframe(df)
    
    logger.info("\n" + db.create_raw_table() + "\n")

    logger.info("\nBegin insert into ANIME_RAW\n")
    logger.info("\n" + db.input_raw_data(df) + "\n")

    print(df.head())

if __name__ == "__main__":
    main()