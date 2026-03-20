import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from dataclasses import dataclass

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
Lineage = []

@dataclass
class lineage_event:
    action: str
    result: str
    timestamp: str

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")

# ------ Load Data -------
def load_data(path: Path) -> pd.DataFrame:
    """
    Load csv file from the given path.\n
    Returns resulting dataframe.
    """
    df = pd.read_csv(path)
    message = "Read " + str(df.shape[0]) + " rows from " + str(path)
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Load data from CSV file at " + str(path),
        result=message,
        timestamp=now_iso()
    ))

    return df

def create_tables():
    """Create tables for raw data, genres, and relationship table."""
    message = str(db.create_raw_table())
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Create raw data table.",
        result=message,
        timestamp=now_iso()
    ))
    
    message = str(db.create_genres_table())
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Create genres table.",
        result=message,
        timestamp=now_iso()
    ))

    message = str(db.create_anime_genres_table())
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Create genres by anime table.",
        result=message,
        timestamp=now_iso()
    ))

    message = str(db.create_type_tables())
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Create tables for series and movies.",
        result=message,
        timestamp=now_iso()
    ))

def input_data(df: pd.DataFrame, genres: list, genres_by_row: dict):
    """Insert data into raw data, genres, and relationship table."""
    logger.info("\nBegin insert into ANIME_RAW.\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result="Begin insert into ANIME_RAW.",
        timestamp=now_iso()
    ))
    message = str(db.input_raw_data(df))
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result=message,
        timestamp=now_iso()
    ))

    logger.info("\nBegin insert into GENRES.")
    Lineage.append(lineage_event(
        action="Insert data",
        result="Begin insert into GENRES.",
        timestamp=now_iso()
    ))
    message = str(db.input_genres(genres))
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result=message,
        timestamp=now_iso()
    ))

    logger.info("\nBegin insert into GENRES_BY_ANIME.\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result="Begin insert into GENRES_BY_ANIME.",
        timestamp=now_iso()
    ))
    message = str(db.input_genres_by_anime(genres_by_row))
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result=message,
        timestamp=now_iso()
    ))

    logger.info("\nBegin insert into MOVIES and SERIES.\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result="Begin insert into MOVIES and SERIES.",
        timestamp=now_iso()
    ))
    message = str(db.input_type_tables())
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Insert data",
        result=message,
        timestamp=now_iso()
    ))

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

    message = f"Removed {initial_rows - df.shape[0]} invalid rows from Dataframe."
    logger.info("\n" + message + "\n")
    Lineage.append(lineage_event(
        action="Transformations",
        result=message,
        timestamp=now_iso()
    ))

    return df

# ------ Analysis -------
def create_graphs(path):
    genres_table = an.get_score_by_genre()
    graph.plot_genres_bar(genres_table, path / "Score_by_genre")
    Lineage.append(lineage_event(
        action="Analysis",
        result="Created image Score_by_genre in " + str(path),
        timestamp=now_iso()
    ))

    year_table = an.get_score_by_year()
    graph.plot_year_bar(year_table, path / "Score_by_year")
    Lineage.append(lineage_event(
        action="Analysis",
        result="Created image Score_by_year in " + str(path),
        timestamp=now_iso()
    ))

# ------ Execute Pipeline ------
def main():
    df = load_data(RAWDATA)

    df = clean_dataframe(df)

    genres = genre.get_all_genres(df, "genres")
    genres_by_row = genre.get_genre_columns(df, genres)
    df = cd.remove_columns(df, list(["genres"]))
    
    create_tables()
    input_data(df, genres, genres_by_row)

    create_graphs(OUTDIR)

    with open(OUTDIR / "logs.txt", "w", encoding="UTF-8") as log_file:
        for _event in Lineage:
            log_file.write(f"{_event.timestamp}\n{_event.action}\n\t Result: {_event.result}\n\n")

if __name__ == "__main__":
    main()