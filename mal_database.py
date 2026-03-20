import mysql.connector
import pandas as pd

def open_connection():
    """Returns connection to database, or error if failed."""
    try:
        conn = mysql.connector.connect(
        host='127.0.0.1',
        user='admin',
        password='4he7fi3m02ud&',
        database='MAL'
        )
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            return "Access to Database denied."
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            return "Database does not exist."
        else:
            return err

    return conn

def create_raw_table() -> str:
    """Create table for raw anime data."""
    conn = open_connection()
    cursor = conn.cursor()

    sql = """
    DROP TABLE IF EXISTS ANIME_RAW;
    CREATE TABLE IF NOT EXISTS ANIME_RAW(
        id          INT PRIMARY KEY,
        title       VARCHAR(200),
        anime_type  VARCHAR(10),
        year        INT,
        score       FLOAT,
        episodes    INT,
        sequel      VARCHAR(5)
    );
    
    """
    try:
        cursor.execute(sql)
        return "Created table ANIME_RAW."
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()
    

def create_genres_table() -> str:
    """Create table for all genres."""
    conn = open_connection()
    cursor = conn.cursor()

    sql = """

    DROP TABLE IF EXISTS GENRES;
    CREATE TABLE IF NOT EXISTS GENRES(
        id          INT PRIMARY KEY,
        genre_name  VARCHAR(25)
    );
    
    """
    try:
        cursor.execute(sql)
        return "Created table GENRES."
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()

def create_anime_genres_table() -> str:
    """Create table with an N to M relationship between anime data and genres"""
    conn = open_connection()
    cursor = conn.cursor()

    sql = """

    DROP TABLE IF EXISTS GENRES_BY_ANIME;
    CREATE TABLE IF NOT EXISTS GENRES_BY_ANIME(
        anime_id          INT,
        genre_id          INT,

        PRIMARY KEY (anime_id, genre_id)
    );
    """

    try:
        cursor.execute(sql)
        return "Created table GENRES_BY_ANIME."
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()


def input_raw_data(df: pd.DataFrame) -> str:
    """Insert raw data from dataframe into ANIME_RAW table."""
    conn = open_connection()
    cursor = conn.cursor()

    try: 
        for row in df.itertuples():
            sql = f"""
                INSERT INTO ANIME_RAW (id, title, anime_type, year, score, episodes, sequel)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);         
            """
            values = (row.animeID, row.title, row.type, int(row.year), row.score, int(row.episodes), row.sequel)
            
            cursor.execute(sql, values)

        conn.commit()
        return f"Inserted {df.shape[0]} rows into ANIME_RAW."
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()

def input_genres(genres: list) -> str:
    """Insert data from genres list into GENRES table."""
    conn = open_connection()
    cursor = conn.cursor()

    try: 
        for genre in genres:
            sql = """
                INSERT INTO GENRES (id, genre_name)            
                    VALUES (%s, %s)
            """

            values = (genres.index(genre), genre)
            cursor.execute(sql, values)
        
        conn.commit()
        return f"Inserted {len(genres)} rows into GENRES."
    
    except mysql.connector.Error as err:
        print(err)
    finally:
        cursor.close()
        conn.close()

def input_genres_by_anime(genres_by_anime: dict) -> str:
    """Insert dictionary of anime and their genres into GENRE_BY_ANIME table."""
    conn = open_connection()
    cursor = conn.cursor()

    try: 
        count = 0
        for show, genres in genres_by_anime.items():
            for genre in genres:

                sql = """
                    INSERT INTO GENRES_BY_ANIME (anime_id, genre_id)            
                        VALUES (%s, %s)
                """

                values = (show, genre)
                cursor.execute(sql, values)
                count += 1
        
        conn.commit()
        return f"Inserted {count} rows into GENRES_BY_ANIME."
    
    except mysql.connector.Error as err:
        print(err)
    finally:
        cursor.close()
        conn.close()

def create_type_tables():
    """Create silver tier tables of MOVIE and SERIES"""
    conn = open_connection()
    cursor = conn.cursor()

    sql = """

    DROP TABLE IF EXISTS MOVIES;
    CREATE TABLE IF NOT EXISTS MOVIES(
        movie_id          INT PRIMARY KEY AUTO_INCREMENT,
        anime_id          INT,
        sequel            VARCHAR(5)
    );

    DROP TABLE IF EXISTS SERIES;
    CREATE TABLE IF NOT EXISTS SERIES(
        series_id         INT PRIMARY KEY AUTO_INCREMENT,
        anime_id          INT,
        sequel            VARCHAR(5)
    );
    """

    try:
        cursor.execute(sql)
        return "Created tables MOVIE and SERIES."
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()

def input_type_tables():
    """Insert data into silver tier tables of MOVIE and SERIES"""
    conn = open_connection()
    cursor = conn.cursor()

    sql = ["""
    INSERT INTO MOVIES(anime_id, sequel) 
        (SELECT id, sequel 
            FROM ANIME_RAW
            WHERE anime_type = "MOVIE");
    """,
    """
    INSERT INTO SERIES(anime_id, sequel) 
        (SELECT id, sequel 
            FROM ANIME_RAW
            WHERE anime_type = "TV");
    """]

    queries = [
        "SELECT * FROM MOVIES;",
        "SELECT * FROM SERIES"
    ]

    try:
        for command in sql:
            cursor.execute(command)
        conn.commit()

        cursor.execute(queries[0])
        movie_rows = len(cursor.fetchall())
        cursor.execute(queries[1])
        series_rows = len(cursor.fetchall())
        return f"Inserted {movie_rows} rows into MOVIES. Inserted {series_rows} rows into SERIES."
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()