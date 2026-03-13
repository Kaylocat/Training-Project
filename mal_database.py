import mysql.connector
import pandas as pd

def open_connection():
    try:
        conn = mysql.connector.connect(
        host='127.0.0.1',
        user='admin',
        password='4he7fi3m02ud&',
        database='MAL'
        )
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access to Database denied.")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)

    return conn

def create_raw_table() -> str:
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
        print(err)
    finally:
        cursor.close()
        conn.close()
    

def create_genres_table() -> str:
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
        print(err)
    finally:
        cursor.close()
        conn.close()

def input_raw_data(df: pd.DataFrame) -> str:
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
        return f"Inserted {df.shape[0]} rows into ANIME_RAW"
    except mysql.connector.Error as err:
        print(err)
    finally:
        cursor.close()
        conn.close()

def input_genres(genres: list) -> str:
    conn = open_connection()
    cursor = conn.cursor()

    try: 
        print("WIP")
    except mysql.connector.Error as err:
        print(err)
    finally:
        cursor.close()
        conn.close()