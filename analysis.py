import mysql.connector
import pandas as pd
from pathlib import Path
import os

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

def get_score_by_genre():
    """Get dataframe for average score by genre for series"""
    conn = open_connection()
    cursor = conn.cursor()
    FILEPATH = Path(__file__).resolve().parent / "Table_temp.csv"

    sql = """
    SELECT g.genre_name, ROUND(AVG(ar.score), 3) as avg_score 
        FROM ANIME_RAW as ar, GENRES as g, SERIES as se, GENRES_BY_ANIME gba
        WHERE ar.id = gba.anime_id AND g.id = gba.genre_id and se.anime_id = ar.id
        GROUP BY g.genre_name
        ORDER BY avg_score DESC;
    """

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        with open(FILEPATH, "w", encoding="UTF-8") as temp_file:
            temp_file.write("genre_name,avg_score\n")
            for row in result:
                temp_file.write(str(row[0]) + "," + str(row[1]) + "\n")
        
        df = pd.read_csv(FILEPATH, index_col=False)
        df["avg_score"] = pd.to_numeric(df["avg_score"], errors="coerce")

        os.remove(FILEPATH)
        return df
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()

def get_score_by_year():
    """Get dataframe for average score by genre for series"""
    conn = open_connection()
    cursor = conn.cursor()
    FILEPATH = Path(__file__).resolve().parent / "Table_temp.csv"

    sql = """
    SELECT ar.year, ROUND(AVG(ar.score), 3) as avg_score
        FROM 
            (SELECT ar.year, ar.score
                FROM ANIME_RAW as ar, SERIES as se
                WHERE ar.id = se.anime_id) as ar
        GROUP BY ar.year
        HAVING COUNT(*) >= 50
        ORDER BY year DESC;
    """

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        with open(FILEPATH, "w", encoding="UTF-8") as temp_file:
            temp_file.write("year,avg_score\n")
            for row in result:
                temp_file.write(str(row[0]) + "," + str(row[1]) + "\n")
        
        df = pd.read_csv(FILEPATH, index_col=False)
        df["avg_score"] = pd.to_numeric(df["avg_score"], errors="coerce")
        df["year"] = pd.to_numeric(df["year"], errors="coerce")

        os.remove(FILEPATH)
        return df
    except mysql.connector.Error as err:
        return err
    finally:
        cursor.close()
        conn.close()