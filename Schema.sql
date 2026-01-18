import pandas as pd
import requests
import pymysql
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Nitin@12",
    "database": "movie_pipeline",
    "port": 3306
}

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_LIMIT = 300

def normalize_title(title):
    if title.endswith(", The"):
        return "The " + title.replace(", The", "")
    return title

def fetch_omdb(title):
    try:
        r = requests.get(
            "http://www.omdbapi.com/",
            params={"t": title, "apikey": OMDB_API_KEY},
            timeout=5
        ).json()
        return r if r.get("Response") == "True" else {}
    except:
        return {}

def main():
    print("Starting ETL pipeline")

    movies = pd.read_csv("movies.csv")
    ratings = pd.read_csv("ratings.csv")

    movies["release_year"] = movies["title"].str.extract(r"\((\d{4})\)")
    movies["title_clean"] = movies["title"].str.replace(r"\s\(\d{4}\)", "", regex=True)

    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in movies.iterrows():
        omdb = {}
        if row.name < OMDB_LIMIT:
            omdb = fetch_omdb(normalize_title(row["title_clean"]))

        cur.execute(
            """
            INSERT IGNORE INTO movies
            (movie_id, title, release_year, director, plot, box_office)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row["movieId"],
                row["title_clean"],
                row["release_year"],
                omdb.get("Director"),
                omdb.get("Plot"),
                omdb.get("BoxOffice")
            )
        )

        for genre in row["genres"].split("|"):
            cur.execute(
                "INSERT IGNORE INTO genres (genre_name) VALUES (%s)",
                (genre,)
            )
            cur.execute(
                """
                INSERT IGNORE INTO movie_genres (movie_id, genre_id)
                SELECT %s, genre_id FROM genres WHERE genre_name=%s
                """,
                (row["movieId"], genre)
            )

    for _, row in ratings.iterrows():
        cur.execute(
            """
            INSERT IGNORE INTO ratings
            (movie_id, user_id, rating, rating_timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (
                row["movieId"],
                row["userId"],
                row["rating"],
                datetime.fromtimestamp(row["timestamp"])
            )
        )

    conn.commit()
    conn.close()

    print("ETL pipeline completed")

if __name__ == "__main__":
    main()
