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


def clean_value(value):
    if pd.isna(value):
        return None
    return value


def normalize_title(title):
    if title.endswith(", The"):
        return "The " + title.replace(", The", "")
    return title


def fetch_omdb(title):
    try:
        response = requests.get(
            "https://www.omdbapi.com/",
            params={"t": title, "apikey": OMDB_API_KEY},
            timeout=5
        )
        data = response.json()
        if data.get("Response") == "True":
            return data
        return {}
    except:
        return {}


def main():
    print("ETL started")

    movies = pd.read_csv("movies.csv")
    ratings = pd.read_csv("ratings.csv")

    print("Movies loaded:", len(movies))
    print("Ratings loaded:", len(ratings))

    movies["release_year"] = movies["title"].str.extract(r"\((\d{4})\)")
    movies["title_clean"] = movies["title"].str.replace(r"\s\(\d{4}\)", "", regex=True)

    conn = pymysql.connect(**DB_CONFIG, autocommit=True)
    cur = conn.cursor()

    print("Loading movies and genres")

    for index, row in movies.iterrows():
        if index == 0:
            print("Processing first movie")

        omdb_data = {}
        if index < OMDB_LIMIT:
            title = normalize_title(row["title_clean"])
            omdb_data = fetch_omdb(title)

        cur.execute(
            """
            INSERT IGNORE INTO movies
            (movie_id, title, release_year, director, plot, box_office)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                clean_value(row["movieId"]),
                clean_value(row["title_clean"]),
                clean_value(row["release_year"]),
                clean_value(omdb_data.get("Director")),
                clean_value(omdb_data.get("Plot")),
                clean_value(omdb_data.get("BoxOffice"))
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
                SELECT %s, genre_id FROM genres WHERE genre_name = %s
                """,
                (row["movieId"], genre)
            )

    print("Movies loaded")

    print("Loading ratings")

    for index, row in ratings.iterrows():
        if index == 0:
            print("Processing first rating")

        cur.execute(
            """
            INSERT IGNORE INTO ratings
            (movie_id, user_id, rating, rating_timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (
                clean_value(row["movieId"]),
                clean_value(row["userId"]),
                clean_value(row["rating"]),
                clean_value(datetime.fromtimestamp(row["timestamp"]))
            )
        )

    print("Ratings loaded")

    conn.close()
    print("ETL completed")


if __name__ == "__main__":
    main()
