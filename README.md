# Movie Data ETL Project

## Overview
This project is a simple ETL pipeline built using Python and MySQL. 
The goal of the project is to read movie and rating data from CSV files, get extra movie details from the OMDb API, and store the final data in a
relational database.

## Files in This Project
- etl.py – Python script that runs the ETL process
- schema.sql – SQL file to create database tables
- queries.sql – SQL queries to check and analyze the data
- README.md – Project explanation

## Tools Used
- Python
- Pandas
- MySQL
- OMDb API
- GitHub

## Running the Project

## Step 1: Cloning the repository
git clone https://github.com/nitinv11/movie-data-pipeline
cd movie-data-pipeline

## Step 2: Installing the required Python packages
pip install pandas requests pymysql python-dotenv

### Step 3: Creating a database
Log in to MySQL and execute 
CREATE DATABASE movie_pipeline;

## Step 4: Creating the required tables
Run the SQL statements from schema.sql and it creates all required tables in the movie_pipeline database.

## Step 5: Creating an OMDb API key and using it for the project
Created a file named `.env` in the project folder and added:

OMDB_API_KEY= "my omdb api key" 

## Step 6: Running the ETL script through bash 

python etl.py

## ETL functions 

### Extract
- Reads movie and rating data from CSV files.
- Calls the OMDb API to get extra movie information.

### Transform
- Cleans movie titles by removing the year.
- Extracts the release year into a separate column.
- Splits genres and stores them in separate tables.
- Handles missing values without failing the program.

### Load
- Inserts cleaned data into MySQL tables.
- Uses `INSERT IGNORE` so the script can be run multiple times safely.

## Design Decisions
- MySQL was used because it fits structured data well.
- Not all movies were sent to the OMDb API to avoid rate limits.
- Missing API values are stored as NULL in the database.

## Challenges Faced
- Some movies were not found in the OMDb API.
- Large files made the script slow when calling the API.
- Missing values caused errors at first, which were fixed by handling NaN values to MySQl equivelant datatype NULL 

## Sample Queries
The `queries.sql` file contains queries to:
- Count total movies and ratings
- Find movies with director information
- Analyze data by genre

## Future Improvements that can be added for a biger scope 
- Adding better logging
- Speeding up API calls
- Caching API responses

## Conclusion
This assignment helped me understand how an ETL pipeline works end to end using real data.I was able to read data from CSV files, enrich it using an external API, clean and transform the data, and load it into a relational database.
Through this assignment project I practiced Python, SQL, and working with APIs, and I also learned how to structure a project and share it using Git. This assignment reflects my current understanding of data engineering concepts and my approach to solving practical problems.