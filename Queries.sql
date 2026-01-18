USE movie_pipeline;

-- Total number of movies and ratings
SELECT COUNT(*) AS total_movies FROM movies;
SELECT COUNT(*) AS total_ratings FROM ratings;

-- Top 10 most rated movies
SELECT m.title, COUNT(r.rating) AS rating_count
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.title
ORDER BY rating_count DESC
LIMIT 10;

-- Top 10 highest rated movies
SELECT m.title, AVG(r.rating) AS avg_rating, COUNT(r.rating) AS rating_count
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.title
HAVING COUNT(r.rating) >= 50
ORDER BY avg_rating DESC
LIMIT 10;

-- Average rating by genre
SELECT g.genre_name, AVG(r.rating) AS avg_rating
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;

-- Movies with OMDb metadata available
SELECT title, director
FROM movies
WHERE director IS NOT NULL
LIMIT 10;

-- Average rating per release year
SELECT release_year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year;
