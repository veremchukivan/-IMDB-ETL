CREATE DATABASE imdb_etl;

CREATE SCHEMA imdb_etl.staging;

USE imdb_etl.staging;

-- creat staging table
CREATE OR REPLACE TABLE movies_staging (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(200),
    year INT,
    date_published DATE,
    duration INT,
    country VARCHAR(250),
    worlwide_gross_income VARCHAR(30),
    languages VARCHAR(200),
    production_company VARCHAR(200)
);

CREATE OR REPLACE TABLE genres_staging (
    movie_id VARCHAR(10),
    genre VARCHAR(50),
    PRIMARY KEY (movie_id, genre)
);

CREATE OR REPLACE TABLE name_staging (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    height INT,
    date_of_birth DATE,
    known_for_movies VARCHAR(100)
);

CREATE OR REPLACE TABLE ratings_staging (
    movie_id VARCHAR(10),
    avg_rating DECIMAL(3,1),
    total_votes INT,
    median_rating INT,
    PRIMARY KEY (movie_id)
);

CREATE OR REPLACE TABLE director_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    PRIMARY KEY (movie_id, name_id)
);

CREATE OR REPLACE TABLE role_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    category VARCHAR(10),
    PRIMARY KEY (movie_id, name_id)
);

-- create stage
CREATE OR REPLACE STAGE imdbSt;

-- load dat
COPY INTO movies_staging
FROM @imdbSt/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @imdbSt/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO role_mapping_staging
FROM @imdbSt/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO director_mapping_staging
FROM @imdbSt/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @imdbSt/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO name_staging
FROM @imdbSt/names.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- dim_movies
CREATE OR REPLACE TABLE dim_movies AS
SELECT DISTINCT
    id AS movie_id,
    title,
    year,
    date_published,
    duration,
    country,
    languages,
    production_company
FROM movies_staging;

-- dim_people
CREATE OR REPLACE TABLE dim_people AS
SELECT DISTINCT
    n.id AS person_id,
    n.name,
    r.category AS role,
    n.known_for_movies,
    n.date_of_birth
FROM name_staging n
LEFT JOIN role_mapping_staging r ON n.id = r.name_id;
-- dim_genres
CREATE OR REPLACE TABLE dim_genres AS 
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY genre) AS genre_id,
    genre AS genre_name                             
FROM genres_staging;

-- fact_movies
CREATE OR REPLACE TABLE fact_movies AS
SELECT DISTINCT
    m.id AS movie_id,                 
    dg.genre_id,                      
    p.person_id AS director_id,                             
    r.total_votes,                    
    r.avg_rating,                     
    m.duration                        
FROM movies_staging m
LEFT JOIN ratings_staging r ON m.id = r.movie_id         
LEFT JOIN genres_staging g ON m.id = g.movie_id           
LEFT JOIN dim_genres dg ON g.genre = dg.genre_name        
LEFT JOIN dim_people p ON p.role = 'Director' AND m.id = p.known_for_movies;

-- delating staging table
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS name_staging;
