-- Vytvorenie databázy
CREATE DATABASE imdb_etl;

-- Vytvorenie schémy pre staging-table
CREATE SCHEMA imdb_etl.staging;

USE imdb_etl.staging;

-- Vytvorenie staging-table
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
    date_of_birdth DATE,
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



CREATE OR REPLACE STAGE imdbSt;


-- Načítanie údajov do tabuliek na analýzu
COPY INTO movies_staging
FROM @imdbSt/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO genres_staging
FROM @imdbSt/ganre.csv
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
CREATE TABLE dim_movies AS
SELECT DISTINCT
    id AS movie_id,
    title,
    year,
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
    n.known_for_movies
FROM name_staging n
LEFT JOIN role_mapping_staging r ON n.id = r.name_id;


-- dim_dates
CREATE OR REPLACE TABLE dim_dates AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY date_published) AS date_id,
    date_published AS full_date,
    YEAR(date_published) AS year,
    MONTH(date_published) AS month,
    DAY(date_published) AS day
FROM movies_staging;


-- dim_genres
CREATE OR REPLACE TABLE dim_genres AS 
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY genre) AS genre_id,  -- jedinečné ID pre každý žáner
    genre AS genre_name                             
FROM genres_staging;

-- Faktografická tabuľka so žánrami

CREATE OR REPLACE TABLE fact_movies AS
SELECT DISTINCT
    m.id AS movie_id,                 
    dg.genre_id,                      
    p.person_id AS director_id,       
    d.date_id,                        
    r.total_votes,                    
    r.avg_rating,                     
    m.duration                        
FROM movies_staging m
LEFT JOIN ratings_staging r ON m.id = r.movie_id         
LEFT JOIN genres_staging g ON m.id = g.movie_id           
LEFT JOIN dim_genres dg ON g.genre = dg.genre_name        
LEFT JOIN dim_people p ON p.role = 'Director' AND m.id = p.known_for_movies 
LEFT JOIN dim_dates d ON m.date_published = d.full_date; 


-- Odstránenie stagingových tabuliek
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS people_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS dates_staging;
