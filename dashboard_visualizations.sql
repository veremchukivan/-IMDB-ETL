-- Graf  1: Porovnanie počtu filmov vyrobených v USA a Indii v roku 2019
SELECT 
    dm.country AS country,
    COUNT(dm.movie_id) AS number_of_movies
FROM dim_movies AS dm
WHERE dm.country IN ('USA', 'India') 
  AND dm.year = 2019
GROUP BY dm.country
ORDER BY number_of_movies DESC;


-- Graf  2: Priemerná dĺžka filmov pre každý žáner
SELECT 
    dg.genre_name AS genre,
    ROUND(AVG(fm.duration), 2) AS avg_duration
FROM 
    dim_genres AS dg
INNER JOIN fact_movies AS fm ON dg.genre_id = fm.genre_id
GROUP BY 
    dg.genre_name
ORDER BY 
    avg_duration DESC;
-- Graf 3: 10 najlepších režisérov podľa počtu nakrútených filmov
SELECT 
    dp.name AS director_name,
    COUNT(fm.movie_id) AS movie_count
FROM 
    fact_movies AS fm
JOIN dim_people AS dp ON fm.director_id = dp.person_id
GROUP BY 
    dp.name
ORDER BY 
    movie_count DESC
LIMIT 10;
-- Graf 4: 3 najlepší režiséri v 3 najlepších žánroch s priemerným hodnotením > 8
WITH top3_genre AS (
    SELECT 
        dg.genre_name AS genre,
        COUNT(fm.movie_id) AS movie_count
    FROM 
        fact_movies AS fm
    INNER JOIN dim_genres AS dg ON fm.genre_id = dg.genre_id
    INNER JOIN ratings_staging AS r ON fm.movie_id = r.movie_id
    WHERE 
        r.avg_rating > 6
    GROUP BY 
        dg.genre_name
    ORDER BY 
        movie_count DESC
    LIMIT 3
),
top3_director AS (
    SELECT 
        dp.name AS director_name,
        COUNT(fm.movie_id) AS movie_count,
        ROW_NUMBER() OVER (PARTITION BY dg.genre_name ORDER BY COUNT(fm.movie_id) DESC) AS director_rank
    FROM 
        fact_movies AS fm
    INNER JOIN dim_people AS dp ON fm.director_id = dp.person_id
    INNER JOIN dim_genres AS dg ON fm.genre_id = dg.genre_id
    INNER JOIN ratings_staging AS r ON fm.movie_id = r.movie_id
    WHERE 
        dg.genre_name IN (SELECT genre FROM top3_genre)
        AND r.avg_rating > 6
    GROUP BY 
        dp.name, dg.genre_name
)
SELECT director_name, movie_count
FROM top3_director
WHERE director_rank <= 3;
-- Graf 5: Najobľúbenejší herci podľa počtu odohraných úloh
SELECT
    dp.name AS actor_name,
    COUNT(*) AS total_roles
FROM 
    role_mapping_staging AS rm
JOIN dim_people AS dp ON rm.name_id = dp.person_id
WHERE 
    rm.category IN ('actor', 'actress')
GROUP BY 
    dp.name
ORDER BY 
    total_roles DESC
LIMIT 10;
--Graf 6: Počet filmov podľa krajín
SELECT 
    dm.country,
    COUNT(dm.movie_id) AS movie_count
FROM 
    dim_movies AS dm
GROUP BY 
    dm.country
ORDER BY 
    movie_count DESC
LIMIT 10;
