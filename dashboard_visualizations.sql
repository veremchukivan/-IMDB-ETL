-- Graf  1: Porovnanie počtu filmov vyrobených v USA a Indii v roku 2019
SELECT 
    m.country AS country,
    COUNT(m.id) AS number_of_movies
FROM 
    movie AS m
WHERE 
    m.country IN ('USA', 'India') 
    AND m.year = 2019
GROUP BY 
    m.country
ORDER BY 
    number_of_movies DESC;

-- Graf  2: Priemerná dĺžka filmov pre každý žáner
SELECT g.genre AS genre,
       ROUND(AVG(m.duration), 2) AS avg_duration
FROM genre AS g
INNER JOIN movie AS m ON g.movie_id = m.id
GROUP BY g.genre
ORDER BY avg_duration DESC;


-- Graf 3: 10 najlepších režisérov podľa počtu nakrútených filmov

SELECT 
    n.name AS director_name,
    COUNT(dm.movie_id) AS movie_count
FROM director_mapping dm
JOIN names n 
    ON dm.name_id = n.id
GROUP BY n.name
ORDER BY movie_count DESC
LIMIT 10;


-- Graf 4: 3 najlepší režiséri v 3 najlepších žánroch s priemerným hodnotením > 8

WITH top3_genre AS (
    SELECT g.genre,
           COUNT(g.movie_id) AS movie_count
    FROM genre AS g
    INNER JOIN ratings AS r ON g.movie_id = r.movie_id
    WHERE r.avg_rating > 8
    GROUP BY g.genre
    ORDER BY movie_count DESC
    LIMIT 3
),
top3_director AS (
    SELECT n.name AS director_name,
           COUNT(g.movie_id) AS movie_count,
           ROW_NUMBER() OVER (ORDER BY COUNT(g.movie_id) DESC) AS director_rank
    FROM names AS n
    INNER JOIN director_mapping AS dm ON n.id = dm.name_id
    INNER JOIN genre AS g ON dm.movie_id = g.movie_id
    INNER JOIN ratings AS r ON r.movie_id = g.movie_id
    WHERE g.genre IN (SELECT genre FROM top3_genre)
      AND r.avg_rating > 8
    GROUP BY n.name
)
SELECT director_name, movie_count
FROM top3_director
WHERE director_rank <= 3;

-- Graf 5: Najobľúbenejší herci podľa počtu odohraných úloh
SELECT
    n.name AS actor_name,
    COUNT(*) AS total_roles
FROM role_mapping AS rm
JOIN names AS n ON rm.name_id = n.id
WHERE rm.category IN ('actor', 'actress')
GROUP BY n.name
ORDER BY total_roles DESC
LIMIT 10;


--Graf 6: Počet filmov podľa krajín
SELECT m.country,
       COUNT(m.id) AS movie_count
FROM movie AS m
GROUP BY m.country
ORDER BY movie_count DESC
LIMIT 10;

