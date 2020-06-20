SELECT a.name AS "Actors that worked with Lerdam"
FROM movie_actors AS m_a 
INNER JOIN movies AS m ON m_a.movie_id = m.id 
INNER JOIN actors AS a ON a.id = m_a.actor_id 
WHERE m.director LIKE '%Lerdam%';


SELECT w.name AS "Most experienced writer", COUNT(w.name) AS frequency
FROM movies AS m
INNER JOIN writers AS w ON m.writer = w.id
WHERE w.name != 'N/A'
GROUP BY w.name
ORDER BY frequency DESC
LIMIT 1;


SELECT a.name AS "Most experienced actor", COUNT(actor_id) AS frequency
FROM movie_actors AS m_a
INNER JOIN actors AS a ON a.id = m_a.actor_id
WHERE a.name != 'N/A'
GROUP BY a.name
ORDER BY frequency DESC
LIMIT 1;
