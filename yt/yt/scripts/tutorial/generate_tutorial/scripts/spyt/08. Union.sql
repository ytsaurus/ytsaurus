-- Query to retrieve statistics about prices and corresponding products
SELECT
   'prices stat' AS description,
   COUNT(DISTINCT nomenclature_id) AS count_unique,
   MIN(nomenclature_id) AS min_id,
   MAX(nomenclature_id) AS max_id
FROM
    yt.`$price`
UNION ALL -- Combines the results of the two queries without removing duplicates
SELECT
   'nomenclatures stat' AS description,
   COUNT(DISTINCT id) AS count_unique,
   MIN(id) AS min_id,
   MAX(id) AS max_id
FROM
    yt.`$nomenclature/@timestamp_-1` n
