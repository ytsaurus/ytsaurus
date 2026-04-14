-- Sorts products by length of name (descending), then by is_rx (ascending).
-- Demonstrates multi-column sorting and use of LENGTH() function.

SELECT
    n.id
   ,n.name
   ,n.is_rx
   ,n.meta_data
FROM `$nomenclature` n
ORDER BY
    LENGTH(name) DESC,
    is_rx
LIMIT 100
