-- Filters price records where price exceeds 50.
-- Demonstrates numeric filtering and LIMIT for previewing results.

SELECT
    p.date
   ,p.price
FROM `$price` p
WHERE p.price > 50
LIMIT 100
