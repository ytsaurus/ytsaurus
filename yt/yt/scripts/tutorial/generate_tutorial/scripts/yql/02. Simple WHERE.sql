SELECT
    p.date
   ,p.price
FROM `$price` p
WHERE p.price > 50
LIMIT 100
