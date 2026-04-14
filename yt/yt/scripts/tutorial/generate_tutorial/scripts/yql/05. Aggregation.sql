-- Computes average, maximum, and minimum prices per nomenclature_id.
-- Demonstrates multiple aggregations in one query with rounding for readability.

SELECT
    p.nomenclature_id
   ,Math::Round(avg(p.price), -2) as avg_price
   ,max(p.price) as max_price
   ,min(p.price) as min_price
FROM `$price` p
GROUP BY
    p.nomenclature_id
LIMIT 100
