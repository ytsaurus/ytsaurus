SELECT
    p.nomenclature_id
   ,Math::Round(avg(p.price), -2) as avg_price
   ,max(p.price) as max_price
   ,min(p.price) as min_price
FROM `$price` p
GROUP BY
    p.nomenclature_id
LIMIT 100
