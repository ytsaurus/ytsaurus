SELECT
    p.nomenclature_id
   ,AVG(p.price) as avg_price
FROM '$price' p
GROUP BY
    p.nomenclature_id;
