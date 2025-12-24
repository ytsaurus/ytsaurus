-- Calculates average price per nomenclature_id.
-- Demonstrates basic GROUP BY usage with aggregation function.
-- Useful for summarizing numerical data across categories.

SELECT
    p.nomenclature_id
   ,AVG(p.price) as avg_price
FROM '$price' p
GROUP BY
    p.nomenclature_id;
