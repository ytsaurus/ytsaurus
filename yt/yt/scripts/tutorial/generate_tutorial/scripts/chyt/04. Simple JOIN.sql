-- Performs a JOIN between price and nomenclature tables to get the latest price
-- for each product name using argMax (which selects the price corresponding to the max date).
-- Useful for enriching time-series data with metadata.

SELECT
    argMax(p.price, p.date) as last_price
   ,n.name
FROM '$price' p
JOIN (
        SELECT
            nm.id
           ,nm.name
        FROM '$nomenclature' nm
    ) n on n.id = p.nomenclature_id
GROUP BY
    n.name
