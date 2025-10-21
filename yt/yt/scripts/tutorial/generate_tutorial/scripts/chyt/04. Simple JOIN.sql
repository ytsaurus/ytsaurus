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
