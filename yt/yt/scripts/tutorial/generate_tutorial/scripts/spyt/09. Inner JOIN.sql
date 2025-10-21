SELECT
    p.date,
    p.nomenclature_id,
    p.price,
    n.name
FROM
    yt.`$price` p
JOIN
    yt.`$nomenclature/@timestamp_-1` n
    ON p.nomenclature_id = n.id -- Joining by nomenclature identifier
ORDER BY
    p.date DESC
LIMIT 10
