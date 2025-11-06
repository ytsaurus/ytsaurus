-- Query to find price records in the 'price' table from the last 3 months
-- that do not have a corresponding item in the 'nomenclature' reference
SELECT
    p.date,
    p.nomenclature_id,
    p.price,
    n.name
FROM
    yt.`$price` p -- Specifying the price table
LEFT JOIN
    yt.`$nomenclature/@timestamp_-1` n -- Performing a left join with the nomenclature table
    ON p.nomenclature_id = n.id -- Matching nomenclature IDs between the two tables
WHERE
    p.date >= date_sub(current_date(), 90) -- Filtering records from the last 3 months
    and n.name IS NULL
