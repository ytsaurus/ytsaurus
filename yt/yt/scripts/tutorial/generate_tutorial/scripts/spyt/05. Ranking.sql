-- Ranking query to assign a rank to each row within partitions of nomenclature_id based on price in descending order
SELECT
    date,
    nomenclature_id,
    price,
    RANK() OVER (
        PARTITION BY nomenclature_id
        ORDER BY price DESC
    ) as price_rank
FROM
    yt.`$price`
LIMIT 10
