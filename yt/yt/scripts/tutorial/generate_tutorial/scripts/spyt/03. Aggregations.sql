-- Aggregation query to calculate average, maximum, and minimum prices by date
SELECT
    date,
    AVG(price) AS avg_price,
    MAX(price) AS max_price,
    MIN(min_price) AS min_min_price
FROM
    yt.`$price` -- Specifying the table to query from
GROUP BY
    date
ORDER BY
    date DESC
