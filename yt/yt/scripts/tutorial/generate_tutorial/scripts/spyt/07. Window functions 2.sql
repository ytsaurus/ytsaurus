-- Window functions query to calculate the cumulative sum of prices for each nomenclature_id over time
SELECT
    date,
    nomenclature_id,
    price,
    SUM(price) OVER (
        PARTITION BY nomenclature_id
        ORDER BY date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_sum_price
FROM
    yt.`$price`
ORDER BY
    nomenclature_id, date
LIMIT 10
