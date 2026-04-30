-- Window functions query to calculate the average price over a 3-day window
-- and the cumulative sum of prices for each nomenclature_id over time
SELECT
    date,
    nomenclature_id,
    price,
    AVG(price) OVER (
        PARTITION BY nomenclature_id
        ORDER BY date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING -- Defining the window as the current row and one row before and after
    ) AS avg_price_3day_window, -- Calculating the average price over the defined window
    SUM(price) OVER (
        PARTITION BY nomenclature_id
        ORDER BY date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_sum_price
FROM
    `$price`
ORDER BY
    nomenclature_id, date
LIMIT 10
