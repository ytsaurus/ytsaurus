-- Window functions query to calculate the average price over a 3-day window
-- See more about window functions: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html
SELECT
    date,
    nomenclature_id,
    price,
    AVG(price) OVER (
        PARTITION BY nomenclature_id
        ORDER BY date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING -- Defining the window as the current row and one row before and after
    ) AS avg_price_3day_window -- Calculating the average price over the defined window
FROM
    yt.`$price`
ORDER BY
    nomenclature_id, date
LIMIT 10
