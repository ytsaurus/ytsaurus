-- Common Table Expression (CTE) to calculate the average price for each nomenclature_id
-- for prices recorded after a specific date
WITH recent_prices AS (
    SELECT *
    FROM yt.`$price`
    WHERE `date` >= '2025-05-01' -- Selecting records with dates greater than or equal to '2025-05-01'
)
SELECT
    nomenclature_id,
    AVG(price) AS avg_recent_price
FROM
    recent_prices -- Using the CTE defined above
GROUP BY
    nomenclature_id
