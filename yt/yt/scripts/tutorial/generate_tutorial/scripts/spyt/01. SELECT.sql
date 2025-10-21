-- Selects distinct dates within the range from '2025-05-01' to '2025-05-31' (exclusive)
SELECT
    distinct date -- Selects unique dates
FROM
    yt.`$price` -- Specifies the table to query from
WHERE
    date >= '$start_date' -- Sets the lower bound for the date range
    AND date < '$end_date' -- Sets the upper bound for the date range (exclusive)
ORDER BY
    date -- Orders the results by date in ascending order
