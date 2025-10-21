-- Query demonstrating various date functions
SELECT
    date, -- Original date column
    year(date) AS year_col, -- Extracts the year from the date
    month(date) AS month_col, -- Extracts the month from the date
    day(date) AS day_col, -- Extracts the day from the date
    date_add(date, 30) AS date_plus_30d, -- Adds 30 days to the original date
    trunc(date, 'MM') AS month_first_day, -- Truncates the date to the first day of the month
    last_day(date) AS last_in_month, -- Returns the last day of the month for the given date
    dayofweek(date) AS week_day -- Returns the day of the week for the given date (1 = Sunday, 2 = Monday, etc.)
FROM
    yt.`$price`
LIMIT 1
