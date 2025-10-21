-- Query to select all records from the nomenclature dynamic table
-- where the value of the is_rx column is TRUE
SELECT
    *
FROM
    -- To read dynamic tables, it is necessary to specify the timestamp for the data snapshot - @timestamp_XXX
    yt.`$nomenclature/@timestamp_-1`
WHERE
    is_rx IS TRUE
