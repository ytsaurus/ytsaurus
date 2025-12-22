-- Groups products by country of origin and shows totals.
-- WITH TOTALS adds a summary row showing overall counts.
-- Useful for quick overviews including subtotals.

    try_get_string(meta_data, "/origin") as country
   ,sum(1) as count
FROM [$nomenclature]
GROUP BY
    country
WITH TOTALS
