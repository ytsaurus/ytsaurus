    try_get_string(meta_data, "/origin") as country
   ,sum(1) as count
FROM [$nomenclature]
GROUP BY
    country
WITH TOTALS
