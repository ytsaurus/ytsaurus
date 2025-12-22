-- Extracts country from YSON-formatted meta_data field and groups by it.
-- Demonstrates working with semi-structured data in YSON format.

    try_get_string(meta_data, "/origin") as country
   ,sum(1) as count
FROM [$nomenclature]
GROUP BY
    country
