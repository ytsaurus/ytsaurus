SELECT
    n.id
   ,n.name
   ,n.is_rx
   ,n.meta_data
FROM `$nomenclature` n
ORDER BY
    LENGTH(name) DESC,
    is_rx
LIMIT 100
