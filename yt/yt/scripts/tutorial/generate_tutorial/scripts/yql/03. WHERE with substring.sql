SELECT
    n.id
   ,n.name
   ,n.is_rx
   ,n.meta_data
FROM `$nomenclature` n
WHERE n.name like "%statin%"
LIMIT 100
