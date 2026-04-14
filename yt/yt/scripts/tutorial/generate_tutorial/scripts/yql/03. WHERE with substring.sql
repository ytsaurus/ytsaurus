-- Filters product names containing the word "statin".
-- Useful for keyword-based filtering in text fields.

SELECT
    n.id
   ,n.name
   ,n.is_rx
   ,n.meta_data
FROM `$nomenclature` n
WHERE n.name like "%statin%"
LIMIT 100
