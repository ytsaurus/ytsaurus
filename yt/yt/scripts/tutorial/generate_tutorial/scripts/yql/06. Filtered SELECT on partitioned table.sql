-- This query filters data from a range of partitions using 'FROM RANGE'
-- and applies an additional filter on nomenclature_id.
-- Useful when you need to scan multiple date partitions but still apply row-level filtering.

SELECT
    o.date
   ,o.order_uuid
   ,o.nomenclature_id
   ,o.quantity
FROM RANGE('$orders', '$start_date', '$end_date') o
WHERE o.nomenclature_id BETWEEN 1 AND 10
LIMIT 10;
