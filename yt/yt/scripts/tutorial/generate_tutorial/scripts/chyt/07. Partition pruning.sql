-- This query demonstrates how to efficiently read from a partitioned table
-- by filtering on partition key ($table_name), which allows for partition pruning.
-- Partition pruning means only relevant partitions are scanned, improving performance.

SELECT
    o.date
   ,o.order_uuid
   ,o.nomenclature_id
   ,o.quantity
FROM ytTables(ytListTables('$orders')) o
WHERE $table_name = '$end_date'
LIMIT 10;
