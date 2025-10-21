SELECT
    o.date
   ,o.order_uuid
   ,o.nomenclature_id
   ,o.quantity
FROM ytTables(ytListTables('$orders')) o
WHERE $table_name = '$end_date'
LIMIT 10;
