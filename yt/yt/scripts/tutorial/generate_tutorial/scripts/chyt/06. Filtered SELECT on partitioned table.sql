SELECT
    o.date
   ,o.order_uuid
   ,o.nomenclature_id
   ,o.quantity
FROM ytTables(ytListTables('$orders' , '$start_date' , '$end_date' )) o
WHERE o.nomenclature_id BETWEEN 1 AND 10
LIMIT 10;
