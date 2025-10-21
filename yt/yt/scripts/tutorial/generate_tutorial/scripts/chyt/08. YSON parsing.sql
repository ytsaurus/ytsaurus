SELECT
    ConvertYson(o.order_meta, 'pretty') as pretty_yson
   ,YPathExtract(o.order_meta, '', 'Tuple(warehouse_rack Int64, order_collector Int64, quality Array(String))') as parsed_yson
FROM ytTables(ytListTables('$orders')) o
WHERE $table_name = '$end_date'
LIMIT 1;
