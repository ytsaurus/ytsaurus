-- This query demonstrates how to work with YSON data in CHYT.
-- YSON is commonly used in YTsaurus to store semi-structured data like metadata.
-- The query does two things:
-- 1. Converts the raw YSON data in 'order_meta' to a human-readable pretty format for inspection.
-- 2. Parses the YSON data into a structured tuple with specific types for further processing.
--    The schema defined in YPathExtract must match the actual structure of the YSON data.
-- This is useful when you need to:
-- - Debug or inspect YSON content
-- - Extract specific fields from complex nested metadata
-- - Transform semi-structured data into structured columns for analysis

SELECT
    ConvertYson(o.order_meta, 'pretty') as pretty_yson -- Human-readable YSON for debugging
   ,YPathExtract(o.order_meta, '', 'Tuple(warehouse_rack Int64, order_collector Int64, quality Array(String))') as parsed_yson -- Structured parsing of YSON data
FROM ytTables(ytListTables('$orders')) o
WHERE $table_name = '$end_date'
LIMIT 1;
