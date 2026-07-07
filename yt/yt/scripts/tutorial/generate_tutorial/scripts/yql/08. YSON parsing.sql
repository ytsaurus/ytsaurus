-- This query demonstrates how to work with YSON data in YQL.
-- YSON is commonly used in YTsaurus to store semi-structured data like metadata.
-- The query does two things:
-- 1. Converts the raw YSON data in 'order_meta' to a human-readable pretty format for inspection.
-- 2. Parses the YSON data into a struct with specific types for further processing.
--    The schema defined in Yson::ConvertTo must match the actual structure of the YSON data.
-- This is useful when you need to:
-- - Debug or inspect YSON content
-- - Extract specific fields from complex nested metadata
-- - Transform semi-structured data into structured columns for analysis

SELECT
    Yson::SerializePretty(order_meta) as pretty_yson -- Human-readable YSON for debugging
   ,Yson::ConvertTo(order_meta, Struct<warehouse_rack: Int64, order_collector: Int64, quality: List<String>>) as parsed_yson -- Structured parsing of YSON data
FROM RANGE('$orders')
WHERE TableName() = '$end_date'
LIMIT 3;
