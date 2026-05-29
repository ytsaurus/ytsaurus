-- The example demonstrates some structure transformation functions
-- and how they can be used to transform some table columns to suit your needs
-- while leaving most of the columns unchanged.

SELECT * FROM (
    SELECT
        CombineMembers( -- сombining fields of several structures (3 in this case) into a new structure
            RemoveMembers(TableRow(), ["order_uuid", "order_meta"]),  -- removing fields which we want to replace
            <| order: Unwrap(Cast(order_uuid as Uuid)) |>,  -- literal structure with a single field
                                                            -- in which we transform the column name and type
                                                            -- Unwrap function ensures that the type conversion was successful
            Unwrap(Yson::ConvertTo( -- parsing the YSON column 'order_meta ' into a struct with specific fiels
                order_meta,         -- which will be added to the output structure independently
                Struct<order_collector: Int64, quality: List<Text>, warehouse_rack: Int64>
            ))
        )
    FROM RANGE('$orders')
) FLATTEN COLUMNS -- Transforms each field of the input struct into separate columns
WHERE "Good" NOT IN quality  -- Filtering is performed by checking whether a value is not in a list of values
LIMIT 7;
