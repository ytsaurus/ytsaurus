# Data type mapping: SPYT, Spark, and {{product-name}}

This article describes the mapping between [{{product-name}} data types](../../../../../user-guide/storage/data-types.md) and [Apache Spark data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) for read and write.

## Notes

1. Type hints: Some mappings can be overridden by [type hints](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md#schema_hint) in field metadata. This enables precise control of type conversion.

2. Arrow support: Some date/time related types do not support the Arrow format and use alternative serialization.

3. Decimal precision: {{product-name}} limits decimal precision to 35. [Learn more about Decimal](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md#schema_decimal)

4. String types: The configuration option stringToUtf8 controls whether strings map to string or utf8 in {{product-name}}. Example:
   ```python
   import spyt
   from pyspark.sql import Row
   from pyspark.sql.types import StructType, StructField, StringType, BinaryType

   spark.createDataFrame(
       spark.sparkContext.parallelize([Row("string", b"binary")]),
       StructType([
           StructField("string", StringType(), nullable=False),
           StructField("binary", BinaryType(), nullable=False),
       ])
   ).write \
   .option("string_to_utf8", "true") \
   .yt("//path/to/table")
   ```

5. Composite types: Complex types such as structs, arrays, and maps are mapped recursively using the same rules for their element types.

For special techniques related to data types, see [Read options](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md) and [Write options](../../../../../user-guide/data-processing/spyt/thesaurus/write-options.md).

## Type mapping when reading from {{product-name}}

| {{product-name}} type | Spark type (Basic) | Spark type (Extended) | Notes |
|-----------------------|--------------------|-----------------------|-------|
| null                  | NullType           | NullType              |       |
| int64                 | LongType           | LongType              |       |
| uint64                | DecimalType(20, 0) | UInt64Type            |       |
| float                 | FloatType/DoubleType | FloatType/DoubleType | Top-level: FloatType. Nested elements: DoubleType. |
| double                | DoubleType         | DoubleType            |       |
| boolean               | BooleanType        | BooleanType           |       |
| string                | StringType         | StringType            |       |
| binary                | BinaryType         | BinaryType            |       |
| any/yson              | BinaryType         | YsonType              |       |
| int8                  | ByteType           | ByteType              |       |
| uint8                 | ShortType          | ShortType             |       |
| int16                 | ShortType          | ShortType             |       |
| uint16                | IntegerType        | IntegerType           |       |
| int32                 | IntegerType        | IntegerType           |       |
| uint32                | LongType           | LongType              |       |
| utf8                  | StringType         | StringType            |       |
| date                  | DateType           | DateType              | Arrow is not supported |
| datetime              | DatetimeType       | DatetimeType          | Arrow is not supported |
| timestamp             | TimestampType      | TimestampType         | Arrow is not supported |
| interval              | LongType           | LongType              | Arrow is not supported |
| date32                | Date32Type         | Date32Type            | Arrow is not supported |
| datetime64            | Datetime64Type     | Datetime64Type        | Arrow is not supported |
| timestamp64           | Timestamp64Type    | Timestamp64Type       | Arrow is not supported |
| interval64            | Interval64Type     | Interval64Type        | Arrow is not supported |
| uuid                  | StringType         | StringType            | Arrow is not supported |
| json                  | StringType         | StringType            | Arrow is not supported |
| void                  | NullType           | NullType              |       |

### Composite types

| {{product-name}} type | Mapping to Spark type |
|-----------------------|-----------------------|
| Optional(inner)       | inner type with nullable = true |
| Dict(key, value)      | MapType(key.innerLevel, value.innerLevel, value.nullable) |
| Array(inner)          | ArrayType(inner.innerLevel, inner.nullable) |
| Struct(fields)        | StructType with corresponding field mappings |
| Tuple(elements)       | StructType with fields named `_1`, `_2`, etc. |
| VariantOverStruct     | StructType with fields prefixed with `_v` and nullable = true |
| VariantOverTuple      | StructType with fields named `_v_1`, `_v_2`, etc., and nullable = true |

## Type mapping when writing to {{product-name}}

| Spark type            | {{product-name}} type | Notes |
|-----------------------|-----------------------|-------|
| NullType              | null                  |       |
| ByteType              | int8                  |       |
| ShortType             | int16 (or uint8 when hinted)  |       |
| IntegerType           | int32 (or uint16 when hinted) |       |
| LongType              | int64 (or uint32 when hinted) |       |
| StringType            | string (or utf8/json/uuid when hinted, or utf8 when stringToUtf8 is enabled) | |
| FloatType             | float                 |       |
| DoubleType            | double                |       |
| BooleanType           | boolean               |       |
| DecimalType           | Decimal(precision, scale) | Precision is limited to 35 |
| ArrayType             | Array(inner)          |       |
| StructType (tuple)    | Tuple(elements)       |       |
| StructType (variant)  | VariantOverStruct/VariantOverTuple | |
| StructType            | Struct(fields)        |       |
| MapType               | Dict(key, value)      |       |
| BinaryType            | binary                |       |
| DateType              | date                  |       |
| DatetimeType          | datetime              |       |
| TimestampType         | timestamp             |       |
| Date32Type            | date32                |       |
| Datetime64Type        | datetime64            |       |
| Timestamp64Type       | timestamp64           |       |
| Interval64Type        | interval64            |       |
| UInt64Type            | uint64                |       |
| YsonType              | any                   |       |