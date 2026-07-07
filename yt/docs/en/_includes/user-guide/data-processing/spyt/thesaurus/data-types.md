# Data type mapping: SPYT, Spark, and {{product-name}}

This article describes the mapping between [{{product-name}} data types](../../../../../user-guide/storage/data-types.md) and [Apache Spark data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) for read and write.

## Features { #features }

1. **Type hints**: Some mappings can be overridden by [type hints](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md#schema_hint) in field metadata. This enables precise control of type conversion.

2. **Decimal precision**: {{product-name}} limits the `precision` of the `decimal` type to 35 (not `scale`). [Learn more about Decimal](../../../../../user-guide/storage/data-types.md#schema_decimal)

3. **String types**: The configuration option `stringToUtf8` controls whether strings map to `string` or `utf8` in {{product-name}}. Example:
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

4. **Composite types**: Complex types such as structs, arrays, and dictionaries are mapped recursively using the same rules for their element types.

5. **Float type when reading** is mapped differently depending on its position in the schema:
   - **Top‑level column** → `FloatType`;
   - **Element inside a composite type** (`Array`, `Dict`, `Struct`, `Tuple`, `Variant`) → `DoubleType`.

   This is due to a feature of the YSON format: YSON does not have a 4‑byte float node, so `float` inside composite types is stored as an 8‑byte double. To avoid data loss when reading, SPYT converts it to `DoubleType`. Top‑level columns are read via the wire protocol, where the `float` type is preserved as `FloatType`.

6. **Arrow is not supported**: For types marked as such, when the [`spark.yt.read.arrow.enabled`](../../../../../user-guide/data-processing/spyt/cluster/configuration.md) option is enabled (default `true`), SPYT automatically switches to the wire protocol.

   {% note warning %}

   If at least one column in the schema contains a type without Arrow support, the entire table is read via the wire protocol, including other columns. Consider this when designing the schema if read performance is important.

   {% endnote %}

For special techniques related to data types, see [Read options](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md) and [Write options](../../../../../user-guide/data-processing/spyt/thesaurus/write-options.md).

## Type mapping when reading from {{product-name}} { #read-types }

| {{product-name}} type | Spark type (Basic) | Spark type (Extended) | Notes |
|-----------------------|--------------------|-----------------------|-------|
| null                  | NullType           | NullType              |       |
| int64                 | LongType           | LongType              |       |
| uint64                | DecimalType(20, 0) | UInt64Type            |       |
| float                 | FloatType/DoubleType | FloatType/DoubleType | Top‑level: `FloatType`; inside a composite type: `DoubleType` (see Features, item 5). Arrow is not supported |
| double                | DoubleType         | DoubleType            |       |
| boolean               | BooleanType        | BooleanType           |       |
| string                | StringType         | StringType            |       |
| binary                | BinaryType         | BinaryType            |       |
| any/yson              | BinaryType         | YsonType              | `YsonType` is a custom SPYT type (see [section below](#yson-type)) |
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
| date32                | Date32Type         | Date32Type            | Custom SPYT wide‑range type (see [section below](#wide-datetime-types)). Arrow is not supported |
| datetime64            | Datetime64Type     | Datetime64Type        | Custom SPYT wide‑range type (see [section below](#wide-datetime-types)). Arrow is not supported |
| timestamp64           | Timestamp64Type    | Timestamp64Type       | Custom SPYT wide‑range type (see [section below](#wide-datetime-types)). Arrow is not supported |
| interval64            | Interval64Type     | Interval64Type        | Custom SPYT wide‑range type (see [section below](#wide-datetime-types)). Arrow is not supported |
| uuid                  | StringType         | StringType            | Arrow is not supported |
| json                  | StringType         | StringType            | Arrow is not supported |
| void                  | NullType           | NullType              |       |

### Composite types { #composite-types }

In the notations below, `inner`, `key`, `value` are nested element types. Each of them follows the “inner level” rules (for example, `float` → `DoubleType`).

| {{product-name}} type | Mapping to Spark type |
|-----------------------|---------------------|
| Optional(inner)       | inner type with nullable = true |
| Dict(key, value)     | `MapType(key_type, value_type, value.nullable)`, where `key_type` and `value_type` are Spark types of the key and value, determined by inner‑level rules |
| Array(inner)          | `ArrayType(inner_type, inner.nullable)`, where `inner_type` is the Spark type of the element according to inner‑level rules |
| Struct(fields)        | StructType with corresponding field mappings |
| Tuple(elements)       | StructType with fields named `_1`, `_2`, etc. |
| VariantOverStruct     | StructType with fields prefixed with `_v` and nullable = true |
| VariantOverTuple      | StructType with fields named `_v_1`, `_v_2`, etc., and nullable = true |

## Type mapping when writing to {{product-name}} { #write-types }

| Spark type            | {{product-name}} type | Notes |
|---------------------|----------------------|-------|
| NullType              | null                  |       |
| ByteType              | int8                  |       |
| ShortType             | int16 (or uint8 when using schema_hint) | See [example below](#schema-hint) |
| IntegerType           | int32 (or uint16 when using schema_hint) |       |
| LongType              | int64 (or uint32 when using schema_hint) |       |
| StringType            | string (or utf8/json/uuid when using schema_hint, or utf8 when stringToUtf8 is enabled) |       |
| FloatType             | float                 |       |
| DoubleType            | double                |       |
| BooleanType           | boolean               |       |
| DecimalType           | Decimal(precision, scale) | `precision` is limited to 35 |
| ArrayType             | Array(inner)          |       |
| StructType (tuple)    | Tuple(elements)       |       |
| StructType (variant)  | VariantOverStruct/VariantOverTuple |       |
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
| YsonType              | any                   | `YsonType` is a custom SPYT type (see [section below](#yson-type)) |

### Redefining type via schema_hint { #schema-hint }

If the default mapping is not suitable, you can explicitly specify the target {{product-name}} type via [schema_hint](../../../../../user-guide/data-processing/spyt/thesaurus/read-options.md#schema_hint).

{% list tabs %}

- Python

  ```python
  df.write.schema_hint({"my_col": "uint8"}).yt("//path/to/table")
  ```

- Scala

  ```scala
  df.write.schemaHint(Map("my_col" -> YtLogicalType.Uint8)).yt("//path/to/table")
  ```

{% endlist %}

## Custom SPYT types { #custom-types }

The types listed below are not part of standard Apache Spark — they are provided by the SPYT library.

### Wide‑range temporal types { #wide-datetime-types }

{{product-name}} has two sets of temporal types:

- **Standard** (`date`, `datetime`, `timestamp`, `interval`) — unsigned integers; range `[1970-01-01, 2105-12-31]`.
- **Wide** (`date32`, `datetime64`, `timestamp64`, `interval64`) — signed integers (int32/int64); range of about 145 000 years into the past and future from the Unix epoch.

Wide types were introduced after standard ones and are preferred for data outside the 1970–2105 range. In Spark, they correspond to custom SPYT types: `Date32Type`, `Datetime64Type`, `Timestamp64Type`, `Interval64Type`.


Learn more about temporal types in {{product-name}} in the [Temporal types](../../../../../user-guide/storage/data-types.md#temporal_types) section.


### YsonType { #yson-type }

`YsonType` is a custom UDT (User Defined Type) storing data as YSON bytes.

- When reading from {{product-name}}: type `any`/`yson` → `YsonType` (extended mode) or `BinaryType` (basic mode).
- When writing to {{product-name}}: `YsonType` → type `any`.


Learn more about working with `YsonType` in the [Python API](../../../../../user-guide/data-processing/spyt/API/spyt-python.md) and [Scala API](../../../../../user-guide/data-processing/spyt/API/spyt-scala.md) sections.
