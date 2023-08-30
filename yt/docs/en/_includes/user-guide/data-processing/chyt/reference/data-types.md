# Data types

ClickHouse has a wide variety of [data types](https://clickhouse.com/docs/ru/sql-reference/data-types) available. {{product-name}} uses its own data type system that significantly overlaps with the ClickHouse type system, but individual {{product-name}} data types may not exist in CH and vice versa. For a complete list of the types supported in the {{product-name}} system, see [{{product-name}} data types](../../../../../user-guide/storage/data-types.md).

## Primitive data types

The table below shows the correspondence between the __primitive__ (not composite) ClickHouse and {{product-name}} types.

| {{product-name}} type | ClickHouse type | Comment |
| -------------- | --------------- | ------------------------------------------------------------ |
| `intN, uintN` | `intN, uintN` | Integer numeric columns from the ClickHouse point look like (U)Int of the appropriate digit capacity. |
| `boolean` | `YtBoolean` | There are no Booleans in regular ClickHouse, they are represented as single-byte unsigned numbers (`UInt8`) with values 0 and 1. CHYT supports the additional `YtBoolean` type that is similar to the `UInt8` type in ClickHouse computations, but is interpreted as a `boolean` when written to a {{product-name}} table. |
| `string, utf8` | `String` | Strings are available as string |
| `double` | `Float64` |                                                              |
| `date` | `Date` |
| `datetime` | `DateTime` |
| `any` | `String` | Any columns are read by the ClickHouse engine as string columns containing data in YSON. See also: [`chyt.composite.default_yson_format`](../../../../../user-guide/data-processing/chyt/reference/settings.md). |
| `T` with `required = %false` | `Nullable(T)` | If a column of a `T` type is not `required`, it is seen as a Nullable column. |

## Composite data types

Useful links:
- [{{product-name}} type system](../../../../../user-guide/storage/objects.md).

In {{product-name}}, the following composite data types are available: lists, dicts, optional types, structures, and tuples. By default, composite values are seen by the CH engine in the same way as the `Any` type, i.e. as a YSON string.

If you configure the `chyt.composite.enable_conversion` setting to 1, CHYT starts converting composite types (specified in the schema, that is, having the `type_v3` type) into the corresponding ClickHouse composite types.

Query example:

```bash
# A query with no composite type conversion enabled. Composite types
# are seen as strings in binary YSON.
yt --proxy <cluster_name> clickhouse execute 'select * from `//home/user/sample_table_composite`' --format Vertical
Row 1:
──────
list:   [foo;bar;]
dict:   [(binary unreadable characters)]
struct: [(binary unreadable characters)]

Row 2:
──────
list:   []
dict:   []
struct: [(binary unreadable characters)]

# A query with enabled conversion.
yt --proxy <cluster_name> clickhouse execute 'select * from `//home/user/sample_table_composite`' --format Vertical --setting chyt.composite.enable_conversion=1 --format Vertical
Row 1:
──────
list:   ['foo','bar']
dict:   [('key1',42),('key2',-17)]
struct: (3.14,'pi')

Row 2:
──────
list:   []
dict:   []
struct: (0,'')

```

Unfortunately, the ClickHouse and {{product-name}} type systems are quite different as far as composite data types are concerned. Below are the main differences.

### Optional types { #optional }

In the Yandex type system, any type `T` can be made optional by surrounding it with `optional<T>`, for example, `optional<optional<int64>>`, `optional<list<string>>`, and `optional<tuple<int64, string>>` data types are possible.

In ClickHouse, placing values into `Nullable` is very limited. In particular, you cannot place into `Nullable`:
- The type that is already `Nullable`.
- `Array`.
- `Tuple`.

Thus, none of the three examples of types above have their own representation in the ClickHouse type system.

To process such types in CHYT, the following conversion is used.

If type `T` corresponds to type `T'` of the ClickHouse type system and type `Nullable(T')` does not exist in CH, `optional<T>` is mapped to `T'` replacing null with the default value for `T'`. Examples:

- `optional<optional<int64>>` is mapped to `Nullable(Int64)`, with two types of different initial `null` merged into one.

- `optional<list<string>>` is mapped to `Array(String)`, with initial `null` turning into empty array `[]`.

- `optional<tuple<int64, string>>` is mapped to `Tuple(Int64, String)`, with initial `null` turning into a tuple of two default values for Int64 and String, i.e. tuple `(0, '')`.

### Dicts { #dict }

In the Yandex type system, for any two types `K` and `V` you can make type `dict<K, V>`. ClickHouse has nothing like dicts, so `dict<K, V>` is mapped to `Array(Tuple(K', V'))`.

See a query example above: the first value in the `dict` column contains the `{'key1' -> 42, 'key2' -> -17}` dict.

### Tuples { #tuple }

Yandex type system tuples are mapped to ClickHouse tuples in a trivial way.

### Structures { #structures }

Structures from the Yandex type system could be mapped to named CH tuples (`Named Tuple`) CH. Unfortunately, ClickHouse has problems with supporting named tuples. They are now supported only on the external level of the complex type description (see the relevant [issue](https://github.com/ClickHouse/ClickHouse/issues/15587)). As a result, you cannot place `Named Tuple` inside, for example, a list. To simplify the current implementation, the decision was made to map the structures to regular tuples.

Thus, you can work with structures as with tuples, i.e. you can get elements from them by index through `.1`, `.2`, and so on or using the `tupleElement` function:

```bash
 yt --proxy <cluster_name> clickhouse execute 'select toTypeName(struct), struct.1, struct.2  from `//home/user/sample_table_composite`' --format Vertical --setting chyt.composite.enable_conversion=1
Row 1:
──────
toTypeName(struct):      Tuple(Float64, String)
tupleElement(struct, 1): 3.14
tupleElement(struct, 2): pi

Row 2:
──────
toTypeName(struct):      Tuple(Float64, String)
tupleElement(struct, 1): 0
tupleElement(struct, 2):

```

### Tagged types { #tagged }

`Tagged` types from the general Yandex type system are mapped to the corresponding types without a tag. The tag information is lost.

### Variants, Named variants, Any { #var-any }

Variants and named variants are not supported. Their display logic is the same as if there is a value of the `Any` type somewhere inside a composite type. Any value of the `Variant`, `Named Variant`, or `Any` type simply turns into a YSON string with a value representation.

## Tips for working with insufficiently schematized data { #schema }

Below are a few methods to get around some data type problems.

Many tables contain dates and times in text or numeric representation (for example, in ISO or Unix ts format). To convert them to a **time** data type, you can use the standard [ClickHouse functions for working with date and time](https://clickhouse.com/docs/ru/sql-reference/functions/date-time-functions), [reinterpret](https://clickhouse.com/docs/ru/sql-reference/functions/type-conversion-functions/#reinterpretasdate), and [CAST](https://clickhouse.com/docs/ru/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast).


{{product-name}} has historically used the `Any` data type for working with composite data (arrays, dicts, structures). Despite the appearance of composite type support in the {{product-name}} type system, a lot of old data still remains represented in `Any`: it will be available as a binary YSON to the ClickHouse engine.

However, `Any` representation as a binary YSON means almost nothing for the ClickHouse engine, since YSON support is not built into ClickHouse. ClickHouse interprets such data simply as a binary string with a custom format. Therefore, the ClickHouse and {{product-name}} compatibility layer provides a number of auxiliary functions that enable you to address over a YSON structure using the [YPath language](../../../../../user-guide/storage/yson-docs.md). And also interpret its nodes as primitive {{product-name}} types (`Int64`, `UInt64`, `Double`, or `Boolean`), strings (`String`), or lists of primitive {{product-name}} types (`Array(Int64)`, `Array(UInt64)`, `Array(Double)`, or `Array(Boolean)`) with the conversion into the corresponding built-in ClickHouse type. For more information about these functions, see [Functions for working with YSON](../../../../../user-guide/data-processing/chyt/reference/functions.md#yson_functions).

{% note warning "Attention!" %}

Conversion from {{product-name}} representation to ClickHouse representation will inevitably result in longer query execution time.

{% endnote %}

