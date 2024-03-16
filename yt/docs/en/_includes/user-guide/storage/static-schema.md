# Data schema

## General description { #schema_overview }

The table schema describes the known columns and their types.

The schema can be read from the `@schema` system attribute that each table has.
To set the schema, set this attribute when creating a table.
To change the schema of an existing table, use the `alter_table` command.
For more information, see [Examples of working with a schema](#examples).

The `TableSchema` type is a list of `ColumnSchema` elements.
Each such element is a dict with a set structure:

| **Name** | **Type** | **Description** | Mandatory |
| ------------ | --------- | ------------------------------------------------------------ | ------------------ |
| `name` | `string` | Column name. | Yes |
| `type` | `string` | Type of column elements. | Yes |
| `type_v3` | `any` | Alternative type representation. This field enables you to specify columns with [composite types](../../../user-guide/storage/data-types.md#schema). | Yes |
| `sort_order` | `string` | The column sort order - either a missing value or the `ascending` value is allowed. If the value is set, the column is key. | No |
| `lock` | `string` | Lock name. For non-key columns only. | Yes |
| `expression` | `string` | An expression defining the value of the computed column field. For key columns only. For more information, see [Sharding](../../../user-guide/dynamic-tables/resharding.md). | Yes |
| `aggregate` | `string` | Aggregate function name. For non-key columns only. | Yes |
| `required` | `boolean` | If set to `%true`, the value in the column must not be `null`. The default value is `required=%false`. A column of the `any` type cannot be `required=%true`. | No |
| `group` | `string` | If two columns have the same group name, the data from those columns will get into the same blocks. You can change the `group` attribute at any time using the `alter-table` command, the changes will be applied to the new chunks. | No |

Schema properties:

- The column name must be a non-empty string. The maximum length is 256 characters.
- The column name cannot begin with system prefix `@`.
- Column names cannot be repeated within a table.
- Key columns must form the schema prefix.
- All key columns must be specified in the schema.
- Up to 32,000 columns are supported, but we recommend limiting the number of columns to four figures.
- A column must always have an explicitly specified type: in the `type` or `type_v3` attribute.
- When reading a schema, both attributes are always returned: `type` and `type_v3` regardless of how the schema was created.
- Computable columns can only be key. Computable columns can depend only on key non-computable columns.

Schema attributes:

- `strict` of the `boolean` type. The default value is `true`. If the table is strict, that is, the attribute has the `true` value, you cannot write data to the table column that is not in the schema. Otherwise, you are allowed to write to a missing column which will automatically appear, but only in a new row.
- `unique_keys` of the `boolean` type. The default value is `false`. If the attribute has the `true` value and the table is sorted, all keys in the table must be unique.

### Supported column types { #types }

{{product-name}} supports the column types described in the [Data types](../../../user-guide/storage/data-types.md) section.
Among them are both primitive and composite types.

Values in the table cells always have the type specified in the schema.

You can use one of the following methods to specify a type in the table schema:

- Using the `type` and (optional) `required` keys, you can specify only primitive or optional primitive types.
- Using the `type_v3` key.

The `type` key expects a string.

The `type_v3` key expects either a string (for primitive types) or a yson dict.
A yson dict always has the `type_name` key that stores the type name.

Other keys depend on the specific type and are described on the [Data types](../../../user-guide/storage/data-types.md) page.

Standard order relations apply for primitive types other than `any` and `yson`.

Order relations also apply for `optional<T>` types where `T` is a primitive comparable type.
It is guaranteed that missing values — values of the `null` type — are smaller than any value of any type.

Comparing elements of comparable types is supported.
The comparison is first made by type and then if the types match, values are compared.
The order between different types is not specified. For example, it makes no sense to compare `100` and `3.14`, because the result is not specified.

### Schema examples { #schema_examples }

{% cut "Schema of a static table after creation (unless explicitly stated otherwise)" %}
```
<strict=%false>[]
```
{% endcut %}

{% cut "Automatically output schema for a table sorted by `key1; key2`." %}

```
<strict=%false>[
  {
      name = "key1";
      type = "any";
      sort_order = "ascending"
      type_v3= {
          type_name=optional;
          item=yson;
      };
  };{
      name = "key2";
      type = "any";
      sort_order = "ascending"
      type_v3= {
          type_name=optional;
          item=yson;
      };
  }
]
```
{% endcut %}

{% cut "Example of a schema for a sorted table." %}
```
[
  {
    name = "key";
    type = "string";
    sort_order = "ascending"
  };{
    name = "subkey";
    type = "string";
    sort_order = "ascending"
  };{
    name = "value";
    type = "string"
  }
]
```
{% endcut %}

{% cut "Example of a schema with grouping for columns that are often used together." %}

```
[
  {
    name = "key";
    type = "string";
    sort_order = "ascending";
    group = "g1"
  };{
    name = "subkey";
    type = "string";
    sort_order = "ascending";
    group = "g1"
  };{
    name = "value";
    type = "string";
    sort_order = "ascending";
  }
]
```
{% endcut %}

{% cut "Example of a schema for a table with logs." %}

```
<strict=%false>[
 {
    "name" = "id";
    "type" = "int64";
 };{
    "name" = "class";
    "type" = "int64";
 };{
    "name" = "uid";
    "type" = "string";
 };{
    "name" = "ip";
    "type" = "int64";
 };{
    "name" = "iso_eventtime";
    "type" = "string";
 };{
    "name" = "error";
    "type" = "string"
 };{
    "name" = "ip6";
    "type" = "string"
 };{
    "name" = "port";
    "type" = "int64"
 };{
    "name" = "comment";
    "type" = "string"
 }
]
```
{% endcut %}

## Weak and strong schema { #schema_mode }

A table has the `schema_mode` attribute that defines the source of the schema origin.

If the schema was not set explicitly by the user, the `@schema_mode` attribute will have the `weak` value. This means that the table schema was created automatically and is only needed to mark up the key columns.
This schema automatically changes if the table loses its sorting property or changes the set of key columns.

For example, an entry with the `<append=%true>` flag to a sorted table without specifying the `sorted_by` attribute causes the table to lose sorting property, which means that its schema changes — at least the `sort_order` attribute.

If the table schema is set explicitly by the user, then `@schema_mode == strong`. The schema of such a table is saved and checked when writing, both in `append` and `overwrite` mode. Implicit transformations with a strong schema are impossible.

```bash
# Create a table without a schema
yt create table //tmp/table_1
213c-a01da-3fc0191-54a9a802
yt get //tmp/table_1/@schema_mode
"weak"
yt get //tmp/table_1/@schema
<
    "unique_keys" = %false;
    "strict" = %false;
> []
# Create a table with explicit indication of a schema
yt create table //tmp/table_2 --attributes '{schema = [{name = a; type = int64}; {name = b; type = string}]}'
213c-b75ef-3fc0191-1e85fe66
yt get //tmp/table_2/@schema_mode
"strong"
yt get //tmp/table_2/@schema
<
    "unique_keys" = %false;
    "strict" = %true;
> [
    {
        "name" = "a";
        "type" = "int64";
    };
    {
        "name" = "b";
        "type" = "string";
    };
]
```

## Strict schematization advantages { #strict_schematization_advantages }

- Performance:
   - If you additionally enable the column-by-column format of storing [chunks](../../../user-guide/storage/chunks.md) `optimize_for = scan`, data will be stored more compactly and reading a subset of columns will be faster.
   - Some table readers can work faster for tables with a schema.
- Convenience and more options:
   - YQL is more convenient for working with schematized tables.
   - In CHYT, you cannot work with unschematized data.


## Inheriting a schema in system operations { #schema_inference }

System operations such as Sort, Erase, Merge, and Remote copy can independently output the output table schema. Schema output rules for operations are regulated by the `schema_inference_mode = (auto|from_input|from_output)` specification. Examples of using the options can be found in the [Examples](#examples) section.

### Ordered merge, Erase

| Option | Description |
| ------------- | ------------------------------------------------------------ |
| `from_input` | The schemas of all tables must match with an accuracy of the `sort_order` attribute and the column order. Then the output table schema matches the input table schema. If there is one input table, sorting is preserved. If input tables had a strong schema, the output table will have a strong schema as well. |
| `from_output` | The output table schema is used, data is validated in relation to this schema. |
| `auto` | If the output table has a strong schema, its compatibility by types with the schema of all highly schematized input tables is checked. If the output table has a weak schema, the schema is derived from the input table schemas similarly to the `from_input` option. |

### Unordered merge

| Option | Description |
| ------------- | ------------------------------------------------------------ |
| `from_input` | The number and types of columns must match, then the output table schema matches the input table schema except for sorting. If input tables had a strong schema, the output table will have a strong schema as well. |
| `from_output` | The output table schema is used, data is validated in relation to this schema. |
| `auto` | If the output table has a strong schema, its compatibility by types with the schema of all highly schematized input tables is checked. If the output table has a weak schema, the schema is derived from the input table schemas (similarly to the `from_input` option). |

### Sorted merge

| Option | Description |
| ------------- | ------------------------------------------------------------ |
| `from_input` | The number and types of columns must match, then the output table schema matches the input table schema, key columns are set according to the `merge_by` parameter of the specification. If input tables had a strong schema, the output table will have a strong schema as well. |
| `from_output` | It is checked that key columns of the output table match the `merge_by` key columns from the specification. |
| `auto` | If the output table has a strong schema, its compatibility by types with the schema of all highly schematized input tables is checked. It is checked that key columns coincide with the `merge_by` parameter. If the output table has a weak schema, the schema is derived from the input table schemas similarly to the `from_input` option. |

### Sort

`Sort` is the only operation that can change a strong table schema. The column order is changed: key columns are moved to the beginning and the `sort_order` attribute is set. This is due to the common scenario of replacing a table with its sorted copy.

| Option | Description |
| ------------- | ------------------------------------------------------------ |
| `from_input` | If all input tables have strong schemas and the schemas match with an accuracy of the `sort_order` attribute and column order, then the output table column types match the types in the input table schemas. The key columns are derived from the operation specification (`sort_by`). If all input tables have a weak schema, the schema will be derived from the key columns. |
| `from_output` | The output table schema is saved, the key columns are changed according to the `sort_by` specification parameter. All key columns must be present in the schema. |
| `auto` | If the output table has a strong schema, its compatibility by types with the schema of all highly schematized input tables is checked. The key columns change according to the specification. If the output table has a weak schema, the schema is derived from the input table schemas (similarly to the `from_input` option). |

## Changing a schema

Changing a schema is an easy operation with metainformation that does not require reading data. To change a schema, use the `alter_table` command.
The schema of an empty table can be changed in a random manner. For non-empty tables, there are several allowed schema change scenarios:

- Adding a non-key column to a strict schema: `strict = %true`.
- Adding a key column to the end of the key in a strict schema: `strict = %true`.
- Deleting a column from a non-strict schema: `strict = %false`.
- Converting a schema from strict (`strict = %true`) to non-strict (`strict = %false`).

Other schema changes are prohibited, because they require data revalidation and such changes can be made through `Merge` or `Map` operations.

There are a number of restrictions on changing the schema:

- Changing the schema does not necessarily result in changing the contents of the table. For example, changing the `strict` attribute of the table from `false` to `true` requires checking that each row in the table contains only the values related to the columns mentioned in the schema and such a check is equivalent to reading the table completely.
- You cannot delete columns if `strict = true`: there is no way to clear the corresponding value in each row. You cannot add columns if `strict = false`: when adding a column of a certain type, you cannot be sure that there are no rows in the table that already contain a value corresponding to this column and it may already belong to another data type.
- You cannot delete key columns from the middle, because this can potentially violate the sort order.
- You cannot enter new computable columns or change the expression or aggregate function of existing columns, because this would require recomputing all rows in the table. But, for example, you can delete the last key columns, because this only weakens the sorting condition for future insertions without changing it for the existing set of rows.

## Examples of working with a schema { #examples }

### Reading a schema

- CLI
   ```
   yt get //home/tutorial/links_sorted_schematized/@schema
   ```

### Creating a table with a schema


- CLI

   ```bash
   yt create table //tmp/table --attributes '{schema = [{name = a; type = int64}; {name = b; type = string}]}'
   ```

- Python

   ```python
   yt.create_table("//tmp/table", attributes={"schema" : [{"name" : "a", "type" : "int64"}, {"name" : "b", "type" : "string"}]})
   ```

- C++

   ```c++
   auto schemaNode = TNode::CreateList()
     .Add(TNode()("name", "a")("type", "int64"))
     .Add(TNode()("name", "b")("type", "string"));
   client->Create(
     "//tmp/table",
     NYT::NT_TABLE,
     TCreateOptions().Attributes(TNode()("schema", schemaNode)));
   ```

### The schema attribute in RichYPath

You can use the `@schema` attribute of the path when writing data using the `write_table` command or at the output of MapReduce operations.

- CLI
   ```bash
   cat data | yt write '<schema=[{name = a; type = int64}; {name = b; type = string}]>//tmp/table'
   ```

### Setting a schema and changing a format for typified and structured data

CLI
```bash
# Enable column-by-column storage format
yt set //tmp/table/@optimize_for scan
# Set a new schema and transfer data to the new storage format
yt merge --mode ordered --src //tmp/table --dst '<schema=[{name = a; type = int64; sort_order = ascending}; {name = b; type = string}]>//tmp/table' --spec '{schema_inference_mode = from_output; force_transform = %true}'
```

There are more automatic solutions:

- Make a request of the `INSERT INTO "path/to/somewhere" SELECT * FROM "source/table"` type via YQ. YQ will try to output the schema by reading the first row or the first rows.

   {% if doc_type=='internal' %}

- InferSchema in c++ .

- If you have a .proto file corresponding to the table, CreateTableSchema can help.

   {% endif %}
