# Static tables

This section provides an introduction to static tables, including their types and [schema](#schema).
In addition to static tables, the {{product-name}} system supports dynamic tables. For more information about the types and features of dynamic tables, see [Dynamic tables](../../../user-guide/dynamic-tables/overview.md).

## General description { #common }

Static tables are a classic table type in the {{product-name}} system.
Such tables are physically divided into parts ([chunks](../../../user-guide/storage/chunks.md)) and each part contains a fragment of table entries.

The name “static” implies that you cannot update existing table data without an overwrite. However, you can still append new records to the end of the table.

In addition, table data can be modified using the following operations (note that in a typical case you will need to read and overwrite entire chunks of the table):

- [Merge](../../../user-guide/data-processing/operations/merge.md)
- [Delete](../../../user-guide/data-processing/operations/erase.md)

Static tables can be [sorted](#sorted_tables) and [unsorted](#unsorted_tables).

### Sorted tables { #sorted_tables }

For a sorted table, a set of immutable key columns is known.
The table entries appear to be physically or logically ordered by key. Therefore, sorted tables enable you to effectively search for data by key.

A sorted table has at least one key column, the `sorted` attribute of the table is `true`. The attribute is read-only.

There are key (sorted) columns in the sorted table schema. They are marked with the `sort_order` field in the schema.

A list of all key columns is available in the `key_columns` and `sorted_by` attributes of the table. The sequence of columns is defined in the table schema. The attributes are read-only.

{% note info "Note" %}

Entries added to the end of the sorted table must not violate the sort order.

{% endnote %}

### Unsorted tables { #unsorted_tables }

For unsorted tables, the concept of a key is not defined, so searching for data by key is possible only if you read the whole table. However, you can access it by row numbers.

{% note info "Note" %}

Appending entries to the end of an unsorted table is the basis of many ways to load data into {{product-name}} clusters.
For this operation to be effective, consider the following:

* Any change request is processed by the master server. This schema does not scale well, so you should not make more than 100 writes per second.
* Writing a small number of rows within a single request results in small chunks which overload the master server with a large amount of metadata and make reading less efficient.

{% endnote %}

## Attributes { #attributes }

Any static table has the attributes represented in the table:

| **Name** | **Type** | **Description** | **Mandatory** |
| ------------- | --------------- | ------------------------------------ |-----------------------|
| `sorted` | `bool` | Whether the table is sorted. | No |
| `key_columns` | `array<string>` | Key column names. | Yes |
| `dynamic` | `bool` | Whether the table is dynamic. | No |
| `schema` | `TableSchema` | Table schema. | No |
| `row_count` | `integer` | Number of rows in the table. | Yes |
| `data_weight` | `integer` | A "logical" amount of uncompressed data written to a table. Depends only on the values in the table cells and the number of rows. Calculated as `row_count + sum(data_weight(value))` for all values in the table cells. `data_weight` for a value depends on the physical type of the value: for `int64`, `uint64`, and `double` — 8 bytes; for `bool` and `null` — 1 byte; for `string` —  string length; for `any` — length of the value serialized in binary YSON. | Yes |

Besides that, a table is an object that owns chunks, meaning it has the associated attributes featured in the [table](../../../user-guide/storage/chunks.md#attributes).

## Static table schema { #schema }

A static table schema is a list of column descriptions. For a detailed description of the schema format, see [Data schema](../../../user-guide/storage/static-schema.md).

## Limitations { #limitations }

A number of size and content type limitations are imposed on table rows and schema:

- The number of columns in a static table cannot exceed 32,768. We do not recommend using more than a thousand columns.
- The column name is an arbitrary byte sequence that can contain from 1 to 256 characters. In addition, it cannot start with the prefix `@`, which is reserved by the system.
- The maximum length of `string` values in a static table is limited by the maximum row weight.
- The maximum row weight is 128 megabytes. The row weight is the sum of the lengths of all values in the given row in bytes. The lengths of the values are counted depending on the type:
   - `int64`, `uint64`, and `double`: 8 bytes.
   - `boolean`: 1 byte.
   - `string`: String length.
   - `any`: The length of the structure serialized in binary [yson](../../../user-guide/storage/yson.md), in bytes.
   - `null`: 0 bytes.
- The maximum key weight in a sorted table is 256 kilobytes. The default limitation is 16 kilobytes. The key weight is counted similarly to the row weight.

   {% note warning "Attention" %}

   We strongly advise against raising the limit on the key weight, because there is a risk of exhaustion of the master server memory. You should only change the `max_key_weight` setting as an extreme measure.

   {% endnote %}
