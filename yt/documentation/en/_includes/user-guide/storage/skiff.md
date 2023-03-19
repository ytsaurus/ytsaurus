# Skiff format

The first part of this section describes the skiff serialization library (`yt/core/skiff`) which helps encode structured data, including data that is not {{product-name}} tables.
The second part describes the use of the library efficiently to exchange table data between job_proxy and user code.

Skiff (from schemaful format) is an interchange [format](../../../user-guide/storage/formats.md) to communicate schematized data between job_proxy and a job.

Format objectives:

- Optimize data serialization and deserialization performance for schematized data.
- Support all the types supported by {{product-name}}.
- Support the reading of tables even if not strongly typed.

## Comparison with other formats

{% cut "protobuf" %}

Protobuf has the following drawbacks:

- For some clients (YQ), message structure is defined at runtime. Protobuf is inconvenient to parse and write for an unknown type.
- Protobuf supports backward compatibility. This results in tags that have to be maintained current.
- Protobuf field order is not specified. This produces extra branches for tags on each field.
- Protobuf frequently uses type varint. For job_proxy that talks to a job via a pipe, it is less costly to write a uint64 number than serialize and deserialize a varint.
- Because of varint, strings and nested messages are prefixed with length. To serialize a message, you need to walk a structure twice.
- In protobuf, repeated fields do not have to be contained in one part of a message: the start of a list can be at the top of a message, and the end at the bottom. This complicates the writing of proper parsers.

{% endcut %}

{% cut "flatbuffers" %}

- For some clients (YQ), message structure is defined at runtime. Flatbuffers is inconvenient to parse and write for an unknown type because there is no reflection.

{% endcut %}

## Skiff serialization library

The skiff serialization format is schematized. To be able to read and interpret data, you need to know the data schema.

### Skiff Schema

A schema describes the structure of an encoded value and the encoding method.
It is a tree-like structure. A single-node tree could be a valid schema. The order or a node's children in a tree is important. Each node's coding type is `wire_type`, leaves have simple types whereas the other tree nodes have compound types. Nodes of a skiff schema may have names, which do not affect data encoding and serve to map a skiff schema to that of a table.

#### Simple types

##### Nothing

`nothing`: no value, special type that can only be used as a child of the `Variant*` / `RepeatedVariant*` compound types.
Encoded as an empty string.

##### Boolean

`boolean`: Boolean type.
<u>Encoded</u> with a single byte: `001` for `true`, `000` for `false`.
<u>Examples</u>: `\x01`, `\x00`.

##### Int64 / Uint64

`uint64` / `int64`: integers.
<u>Encoded</u> with an 8-byte number in little-endian format.
<u>Examples</u>: `\x2a\x00\x00\x00\x00\x00\x00\x00` (42), `\x94\x88\x01\x00\x00\x00\x00\x00` (100500).

##### Double

`double`: 8-byte representation of a double.
<u>Example</u>: `\x9B\x91\x04\x8B\x0A\xBF\x05\x40` (2.718281828).

##### String32

`string32`: string length encoded as a 32-bit uint in little-endian format, followed by the string itself.
<u>Example</u>:`\x06\x00\x00\x00foobar`

##### Yson32

`yson32`: value of an arbitrary type.
The length of the encoded YSON is encoded as a 32-bit uint followed by the encoded YSON itself.
<u>Examples</u>: `\x09\x00\x00\x00{foo=bar}`, `\x07\x00\x00\x00100500u`

#### Compound types

##### Variant8 / variant16

`variant8` / `variant16`: algebraic data types.

Encoded by an 8-bit (for `variant8`) or a 16-bit (for `variant16`) unsigned number (`i` tag) followed by a value described by the child node type at `i`.


- `variant8` / `variant16` can have type `nothing` as one of its nodes. Since type `nothing` is encoded by an empty string, string `\x00` is a valid value of type `variant8`.
- Bit depths depend on practical considerations. The variant tag will indicate the presence or the absence of a value or a table index.

##### RepeatedVariant8 / RepeatedVariant16

`repeated_variant8` / `repeated_variant16`: variant list.
Encoded with an 8-bit (for `repeated_variant8`) or a 16-bit (for `repeated_variant16`) tag as a continuous sequence of variants ending with special tag `0xFF` (for `repeated_variant8`) or `0xFFFF` (for `repeated_variant16`) that indicates the sequence end.

- Like the regular `variant8`, can have type `nothing` as one of its child nodes.
- The type's bit depth depends on practical considerations based on the need to define a transmission format for table data (essentially, the variant tag will be used as a sparse column index).

##### Tuple

`tuple`: fixed-size tuple of values.
Encoded as a series of values, the first of the same type as the first child node, the second the same type as the second child node, and so on.

## Using the skiff serialization library to transmit table data

### Skiff schema of a table

A format's attributes for each table to be transmitted are used to send a table skiff schema (`table_skiff_schema`). This is a skiff schema with a number of constraints.

Input and output format behavior is different. The input format concatenates all tables and transmits them through the same pipe whereas the output format reads each table from its own pipe.

A job receives values with the general schema as its input. The general schema has a `Variant16` node at its root with all the table skiff schemas as children.
From the output stream of the i-th output table, job_proxy attempts to read values described by the schema with the `Variant16` root node with the skiff schema of the i-th table as the only child.

### Limitations of a table's skiff schema.

The following limitations exist on a schema for transmitting table data:

- A node of type `tuple` must serve as the root node of a table's skiff schema.
- All the children of the root node of a table's skiff schema must be named. {{product-name}} uses them to determine which field to use for values from this or that column.
- The names of the children of a table skiff schema root node may include special names that start with `$`. Those children whose names do not begin with this character make up the set of dense nodes. These are the names of columns whose values will be placed in these fields.
- The children of the root node of a table's skiff schema may include a node named `$other_columns`. This node must be of type `yson32` and be the last one.
- The children of the root node of a table's skiff schema may include a node named `$sparse_columns`. This node must be of type `RepeatedVariant16`. Its children make up the set of sparse nodes and must also be named. The `$sparse_columns` node must be the one before last if `$other_columns` exists or must come last otherwise.
- The children of the root node of a table's skiff schema may include a node specially named `$key_switch`, which must be of type `boolean`.
- The children of the root node of a table's skiff schema may include nodes specially named `$row_index / $range_index`. They must be of type `variant8<nothing;int64>`
- All the dense nodes must have a schema that:
   - Either describes a simple type, such as `int64, uint64, boolean, double, string32, yson32`.
   - Or describes type `variant8<nothing; TYPE;>`, where TYPE is a simple type, such as `int64, uint64, boolean, double, string32, yson32`.
- All the sparse nodes must have a schema that describes a simple type, such as `int64, uint64, boolean, double, string32, yson32`.

When you serialize a table row, for every value, you need to find a dense or a sparse node with the same name as the name of a column.
Values described by dense nodes are always serialized as required by the skiff schema's root node (tuple). If there is no value for a dense node or if such a value is of type `NULL`, the corresponding dense node in the skiff schema must be of type `variant8`.
Values described by the sparse part are only serialized if present and of type other than `NULL`. They find their way into the `$sparse_columns` field as a list.

Those table row values that fail to be included in the dense or the sparse part, are sent to the `$other_columns` column. This column will store a yson map containing such values. Having no `$other_columns` column in the presence of a value not described in the dense or the sparse part will generate an error.

### Format configuration

You describe the skiff format with attributes, same as other formats. The attributes must include the `table_skiff_schemas` key with a list of table skiff schema descriptions. In addition, there may be a key called `skiff_schema_registry`. If it is there, it must include a YSON map storing a name representation in an auxiliary skiff schema.
In `table_skiff_schemas` and `skiff_schema_registry`, a skiff schema may be described using one of the two methods below:

- A string starting with `$`. Then, the =`$` character will be stripped, and the remaining string will be found in the `skiff_schema_registry` map. A schema is defined as a schema described with a value based on the relevant key.
- Map with fields:
   - (required) `wire_type`: skiff node type in the tree.
   - (optional) `name`: node name.
   - (optional) `children`: node children. For compound types, this field must be included. A child may either reference the description in `skiff_schema_registry`, or be a map.

`skiff_schema_registry` is required to enable the running of operations with a large number of input tables with identical schemas.

Example:

```
<
    "table_skiff_schemas" = [
        "$table1"
    ];
    "skiff_schema_registry" = {
        "table1" = {
            "children" = [
                {
                    "name" = "uint64_column";
                    "wire_type" = "uint64"
                };
                {
                    "name" = "int64_column";
                    "wire_type" = "int64"
                };
                {
                    "name" = "boolean_column";
                    "wire_type" = "boolean"
                };
                {
                    "name" = "string32_column";
                    "wire_type" = "string32"
                };
                {
                    "name" = "yson32_column";
                    "wire_type" = "yson32"
                }
            ];
            "wire_type" = "tuple"
        }
    }
> "skiff"
```

