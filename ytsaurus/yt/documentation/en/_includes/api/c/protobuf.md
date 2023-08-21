# Protobuf representation of tables

This section describes the use of the [protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers) structured data transfer protocol for working with tables in the C++ API.

## Introduction { #introduction }

The C++ API enables you to use protobuf classes (messages) to read and write tables both by the client and inside the job.

We recommend using the [proto2](https://protobuf.dev/programming-guides/proto/) version. If you use [proto3](https://protobuf.dev/programming-guides/proto3/), errors may occur: for example, when trying to write `0` in the `required=%true` field.

### How it works

The user describes the proto structure in the `.proto` file. The proto structure can be marked with different [flags](#flags). Flags affect how {{product-name}} will generate or interpret the fields specified within messages.

## Elementary types { #primitive }

To work with the [{{product-name}} data type](../../../user-guide/storage/data-types.md) from the first column, you can use the corresponding protobuf type from the second column in the table below.

| **{{product-name}}** | **Protobuf** |
|---------------------------------|----------------------------------------------|
| `string`, `utf8` | `string`, `bytes` |
| `int{8,16,32,64}` | `int{32,64}`, `sint{32,64}`, `sfixed{32,64}` |
| `uint{8,16,32,64}` | `uint{32,64}`, `fixed{32,64}` |
| `double` | `double`, `float` |
| `bool` | `bool` |
| `date`, `datetime`, `timestamp` | `uint{32,64}`, `fixed{32,64}` |
| `interval` | `int{32,64}`, `sint{32,64}`, `sfixed{32,64}` |

If the range of the integer {{product-name}} type does not correspond to the range of the protobuf type, a check is performed at the time of encoding or decoding.

## Optional/required properties { #optional_required }

The `optional`/`required` properties in protobuf do not have to correspond to the options of columns in YT. The system always checks when protobuf messages are encoded or decoded.

For example, if the `foo` column in {{product-name}} has the `int64` type (`required=%true`), the following field can be used for its representation:

```protobuf
  optional int64 foo = 42;
```
There will be no errors as long as all protobuf messages that are written to the table have the filled in `foo` field.

## Embedded messages { #embedded }

Historically, all embedded protobuf structures are written in {{product-name}} as a byte string storing the [serialized](#serialization) representation of the embedded structure.

Embedded messages are quite efficient, but do not enable you to conveniently represent values in the web interface or work with them using other methods (without the help of protobuf).

This method can also be specified explicitly by setting a special [flag](#flags) in the field:

```
optional TEmbeddedMessage UrlRow_1 = 1 [(NYT.flags) = SERIALIZATION_PROTOBUF];
```

You can specify an alternative flag, then {{product-name}} will refer the field to the `struct` type:

```protobuf
optional TEmbeddedMessage ColumnName = 1 [(NYT.flags) = SERIALIZATION_YT];
```

To use embedded messages:

- The table must have a [schema](../../../user-guide/storage/static-schema.md).
- The corresponding column (in the example: `ColumnName`) must have the [struct](../../../user-guide/storage/data-types.md#schema_struct) {{product-name}} type.
- The `struct` type fields must correspond to the fields of the embedded message (in the example: `TEmbeddedMessage`).

{% note warning %}

The flag in embedded messages is not inherited by default. If the `SERIALIZATION_YT` flag is set for the field with the `T` type, the default behavior for structures embedded in `T` will still correspond to the `SERIALIZATION_PROTOBUF` flag.

{% endnote %}

## Repeated fields { #repeated }

To work with repeated fields, you must explicitly specify the `SERIALIZATION_YT` flag:

```protobuf
repeated TType ListColumn = 1 [(NYT.flags) = SERIALIZATION_YT];
```

In YT, such a field will have the `list` type. To use repeated fields:

- The table must have a [schema](../../../user-guide/storage/static-schema.md).
- The corresponding column (in the example: `ListColumn`) must have the [list](../../../user-guide/storage/data-types.md#schema_list) {{product-name}} type.
- The element of the `list` {{product-name}} type must correspond to the protobuf column type (in the example: `TType`). This can be an [elementary](#primitive) type or an [embedded message](#embedded).


{% note warning %}

The `SERIALIZATION_PROTOBUF` flag for repeated fields is not supported.

{% endnote %}

## Oneof fields { #oneof }

By default, fields within a `oneof` group correspond to the [variant](../../../user-guide/storage/data-types.md#schema_variant) {{product-name}} type. For example, the message below will correspond to the structure with the `x` field of the `int64` type and the `my_oneof` field of the `variant<y: string, z: bool>` type:

```protobuf
message TMessage {
    optional int64 x = 1;
    oneof my_oneof {
        string y = 2;
        bool z = 3;
    }
}
```

If you infer the schema using `CreateTableSchema<T>()`, a similar type will be inferred.

To make `oneof` groups correspond to the fields of the structure where they are described, use the `(NYT.oneof_flags) = SEPARATE_FIELDS` flag:

```protobuf
message TMessage {
    optional int64 x = 1;
    oneof my_oneof {
        option (NYT.oneof_flags) = SEPARATE_FIELDS;
        string y = 2;
        bool z = 3;
    }
}
```
This message will correspond to the structure with optional fields `x`, `y`, and `z`.

## Map fields { #map }

There are 4 options for displaying such a field in a column in a table. See the example below:

```protobuf
message TValue {
    optional int64 x = 1;
}

message TMessage {
    map<string, TValue> map_field = 1 [(NYT.flags) = SERIALIZATION_YT];
}
```

Depending on its flags, the `map_field` field can correspond to:
- The list of structures with the `key` field of the ` string` type and the `value` field of the `string` type, which will contain the serialized protobuf `TValue` (as if the `SERIALIZATION_PROTOBUF` flag is set for the `value` field). In this case, the default flag is `MAP_AS_LIST_OF_STRUCTS_LEGACY`.
- The list of structures with the `key` field of the `string` type and the `value` field of the `Struct<x: Int64>` type (as if the `SERIALIZATION_PROTOBUF` flag is set for the `value` field). In this case, the default flag is `MAP_AS_LIST_OF_STRUCTS`.
- The `Dict<String, Struct<x: Int64>>>` dict: the `MAP_AS_DICT` flag.
- The optional `Optional<Dict<String, Struct<x: Int64>>>>` dict: the `MAP_AS_OPTIONAL_DICT` flag.

## Flags { #flags }

You can use flags to customize the protobuf behavior. To do this, connect the library `.proto` file.

```protobuf
import "mapreduce/yt/interface/protos/extension.proto";
```

Flags can correspond to messages, `oneof` groups, and message fields.

You can specify flags at the level of the `.proto` file, message, `oneof` group, and message field.


### SERIALIZATION_YT, SERIALIZATION_PROTOBUF { #serialization }

Behavior of these flags is described [above](#embedded).

By default, `SERIALIZATION_PROTOBUF` is implied where relevant. You can change the flag for a single message:

```protobuf
message TMyMessage
{
    option (NYT.default_field_flags) = SERIALIZATION_YT;
    ...
}

```

### OTHER_COLUMNS { #other_columns }

You can mark the field of the `bytes` type with the `OTHER_COLUMNS` flag. This field can contain a YSON map that contains representations of all fields not described by other fields of this protobuf structure.

```protobuf
message TMyMessage
{
    ...
    optional bytes OtherColumns = 1 [(NYT.flags) = OTHER_COLUMNS];
    ...
}
```

### ENUM_STRING / ENUM_INT { #enum_int }

You can mark the fields of the `enum` type with the `ENUM_STRING`/`ENUM_INT` flags:

- If the field is marked `ENUM_INT`, it will be saved to the column as an integer.
- If the field is marked `ENUM_STRING`, it will be saved to the column as a string.

By default, `ENUM_STRING` is implied.

```protobuf
enum Color
{
    WHITE = 0;
    BLUE = 1;
    RED = -1;
}

...
optional Color ColorField = 1 [(NYT.flags) = ENUM_INT];
...
```

### ANY { #any }

You can mark the fields of the `bytes` type with the `ANY` flag. Such fields contain a YSON representation of a column of any simple type.
For example, you can write the following code for the `string` type column:

```protobuf
// message.proto
message TMyMessage
{
    ...
    optional bytes AnyColumn = 1 [(NYT.flags) = ANY];
    ...
}
```

```c++
// main.cpp
TNode node = "Linnaeus";
TMyMessage m;
m.SetAnyColumn(NodeToYsonString(node));
```
