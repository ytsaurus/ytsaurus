# Data types

This section describes the data types supported by {{product-name}} and the way they are described in the schema and represented in the formats.

## Overview { #overview }
{{product-name}} supports a number of primitive types:
- `string`;
- `integer`;
- `boolean`;
- `float`;
- `double`;
- `date`;
- `datetime`;
- `timestamp`;
- `interval`.

As well as the following composite (complex) types:

- `optional`;
- `list`;
- `struct`;
- `tuple`;
- `variant`;
- `tagged`.

You can use one of the following two methods to specify a type in a table schema:

- Using the `type` and (optionally) the `required` keys: historically, the first method but it is only good for defining primitive or optional primitive types.
- Using the `type_v3` key.

The `type` key always expects a string.
The `type_v3` key expects either a string for primitive types or a YSON dictionary.
A yson dict always has the `type_name` key that stores the type name.
The remaining keys depend on the specific type and are described below.

## Describing types in a schema { #schema }

### Primitive types { #schema_primitive }

You can define primitive types in a schema both using the `type` and the `type_v3` keys.

If primitive type `T` is defined using `type`, {{product-name}} will additionally check the `required` key in the schema:

- `required=%true`: the column will have a strictly defined type. `Null` or empty values are illegal.
- `required=%false`: the column will be of type `optional<T>`. Any values of a primitive type and the `Null` value will be legal.

The table lists the supported types and their representation in the `type`/`type_v3` keys.

| **Description** | **Representation in `type`** | **Representation in `type_v3`** |
|--------------|----------------------------|-------------------------------|
| an integer belonging to the range `[-2^63, 2^63-1]` | `int64` | `int64` |
| an integer belonging to the range `[-2^31, 2^31-1]` | `int32` | `int32` |
| an integer belonging to the range `[-2^15, 2^15-1]` | `int16` | `int16` |
| an integer belonging to the range `[-2^7, 2^7-1]` | `int8` | `int8` |
| an integer belonging to the range `[0, 2^64-1]` | `uint64` | `uint64` |
| an integer belonging to the range `[0, 2^32-1]` | `uint32` | `uint32` |
| an integer belonging to the range `[0, 2^16-1]` | `uint16` | `uint16` |
| an integer belonging to the range `[0, 2^8-1]` | `uint8` | `uint8` |
| an 8-byte real number | `double` | `double` |
| a 4-byte real number | `float` | `float` |
| a standard `true/false` boolean | `boolean` | `bool` (different from `type`) |
| a random sequence of bytes | `string` | `string` |
| a proper UTF8 sequence | `utf8` | `utf8` |
| an integer in the range `[0, 49673 - 1]` <br> represents the number of days from the Unix epoch; <br> is the represented data range: `[1970-01-01, 2105-12-31]` | `date` | `date` |
| an integer in the range `[0, 49673 * 86400 - 1]`, <br> represents the number of seconds since the Unix epoch; <br> is the represented time interval: `[1970-01-01T00:00:00Z, 2105-12-31T23:59:59Z]` | `datetime` | `datetime` |
| an integer in the range `[0, 49673 * 86400 * 10^6 - 1]`, <br> represents the number of microseconds since the Unix epoch; <br> is the represented time interval: `[1970-01-01T00:00:00Z, 2105-12-31T23:59:59.999999Z]` | `timestamp` | `timestamp` |
| an integer in the range `[- 49673 * 86400 * 10^6 + 1, 49673 * 86400 * 10^6 - 1]`, <br> represents the number of microseconds between two timestamps | `interval` | `interval` |
| a random YSON structure, <br> is physically represented by a byte sequence, <br> cannot have `required=%true` | `any` | `yson` (different from `type`) |

Schema example:

```
type_v3=utf8
```
```
type_v3=bool
```
```
type_v3=yson
```

### Decimal { #schema_decimal }

The values of type `decimal(p, s)` are real numbers with the specified precision.

To define this type in a schema, specify the following keys:

- `type_name`: value of `decimal`.
- `precision`: total number of decimal digits in the representation of a number, `precision` must be in the range `[1, 35]`.
- `scale`: number of digits to the right of the decimal point in the representation of a number, `scale` must be in the range `[0, precision]`.

Schema example:

```
type_v3={
    type_name=`decimal`
    precision=10;
    scale=2;
}
```

The values `3.14`, `-2.71`, `9.99` may be of type `decimal(3, 2)` (`precision=3`, `scale=2`).

The type supports a number of special values, such as `nan`, `+inf`, `-inf`.

#### Description of binary representation { #schema_decimal_binary }

There is a special binary representation of `decimal` numbers,
that many formats like `yson` use by default.

For the purposes of this representation, the values of `decimal(p, s)` types are maintained as binary strings. Binary string length
depends on `precision`.

| **Precision** | **Number of bits in the representation** | **Number of bytes in the representation** |
|---------------|-------------------------------|--------------------------------|
| 1-9 | 32 | 4 |
| 10-18 | 64 | 8 |
| 19-35 | 128 | 16 |

You need to perform the following steps to obtain a binary representation of a `decimal` number. These steps will be illustrated with the values `3.1415`, `-2.7182` of type `decimal(5, 4)`.
1. Take an integer made up of the value's digits. The number of bits is taken from `precision` in the table above. In this example, 32-bit numbers `31415`, `-27182`.
2. Write the number as a big-endian sequence. In this example, the strings are `\x00\x00\x7A\xB7`, `\xFF\xFF\x95\xD2`.
3. Invert the most significant bit. In this example, the strings are `\x80\x00\7A\xB7`, `\x7F\xFF\x95\xD2`.

The integer representations of the special values of `nan`, `+inf`, `-inf` for the first step are shown in the table below:
| **Special value** | **Integer representation** |
|--------------------------|---------------------------------|
| `nan`  | `INT_MAX` |
| `+inf` | `INT_MAX - 1` |
| `-inf` | `- INT_MAX + 1` |


### Optional type { #schema_optional }

The `optional<T>` type means that a value may be of type `T` or be empty.

{% note info "Please note" %}

Each use of `optional` for a type adds new values.
For instance, `optional<optional<bool>>` may take on the following values:

- The external `optional` is empty.
- The external `optional` is non-empty, and the internal one is empty.
- All the `optionals` are non-empty, the values are `true` or `false`.

{% endnote %}

Legacy columns containing the `type=T;required=false` attributes correspond to type `optional<T>` defined using `type_v3`.

To define type `optional`, specify the keys below:

- `type_name`: value of `optional`.
- `item`: element type description.

Schema example:

```
type_v3={
  type_name=optional;
  item=string;
}
```
```
type_v3={
  type_name=optional;
  item={
    type_name=optional;
    item=bool;
  }
}
```

### List { #schema_list }

Values of type `list<T>` are lists of elements of type `T`.

To define the type in the schema, specify the keys below:

- `type_name`: value of `list`.
- `item`: element type description.

Schema example:

```
type_v3={
  type_name=list;
  item=string;
}
```

```
type_v3={
  type_name=list;
  item={
    type_name=list;
    item=double;
  }
}
```

### Struct { #schema_struct }

A collection of named fields with specified value types.

To define this type in a schema, specify the following keys:

- `type_name`: value of `struct`.
- `members`: list of dictionaries with keys:
   - `name`: field name, must be a non-empty utf8 string.
   - `type`: field type.

Schema example:

```
type_v3={
  type_name=struct;
  members=[
    {
      name=foo;
      type=int32;
    };
    {
      name=bar;
      type={
        type_name=optional;
        item=string;
      }
    };   
  ]
}
```

### Tuple { #schema_tuple }

A collection of unnamed fields of certain predefined types.

To define this type, you need to specify the following keys in the schema:

- `type_name`: value of `tuple`.
- `elements`: list of dictionaries with keys:
- `type`: element type.

Schema example:

```
type_v3={
  type_name=tuple;
  elements=[
     {
       type=double;
     };
     {
       type=double;
     };
  ]
}
```

### Variant { #schema_variant }

Variant is strictly a single value from a defined collection of types.
A variant may be of one of two types:

- Variant over struct. Each type has a name (as in a struct), and each variant value is labeled with the name of the relevant variant element value.
- Variant over tuple. In this case, all the elements are unnamed, and each value is labeled with an index.

To define this type, specify the following keys in a schema:

- `type_name`: value of `variant`.
- `elements` or `members` (not both): the keys have the same structure similar as these keys in `tuple` / `struct`:
* `elements`: for the option with unnamed elements with the key itself containing a list of dictionaries with keys:
   - `type`: element type.
* `members`: for the option with named elements with the key itself containing a list of dictionaries with keys:
   - `name`: element name, must be a non-empty utf8 string.
   - `type`: description of element type.

Schema example:

```
type_v3={
  type_name=variant;
  members=[
     {
       name=int_field;
       type=int64;
     };
     {
       name=string_field;
       type=string;
     };
  ]
}
```

```
type_v3={
  type_name=variant;
  elements=[
     {
       type=int32;
     };
     {
       type=string;
     };
     {
       type=double;
     };
  ]
}
```

### Dict { #schema_dict }

A dict is a sequence of key/value pairs.
{{product-name}} does not check the keys for uniqueness or order.
However, most clients will upload data to an actual dictionary while processing, and the value for non-unique keys will be lost.

To define this type, specify the following keys in a schema:

- `type_name`: value of `dict`.
- `key`: description of key type.
- `value`: value type description.

Schema example:

```
type_v3={
  type_name=dict;
  key=int64;
  value={
    type_name=optional;
    item=string;
  };
}
```

### Tagged { #schema_tagged }
The `tagged` type helps annotate other types with a string. Any value of type `T` can serve as a value for type `tagged<TAG_NAME,T>`,
however, the types themselves will be considered different wherever {{product-name}} compares schemas. For instance, when the possibility of merging two tables into one is being checked.

To define this type, specify the following keys in a schema:

- `type_name`: value of `tagged`.
- `tag`: tag name, must be a non-empty utf8 string.
- `item`: element type description.
   —
   Schema example:

```
type_v3={
  type_name=tagged;
  tag="image/svg";
  item="string";
}
```

## Representing compound types in formats { #formats }

Formats are used to read and write tables.
Some formats do not support composite data, and some, such as dsv / schemaful_dsv, will return an error in response to an attempt to read a composite value. For instance, `Values of type "any" are not supported by the chosen format`.

### YSON  { #yson }

There are two YSON representations of composite types. The representation of types `struct` and `variant` are different:
the default representation is more convenient to use,
while the alternative representation yields better storage and processing performance.

You can switch between the representations by using the `complex_type_mode` flag. Legal values: `named` / `positional`.
Type representation descriptions are provided below. Unless otherwise specified, a type representation does not depend on the `complex_type_mode` setting.

#### Primitive types { #yson_primitive }

Primitive types have a linear representation as a single YSON value.
The table shows a mapping between the primitive and the YSON types.

| **`type` / `type_v3`** | **Yson representation** |
|------------------------|--------------------------|
| `int64` | signed number |
| `int32` | signed number |
| `int16` | signed number |
| `int8` | signed number |
| `uint64` | unsigned number |
| `uint32` | unsigned number |
| `uint16` | unsigned number |
| `uint8` | unsigned number |
| `double` | floating point number |
| `boolean` / `bool` | boolean value |
| `string` | string |
| `utf8` | string |
| `date` | unsigned number |
| `datetime` | unsigned number |
| `timestamp` | unsigned number |
| `interval` | signed number |
| `any` / `yson` | value-dependent |


#### Decimal { #yson_decimal }

Type `decimal(p, s)` is encoded as a YSON string with a [binary representation](#schema_decimal_binary) of the `decimal` number.

#### Optional { #yson_optional }
The representation of type `optional` depends on its inner type.
This is required for backward compatibility with the columns with `required=%false`.
If `T` is an arbitrary type that is not `optional`, `optional<T>` is represented as follows:

- The `Null` value that is `optional` is represented by `#`.
- Otherwise, the `T` type value uses the conventional representation.

If `T` is an arbitrary type that is `optional`, `optional<T>` is represented as follows:

- The `Null` value of the outer `optional` is represented as `#`.
- Otherwise, the `[ v ]` representation is used (yson list of length 1), where `v` is the YSON representation of type `T`.

Example values, type `optional<int64>`:

```
#
```

```
-42
```

Example values, type `optional<optional<int64>>`:

```
#
```

```
[ # ]
```

```
[ -42 ]
```

#### List { #yson_list }

Type `list<T>` is encoded as a YSON list whose elements are encoded representations of elements of type `T`.

Example value, type `list<int64>`:
```
[]
```

```
[42; -1;]
```

#### Struct { #yson_struct }

A struct representation depends on the values of the `complex_type_mode` flag.

##### Named representation (default) { #yson_struct_named }

The representation being described applies to the situation
when the YSON option of format `complex_type_mode` is not set or is set to `complex_type_mode=named`.

The struct is represented by a YSON dictionary where field names serve as keys and the contents of these fields are the values.

Example values, type `struct<Foo:int64;Bar:optional<utf8>>`:
```
{Foo=42;Bar=#;}
```

```
{Foo=-5;Bar="minus five";}
```

##### Positional representation { #yson_struct_positional }

Where the `complex_type_mode=positional` option is set for the YSON format, a different representation is used.

A struct is encoded as a YSON list with the i-th position containing a YSON representation of the struct's i-th field.
The list may contain fewer elements than the number of fields in your struct, which means that the remaining types must be `optional<T>`,
and the fields are considered to have an empty `optional` value.

Example values, type `struct<Foo:int64;Bar:optional<utf8>>`:

```
[42; #;]
[42]
```

```
[-5;"minus five";]
```

#### Tuple { #yson_tuple }

Type `tuple` is encoded as a fixed-length YSON list. The i-th position contains the i-th field's encoded value.

Example values, type `tuple<int64;optional<utf8>>`:

```
[42; #;]
```

```
[-5;"minus five";]
```


#### Variant { #yson_variant }

##### Unnamed variant { #yson_variant_tuple }

The unnamed option is represented by a YSON list of length 2 that includes the following elements:

- Alternative number (indexed at 0).
- An encoded value of the relevant alternative.

Example values, type `variant<int64;optional<utf8>>`:

```
[0; 42]
```

```
[1; #]
```

```
[1; "foo bar";]
```

##### Named variant option { #yson_variant_struct }

###### Named representation (default) { #yson_variant_struct_named }

The representation being described applies to the situation
when the YSON option of format `complex_type_mode` is not set or is set to `complex_type_mode=named`.

The named option is represented by a YSON list of length 2 that includes the following elements:

- Alternative name.
- An encoded value for the relevant alternative.

Example values, type `variant<Foo:int64;Bar:optional<utf8>>:

```
[Foo; 42]
```

```
[Bar; #]
```

```
[Bar; "foo bar";]
```

###### Positional representation { #yson_variant_struct_positional }

If the `complex_type_mode=positional` option is set for a format.

The named option is represented by a YSON list of length 2 that includes the following elements:

- Index of alternative.
- An encoded value for the relevant alternative.

Example values, type `variant<Foo:int64;Bar:optional<utf8>>`:

```
[Foo; 42]
```

```
[Bar; #]
```

```
[Bar; "foo bar";]
```

#### Dict { #yson_dict }

Type `dict` is represented as a YSON list with each element being a YSON list of 2 elements: a key and a value.

Example values of type `dict<int32;string>`:

```
[[1;"one"];[4;"four"]]
```
```
[]
```

A `dict` may be represented as a YSON dictionary; however, a YSON dictionary only supports strings as keys whereas `dict` also supports other keys.

#### Tagged { #yson_tagged }

Type `tagged` does not change its elements' YSON representations.