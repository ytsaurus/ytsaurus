# YSON

This section contains information about YSON, a JSON-like data format.

The main differences between YSON and JSON are:

1. Support for binary representation of scalar types (numbers, strings, and boolean types).
2. [Attributes](#attributes): an arbitrary dict that can be set additionally on a literal of any type, including scalar ones.

Besides that, there are syntactic differences:

1. A semicolon is used as a separator instead of a comma.
2. In dicts, a key is not separated from a value by a colon, but by an equal sign: `=`.
3. String literals only have to be enclosed in quotes if there is a parsing.

The following set of **scalar** types is available:

1. [Strings](#string) (`string`).
2. [Signed](#int) and [unsigned](#uint) 64-bit integers (`int64` and `uint64` ).
3. Double-precision [floating-point numbers](#double) (`double`).
4. [Boolean](#boolean) (logical) type (`boolean`).
5. A [special entity type](#entity) with only one value, a literal (`#`).

Scalar types usually have both textual and binary representation.

There are two **composite** types:

1. [List](#list) (`list`).
2. [Dict](#map) (`map`).

## Scalar types { #scalar_types }

### Strings { #string }

There are three types of string tokens:

1. **Identifiers** are strings that match the regular expression ` [A-Za-z_][A-Za-z0-9_.\-]*`. It describes the set of possible C identifiers, extended with the `-` and `.` characters. An identifier specifies a string with identical content and is used primarily for brevity (no need to use quotes).

   Examples:

   - `abc123`;
   - `_`;
   - `a-b`.

2. **Text strings**: [C-escaped](https://en.wikipedia.org/wiki/Escape_sequences_in_C) strings in double quotes.

   Examples

   - `"abc123"`;
   - `""`;
   - `"quotation-mark: \", backslash: \\, tab: \t, unicode: \xEA"`.

3. **Binary strings**: `\x01 + length (protobuf sint32 wire format) + data (<length> bytes)`.

### Signed 64-bit integers (`int64`) { #int }

Two methods of writing:

1. **Text** (`0`, `123`, `-123`, `+123`).
2. **Binary**: `\x02 + value (protobuf sint64 wire format)`.

### Unsigned 64-bit integers (`uint64`) { #uint }

Two methods of writing:

1. **Text** (`10000000000000`, `123u`).
2. **Binary**: `\x06 + value (protobuf uint64 wire format)`.

### Floating-point numbers (`double`) { #double }

Two methods of writing:

1. **Text**: `0.0`, `-1.0`, `1e-9`, `1.5E+9`, `32E1`.
2. **Binary**: `\x03 + protobuf double wire format`.

{% note warning "Attention" %}

Textual representation of floating-point numbers involves rounding. The result value may become different after a round of serialization and parsing. To store an accurate value, use binary representation.

{% endnote %}

### Boolean literals (`boolean`) { #boolean }

Two methods of writing:

1. **Text** (`%false`, `%true`).
2. **Binary** (`\x04`, `\x05`).

### Entity (`entity`) { #entity }

Entity is an atomic scalar value with no content of its own. There are various scenarios in which this type can be useful. For example, entity often means null. In addition, when a `get` request is made to a [Cypress](../../../user-guide/storage/cypress.md) subtree, [files](../../../user-guide/storage/objects.md#files) and [tables](../../../user-guide/storage/objects.md#tables) are returned as entities (actual data is stored in the [attributes](#attributes) of that node).

Lexically, entity is encoded by the `#` symbol.

### Special literals { #special_literals }

Special tokens:
`;`, `=`, `#`, `[`, `]`, `{`, `}`, `<`, `>`, `)`, `/`, `@`, `!`, `+`, `^`, `:`, `,`, `~`.
Not all of these symbols are used in YSON, some are used in [YPath](../../../user-guide/storage/ypath.md).

## Composite types { #composite_types }

### List (`list`) { #list }

Set as follows: `[value; ...; value]` where `value` is a literal of some scalar or composite type.

Example: `[1; "hello"; {a=1; b=2}]`.

### Dict (`map`) { #map }

Set as follows: `{key = value; ...; key = value}`. Here `*key*` is a string literal and `value` is a literal of some scalar or composite type.

Example: `{a = "hello"; "38 parrots" = [38]}`.

### Attributes { #attributes }

It is possible to set attributes on any literal in YSON, in the following format: `<key = value; ...; key = value> value`. Inside angle brackets, the syntax is similar to the dict. For example, `<a = 10; b = [7,7,8]>"some-string"` or `<"44" = 44>44`. But most often attributes can be found on literals like `entity`, for example, `<id="aaad6921-b5704588-17990259-7b88bad3">#`.

## Grammar {#grammar}

YSON has three data types:

  1. **Node** (single tree; `<tree>` in the example)
  2. **ListFragment** (`semicolon`-separated values; `<list-fragment>` in the example)
  3. **MapFragment** (`semicolon`-separated key-value pairs; `<map-fragment>` in the example)


Grammar (whitespace characters are ignored and may appear in any number between tokens):

```antlr
          <tree> = [ <attributes> ], <object>;
        <object> = <scalar> | <map> | <list> | <entity>;

        <scalar> = <string> | <int64> | <uint64> | <double> | <boolean>;
          <list> = "[", <list-fragment>, "]";
           <map> = "{", <map-fragment>, "}";
        <entity> = "#";
    <attributes> = "<", <map-fragment>, ">";

 <list-fragment> = { <list-item>, ";" }, [ <list-item> ];
     <list-item> = <tree>;

  <map-fragment> = { <key-value-pair>, ";" }, [ <key-value-pair> ];
<key-value-pair> = <string>, "=", <tree>;  % Key cannot be empty
```

You may omit the semicolon ('`;`') after the last element inside `<list-fragment>` and `<map-fragment>`. The following constructs are considered valid when reading data:

#|
|| Ending with `;` | Shorthand syntax ||
||

```yson
<a=b;>c
{a=b;}
1;2;3;
```

|

```yson
<a=b>c
{a=b}
1;2;3
```

 ||
|#


## Examples {#examples}

- Map (Node)

  ```yson
  { performance = 1 ; precision = 0.78 ; recall = 0.21 }
  ```

- Map (Node)

  ```yson
  { cv-precision = [ 0.85 ; 0.24 ; 0.71 ; 0.70 ] }
  ```


- List (Node)

  ```yson
  [ 1; 2; 3; 4; 5 ]
  ```


- String (Node)

  ```yson
  foobar
  ```

  ```yson
  "hello world"
  ```

- Int64 (Node) `42`

- Double (Node) `3.1415926`

- ListFragment

  ```yson
  { key = a; value = 0 };
  { key = b; value = 1 };
  { key = c; value = 2; unknown_value = [] }
  ```

- MapFragment

  ```yson
  do = create; type = table; scheme = {}
  ```

- HomeDirectory (Node)

  ```yson
  { home = { sandello = { mytable = <type = table> # ; anothertable = <type = table> # } ; monster = { } } }
  ```

## Working with YSON from code { #working_from_code }

Users usually do not have to work directly with YSON. When using one of the official {{product-name}} clients, YSON structures will be expressed as follows:

1. **C++**:[`TNode`](https://github.com/ytsaurus/ytsaurus/blob/main/library/cpp/yson/node/node.h) is a class that provides dynamic DOM-like representation of a YSON document.
2. **Python**: [`YsonType`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/python/yt/yson/yson_types.py): YSON types mimic Python types. You can get YSON attributes of object `x` like this: `x.attributes`, this is a Python dict.
3. **Java**: [`YTreeNode`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/yson-tree/src/main/java/tech/ytsaurus/ysontree/YTreeNode.java) is an interface that provides dynamic DOM-like representation of a YSON document.

