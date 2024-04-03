# Formats

This section describes the data types and table data representation formats supported by {{product-name}}.

## Data types { #data_types }

The {{product-name}} system stores data of the following types:

- [Structured data](#structured_data)
- [Table data](#table_data)
- [Binary data](#binary_data)

Different formats enable users to work with different types of data.

### Structured data { #structured_data }

Structured data is a node tree with attributes, where nodes correspond to [YSON types](../../../user-guide/storage/yson.md) (map, list, primitive types) with attributes.
Structured data can be obtained using the [`get`](../../../api/commands.md#get) command.

Supported formats: `json`, `yson`.

Example: [Cypress](../../../user-guide/storage/cypress.md) nodes, their attributes.

### Table data { #table_data }

Table data is a sequence of rows where each row is logically a key-value pair, the key being a string, and the value is structured data.

Supported formats: `json`, `yson`, `dsv`, `schemaful_dsv`, `protobuf`, `arrow`.

Examples: Input and output data in operations, input data of the [`write_table`](../../../api/commands.md#write_table) command, output data of the [`read_table`](../../../api/commands.md#read_table) command.

### Binary data { #binary_data }

Binary data is a sequence of bytes. For example, files store binary data.

Supported formats: Cannot set a format. Representation only as a byte array.

Examples: Input data of the [`upload`](../../../api/commands.md#write_file) command and output data of the [`download`](../../../api/commands.md#read_file) command.

## Table data representation formats { #table_formats }

{{product-name}} supports several data formats, which determine in what form data is transmitted to or received from the system.
A format must be specified for all [`yt` utility](../../../api/cli/cli.md) commands that return or accept data.

Reading a table in DSV format:

```bash
yt read //some/table --format dsv
```

{{product-name}} supports the following data representation formats:

* [YSON](#yson)
* [JSON](#json)
* [DSV (TSKV)](#dsv)
* [SCHEMAFUL_DSV](#schemaful_dsv)

{% if audience == "internal" %}
For illustration purposes, let's see how the same [table](https://yt.{{internal-domain}}/{{prestable-cluster}}/navigation?path=//home/tutorial/staff_unsorted_sample) will look in different formats.
{% endif %}

## Example { #example }

The table used in the examples:

| name | uid |
| :----------| :---------------: |
| Elena | 95792365232151958 |
| Denis | 78086244452810046 |
| Mikhail | 70609792906901286 |
| Ilya | 15696008603902587 |
| Oxana | 76840674253209974 |
| Alexey | 15943558469181404 |
| Roman | 37865805882228106 |
| Anna | 35039450424270744 |
| Nikolai | 45320538587295288 |
| Karina | 20364947097122776 |

## YSON { #yson }

The YSON format is the main format in {{product-name}}: it is used to store table data, Cypress node attributes, and many other objects. YSON is largely similar to JSON, but the former has node attributes and supports binary data.
For a detailed specification, see the [YSON](../../../user-guide/storage/yson.md) section.

To read a table in YSON format:

```bash
yt read --proxy {{prestable-cluster}} --format '<format=pretty>yson' '//home/tutorial/staff_unsorted_sample'
```

Table representation in YSON format:

```yson
{
    "name" = "Elena";
    "uid" = 95792365232151958;
};
{
    "name" = "Denis";
    "uid" = 78086244452810046;
};
{
    "name" = "Mikhail";
    "uid" = 70609792906901286;
};
{
    "name" = "Ilya";
    "uid" = 15696008603902587;
};
{
    "name" = "Oxana";
    "uid" = 76840674253209974;
};
{
    "name" = "Alexey";
    "uid" = 15943558469181404;
};
{
    "name" = "Roman";
    "uid" = 37865805882228106;
};
{
    "name" = "Anna";
    "uid" = 35039450424270744;
};
{
    "name" = "Nikolai";
    "uid" = 45320538587295288;
};
{
    "name" = "Karina";
    "uid" = 20364947097122776;
};
```

**Parameters**

The values are specified in parentheses by default.
- **format** (`binary`) — describes the YSON variant to be used in generating data. Specifying the variant is only important when setting the output format in relation to the system, while for the input format the system reads all the YSON varieties homogeneously:
   - `binary` — binary
   - `text` — text without formatting
   - `pretty` — text with formatting
- **skip_null_values** (`%false`) — enable skipping `null` values in schematized tables. By default, all values are written.
- **enable_string_to_all_conversion** (`false`) — enable conversion of string values to numeric and boolean types. For example, `"42u"` is converted to `42u`, and `"false"` is converted to `%false`.
- **enable_all_to_string_conversion** (`false`) — enable conversion of numeric and boolean values to string type. For example, `3.14` becomes `"3.14"`, and `%false` becomes `"false"`.
- **enable_integral_type_conversion** (`true`) — enable conversion of `uint64` to `int64` and vice versa. This option is enabled by default. If an overflow occurs during conversion, you will see a corresponding error.
- **enable_integral_to_double_conversion** (`false`) — enable conversion of `uint64` and `int64` to `double`. For example, integer `42` becomes decimal number `42.0`.
- **enable_type_conversion** (`false`) — enable all of the above options. In most cases, this option will suffice.
- **complex_type_mode** (`named`) — representation of composite types, structs, and variants.
   Possible values are `named` and `positional`. For more information, see [this section](../../../user-guide/storage/data-types.md#yson).
- **string_keyed_dict_mode** (`positional`) — representation of dictionaries with string keys. Possible values are `named` and `positional`. For more information, see [this section](../../../user-guide/storage/data-types.md#yson).
- **decimal_mode** (`binary`) — representation of the `decimal` type.
   Possible values are `text` and `binary`. For more information, see [this section](../../../user-guide/storage/data-types.md#yson).
- **time_mode** (`binary`) — representation of `date`, `datetime`, and `timestamp` types. Possible values are `text` and `binary`. For more information, see [this section](../../../user-guide/storage/data-types.md#yson).
- **uuid_mode** (`binary`) — representation of the `uuid` type. Possible values are `binary`, `text_yql`, and `text_yt`. For more information, see [this section](../../../user-guide/storage/data-types.md#yson).

## JSON { #json }

[JSON](https://en.wikipedia.org/wiki/JSON) is a common format for displaying structured data.

JSON is convenient to use thanks to popular tools such as [jq](https://stedolan.github.io/jq/). And there is no need to install additional libraries to support YSON.
Detailed [JSON](https://www.json.org/json-en.html) specification.

YSON is the main format of {{product-name}}. Unlike JSON, YSON has attributes. Therefore, to convert data from YSON to JSON, you need to encode attributes in JSON.
To do this, convert the node with attributes to map-node with two keys:
- `\$value` — its value is taken as the entire current node without attributes.
- `\$attributes` — its value is taken as the `map` of attributes.

For example, `<attr=10>{x=y}` is represented as `{"$value": {"x": "y"}, "$attributes": {"attr": 10}}`.

The strings in {{product-name}} — YSON strings — are byte strings, while JSON uses Unicode encoding.
The format has an `encode_utf8` setting that allows you to manage conversions. The `encode_utf8` defaults to `%true`.

- encode_utf8=%true { #utf8-true }

To convert a YSON string to a JSON string, convert each byte to a Unicode character with the corresponding number and encode it in UTF-8. This conversion occurs, for example, when reading table data using the [`read_table`](../../../api/commands.md#read_table) command.

When a JSON string is converted to a YSON string, the sequence of Unicode characters of the JSON string is checked to make sure each of them is in the 0–255 range. They are then converted to the appropriate bytes. This conversion is performed when writing table data using the [`write_table`](../../../api/commands.md#write_table) command.

{% note warning "Attention" %}

With `encode_utf8=%true`, Unicode characters outside the 0..255 range in the JSON string are not allowed.

{% endnote %}

- encode_utf8=%false { #utf8-false }

To convert a YSON string to a JSON string, the YSON string must contain a valid UTF-8 sequence. It will be converted to a Unicode JSON string.

During JSON string to YSON string conversion, UTF-8 Unicode characters are encoded into byte sequences. This conversion is performed with the [`write_table`](../../../api/commands.md#write_table) command.

{% note warning "Attention" %}

In general, byte sequences in tables are not always UTF-8 sequences. Therefore, with `encode_utf8=%false` not all YSON strings are readable.

{% endnote %}

UTF-8 strings:

{% list tabs %}
- Python

   ```python
   yt.write_table("//path/to/table", [{"key": "Ivan"}], format=yt.JsonFormat(attributes={"encode_utf8": False}))
   ```

- CLI

   ```bash
   echo '{"key": "Jane"}{"key":"Doe"}' | YT_PROXY={{production-cluster}} yt write --table "//path/to/table" --format="<encode_utf8=%false>json"
   ```
{% endlist %}

{% note info "Note" %}

When the user reads the table in JSON format, each record is made in a separate line. The `pretty` format is not supported.

{% endnote %}

Reading a table in JSON format:

```bash
yt read --proxy {{prestable-cluster}} --format json '//home/tutorial/staff_unsorted_sample'
```

Table representation in JSON format:

```json
{"name":"Elena","uid":95792365232151958}
{"name":"Denis","uid":78086244452810046}
{"name":"Mikhail","uid":70609792906901286}
{"name":"Ilya","uid":15696008603902587}
{"name":"Oxana","uid":76840674253209974}
{"name":"Alexey","uid":15943558469181404}
{"name":"Roman","uid":37865805882228106}
{"name":"Anna","uid":35039450424270744}
{"name":"Nikolai","uid":45320538587295288}
{"name":"Karina","uid":20364947097122776}
```

**Parameters**

The values are specified in parentheses by default.
- **format** (`text`) — describes the JSON format in which data is to be generated. Matters only when the output format is specified relative to the system. For the input format, all JSON varieties are read by the system homogeneously:
   - `text` — text without formatting
   - `pretty` — text with formatting
- **attributes_mode** (`on_demand`) — attribute mode:
   - `always` — each node turns into a map with `\$value` and `\$attributes`.
   - `never` — attributes are ignored.
   - `on_demand` — only nodes with non-empty attributes are converted to a map with `\$value` and `\$attributes`.
- **encode_utf8** (`true`) — enable UTF-8 character interpretation in bytes with corresponding numbers.
- **string_length_limit** — limit on the length of the string in bytes. If the limit is exceeded, the string is trimmed and the result is written as `{$incomplete: true, $value:...}`.
- **stringify** (`false`) — enable conversion of all scalar types to strings.
- **stringify_nan_and_infinity** (`false`) — enable conversion of `Nan` and `Infinity` to strings. You can't use this together with `support_infinity`.
- **support_infinity** (`false`) — allow the `Nan` value for the `double` type. You can't use this together with `stringify_nan_and_infinity`.
- **annotate_with_types** — enable type annotation `{$type: "uint64", $value: 100500}`.
- **plain** — enable the JSON parser option, which disables accounting for the logic regarding special keys `\$attributes`, `\$value`, `\$type` and parses JSON as is. When the option is enabled, the parser works much faster.
- **enable_string_to_all_conversion** (`false`) — enable conversion of string values to numeric and `boolean` types. For example, `"42u"` is converted to `42u`, and `"false"` is converted to `%false`.
- **enable_all_to_string_conversion** (`false`) — enable conversion of numeric and `boolean` values to string type. For example, `3.14` becomes `"3.14"`, and `%false` becomes `"false"`.
- **enable_integral_type_conversion** (`true`) — enable conversion of `uint64` to `int64` and vice versa. If an overflow occurs during conversion, you will see a corresponding error.
- **enable_integral_to_double_conversion** (`false`) — enable conversion of `uint64` and `int64` to `double`. For example, integer `42` becomes decimal number `42.0`.
- **enable_type_conversion** (`false`) — enable all of the above options. In most cases, this option will suffice.

{% note warning "Attention" %}

JSON represents bytes numbered 0–32 (namely \u00XX) in a non-compact way. When you try to write a long string with such data, an **Out of memory** error may occur in the Yajl library, because the memory amount limit is `2 * row_weight`. In that case, the JSON format is not recommended.

{% endnote %}

## DSV (TSKV) { #dsv }

**DSV** — Delimiter-Separated Values.
**TSKV** — Tab-Separated Key-Value.

A format that is widely used for storing logs and working with them. DSV and TSKV are two names of the same format.
This format only supports flat records with an arbitrary column set and string values. Records are separated by the line break character `\n`.
Fields in a record are separated by the tab character `\t`.
For more information, read below.

Example record: `time=10\tday=monday\n`.

{% note warning "Attention" %}

The format does not support attributes and typed values. In the example, `10` will be recognized by the system as a string, not a number.

{% endnote %}

Reading a table in DSV format:

```bash
yt read --proxy {{prestable-cluster}} --format dsv '//home/tutorial/staff_unsorted_sample'
```

Table representation in DSV format:

```
name=Elena      uid=95792365232151958
name=Denis	uid=78086244452810046
name=Mikhail	uid=70609792906901286
name=Ilya	uid=15696008603902587
name=Oxana	uid=76840674253209974
name=Alexey	uid=15943558469181404
name=Roman	uid=37865805882228106
name=Anna	uid=35039450424270744
name=Nikolai	uid=45320538587295288
name=Karina	uid=20364947097122776
```

Reading a table with some parameters:

```bash
yt read --proxy {{prestable-cluster}} --format '<field_separator=";";key_value_separator=":">dsv' '//home/tutorial/staff_unsorted_sample'
```

**Parameters:**

The values are specified in parentheses by default.
- **record_separator** (`\n`) — record separator.
- **key_value_separator** (`=`) — key-value pair separator.
- **field_separator** (`\t`) — separator of fields in a record.
- **line_prefix** (no column by default) — mandatory column at the start of each record.
- **enable_escaping** (`true`) — enable escaping for records.
- **escape_carriage_return** (`false`) — enable escaping for the `\r` character.
- **escaping_symbol** (`\`) — escape character.
- **enable_table_index** (`false`) – enable output of the input table index.
- **table_index_column** (`@table_index`) – name of the column with the table index.
- **enable_string_to_all_conversion** (`false`) — enable conversion of string values to numeric and boolean types. For example, `"42u"` is converted to `42u`, and `"false"` is converted to `%false`.
- **enable_all_to_string_conversion** (`false`) — enable conversion of numeric and boolean values to string type. For example, number `3.14` is converted to `"3.14"`, and `%false` to `"false"`.
- **enable_integral_type_conversion** (`true`) — enable conversion of `uint64` to `int64` and vice versa. If an overflow occurs during conversion, you will see a corresponding error.
- **enable_integral_to_double_conversion** (`false`) — enable conversion of `uint64` and `int64` to `double`. For example, integer `42` is converted to decimal number `42.0`.
- **enable_type_conversion** (`false`) — enable all of the above options. In most cases, this option will suffice.

{% note info "Note" %}

In practice, an arbitrary string is a valid DSV record. If there is no key-value separator after the key, the field is ignored.

{% endnote %}

## SCHEMAFUL_DSV { #schemaful_dsv }

This format is a variation of DSV. With SCHEMAFUL_DSV, data is generated as a set of values separated by tabs.
The format has a mandatory `columns` attribute, in which you specify the names of the columns you need, separated by a semicolon. If the value of one of the columns is missing from at least one of the records, an error occurs (see **missing_value_mode**).

For example, if a table has two records `{a=10;b=11} {c=100}` and the format `<columns=[a]>schemaful_dsv` is specified, the following error occurs — `Column "a" is in schema but missing`.
If only the first record is in the table, the job will receive record `10` as input.

{% note info "Note" %}

When working with {{product-name}} CLI, attributes must be written in quotes. For example: `--format="<columns=[a;b]>schemaful_dsv"`

{% endnote %}

Reading a table in SCHEMAFUL_DSV format:

```bash
yt read --proxy {{prestable-cluster}} --format '<columns=[name;uid]>schemaful_dsv' '//home/tutorial/staff_unsorted_sample'
```

Table representation in SCHEMAFUL_DSV format:

```schemaful_dsv
Elena   95792365232151958
Denis	78086244452810046
Mikhail	70609792906901286
Ilya	15696008603902587
Oxana	76840674253209974
Alexey	15943558469181404
Roman	37865805882228106
Anna	35039450424270744
Nikolai	45320538587295288
Karina	20364947097122776
```

**Parameters:**

The values are specified in parentheses by default.
- **record_separator** (`\n`) — record separator.
- **field_separator** (`\t`) — field separator.
- **enable_table_index** (`false`) – enable printing of table switching in input data.
- **enable_escaping** (`true`) – enable escaping for special characters `\n`, `\t`, and `\`.
- **escaping_symbol** (`\`) – escape character.
- **columns** – list of columns to be formatted, for example `<columns=[columnA;columnB]>schemaful_dsv`.
- **missing_value_mode** (`fail`) — behavior in the absence of a value (`NULL`):
   - `skip_row` — skip a row.
   - `fail` — stop the operation if the specified column is missing in one of the rows.
   - `print_sentinel` — instead of missing values, set `missing_value_sentinel`, whose default is an empty string.
- **missing_value_sentinel** (`empty line`) — value of the missing column for **print_sentinel** mode.
- **enable_column_names_header** (`false`) — can only be used for generating data in SCHEMAFUL_DSV format, but not for parsing such data. If the value is `%true`, the first row will have column names instead of values.
- **enable_string_to_all_conversion** (`false`) — enable conversion of string values to numeric and boolean types. For example, `"42u"` is converted to `42u`, and `"false"` is converted to `%false`.
- **enable_all_to_string_conversion** (`false`) — enable conversion of numeric and boolean values to string type. For example, `3.14` becomes `"3.14"`, and `%false` becomes `"false"`.
- **enable_integral_type_conversion** (`true`) — enable conversion of `uint64` to `int64` and vice versa. This option is enabled by default. If an overflow occurs during conversion, you will see a corresponding error.
- **enable_integral_to_double_conversion** (`false`) — enable conversion of `uint64` and `int64` to `double`. For example, integer `42` becomes decimal number `42.0`.
- **enable_type_conversion** (`false`) — enable all of the above options. In most cases, this option will suffice.

## ARROW { #Arrow }

[Apache Arrow](https://arrow.apache.org/overview) is a binary format developed as a language-agnostic standard for columnar memory representation to facilitate interoperability.

Format advantages:

- Unlike row-based representation, where the data in each row is stored together, Arrow organizes data into columns. This improves the performance of some analytical queries.

- Arrow supports composite data types, such as structs, lists, dictionaries, and others.

### Arrow in {{product-name}}

Reading a table in Arrow format returns a concatenation of multiple [IPC Streaming Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) streams.

```
<SCHEMA>
<DICTIONARY 0>
...
<DICTIONARY k - 1>
<RECORD BATCH 0>
...
<DICTIONARY x DELTA>
...
<DICTIONARY y DELTA>
...
<RECORD BATCH n - 1>
<EOS 0x00000000>
<SCHEMA>
...
<EOS 0x00000000>
...
<EOS 0x00000000>
```

This returns a concatenation of multiple streams instead of a single stream, because {{product-name}} can store the same column differently across different chunks. For example, column data can be stored either explicitly in its entirety or as a dictionary (similar to an [Arrow dictionary](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)). When representing data in Arrow format, we store the encoding as a dictionary and return a concatenation of multiple streams if a column is encoded in its entirety in one chunk and as a dictionary in another.

Limitations and notes:

- Reading and writing is only available for schematized tables.

- For more efficient reading, we recommend reading Arrow tables with columnar chunk storage (`optimize_for = scan`).

{% note warning "Attention" %}

Writing to Arrow isn't currently optimized for tables with columnar chunk storage and involves two-fold conversion of data: from columnar to row-based representation and back.

{% endnote %}

### Types

Table reads currently support the following types:

- `string` is displayed as [arrow::binary](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6BINARYE).
- `integer` is displayed as one of the [arrow::integer](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type5UINT8E) types depending on its
   bit count and signedness.
- `boolean` is displayed as [arrow::bool](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type4BOOLE).
- `float` is displayed as [arrow::float](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type5FLOATE).
- `double` is displayed as [arrow::double](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6DOUBLEE).
- `date` is displayed as [arrow::date32](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6DATE32E).
- `datetime` is displayed as [arrow::date64](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6DATE64E).
- `timestamp` is displayed as [arrow::timestamp](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type9TIMESTAMPE).
- `interval` is displayed as [arrow::int64](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type5INT64E).
- `Complex types` are represented as [arrow::binary](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6BINARYE) with [YSON representation](https://yt.yandex-team.ru/docs/user-guide/storage/data-types#yson) of these types.

Writes support the same types as reads, as well as the following:

- [arrow::list](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type4LISTE) is represented as `list`.
- [arrow::map](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type3MAPE) is represented as `dict`.
- [arrow::struct](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6STRUCTE) is represented as `struct`.

Other notes for working with types:

- When reading a table, almost any type can be returned as [arrow::dictionary](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type10DICTIONARYE), where the dictionary elements are the column values, and the indexes specify the order of those values. For more information, see [this section](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout).

- The Arrow format supports one level of nesting, "optional". Deeper nesting is returned as the binary type during reads, same as for other composite data types.

### Examples of operations

Reading an Arrow table:

{% list tabs %}
- Python

   ```python
   yt.read_table("//path/to/table", format=yt.format.ArrowFormat(), raw=True)
   ```

- CLI

   ```bash
    yt read --table "//path/to/table" --format arrow
   ```
{% endlist %}

Arrow map operation:

```bash
yt map --input-format arrow --output-format arrow  --src "//path/to/input_table" "cat" --dst '<schema=[{name=item_1;type=int64};{name=item_2;type=string}]>//path/to/output_table'
```

### Operations with multiple tables

If you pass multiple tables as input to an Arrow operation, it will output a stream of multiple concatenated IPC Streaming Format streams, where each segment of the stream may belong to one of the tables. The table index is passed as [schema metadata](https://arrow.apache.org/docs/format/Columnar.html#schema-message) under the name `TableId`. Learn more about [Arrow metadata](https://arrow.apache.org/docs/format/Columnar.html#custom-application-metadata).


## PROTOBUF { #PROTOBUF }

Protobuf is a structured data transfer protocol for working with tables in the C++ API.

Learn more about [Protobuf](../../../api/cpp/protobuf.md).
