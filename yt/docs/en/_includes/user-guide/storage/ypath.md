# YPATH

This section contains information about YPath, a language that describes paths to objects in the {{product-name}} system.

YPath is a language for describing paths that identify objects in the {{product-name}} system. The language enables you to refer to nodes and specify annotations that may be useful when performing operations on nodes, such as writing to a table. You can use annotations to determine whether new data will be added to the end of the table or whether the table will be completely overwritten.

For example:

- `//home/user/table`: The path to `table` in the user's home folder.
- `#0-25-3ec012f-406daf5c/@type`: The path to the `type` attribute of the object with the `0-25-3ec012f-406daf5c` ID.
- `//home/user/table[#10:#20]`: Rows 10 through 19 of `table` in the user's home folder.

There are several types of YPath. In the simplest case, YPath looks like a filesystem-like path, starting with a double slash `//`.

## Simple YPath { #simple_ypath }

### Lexis { #simple_ypath_lexis }

The string that encodes simple YPath is split into the following **tokens**:

1. **Special characters**: Forward slash (`/`), at sign (`@`), ampersand (`&`), and asterisk (`*`).
2. **Literals**: Maximum non-empty sequence of non-special characters. Literals allow escaping of the `\<escape-sequence>` form where `<escape-sequence>` can be one of the characters: `\`, `/`, `@`, `&`, `*`, `[`, or `{`. Literals also allow an expression of the `x<hex1><hex2>` form where `<hex1>` and `<hex2>` are hexadecimal digits.

### Syntax and semantics { #simple_ypath_syntax }

YPath is structured as `<root-designator><relative-path>`. Here `<root-designator>` can be of two types:

1. **Cypress root**: `/` token.
   Example: `//home/user`. Here `<root-designator>` is `/` and `<relative-path>` is `/home/user`.
2. **Object root**: A token literal that encodes a string of an `#<id>` form.
   Example: `#1-2-3-4/@type`. Here `<root-designator>` is `#1-2-3-4` and `<relative-path>` is `/@type`.

Thus, `<root-designator>` defines a starting point to which `<relative-path>` applies. The latter is disassembled sequentially from left to right, resulting in tree movement steps of the following types:

- **Transition to descendant**: A sequence of a `/` token and a literal.
   This step type applies to dicts and lists. In case of a dict, the literal must contain the descendant name. Example: `/child`: Transition to a descendant named `child`.
   In case of a list, the literal must contain an integer in decimal system — the descendant number. Descendants in the list are numbered from zero. Negative numbers that enumerate descendants from the end of the list are also allowed. Example: `/1` is transition to the second descendant in the list, `/-1` is transition to the last descendant in the list.
- **Transition to attribute**: A sequence of `/@` tokens and a literal.
   This type of step is applicable at any point in the path and means transition to an attribute with the given name. Example: `/@attr`: Transition to an attribute named `attr`.

{% note info "Note" %}

In YPath, a relative path starts with a slash. Thus, the slash serves not as a separator as in the case of file systems, but as a start a command to move in the tree. In particular, the usual string concatenation is sufficient to merge two YPaths.

{% endnote %}

### Examples { #simple_ypath_examples }

```json
% Cypress root
/

% The root directory has a home node, it has a user node.
//home/user

% Initial element in the //home/user/list  list
//home/user/list/0

% Last element in the //home/user/list list
//home/user/list/-1

% The attr attribute of the //home/user node
//home/user/@attr

% The object with the 1-2-a-b ID
#1-2-a-b

% The child descendant of the object with the 1-2-a-b ID
#1-2-a-b/child

% The attr attribute of the object with the 1-2-a-b ID
#1-2-a-b/@attr
```

### Behavior specifics { #simple_ypath_behavior }

In addition to the rules above, there are a number of special rules. Most of them are due to the fact that YPath can identify not only existing metainformation entities, but also new ones created at the moment of execution of the command the argument of which is this YPath.

- **Specify all descendants**: You can use the `remove_node` command to remove all descendants of a dict or list. To do this, use the `*` token instead of the name.
   Example:

   ```json
   % Clear the user's home folder
   yt remove //home/user/*
   ```

- **Specify all attributes**: `/@` is a YPath form that designates all attributes of an object. It can be used with read (`get_node`, `list_nodes`) and change (`set_node`) commands to access the full collection of all attributes of an object. However, changes via `set_node` apply only to custom attributes.

   Examples:

   ```json
   % Get the values of all attributes of the user's home folder
   yt get //home/user/@

   % Get the names of all attributes of the user's home folder
   yt list //home/user/@

   % Remove all custom attributes
   yt set //home/user/@ '{}'

   % Set one custom attribute and remove all other existing custom attributes
   yt set //home/user/@ '{attr=value}'

   % Set one custom attribute, do nothing with other custom attributes
   yt set //home/user/@attr value

   % Set the nested custom attribute
   yt set //home/user/@attr/some/key value
   ```

- **Specify insertion position in the list**: Use commands that create new nodes (for example, `set_node`) to specify the position of the node to be created in the list in relation to existing ones. To do this, use the special `begin` (beginning of the list), `end` (end of the list), `before:<index>` (position before the descendant with the `<index>` number), and `after:<index>` (position after the descendant with the `<index>` number) strings. Examples:

   ```json
   % Add the "value" element to the end of the //home/user/list list
   yt set //home/user/list/end value

   % Insert the "value" element at the beginning of the //home/user/list list
   yt set //home/user/list/before:0 value
   % Or
   yt set //home/user/list/begin value

   % Insert the "value" element before the second element in the //home/user/list list
   yt set //home/user/list/before:2 value

   % Insert the "value" element after the fifth element in the //home/user/list list
   yt set //home/user/list/after:5 value
   ```

- **Disable redirection**: The `&` token can be used to suppress redirections via a [symbolic link](../../../user-guide/storage/links.md).
   Examples:

   ```json
   % Find the ID of the link object itself, not the object the link points to
   yt get //home/user/link&/@id
   ```

## Rich YPath { #rich_ypath }

**Rich** (**rich**) YPath is an extended YPath that comes with additional annotations in the string form. These annotations can be specified in two ways: either as **attributes** (key-value pairs) or **syntactically** by adding special prefixes and suffixes to a simple YPath, which turns it into the `<prefix><simple-ypath><suffix>` string.

If there is no `<prefix>` and `<suffix>`, rich  YPath can have, for example, the following form:

```json
<
  append = %true;
  compression_codec = lz4;
>
//home/user/table
```

This path points to the user's home folder and has two attributes.

An arbitrary set of attributes can be specified on any path. Their exact meaning depends on how this path is used in the application. For example, for an operation that writes to a table, it is natural to support the `append` attribute.  For input tables of some mapreduce operations, you can specify the `foreign` flag which means that this table is secondary (a reference table), so the system needs to perform join of a special form.

Attributes whose names are unknown to the system are ignored by the system, but still may be interpreted by the application.

The path prefix and suffix enable you to encode attributes in a more compact form. Both of these parts use [YSON](../../../user-guide/storage/yson.md). Learn more about them:

### Prefix { #rich_ypath_prefix }

The `<prefix>` part is either empty or sets attributes as a map-fragment in YSON format.
Example: `<append=%true>//home/user/table` sets the path to the table in the user's home folder and also passes the additional `append` attribute to us.

The main purpose of the prefix is to make it easier to pass attributes when using commands from the console.

### Suffix { #rich_ypath_suffix }

The `<suffix>`part is either empty or indicates columns and/or ranges of rows in the referenced table.

The column selection modifier, if any, is always specified first, followed by the row selection modifier, if there is one.

The **column selection modifier** is a comma-separated set of column names in curly brackets. Each name is represented by a YSON string.

Examples of column selection modifiers: `{a}`, `{a,b}`, and `{}` .

The formal grammar of column selection modifiers:

```json
<column-selector> = '{' { <column-selector-item> ',' } [ <column-selector-item> ] '}'
<column-selector-item> = <string>
```
The **row selection modifier** is a comma-separated set of table row ranges, specified in square brackets. Each range is a pair of lower- and upper-range boundaries separated by a colon. Either or both boundaries may be missing - in that case, the range extends without a boundary on that side.
A range may only include one row - in this case, that exact row is specified instead of a pair of boundaries.
The boundary can either be a number with the `#` symbol appended to the left (in this case, it is the row number in the table) or a list of values (in this case, it is a boundary on the values of the key columns by which the table is sorted). The specified list can be shorter than the number of fields by which the table is sorted. In this case, only the columns that are the key prefix are considered. It is not allowed to specify the set of columns that does not form a prefix of the table key. If the list of columns contains just one element, that element does not have to be enclosed into parenthesis. That element, however, must be a valid YSON value of scalar type: `int64`, `uint64`, `string`, `double`, or `bool`.

{% note info "Note" %}

Note that from the point of view of the system, `int64` and `uint64` are different, uncomparable, types. If the table contains data of the `uint64` type, you also need to specify unsigned limits with the `u` suffix when selecting rows.

{% endnote %}

Examples of row selection modifiers: `[:]`, `[#10:#100]`, `[a:m]`, `[(abc,8):(xyz,5)]`, `[:5.0,10.0:]`, `[#10]`, `[a,#100,#1:#2]`, and `[100u:200u]`. In the latter case, the data contains unsigned numbers.

The formal grammar of row selection modifiers:

```json
<row-selector> = '[' { <row-range> ',' } <row-range> ] ']'

<row-range> = <row-index-range-selector> | <row-key-range-selector> | <row-index> | <row-composite-key> | ''

<row-index-range-selector> = [ <row-index> ] ':' [ <row-index> ]

<row-index> = '#' <int64>

<row-key-range-selector> = [ <row-composite-key> ] ':' [ <row-composite-key> ]

<row-composite-key> = <row-key> | <row-key-tuple>

<row-key-tuple> = '(' { <row-key> ',' } [ <row-key> ] ')'

<row-key> = <string> | <int64> | <uint64> | <bool> | <double>
```

{% note info "Note" %}

Composite keys are compared lexicographically. First, the first element is compared. If it matches, the second element is compared, and so on. If the key `A` is an element-wise prefix of key `B`, then, by convention, `A <= B`. Within each type, values are compared in a natural way. Comparing values of different types depends only on the types, not on the values. The order of types is not specified ( `0 < 1`, `0.0 < 1.0`, but you cannot rely on `0.0 < 1`).

{% endnote %}

### Examples { #rich_ypath_examples }

{% note info "Note" %}

Rich paths should be in single quotes for proper parsing in bash. Quotes are omitted below.

{% endnote %}

```json
% Read the entire '//home/user/table' table
yt read //home/user/table

% Read all rows of the '//home/user/table' table, but select no columns
yt read //home/user/table{}

% Read the 'a' column
yt read //home/user/table{a}

% Read the 'a', 'b' columns
yt read //home/user/table{a,b}

% Read the first 100 rows of the '//home/user/table' table (and select all columns)
yt read //home/user/table[#0:#100]

% Read all rows, skipping the first 100
yt read //home/user/table[#100:]

% Read row number 100 (counting from 0)
yt read //home/user/table[#100]

% Read rows with the 'abc' key (or the first key coordinate)
yt read //home/user/table["abc":"abc\x00"]

% The request above can also be expressed shorter:
yt read //home/user/table["abc"]

% Read rows with keys no smaller than '(a,1)'
yt read //home/user/table[(a,1):]

% Read rows with the first two key components equal to 'a' and '1', respectively
yt read //home/user/table[(a,1)]

% Read rows with the first 'a' key and the second key starting with 'b', and if the second key is 'b', then the third key is no smaller than '1'.
yt read //home/user/table[(a,b,1):(a,c)]

% Read rows with the first key starting with 'a' and rows with the first 'b' key and the second key no smaller than '1'.
yt read //home/user/table[a:(b,1)]

% Read rows with the first key starting with 'a', rows with the first 'b' key and the second key smaller than '1', and rows with the first 'b' key, the second '1' key, and the third key that is negative of the double type. Select the 'ab', 'ac' columns
yt read //home/user/table{ab,ac}[a:(b,1,0.0)]

% Convert table1 with the a,b,c,d columns into table2 with the a,b,c columns and sorting by the a,b fields
yt sort --src "//home/users/table1{a,b,c}" --dst //home/users/table2 --sort-by "a" --sort-by "b" --spec '{schema_inference_mode = from_output}'

% Convert table1 with the a,b,c,d columns into table2 with the a,b,c columns
yt merge --src "//home/users/table1{a,b,c}" --dst //home/users/table2 --spec '{schema_inference_mode = from_output}'
```

## YPath canonical form { #ypath_canonical_form }

[Rich YPath](#rich_ypath) can be brought into **canonical form**. In the canonical form, there is no prefix and suffix. Instead, all auxiliary information is explicitly put in the attributes assigned to the [simple YPath](#simple_ypath).

Example: after canonicalizing the `<append=true>//home/user/table[#10:#20]` path, you get the following YSON structure:

```json
<
  append = true;
  ranges = [
    {
      lower_limit = {row_index = 10};
      upper_limit = {row_index = 20}
    }
  ]
>
"//home/user/table"
```

Internally, the {{product-name}} system prefers to work with the canonical form of paths. Canonical form is larger, so it is often more convenient to work with a form where both the prefix and the suffix are allowed. The more convenient form will still be accepted by the system as input.

```
<append=true>//home/user/table[#10:#20]
```
Prefix: `<append=true>`.

Suffix: `[#10:#20]`.

This will be converted to a more verbose string inside the system:

```
  ranges = [
    {
      lower_limit = {row_index = 10};
      upper_limit = {row_index = 20}
    }
  ]
```

A `parse_ypath` command of the driver canonicalizes a rich YPath.

## Supported attributes { #known_attributes }

[Rich YPath](#rich_ypath) can be annotated with arbitrary attributes, allowing for expansion. The table contains a list of attributes recognized by the system.

| **Attribute** | **Type** | **Description** |
| ------------------- | ----------------------------- | ------------------------------------------------------------ |
| `append` | `bool` | Recognized by the data write commands (`write_table` and `write_file`) and means that new data is added to the end of the existing data, but does not overwrite it. It is also recognized on the output paths of tables in the specifications of all scheduler operations and has similar semantics. |
| `sorted_by` | `array<string>` | Recognized by the table data write command (`write_table`). By specifying this attribute, the user promises that the written data is ordered by the specified set of columns. The system checks this property. The keys must go in a non-descending order. When new data is being written, it is also checked that the first key to be written is no smaller than the last existing one. The attribute is also recognized by the system on the output paths of tables in the specifications of all scheduler operations and has similar semantics. In the latter case, it is also additionally required that the ranges of keys generated by different jobs have no intersections on the inner point. Intersections on the boundaries are allowed. |
| `ranges` | `array<ReadRange>` | It is recognized by the data read commands (`read_table` and `read_file`) and specifies a list of ranges for reading. If it is missing, the entire dataset is read. It is also recognized on the input paths of tables in the specifications of all scheduler operations. A description of the `ReadRange` dict structure is given in the table below. |
| `columns` | `array<string>` | Recognized by the table data read command (`read_table`) and specifies a list of names of readable columns. If it is missing, all columns are read. It is also recognized on the input paths of tables in the specifications of all scheduler operations. |
| `optimize_for` | `string` | Sets the storage format for the created table. The attribute can be specified with the `write_table` command and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `compression_codec` | `string` | Sets the compression format for the created table. The attribute can be specified with the `write_file` and `write_table` commands and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `erasure_codec` | `string` | Enables erasure coding for the created table. The attribute can be specified with the `write_file` and `write_table` commands and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `schema` | `yson-list` with a valid schema | Validates data with respect to the specified schema and places this schema on the table at the end of the operation. The attribute can be specified with the `write_table` command and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `transaction_id` | `string` of the id form | Specifies to the scheduler under which transaction the input table should be accessed if this attribute is specified on it. |
| `rename_columns` | `yson-map` | Recognized on the input paths of tables. Renames the column names according to the mapping before feeding the table to the input of the operation. This attribute **is not compatible** with `teleport` tables. |

The `ranges` attribute specifies which ranges of table rows should be read. The specified ranges will be read sequentially, in the order in which they are specified in the attribute. In particular, if some specified ranges overlap, the output will be repeated.

| **Attribute** | **Type** | **Description** |
| ------------- | ----------- | ------------------------------------------------------------ |
| `lower_limit` | `ReadLimit` | Lower read limit (inclusive) |
| `upper_limit` | `ReadLimit` | Upper read limit (not inclusive) |
| `exact` | `ReadLimit` | Exact read limit value (cannot be specified with lower_limit or upper_limit) |

`ReadLimit` describes a separate read limit. For sorted tables, the limit can be defined by a key. Its length does not have to be equal to the number of key columns in the table. A lexicographic comparison of sequences will occur instead. The limit can also be defined by a row number (for tables and logs only), a chunk number, or a byte offset (for files only). An arbitrary number of parameters can be specified in the limit. They all apply concurrently, reinforcing each other, that is, reducing the range of selectable values.

| **Attribute** | **Type** | **Description** |
| ----------- | ------- | ---------------------------------------------------- |
| `key` | `list` | List of values that form the key (for tables only) |
| `row_index` | `int` | Row number (for tables only) |
| `offset` | `int` | Byte offset (for files only) |

YPath attributes on the paths of **files** supported by the {{product-name}} system

| **Attribute** | **Type** | **Description** |
| ----------------------- | ----------------------------- | ------------------------------------------------------------ |
| `file_name` | `string` | Recognized when ordering a file in the `sandbox` of the job. The attribute specifies the relative path on which to place the file. |
| `executable` | `bool` | Recognized when ordering a file in the `sandbox` of the job. The attribute tells the system to set an executable bit on the ordered file. |
| `bypass_artifact_cache` | `bool` | Recognized when ordering a file in the `sandbox` of the job. The attribute enables direct file download to the `sandbox` of the job, bypassing the file cache on the node. |
| `format` | `string` | Recognized when ordering a table file in the `sandbox` of the job. The attribute specifies the format in which the table should be formatted when it is downloaded to the `sandbox` of the job. |
