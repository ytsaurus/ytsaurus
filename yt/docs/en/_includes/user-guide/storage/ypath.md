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
2. **Literals**: The maximum non-empty sequence of non-special characters. Literals allow escaping of the `\<escape-sequence>` form where `<escape-sequence>` can be one of the characters: `\`, `/`, `@`, `&`, `*`, `[`, or `{`. Literals also allow an expression of the `x<hex1><hex2>` form where `<hex1>` and `<hex2>` are hexadecimal digits.

### Syntax and semantics { #simple_ypath_syntax }

YPath is structured as `<root-designator><relative-path>`. Here `<root-designator>` can be of two types:

1. **Cypress root**: `/` token.
   Example: `//home/user`. Here `<root-designator>` is `/` and `<relative-path>` is `/home/user`.
2. **Object root**: A token literal that encodes a string of an `#<id>` form.
   Example: `#1-2-3-4/@type`. Here `<root-designator>` is `#1-2-3-4` and `<relative-path>` is `/@type`.

Thus, `<root-designator>` defines a starting point to which `<relative-path>` applies. The latter is disassembled sequentially from left to right, resulting in tree movement steps of the following types:

- **Transition to descendant**: A sequence of a `/` token and a literal.
   This step type applies to dicts and lists. In case of a dict, the literal must contain the descendant name. Example: `/child`: Transition to a descendant named `child`.
   In case of a list, the literal must contain an integer in the decimal system — the descendant number. Descendants in the list are numbered from zero. Negative numbers that enumerate descendants from the end of the list are also allowed. Example: `/1` is transition to the second descendant in the list, `/-1` is transition to the last descendant in the list.
- **Transition to attribute**: A sequence of `/@` tokens and a literal.
   This type of step is applicable at any point in the path and means transition to an attribute with the given name. Example: `/@attr`: Transition to an attribute named `attr`.

{% note info "Note" %}

In YPath, a relative path starts with a slash. Thus, the slash serves not as a separator as in the case of file systems, but as a start of a command to traverse the metainformation tree. Consequently, the usual string concatenation is sufficient to merge two YPaths.

{% endnote %}

### Examples { #simple_ypath_examples }

```
% Cypress root
/

% The root directory has a home node, it has a user node.
//home/user

% Initial element in the //home/user/list list
//home/user/list/0

% Last element in the //home/user/list list
//home/user/list/-1

% The attr attribute of the //home/user node
//home/user/@attr

% The object with ID 1-2-a-b
#1-2-a-b

% The child descendant of the object with ID 1-2-a-b
#1-2-a-b/child

% The attr attribute of the object with ID 1-2-a-b
#1-2-a-b/@attr
```

### Behavior specifics { #simple_ypath_behavior }

In addition to the above rules, there are some special agreements. Most of them are due to the fact that YPath can identify not only existing metainformation entities, but also new ones created at the moment of execution of the command the argument of which is this YPath.

- **Specify all descendants**: You can use the `remove_node` command to remove all descendants of a dict or list. To do this, use the `*` token instead of the name.
   Example:

   ```
   % Clear the user's home folder
   yt remove //home/user/*
   ```

- **Specify all attributes**: `/@` is a YPath form that designates all attributes of an object. It can be used with read (`get_node`, `list_nodes`) commands to access the full collection of all attributes of an object.

   Examples:

   ```
   % Get the values of all attributes of the user's home folder
   yt get //home/user/@

   % Get the names of all attributes of the user's home folder
   yt list //home/user/@
   ```

The full collection of all attributes can also be *modified* via the `set_node` command. However, one should be warned that doing so will remove all attributes not mentioned in the value being set.

   Examples:

   ```
   % Remove all attributes
   yt set //home/user/@ '{}'

   % Set two attributes and remove all other existing attributes
   yt set //home/user/@ '{attr1=value1; attr2=value2}'
   ```

This way of modifying attributes should not be used when working with Cypress (as opposed to an abstract YSON document) because it may lead to an inadvertent removal of system attributes of a node. Instead, individual attributes should be set.

   Examples:

   ```
   % Set one attribute, do nothing with other attributes
   yt set //home/user/@attr value

   % Set the nested attribute
   yt set //home/user/@attr/some/key value
   ```

- **Specify insertion position in the list**: Use commands that create new nodes (for example, `set_node`) to specify the position of the node to be created in the list in relation to existing ones. To do this, use the special `begin` (beginning of the list), `end` (end of the list), `before:<index>` (position before the descendant with the `<index>` number), and `after:<index>` (position after the descendant with the `<index>` number) strings. Examples:

   ```
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

   ```
   % Find the ID of the link object itself, not the object the link points to
   yt get //home/user/link&/@id
   ```

## Rich YPath { #rich_ypath }

**Rich** YPath is an YPath extended with additional annotations. These annotations can be specified in two ways: either as **attributes** (key-value pairs) or **syntactically** by adding special prefixes and suffixes to a simple YPath, which turns it into the `<prefix><simple-ypath><suffix>` string.

If there is no `<prefix>` and `<suffix>`, rich  YPath can have, for example, the following form:

```
<
  append = %true;
  compression_codec = lz4;
>
//home/user/table
```

This path points to the user's home folder and has two attributes.

An arbitrary set of attributes can be specified on any path. Their exact meaning depends on how this path is used in the application. For example, when writing to a table, it is natural to support the `append` attribute. For input tables of some mapreduce operations, you can specify the `foreign` flag, which means that this table is secondary (a reference table), so the system needs to perform join of a special form.

Attributes whose names are unknown to the system are ignored by the system, but still may be interpreted by the application.

The path prefix and suffix enable you to encode attributes in a more compact form. Both of these parts use [YSON](../../../user-guide/storage/yson.md). Learn more about them:

### Prefix { #rich_ypath_prefix }

The `<prefix>` part is either empty or sets attributes as a map-fragment in [YSON](../../../user-guide/storage/yson.md) format.
Example: `<append=%true>//home/user/table` sets the path to the table in the user's home folder and also passes the additional `append` attribute to us.

In addition to this attribute map, there is a shorthand to specify `cluster` attribute: `my-cluster://path/to/object`. This shorthand is equivalent to specifying `cluster` manually in the attribute map: `<cluster="my-cluster">//path/to/object`.

If both `cluster` attribute and this shorthand are used, the shorthand has the preference.

The main purpose of the prefix is to make it easier to pass attributes when using commands from the console.

### Suffix { #rich_ypath_suffix }

The `<suffix>`part is either empty or indicates columns and/or ranges of rows in the referenced table.

The column selection modifier, if any, is always specified first, followed by the row selection modifier, if there is one.

The **column selection modifier** is a comma-separated set of column names, specified in curly brackets. Each name is represented by a YSON string.

Examples of column selection modifiers: `{a}`, `{a,b}`, and `{}` .

The formal grammar of column selection modifiers:

```
<column-selector> = '{' { <column-selector-item> ',' } [ <column-selector-item> ] '}'
<column-selector-item> = <string>
```

The **row selection modifier** is a comma-separated set of table row ranges, specified in square brackets. Each range is a pair of lower- and upper-range boundaries separated by a colon. Either or both boundaries may be missing. In that case, the range extends without a boundary on that side.
The boundary can either be a number with the `#` symbol appended to the left (in this case, it is the row number in the table) or a list of values (in this case, it is a boundary on the values of the key columns by which the table is sorted). The specified list can be shorter than the number of fields by which the table is sorted. If the list of columns contains just one element, that element does not have to be enclosed in parentheses. That element, however, must be a valid YSON value of scalar type: `int64`, `uint64`, `string`, `double`, or `bool`.

For information about the modifier semantics, see [this section](#key_selectors).

Examples of row selection modifiers: `[:]`, `[#10:#100]`, `[a:m]`, `[(abc,8):(xyz,5)]`, `[:5.0,10.0:]`, `[#10]`, `[a,#100,#1:#2]`, and `[100u:200u]`. In the latter case, the data contains unsigned numbers.

The formal grammar of row selection modifiers:

```
<row-selector> = '[' { <row-range> ',' } [ <row-range> ] ']'

<row-range> = <row-index-range-selector> | <row-key-range-selector> | <row-index> | <row-composite-key> | ''

<row-index-range-selector> = [ <row-index> ] ':' [ <row-index> ]

<row-index> = '#' <int64>

<row-key-range-selector> = [ <row-composite-key> ] ':' [ <row-composite-key> ]

<row-composite-key> = <row-key> | <row-key-tuple>

<row-key-tuple> = '(' { <row-key> ',' } [ <row-key> ] ')'

<row-key> = <string> | <int64> | <uint64> | <bool> | <double>
```

### Examples { #rich_ypath_examples }

{% note info "Note" %}

Rich paths oftentimes need to be enclosed in single quotes to be parsed by the shell properly.

{% endnote %}

```
% Read the entire '//home/user/table' table
yt read '//home/user/table'

% Read all rows of the '//home/user/table' table without selecting any columns
yt read '//home/user/table{}'

% Read column 'a'
yt read '//home/user/table{a}'

% Read columns 'a' and 'b'
yt read '//home/user/table{a,b}'

% Read the first 100 rows of the '//home/user/table' table and select all columns
yt read '//home/user/table[#0:#100]'

% Read all rows, skipping the first 100
yt read '//home/user/table[#100:]'

% Read row 100 (counting from 0)
yt read '//home/user/table[#100]'

% Read rows with the 'abc' key (or the first key coordinate)
yt read '//home/user/table["abc":"abc\x00"]'

% The query above can also be expressed in a more concise way:
yt read '//home/user/table["abc"]'

% Read rows with keys no smaller than '(a,1)'
yt read '//home/user/table[(a,1):]'

% Read rows with the first two key components equal to 'a' and '1', respectively
yt read '//home/user/table[(a,1)]'

% Read rows where the first key is 'a' and the second key starts with 'b'. If the second key is 'b', then the third key must be no smaller than '1'.
yt read '//home/user/table[(a,b,1):(a,c)]'

% Read rows where the first key starts with 'a' and rows where the first key is 'b' and the second is smaller than '1'.
yt read '//home/user/table[a:(b,1)]'

% Read rows where the first key starts with 'a', rows where the first key is 'b' and the second is smaller than '1', and rows where the first key is 'b', the second is '1', and the third has a negative value of the double type. Select columns 'ab', 'ac'
yt read '//home/user/table{ab,ac}[a:(b,1,0.0)]'

% Convert table1 with columns a,b,c,d to table2 with columns a,b,c and sort by fields a,b
yt sort --src '//home/users/table1{a,b,c}' --dst //home/users/table2 --sort-by a --sort-by b --spec '{schema_inference_mode = from_output}'

% Convert table1 with columns a,b,c,d to table2 with columns a,b,c
yt merge --src '//home/users/table1{a,b,c}' --dst //home/users/table2 --spec '{schema_inference_mode = from_output}'
```

## YPath canonical form { #ypath_canonical_form }

[Rich YPath](#rich_ypath) can be brought into **canonical form**. In canonical form, there is no prefix and suffix. Instead, all auxiliary information is explicitly put in the attributes assigned to the [simple YPath](#simple_ypath).

Example: after canonicalizing the `<append=true>//home/user/table[#10:#20]` path, you get the following YSON structure:

```
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

Internally, the {{product-name}} system prefers to work with the canonical form of paths. Canonical form is larger, so the system allows to use the more convenient form with a prefix and suffix in the input paths:

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

The `parse_ypath` driver [command](../../../api/commands.md#parse_ypath) canonicalizes a rich YPath.

## Supported attributes { #known_attributes }

[Rich YPath](#rich_ypath) can be annotated with arbitrary attributes, allowing for expansion. The table contains a list of attributes recognized by the system.

| **Attribute** | **Type** | **Description** |
| ------------------- | ----------------------------- | ------------------------------------------------------------ |
| `append` | `bool` | Recognized by the data write commands (`write_table` and `write_file`) and means that new data is added to the end of the existing data, but does not overwrite it. It is also recognized on the output paths of tables in the specifications of all scheduler operations and has similar semantics. |
| `sorted_by` | `array<string>` | Recognized by the table data write command (`write_table`). By specifying this attribute, the user promises that the written data is ordered by the specified set of columns. The system checks this property. The keys must go in non-descending order. When new data is being written, it is also checked that the first key to be written is no smaller than the last existing one. The attribute is also recognized by the system on the output paths of tables in the specifications of all scheduler operations and has similar semantics. In the latter case, it is also additionally required that the ranges of keys generated by different jobs have no intersections on the inner point. Intersections on the boundaries are allowed. |
| `ranges` | `array<ReadRange>` | Recognized by the data read commands (`read_table` and `read_file`) and specifies a list of ranges for reading. If it is missing, the entire dataset is read. It is also recognized on the input paths of tables in the specifications of all scheduler operations. A description of the `ReadRange` dict structure is given in the table below. |
| `columns` | `array<string>` | Recognized by the table data read command (`read_table`) and specifies a list of names of readable columns. If it is missing, all columns are read. It is also recognized on the input paths of tables in the specifications of all scheduler operations. |
| `optimize_for` | `string` | Sets the storage format for the created table. The attribute can be specified with the `write_table` command and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `compression_codec` | `string` | Sets the compression format for the created table. The attribute can be specified with the `write_file` and `write_table` commands and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `erasure_codec` | `string` | Enables erasure coding for the created table. The attribute can be specified with the `write_file` and `write_table` commands and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `schema` | `yson-list` with a valid schema | Validates data with respect to the specified schema and places this schema on the table at the end of the operation. The attribute can be specified with the `write_table` command and on the output paths of operations. This attribute **is not compatible** with `<append=true>`. |
| `transaction_id` | `string` of the id form | Specifies to the scheduler under which transaction the input table should be accessed if this attribute is specified on it. |
| `rename_columns` | `yson-map` | Recognized on the input paths of tables. Renames the column names according to the mapping before feeding the table to the input of the operation. This attribute **is not compatible** with `teleport` tables. |
| `cluster` | `string` | Recognized on the input paths of tables. Makes the scheduler to treat the table as a remote table from another cluster.

The `ranges` attribute specifies which ranges of table rows should be read. The specified ranges will be read sequentially, in the order in which they are specified in the attribute. If the ranges overlap, the output will be repeated.

| **Attribute** | **Type** | **Description** |
| ------------- | ----------- | ------------------------------------------------------------ |
| `lower_limit` | `ReadLimit` | Lower read limit |
| `upper_limit` | `ReadLimit` | Upper read limit |
| `exact` | `ReadLimit` | Exact read limit value (cannot be specified with lower_limit or upper_limit) |

All limit types except for `key_bound` are inclusive in the `lower_limit` attribute and exclusive in the `upper_limit` attribute. Whether a `key_bound` limit is inclusive or exclusive is specified explicitly within the key bound itself. Learn more in the section on [slices by key](#key_selectors).

`ReadLimit` describes a separate read limit. Limits can be defined using one of the six **selectors**: key, key bound, row index, chunk index, tablet index, or byte offset. You can specify multiple selectors for a single limit. In this case, they all apply together, reinforcing each other.

| **Attribute** | **Type** | **Description** |
| ----------- | ------- | ---------------------------------------------------- |
| `key` | `list` | List of values that form the key (for sorted tables only) Learn more in [this section](#key_selectors) |
| `key_bound` | `list` | Inclusive or exclusive key prefix limit. Cannot be used with exact limits. Learn more in [this section](#key_selectors) |
| `row_index` | `int` | Row number (for tables only) |
| `offset` | `int` | Byte offset (for files only) |
| `chunk_index` | `int` | Chunk index |
| `tablet_index` | `int` | Tablet index (for ordered dynamic tables only) |

YPath attributes on the paths of **files** supported by the {{product-name}} system

| **Attribute** | **Type** | **Description** |
| ----------------------- | ----------------------------- | ------------------------------------------------------------ |
| `file_name` | `string` | Recognized when ordering a file in the `sandbox` of the job. The attribute specifies the relative path on which to place the file. |
| `executable` | `bool` | Recognized when ordering a file in the `sandbox` of the job. The attribute tells the system to set an executable bit on the ordered file. |
| `bypass_artifact_cache` | `bool` | Recognized when ordering a file in the `sandbox` of the job. The attribute enables direct file download to the `sandbox` of the job, bypassing the file cache on the node. |
| `format` | `string` | Recognized when ordering a table file in the `sandbox` of the job. The attribute specifies the format in which the table should be formatted when it is downloaded to the `sandbox` of the job. |


## Sorted table slices by key { #key_selectors }

The system supports two types of selectors based on key column values. These selectors appear similar but differ in behavior. The first type is historically older and is implemented by the `key` selector. You can also specify it in the [YPath suffix](#rich_ypath_suffix). The second type is newer and is implemented by the `key_bound` selector. It is only available via the `ranges` attribute in the YPath prefix.

For new applications, we recommend using the `key_bound` selector because of its more transparent behavior and greater flexibility in use, even though it is more verbose.

{% note info "Note" %}

As a reminder, {{product-name}} defines a linear order for all possible values that may occur in a table. Specifically, int64, uint64, double, and boolean values are ordered naturally within their respective types, while string values are ordered lexicographically. Two values of different data types are compared according to the implementation-defined rules for how the types themselves are compared. For example, any int64 is less than any uint64, and any null is less than any non-null (`#`) value.


Because of this, you should always pay attention to the respective data types when comparing values. Specifically, unsigned integers must be specified with the suffix `u`, and fractions should have a decimal point.

{% endnote %}

### `key` selector

The `key` selector can only be used for sorted tables where all key columns are sorted in ascending order. The `key` selector's type is a list of values, with its behavior defined as follows.

Let's examine various tuples consisting of {{product-name}} type system values. We will define a linear lexicographic order for these tuples. For example, as follows:

`[] < ["a"] < ["a"; 3] < ["a"; 4] < ["b"] < ["b"; 2]`

Using this order, we can compare two tuples of any length. The comparison result is determined by the first differing tuple components, provided they exist. If not, the shorter tuple is considered smaller.

If the `key` selector is specified as part of the `lower_limit` attribute (or as a lower limit preceding the colon in a row selection modifier within the YPath suffix), it only returns those rows where the **full** key, represented as a tuple, is greater than or equal to the `key` tuple based on the comparison logic described above. This means that the definition of the `key` selector remains valid even if it includes a tuple with fewer or more values than the number of key columns in the table.

If the `key` selector is specified as part of the `upper_limit` attribute (or as an upper limit following the colon in a row selection modifier within the YPath suffix), it only returns those rows where the **full** key, represented as a tuple, is **strictly** smaller than the `key` tuple.

If the `key` selector is specified as part of the `exact` attribute (or as the only limit in a colonless row selection modifier within the YPath suffix), it only returns those rows where the full key contains the `key` tuple as its prefix.

#### Examples

Suppose the `//tmp/t` table has two key columns and contains the following keys:

```
["a"; 1];
["a"; 3];
["a"; 5];
["b"; 2];
["b"; 4];
["c"; 0];
```

The `//tmp/t[(b):]` slice, also expressed as `<ranges = [{lower_limit = {key = [b]}}]>//tmp/t`:
```
["b"; 2];
["b"; 4];
["c"; 0];
```

The `//tmp/t[(b, 2, 56):]` slice (note that a key within a slice can have more values than there are key columns present in the table):
```
["b"; 4];
["c"; 0];
```

The `//tmp/t[:(c, 0)]` slice:
```
["a"; 1];
["a"; 3];
["a"; 5];
["b"; 2];
["b"; 4];
```

The `//tmp/t[(b)]` slice:
```
["b"; 2];
["b"; 4];
```

{% note info "Note" %}

Obtaining an exclusive lower limit or an inclusive upper limit using the `key` selector requires specifying the tuple's successor according to the linear comparison rules defined for the tuples above, rather than the tuple itself. Doing so can often be challenging. For this reason, the service introduced the `key_bound` selector, which is detailed in the following section.

{% endnote %}

### `key_bound` selector

The `key_bound` selector is available for use in sorted tables where key columns may be sorted in both descending and ascending order. The value of the `key_bound` selector is a list containing two values. The first one specifies the **relation**, which is represented by one of the four strings — `">", ">=", "<", "<="`, — while the second one is a list of values that defines the key **prefix**. The `>` and `>=` relational operators can only occur in the `lower_limit` attribute, while `<` and `<=` only occur in `upper_limit`. The `exact` attribute does not allow `key_bound` selectors.

Examples of `key_bound` selectors:
- `[">"; ["a"; 3]]`
- `["<="; ["b"]]`
- `[">="; []]`

Let's examine the semantics of this selector. Consider a table with `N` key columns and a key bound. Suppose that the length of the key bound prefix is `K`. To apply the key bound to this table, `K` must be between 0 and `N`. Attempting to specify a longer prefix in the key bound will result in an error.

To determine if a row with a specific key matches the selector, we need to extract a prefix of length `K` from that key and perform a lexicographic comparison of the resulting value against the key bound prefix. If the result of the comparison matches the specified relation, the selector returns the row, otherwise it does not.

{% note info "Note" %}

If a key column's `sort_order = descending`, the results of all comparisons involving the corresponding key tuple components are inverted. For instance, in this case, the relations `5u < 2u` and `"foo" < #` are true.

Also note that the behavior of the `key_bound` selector only defines the comparison of tuples of the same length (represented as `K`), so defining the comparison of tuples of different lengths with sorting in descending order is not necessary.

{% endnote %}

Specifically, according to the definition above, the selectors `[">="; []]` and `["<="; []]` match any row because the tuple `[]` corresponds to a key prefix of length 0, while the selectors `[">"; []]` and `["<"; []]` do not match any rows.

#### Examples

Let's examine the `//tmp/t` table with the keys from the example above:
```
["a"; 1];
["a"; 3];
["a"; 5];
["b"; 2];
["b"; 4];
["c"; 0];
```

The `<ranges = [{lower_limit = {key_bound = [">"; ["a"; 3]]}}]>//tmp/t` slice:
```
["a"; 5];
["b"; 2];
["b"; 4];
["c"; 0];
```

The `<ranges = [{upper_limit = {key_bound = ["<="; ["b"]]}}]>//tmp/t` slice:
```
["a"; 1];
["a"; 3];
["a"; 5];
["b"; 2];
["b"; 4];
```

{% note info "Note" %}

Note that although the tuple `["b"; 2]` is lexicographically greater than the tuple `["b"]`, truncating the key `["b"; 2]` to a length of 1 transforms it into the tuple `["b"]`, allowing it to match the key bound `[“<=”; [“b”]]`. This behavior of the `key_bound` selector differs from that of the `key` selector, providing the former with greater flexibility, since specifying a similar slice using the `key` selector is a non-trivial task.

{% endnote %}

The `<ranges = [{lower_limits = {key_bound = [">="; ["b"; 2; 56]]}}]>//tmp/t` slice is not valid, and attempting to use it results in the error: `Key bound length must not exceed schema key prefix length`.
