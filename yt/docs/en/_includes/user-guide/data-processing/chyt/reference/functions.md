# Functions

This section describes CHYT-specific functions.

## Functions for working with YSON { #yson_functions }

### YPathType (simple types and arrays) { #ypath_simple_types }
`YPath<Type>[Strict](yson, ypath)`: Extracts a `Type` type value at `ypath` from the string `yson`.

`Type` can be one of the following: `Int64`, `UInt64`, `Boolean`, `Double`, `String`, `ArrayInt64`, `ArrayUInt64`, `ArrayBoolean`, or `ArrayDouble`.

The optional `Strict` suffix indicates whether the function is strict.

Examples of possible function names: `YPathInt64`, `YPathStringStrict`, `YPathArrayDouble`.

These functions work as follows:

1. The passed `yson` string is interpreted as a YSON document.
2. The document is addressed at `ypath` relative to the document root.
3. The contents of the document are then interpreted at `ypath`, which points to the `Type` type.

Errors may occur at each step of this function operation:

1. The string may not be a YSON document (this is possible when a string constant is specified as a function argument, but not when reading actual Any columns from {{product-name}}).
2. The path may be missing from the document.
3. The path may correspond to a data type other than `Type` (for example, a string literal instead of a numeric value).

The difference between the strict (version with the `Strict` suffix) and non-strict function versions lies in how they handle errors: the strict version crashes the query, while the non-strict version returns the default value for the corresponding column type (`Null`, `0`, empty string, or empty list) and continues operation.

Usage examples (the first argument in each of these examples is a string literal containing YSON, but it can be replaced by any Any column of the table):

```sql
SELECT YPathInt64('{key1=42; key2=57; key3=29}', '/key2');
57

SELECT YPathInt64('[3;4;5]', '/1');
4

SELECT YPathString('["aa";"bb";"cc"]', '/1');
"bb"

SELECT YPathString('{a=[{c=xyz}; {c=qwe}]}', '/a/1/c');
"qwe"

SELECT YPathInt64('{key=xyz}', '/key');
0

SELECT YPathInt64Strict('{key=xyz}', '/key');
std::exception. Code: 1001, type: NYT::TErrorException, e.what() = Node /key2 has invalid type: expected one of {"int64", "uint64"}, actual "string"
    code            500
    origin          ...

SELECT YPathString('{key=3u}', '/nonexistentkey');
""

SELECT YPathArrayInt64('[1;2;3]', '');
[1, 2, 3]

SELECT YPathUInt64('42u', '');
42
```

Suppose that a table has two `Any` columns, `lhs` and `rhs`, both containing lists of numbers of equal length.

<small>Table 1: Example of a table with `Any` columns</small>

| lhs | rhs |
| --------- | --------- |
| [1, 2, 3] | [5, 6, 7] |
| [-1, 1] | [1, 3] |
| [] | [] |

Then we can calculate the scalar product of `lhs` and `rhs` vectors as follows:

```sql
SELECT arraySum((x, y) -> x*y, YPathArrayInt64(lhs, ''), YPathArrayInt64(rhs, '')) FROM "//path/to/table"
38
2
0
```

### YPathExtract (composite data type) { #ypath_complex_types }
`YPathExtract[Strict](yson, ypath, type)`: Extracts a value of an arbitrary `type` type value at `ypath` from the string `yson`.

This function is similar to the `YPath<Type>` functions but allows specifying an arbitrary return type using the additional `type` argument. It also supports composite, or compound, data types, for which there is no corresponding `YPath<Type>` version.

{% note warning "Attention" %}

With the `YPathExtract` function, you can also extract simple type values. For example, `YPathExtract(yson, ypath, 'Int64')` returns the same result as `YPathInt64(yson, ypath)`. However, the `YPath<Type>` functions are more specialized and better optimized, so we recommend using the `YPathExtract` function only when there is no specialized function for the type you want to extract.

{% endnote %}

Example:
```sql
SELECT YPathExtract('[[1;2];[2;3]]', '', 'Array(Array(Int64))');
[[1, 2], [2, 3]]

SELECT YPathExtract('{a=1;b="abc"}', '', 'Tuple(a Int64, b String)');
("a": 1, "b": "abc")

SELECT YPathExtract('{x=["abc";"bbc"]}', '/x', 'Array(String)')
["abc", "bbc"]
```

### YPathRaw (raw YSON) { #ypath_raw }

`YPathRaw[Strict](yson, ypath, [format])`: Extracts a raw YSON value at `ypath` from the string `yson`.

You can use this function when the type of the result is unknown or varies between different rows in the same column.

With the optional `format` parameter, you can specify the representation format of the returned YSON value. Supported formats are the following: `binary` (default), `text`, `pretty`, `unescaped_text`, or `unescaped_pretty`.

Examples:

```sql
SELECT YPathRaw('{x={key=[1]}}', '/x', 'text');
'{"key"=[1;];}'

SELECT YPathRaw('{a=1}', '/b', 'text');
NULL
```

### YSON family functions* { #yson_extract }

ClickHouse supports `JSON` family functions* for working with JSON format. To ensure a workflow that is more similar to standard ClickHouse, CHYT offers complete equivalents of these functions that are compatible with YSON format.

The function arguments and their returned values are described in the [ClickHouse documentation](https://clickhouse.com/docs/{{lang}}/sql-reference/functions/json-functions).

The currently supported YSON function equivalents are as follows:

`YSONHas`, `YSONLength`, `YSONKey`, `YSONType`, `YSONExtractInt`, `YSONExtractUInt`, `YSONExtractFloat`, `YSONExtractBool`, `YSONExtractString`, `YSONExtract`, `YSONExtractKeysAndValues`, `YSONExtractRaw`, `YSONExtractArrayRaw`, `YSONExtractKeysAndValuesRaw`.

{% note info "Note" %}

Unlike the `YPath` functions, in which array elements are indexed from 0, indexing in `YSON` functions* is one-based.

{% endnote %}

{% note warning "Attention" %}

These functions are implemented using shared ClickHouse code and don't have optimal performance. To speed up computations on large amounts of data, we recommend using the more optimal `YPath<Type>` functions.

{% endnote %}


Example:

```sql
-- Equivalent to YPathString('["aa";"bb";"cc"]', '/0')
-- Note: indexing starts from 1
SELECT YSONExtractString('["aa";"bb";"cc"]', 1);
"aa"

-- Equivalent to YPathString('{a=[{c=xyz}; {c=qwe}]}', '/a/1/c')
SELECT YSONExtractString('{a=[{c=xyz}; {c=qwe}]}', 'a', 2, 'c');
"qwe"
```

### YSON representation formats

Data from `Any` type columns can be stored in binary `YSON` representation. Since it's not always convenient to read such data, CHYT has the `ConvertYSON` function that converts different `YSON` representations into each other.

`ConvertYson(yson, format)`: Converts a string with a YSON document to the specified format.

In total, there are five possible representation formats:

- `binary`
- `text`
- `pretty`
- `unescaped_text`*
- `unescaped_pretty`*

{% note info "* Note" %}

{{product-name}} offers three representation formats: `binary`, `text`, and `pretty`.

In `text` and `pretty` representation formats, string values escape all non-ASCII characters, including Cyrillic letters and various UTF-8–encoded characters. Two additional formats, `unescaped_text` and `unescaped_pretty`, were added to CHYT to make the resulting YSON containing strings with Cyrillic characters more human-readable. These formats only differ from `text` and `pretty` in that they only escape special YSON characters in string values. All other string characters are stored without changes.

{% endnote %}

Examples:

```sql
SELECT ConvertYson('{x=1}', 'text');
'{"x"=1;}'

SELECT ConvertYson('{x=1}', 'pretty');
'{
    "x" = 1;
}'

SELECT ConvertYson('{x=1}', 'binary');
'{\u0001\u0002x=\u0002\u0002;}'

SELECT ConvertYson('{x="This example shows escaping the © character"}', 'text');
'{"x"="This example shows escaping the \xC2\xA9 character";}'

SELECT ConvertYson('{x="Here, the © character is not escaped"}', 'unescaped_text');
'{"x"="Here, the © character is not escaped";}'
```

### Example of working with YSON
```sql
SELECT operation_id, task_id, YPathInt64(task_spec, '/gpu_limit') as gpu_limit, YPathInt64(task_spec, '/job_count') as job_count FROM (
    SELECT tupleElement(tasks, 1) as task_id, tupleElement(tasks, 2) as task_spec, operation_id FROM (
        SELECT operation_id, tasks FROM (
            Select YSONExtractKeysAndValuesRaw(COALESCE(tasks, '')) as tasks, operation_id FROM (
                SELECT YSONExtractRaw(spec, 'tasks') as tasks, operation_id
                FROM `//home/dev/chyt_examples/completed_ops`
                WHERE YSONHas(spec, 'tasks')
            )
        )
        ARRAY JOIN tasks
    )
)
ORDER BY gpu_limit DESC;
```

## Getting a clique version { #version_functions }
#### Getting a CHYT version { #chyt_version }

`chytVersion()`: Gets a string representation of the CHYT server version.

Example:
```sql
SELECT chytVersion()
```

#### Getting a {{product-name}} version { #yt_version }

`ytVersion()`: Gets a string representation of the {{product-name}} code version used in the current CHYT version.

Example:
```sql
SELECT ytVersion()
```

## Working with Cypress { #cypress }

{% note info "Note" %}

All functions used to work with Cypress are table functions in ClickHouse terms. This means that their returned value can be used in place of a table: for example, in the `FROM` section of a `SELECT` query.

{% endnote %}


### ytListNodes { #list_nodes }
`ytListNodes[L](dir_path, [from, [to]])`: Gets a list of all nodes and their attributes in the directory at `dir_path`.

The result contains one row for each node in the directory at `dir_path`. Each row contains:

- Two predefined columns, `$key` and `$path`, which hold the node's name and full path (always starts with `dir_path`). The values in these columns may differ from the `key` and `path` attributes if the node is a link (`link` type) to another node.

- Three columns corresponding to the `resource_usage` attribute fields: `disk_space`, `tablet_count`, and `master_memory`.

- A large number of columns with names matching the corresponding node attributes: `key`, `path`, `account`, `owner`, `erasure_codec`, `id`, `acl`, and other. These columns include almost every system attribute.


{% note info "Note" %}

Since users don't normally need all the node attributes, most of the columns are virtual in ClickHouse terms. This means that by default they are not returned for `SELECT * FROM ytListNodes(...)` and `DESCRIBE ytListNodes(...)` expressions but can be requested explicitly:

```sql
SELECT id, key_columns, * FROM ytListNodes(...)
```

{% endnote %}

By default, the `ytListNodes` function doesn't expose links (nodes of `link` type). This means that for these nodes, the function returns the attributes of the link node itself, not the node it refers to. To change this behavior and expose all links, use the `L` function modifier (`ytListNodesL`). Keep in mind that links leading to a non-existent node remain unexposed, so the result may still contain `link` type strings that correspond to unexposed links.

{% note warning "Attention" %}

Exposing each link requires an additional query to the master server, so executing the `ytListNodeL` function may take longer and be more resource-intensive than `ytListNode`. We strongly discourage using `ytListNodeL` for directories with a large number of links.

{% endnote %}

With the two optional string arguments `from` and `to`, you can filter the returned nodes by name (`$key` column). Keys are compared lexicographically.

Example:
```sql
SELECT * FROM ytListNodes('//some/dir/path');
SELECT sum(disk_space), sum(chunk_count) FROM ytListNodes('//some/dir/path');
SELECT * FROM ytListNodesL('//some/dir/path');
```

### ytListTables { #list_tables }
`ytListTables[L](dir_path, [from, [to]])`: Gets a list of all tables and their attributes in the directory.

The function arguments and the structure of the returned value are similar to the `ytListNodes[L]` function.

The function result is equivalent to the output of `ytListNodes[L]` with `WHERE type = 'table'` filtering applied.

The `L` suffix has a similar meaning and exposes all links before filtering. Thus, the result of a `ytListTables` call only contains the tables located in the specified directory, whereas the result of `ytListTablesL` also contains the tables linked in the specified directory.

```sql
SELECT path, chunk_count FROM ytListTables('//dome/dir/path');
```

### ytListLogTables { #list_log_tables }
`ytListLogTables(project_name_or_path, [from_datetime, [to_datetime]])`: Gets a non-overlapping list of log tables and their attributes from the specified project.

`project_name_or_path` can hold either the name of the project with the logs (if the project is located in the `//logs` directory) or the full path to the directory with the logs.

The function reads the log tables in four subdirectories (`1h`, `30m`, `1d`, and `stream/5m`), then merges the sets with these tables, avoiding any overlaps in the time intervals of the tables.

With the two optional arguments `from_datetime` and `to_datetime`, you can specify the log time interval that you need. Log tables that don't have any overlaps with the specified interval are filtered out.

{% note info "Note" %}

Filtering by time interval is only performed at the table level. Log tables returned in the function result may cover a longer interval than that specified in the arguments.

{% endnote %}

Example:
```sql
SELECT * FROM ytListLogTables('chyt-structured-http-proxy-log', today() - 1);
SELECT * FROM ytListLogTables('//logs/chyt-structured-http-proxy-log', today() - 5, today() - 4);
```

### ytNodeAttributes { #node_attributes }
`ytNodeAttributes(path1, [path2, [...]])`: Gets the attributes of all the nodes specified in the arguments.

The returned value contains a single string with attributes for each path specified in the function arguments. If the specified path is invalid or doesn't exist, the function generates an error.

The structure of the returned value is similar to the `ytListNodes` function.

Example:
```sql
SELECT * FROM ytNodeAttributes('//sys', '//sys/clickhouse')
```

## Reading multiple tables { #multiple_tables_read }

{% note info "Note" %}

All functions used to read sets of tables are table functions in ClickHouse terms. This means that their returned value can be used in place of a table: for example, in the `FROM` section of a `SELECT` query.

{% endnote %}

### ytTables { #yt_tables }
`ytTables(arg1, [arg2, ...])`: Reads the union of a set of tables specified in the arguments.

Each argument can be one of the following:
- A string representing the path to a specific table.
- A `ytListNodes[L]`, `ytListTables[L]`, or `ytListLogTables` function.
- A subquery returning an arbitrary set of table paths*.

{% note warning "* Attention!" %}

Passing a subquery that returns multiple paths as a function argument doesn't conform to the ClickHouse syntax. Though you can do this in the current versions of CHYT and ClickHouse, this may be deprecated in the future releases. The proper way to pass these subqueries is by using the `view` function.

In addition, we're aware of a bug in the ClickHouse optimizer that may incorrectly move an external `WHERE` condition inside such a subquery, resulting in the error `Missing columns: '<column_name>' while processing query`. You can avoid this error by setting `enable_optimize_predicate_expression=0`.

{% endnote %}

The function returns the union of the set of all tables specified in the arguments. This function works similarly to the ClickHouse's [merge](https://clickhouse.com/docs/en/sql-reference/table-functions/merge) function but offers a more flexible framework for specifying multiple tables and is optimized for work with {{product-name}} tables.

If tables in the specified set have different schemas, the output will use a common schema that allows reading all the specified tables.

{% cut "Algorithm for inferring a common schema" %}

**Compatible types**
* For each column, the algorithm selects the most generic type that all types in the column can be cast to.
* For example, for `optional<int32>` and `int64`, the most generic type is `optional<i64>`.
* **Please note** that due to the way data is stored in {{product-name}}, unsigned numeric types are incompatible with signed ones (that is, `int32` and `uint32` are incompatible).

**Incompatible types**
* If the column types in different tables are incompatible, CHYT proceeds based on the set `chyt.concat_tables.type_mismatch_mode` option value.
* For example, `string` and `int64` types are incompatible.
* Possible option values are `drop`, `throw`, and `read_as_any`.
* By default, the function throws a query execution error (`throw` value).
* You can read columns of different types as YSON strings by setting the `read_as_any` option value. This may be inefficient, so we recommend that you structure your tables properly.
* If these columns are not needed for the query's execution, you can discard them by setting the `drop` option value.

**Missing column**
* If a column is missing in one or multiple input tables, CHYT proceeds based on the set `chyt.concat_tables.missing_column_mode` option value.
* Possible values are `drop`, `throw`, and `read_as_null`.
* By default, the column becomes optional, and values for the tables that don't have this column will be read as `NULL` (`read_as_null` value).
* If you don't expect missing columns in any of the tables, you can set the `throw` option value to generate a query execution error when this happens.
* As with incompatible types, you can discard these columns by setting the `drop` option value.
* **Please note** that the `missing_column_mode` option only affects the function behavior if a table that is missing a column has a strict schema. If a column is missing in a non-strict schema, it may still exist in the actual data with any type. In this case, the algorithm makes a pessimistic assumption that the column is present with an incompatible type, and its subsequent behavior is determined by the previously described option `type_mismatch_mode`.

{% endcut %}

{% note info "Note" %}

In addition to the main columns read from the tables themselves, the output also always contains the virtual columns `$table_index`, `$table_name`, and `$table_path`. You can use these columns to identify the table that each row was read from. For more information about virtual columns, see the [relevant section](../../../../../user-guide/data-processing/chyt/yt-tables.md#virtual_columns).

{% endnote %}

Example:
```sql
-- Read the union of 2 tables:
SELECT * FROM ytTables('//tmp/t1', '//tmp/t2')
-- Read the union of all tables from the directory '//tmp/dir':
SELECT * FROM ytTables(ytListTables('//tmp/dir'))
-- Read the union of all tables from the directory and their names:
SELECT *, $table_name FROM ytTables(ytListTables('//tmp/dir'))
-- Read the last (lexicographically) table:
SELECT * FROM ytTables((
  SELECT max(path) FROM ytListTables('/tmp/dir')
))
-- Read tables with a specific suffix:
SELECT * FROM ytTables((
  SELECT concat(path, '/suffix') FROM ytListNodes('//tmp/dir')
))
```


### concatYtTables { #concat_yt_tables }

`concatYtTables(table1, [table2, [...]])`: Reads the union of multiple {{product-name}} tables.

This is a deprecated variant of the `ytTables` function. Only table paths can be used as arguments.

Example:
```sql
SELECT * FROM concatYtTables('//tmp/sample_table', '//tmp/sample_table2');
```

### concatYtTablesRange { #concat_yt_tables_range }

`concatYtTablesRange(cypress_path, [from, [to]])`: Reads the union of all tables in the directory at `cypress_path`.

This is a deprecated function equivalent to `ytTables(ytListTables(cypress_path, from, to))`.

Example:
```sql
SELECT * FROM concatYtTablesRange("//tmp/sample_tables");
SELECT * FROM concatYtTablesRange('//tmp/sample_tables', '2019-01-01');
SELECT * FROM concatYtTablesRange('//tmp/sample_tables', '2019-08-13T11:00:00');
```
