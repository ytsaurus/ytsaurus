# Functions

This article describes CHYT-specific features.

## Functions for working with YSON { #yson_functions }

CHYT introduces auxiliary functions that look like `YPathTYPENAME[Strict](yson, ypath)` where `TYPENAME` is one of the 9 possible variants: `Int64`, `UInt64`, `Boolean`, `Double`, `String`, `ArrayInt64`, `ArrayUInt64`, `ArrayBoolean`, or `ArrayDouble`, and the `Strict` suffix means the function strictness.

The function principle is as follows:

1. A string contained in the `yson` argument as a YSON document is selected.
2. Addressing is performed in this document on the `ypath` path relative to the root document.
3. The contents of the document are interpreted on the `ypath` path to the `TYPENAME` type.

Errors may occur at each step of this function operation:

1. A string may not be a YSON document (this is possible when a string constant is specified as a function argument, but not when reading actual Any columns from {{product-name}}).
2. This path may be missing from the document.
3. This path can correspond to a data type other than `TYPENAME` (for example, a string literal instead of a numeric value).

The difference between `Strict` and `non-Strict` function versions is in the error handling mode: the `Strict` version will crash the query, while the `non-Strict` version will return as the result the default value for the corresponding column type (`Null`, `0`,` empty string, or empty list) and continue the operation.

Below are the examples. The first argument everywhere is a string literal containing YSON, but it can be replaced by any `Any` column of the table.


```sql
SELECT YPathInt64('{key1=42; key2=57; key3=29}', '/key2')
57

SELECT YPathInt64('[3;4;5]', '/1')
4

SELECT YPathString('["aa";"bb";"cc"]', '/1')
"bb"

SELECT YPathString('{a={[{c=xyz}; {c=qwe}]}}', '/a/1/c')
"qwe"

SELECT YPathInt64('{key=xyz}', '/key')
0

SELECT YPathInt64Strict('{key=xyz}', '/key')
std::exception. Code: 1001, type: NYT::TErrorException, e.what() = Node /key2 has invalid type: expected one of {"int64", "uint64"}, actual "string"
    code            500
    origin          ...

SELECT YPathString('{key=3u}', '/nonexistentkey')
""

SELECT YPathArrayInt64('[1;2;3]', '')
[1, 2, 3]

SELECT(YPathUInt64('42u', '')
42
```

Suppose that a table has two columns — `lhs` and `rhs` — that are `Any` columns containing lists of numbers of equal length.

```sql
| lhs       | rhs       |
| --------- | --------- |
| [1, 2, 3] | [5, 6, 7] |
| [-1, 1]   | [1, 3]    |
| []        | []        |
```

Then we can construct the scalar product of vectors `lhs` and `rhs` using the construction below.


```sql
SELECT arraySum((x, y) -> x*y, YPathArrayInt64(lhs, ''), YPathArrayInt64(rhs, '')) FROM "//path/to/table"
38
2
0
```

There are also `YPathRaw[Strict](yson, ypath, [format])` functions which, in addition to the two mandatory parameters, have the optional `format` parameter (the default value is `binary`).
These functions return the entire subtree as a YSON string in the requested format.

For complex types, the `YPathExtract[Strict](yson, ypath, type)` function taking the returned type in the form of a string as the third parameter is supported.

Examples:

```sql
SELECT YPathRaw('{x={key=[1]}}', '/x', 'text')
'{"key"=[1;];}'
SELECT YPathExtract('{x=["abc";"bbc"]}', '/x', 'Array(String)')
["abc", "bbc"]
SELECT YPathExtract('["abc";"bbc"]', '', 'Array(String)')
["abc", "bbc"]
```

To make working with JSON in ClickHouse and working with YSON in CHYT similar, analogs of all functions from original ClickHouse that start with the word `JSON` were supported.
For more information about the semantics of these functions, see the [ClickHouse documentation](https://clickhouse.com/docs/ru/sql-reference/functions/json-functions/).
All YSON analogs differ from JSON functions only by the first letter in their names (`YSON*` instead of `JSON*`).

{% note warning "Attention!" %}

Unlike `YPath*` functions, these functions use numbering from 1 rather than from 0.
Besides that, the order of the fields in the dicts in YSON format is not defined, and, accordingly, accessing an element in the dict by index is not supported.

{% endnote %}

### YSON representation formats { #formats }

Data from columns of the `Any` type can be stored in binary `YSON` representation. Since it is not always convenient to read such data, CHYT has the `ConvertYSON` function that converts different `YSON` representations into each other.

`ConvertYson(YsonString, format)`

There are a total of three possible representation formats:

- `binary`.
- `text`.
- `pretty`.

Examples:

```sql
SELECT ConvertYson('{x=1}', 'text')
'{"x"=1;}'

SELECT ConvertYson('{x=1}', 'pretty')
'{
    "x" = 1;
}'

SELECT ConvertYson('{x=1}', 'binary')
'{\u0001\u0002x=\u0002\u0002;}'
```

### Examples of working with YSON { #examples }

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

## Get version functions { #version_functions }

- Getting a CHYT version
```sql
SELECT chytVersion()
```
- Getting a {{product-name}} version
```sql
SELECT ytVersion()
```

