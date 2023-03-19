# Query language

This section describes an SQL language dialect that enables you to read and transform data on the system side.

## Overview

This feature is supported only for dynamic tables.

{% note info "Note" %}

An SQL language dialect is not a full-fledged implementation of the SQL language. This query language should be considered an extended data-reading operation, allowing some of the data processing work to be done on the server side.

{% endnote %}

{% note warning "Attention!" %}

The feature is not a replacement of MapReduce operations. The query execution engine has no scheduler and specifying a heavy query with full scan can cause cluster degradation. The {{product-name}} service team is working to add more reliability when processing large queries, but there are no plans to develop the described SQL language dialect to replace MapReduce.

{% endnote %}

## Examples

Reading from the table:
```bash
yt select-rows "* FROM [//mytable] LIMIT 10" --format json
```
Counting the number of rows:
```bash
yt select-rows "SUM(1) FROM [//mytable] GROUP BY 1" --format json
```
Queries with grouping:
```bash
yt select-rows "day, MIN(price), MAX(price) FROM [//mytable] WHERE is_ok = 1 GROUP BY timestamp / 86400 AS day" --format json
yt select-rows "item_id FROM [//mytable] WHERE (user_id, order_id) IN ((1, 1), (1, 2), (10, 10), (10, 11))" --format json
```

## Query syntax

```bash
* | <select-expr-1> [AS <select-alias-1>], <select-expr-2> [AS <select-alias-2>], ...
FROM [//path/to/table] [JOIN [//path/to/rhs1] USING <column-1>, <column-2>, ...]
[WHERE <predicate-expr>]
[GROUP BY <group-by-expr-1> [AS <group-by-alias-1>], <group-by-expr-2> [AS <group-by-alias-2], ...]
[ORDER BY <column-1>, <column-2>, ...]
[OFFSET <n>]
[LIMIT <m>]
```

In general, the row order in the query output is not defined, but in cases of ORDER BY and LIMIT, such guarantees are given.

### SELECT section { #select }
SELECT syntax:
```bash
* | <select-expr-1> [AS <select-alias-1>], <select-expr-2> [AS <select-alias-2>], ...
```
Defines the query output columns. If an asterisk (`*`) is specified, all input columns of the main table will be used as output columns. Alternatively, you can specify random expressions for the calculation. For queries without grouping, all columns of the source tables can be in the expression. In case of queries with grouping, only the key columns used for grouping or aggregates can be in the expression.

### FROM section ... JOIN { #from_join }
FROM syntax ... JOIN:
```bash
FROM [//path/to/table] [[AS] <table-alias>] [[LEFT] JOIN [//path/to/dict-1] [[AS] <table-alias>] (ON <expression> = <foreign-expression> | USING <column-1>, <column-2>, ...) [AND <predicate>]] ...
```

Defines the query data sources. The table immediately following FROM is considered the master table and is used when coordinating query execution. It is also allowable to specify auxiliary reference tables in the JOIN section. The reference tables are connected to the main table if the specified key matches exactly. Without specifying the LEFT keyword, INNER JOIN will be executed. You can additionally specify an expression for reference table filtering which will be executed when reading data before join with the main table is executed.
To combine tables across multiple columns, a tuple must be written in the ON condition.

```bash
FROM [//x] AS x JOIN [//y] AS y ON (x.a, x.b) = (y.a, y.b)
```
It is allowed to write expressions in the JOIN condition:
```bash
ON (if(is_null(x.a), 0, x.a), x.b) = (y.a, y.b)
```

For more information about JOINS, their features, and limitations, see the [JOINS](#joins) section.

### WHERE section { #where }
WHERE syntax:
```bash
WHERE <predicate-expr>
```
Defines the primary data filter.

### GROUP BY section { #group_by }
GROUP BY syntax:
```bash
GROUP BY <group-by-expr-1> [AS <group-by-alias-1>], <group-by-expr-2> [AS <group-by-alias-2], ...
```
Defines data grouping by key.

### HAVING section { #having }
HAVING syntax:
```bash
HAVING <predicate-expr>
```
Defines the filtering condition after grouping.

### WITH TOTALS modifier { #with_totals }

Used in conjunction with grouping and adds an extra row to the result, with `NULL` values in the grouping keys and total aggregates by grouped data in the values. The row added by the summary aggregate is not processed in ORDER BY ... LIMIT. When the HAVING section is used, the final aggregates can be counted both before and after filtering. The order in which the rows with the resulting aggregates are arranged in the resulting row set is unspecified. The added row can be either the first or the last one. It can be distinguished by a `NULL` key.

WITH TOTALS syntax:
```bash
GROUP BY ... WITH TOTALS
GROUP BY ... WITH TOTALS HAVING ...
GROUP BY ... HAVING ... WITH TOTALS
```

### ORDER BY section { #order_by }
ORDER BY syntax:
```bash
ORDER BY <order-by-expr-1> [ASC|DESC], <order-by-expr-2> [ASC|DESC], ...
```
Defines the ordering of the output row set by the specified expressions. The default sorting order is ascending (ASC). The DESC keyword is specified for descending sorting. Sorting is applied after grouping, so only the key grouping columns or the counted aggregates can be in the expressions. The sorting is partial, i.e. only N smallest rows are returned as a result (the N parameter is specified in the LIMIT section).

### OFFSET section { #offset }
OFFSET syntax:
```bash
OFFSET <n>
```
Specifies the number of skipped rows from the output set of rows.
There can be no effective implementation of OFFSET in the query language under the current data storage schema. The OFFSET N LIMIT M query will read N + M rows. To implement this effectively, there must be an index from the row number to the key.
The proper way to perform paginated output is to make LIMIT queries, but instead of using OFFSET, remember the values of the last_key = (key_column1, key_column2, ...) key of the current query result and send the next query with the where (key_column1, key_column2, ...) > last_key condition.

### LIMIT section { #limit }
LIMIT syntax
```bash
LIMIT <m>
```
Defines the size of the output row set. If there is an ORDER BY section, it sets a limit on the size of the heap. Otherwise, it sets a limit on the order and amount of scanned data. The LIMIT query scans the data sequentially by the primary key until the target size is reached. In this case, the order of output rows is defined unambiguously.

{% note info "Note" %}

When using grouping with a limit (without ORDER BY), the data will be read in the order of the primary table key.
If there are no aggregate columns, the query will end once the required set of grouped rows is accumulated.
If there are aggregate columns, extra grouping will be performed for the keys accumulated in the number specified in LIMIT.
When using LIMIT without ORDER BY, please note that LIMIT will be applied to the previous operator. For example, if you use GROUP BY ... WITH TOTALS and LIMIT, then the final aggregates will be calculated from the data that fits within the limit. Data above the limit will not even be read. If ORDER BY is added, LIMIT will affect it and GROUP BY will process all read data. Respectively, TOTALS will be calculated from the larger data set.

{% endnote %}

## Expression syntax { #expression_syntax }

```bash
<literal>
<identifier>
```
Arithmetic operators:
```bash
<expr> + <expr>
<expr> - <expr>
<expr> * <expr>
<expr> / <expr>
<expr> % <expr>
```
Bitwise operators:
```bash
<expr> ~ <expr>
<expr> | <expr>
<expr> & <expr>
<expr> >> <expr>
<expr> << <expr>
```
Logical operators:
```bash
<expr> AND <expr>
<expr> OR <expr>
NOT <expr>
```
Comparison operators:
```bash
<expr> = <expr>
<expr> != <expr>
<expr> < <expr>
<expr> <= <expr>
<expr> > <expr>
<expr> >= <expr>
<expr> BETWEEN <literal> AND <literal>
<expr> BETWEEN (<literal> AND <literal>, ...)
```
IN operator:
```bash
<expr> IN (<literal>, ...)
```
Functions:
```bash
FN(<expr>, ...)
```

Priorities:

1. `unary + - ~`
2. `binary * / %`
3. `binary + -`
4. `binary >> <<`
5. `binary &`
6. `binary |`
7. `binary < <= > >= IN BETWEEN`
8. `binary =`
9. `unary NOT`
10. `binary AND`
11. `binary OR`

Each expression has its type: `int64`, `uint64`, `boolean`, `double`, `string`, or `any`. The typification of expressions is strict. If a typification error occurs, query execution stops and an error is returned to the user.

Arithmetic operators take numeric types (`int64`, `uint64`, `double`) as input and return a value of the same type.

Logical operators take and return values of the `boolean` type.

Comparison operators, IN and BETWEEN operators take values of the same type as input and return values of the `boolean` type. They also allow the use of tuples.

Tuple usage example:
```bash
(a, b) >= (valA, valB)
(a, b) IN ((valA0, valB0), (valA1, valB1), ...)
(a, b) BETWEEN ((16202224, 1) AND (16202224, 3), (2011740432, 6) AND (2011740432), (591141536) AND (591141536, 2), ...)
```
Functions in expressions can be of two types: scalar applied to a tuple of values and aggregate applied to a group of input data corresponding to a single key. Scalar functions can be used in any expression. Aggregation functions can only be used in the SELECT section if there is a GROUP BY section.

### IDs { #identifiers }
Column names containing special characters must be wrapped in square brackets.
**Examples**: `t.[a.b], t.[c-1], t.[[d]] from [//path] as t` where `a.b`, `c-1`, and `[d]` are column names.
In the resulting schema, the square brackets in the column names will be omitted wherever possible.
For the `[my.column-1], [my+column-2] from ...` query, the schema will be `"my.column-1", "my+column-2"`.
If expressions without an alias (`AS name`) are specified in the query, and the columns contain special characters, square brackets will be added.
For the `[my.column-1] + x, [my+column-2] from ...` query, the schema will be `"[my.column-1] + x", "my+column-2"`.

### Synonyms { #synonims }
Expressions can be named using the AS keyword. Synonym support is implemented similarly to [ClickHouse](https://clickhouse.yandex). Unlike standard SQL, synonyms can be declared not only at the top level of expressions: `((1 as x) + x) as y`. In addition, synonyms can be used in all query sections. **For example**: `a from [...] group by x % 2 as a`.

This behavior is compatible with standard SQL as long as no synonyms are used that overlap the column names of the source tables.
The table shows an example of using synonyms for a table with the `UpdateTime` column.

Synonym usage example:

| Expression | {{product-name}} | In standard SQL |
| --------- | ---- | ----------------- |
| `UpdateTime / 1000 as UpdateTime, UpdateTime as b` | As a result, UpdateTime = b | As a result, UpdateTime != b |
| `UpdateTime / 3600 % 24 as Hour from ... group by Hour` | Grouping will be by `UpdateTime / 3600 % 24` | Grouping will be by `UpdateTime / 3600 % 24` |
| `UpdateTime / 3600 % 24 as Hour where Hour = ...` | Filtering will be by `UpdateTime / 3600 % 24` | Filtering will be by `UpdateTime / 3600 % 24` |
| `UpdateTime / 1000 as UpdateTime where in (...)` | Filtering will be by `UpdateTime / 1000` | Filtering will be by the `UpdateTime` source field |
| `UpdateTime / 1000 as UpdateTime from ... group by UpdateTime` | Grouping will be by `UpdateTime / 1000` | Grouping will be by the `UpdateTime` source field |
| `UpdateTime from ... group by UpdateTime / 1000 as UpdateTime` | Grouping will be by `UpdateTime / 1000` | AS is not supported within group by |
| `max(UpdateTime) as UpdateTime where UpdateTime between ...` | There will be an error because where cannot use an aggregate | Filtering will be by the `UpdateTime` source field |

### Literals { #literals }
Expressions may contain literals that encode typified values.

To set numbers with a sign, decimal numbers are used (for example, `100,500` or `42`). To set numbers without a sign, decimal numbers with the `u` suffix are used (for example, `100,500u`, `42u`).

Floating point numbers use a C-like decimal notation (for example, `3.1415926`) with an integer and fractional part separator.

C-like string literals (like `"hello"` or `"world"`) are used to specify strings.

### Scalar functions

{% note info "Note" %}

Some functions are polymorphic: they can take values of several types. The signatures of such functions are described either using type variables (denoted by capital letters: `A`, `B`, ...), or using an explicit enumeration of taken types (denoted: `int64 | uint64`).

{% endnote %}

#### General functions
`if(cond, then, else) :: boolean -> A -> A -> A`
Conditional operator. If the `cond` expression is true, the function result is the `then` value. Otherwise, it is `else`.

`is_null(x) :: A -> boolean`
Checks whether the specified value is `NULL`.

`transform(a, (a1, a2, ...), (b1, b2, ...)) :: A -> List[A] -> List[B] -> B`
`transform[a2), ((a11, a12), ...), (v1, ...](a1,) :: Tuple -> List[Tuple] -> List[B] -> B`
Convert a value (or tuple) according to an explicit display of some elements on other elements.
If there is no corresponding value, `NULL` is returned.
The complexity is constant.

#### Working with strings
`is_substr(s, t) :: string -> string -> boolean`
Checks whether the `s` string is the `t` substring.

`is_prefix(p, t) :: string -> string -> boolean`
Checks whether the `p` string is the `t` prefix.

`lower(s) :: string -> string`
Converts the `s` string to the lower case.

#### Working with YSON

To extract data from values containing [YSON](../../../user-guide/storage/formats.md#yson), a set of functions exists:

- `try_get_int64`;
- `get_int64`;
- `try_get_uint64`;
- `get_uint64`;
- `try_get_double`;
- `get_double`;
- `try_get_boolean`;
- `get_boolean`;
- `try_get_string`;
- `get_string`;
- `try_get_any`;
- `get_any`.

Functions take two arguments: ` (yson, path)` where `yson` is a value of the `any` type containing YSON and `path` is the path to the desired field in YPATH format.

The version with the `try_` prefix returns `NULL` if there is no field of the desired type in the specified path. The version without `try_` returns an error if there is no field.

Example:
For the table row `{column=<attr=4>{key3=2;k={k2=<b=7>3;k3=10};lst=<a=[1;{a=3};{b=7u}]>[0;1;<a={b=4}>2]}}` `try_get_uint64(column, "/lst/@a/2/b")` will return the `7u` value.

`any_to_yson_string(yson) :: any -> string`
Converts a value of the `any` type into a string containing its binary-YSON representation.

{% note warning "Attention!" %}

A fixed key order in dictionaries is not guaranteed, so comparisons and groupings may not behave as expected.

{% endnote %}

`list_contains(list, value) :: any -> (string | int64 | uint64| boolean) -> boolean`
Searches for `value` in the `list` YSON list of the `any` type, the `value` value of the scalar type. The list does not have to be homogeneous (i.e. it can contain values of different types), the comparison is performed taking the type into account.

#### Working with regular expressions
The [Google RE2](https://github.com/google/re2) library is used to work with regular expressions. The library operates in UTF-8 mode. The syntax of regular expressions is described on a separate [page](https://github.com/google/re2/wiki/Syntax).

`regex_full_match(p, s) :: string -> string -> boolean`
Checks that the `s` string matches the `p` regular expression.

`regex_partial_match(p, s) :: string -> string -> boolean`
Checks that the `s` string contains a substring that matches the `p` regular expression.

`regex_replace_first(p, s, r) :: string -> string -> string -> string`
Replaces the first occurrence of the `p` regular expression in the `s` string with the `r` string.

`regex_replace_all(p, s, r) :: string -> string -> string -> string`
Replaces all occurrences of the `p` regular expression in the `s` string with the `r` string.

`regex_extract(p, s, r) :: string -> string -> string -> string`
Enables the substrings that match the `s` regular expression to be extracted from the `s` string and replaced `r`.
For example, the result of the `regex_extract("([a-z]*)@(.*)", "email foo@bar.com", "\\1 at \\2")` expression will be `foo at bar.com`.

`regex_escape(p) :: string -> string`
Shileds special characters of the regular expression language in the `p` string.

#### Working with dates
UTC timezone is used when rounding

`timestamp_floor_year(t) :: int64 -> int64 `
Get the timestamp of the year (as of 0:00 on the first of January) for the specified timestamp

`timestamp_floor_month(t) :: int64 -> int64 `
Get the timestamp of the month (as of 0:00 of the first day of the month) for the specified timestamp

` timestamp_floor_week(t) :: int64 -> int64 `
Get the timestamp of the week (as of 0:00 on Monday) for the specified timestamp

` timestamp_floor_day(t) :: int64 -> int64 `
Get the timestamp of the day (as of 0:00 of the start of the day) for the specified timestamp

` timestamp_floor_hour(t) :: int64 -> int64 `
Get the timestamp of the hour (0 min 0 sec of the start of the hour) for the specified timestamp

#### Hashing

`farm_hash(a1, a2, ...) :: (int64 | uint64 | boolean | string)* -> uint64`
Calculates [FarmHash](https://code.google.com/p/farmhash/) from the specified set of arguments.

#### Converting types

`int64(x) :: any | int64 | uint64 | double -> int64`
`uint64(x) :: any | int64 | uint64 | double -> uint64`
`double(x) :: any | int64 | uint64 | double -> double`
`boolean(x) :: any -> boolean`
`string(x) :: any -> string`
Converts the `x` numeric argument to the target type. Rounding and overflow rules are standard and C-like.

To convert a numeric type to a string, there is a function:
`numeric_to_string :: int64 | uint64 | double -> string`
To convert a string to a number:
`parse_int64 :: string -> int64`
`parse_uint64 :: string -> uint64`
`parse_double :: string -> double`

#### NULL value
In most operators, using `NULL` values in operands results in a `NULL` value. Comparison operators have a different behavior. The result in them is always `boolean`. This is to ensure that the behavior of comparison operations is the same both when ordering data in the table by key and when calculating expressions within a query. The need to compare with `NULL` is due to the desire to make the order relation on the keys complete. `NULL` is considered less than the other values.

### Aggregation functions { #aggregation_functions }
The following aggregation functions are supported in the query language:

- `sum`: Sum calculation.
- `min`: Minimum calculation.
- `max`: Maximum calculation.
- `avg`: Average calculation.
- `cardinality`: Calculation of the number of different elements using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

## Executing a query { #query_execution }

The query is executed on the cluster in a distributed way. The query execution process is conventionally divided into two phases: coordination and execution. During coordination, multiple cluster nodes are defined to execute the query, and during execution, the data is processed directly. A typical scenario: a user-defined query is decomposed into multiple execution fragments processed by cluster nodes, the final result is generated on the client as a result of merging intermediate results received from cluster nodes.

There is an [`explain_query`](#explain_query) command to display information about the query plan.

### Coordination
The process of splitting a query into multiple parallel _execution fragments_ in order to distribute the load is called coordination. The query coordination occurs both on the client and on the cluster nodes. The coordination process itself consists of the following steps:

1. Define the ranges of the source data.
2. Splitting the query to process parts of the input data independently.

The input data for the query and its splitting options are determined by the structure of the main table specified in the query and the filtering predicate. Execution of queries on top of **[dynamic](../../../user-guide/dynamic-tables/overview.md)** (sorted and ordered) tables is currently supported.

When working with sorted dynamic tables, the data splitting is two-level: the table is initially split into _tablets_ and the query is sent to the machines serving the selected tablets, and then on each machine the tablet is further split into partitions to ensure concurrent execution within each machine. For more information about tablets, see [Dynamic tables](../../../user-guide/dynamic-tables/overview.md).

In the process of coordination, the cluster nodes that contain the data needed to execute the query are selected. The initial query is split into _execution fragments_ and sent to the selected nodes. On each node, query execution can be additionally paralleled if a large enough amount of data is affected. Concurrency of query execution on a single node is limited by the `max_subqueries` query parameter and the number of threads allocated to query execution in the cluster node configuration.

When the query is split for concurrent execution, the calculation of the (`WHERE`) predicates, (`GROUP BY`) groupings, and (`ORDER BY`) sortings is moved as close as possible to reading the data and only the minimum necessary operators remain on the coordinator. For example, if the splitting of the input data results in exactly one part, then the entire filtering and grouping will be performed by the subordinate node and the coordinator will do nothing with the data.

### Cutting off input data { #input_data_cut }

During coordination, the (`WHERE`) filtering predicate is analyzed. Based on a set of key table columns and predicate conditions, the data read ranges are output.
Range output can account for logical connectives (`AND`, `OR`, `NOT`), comparison operators, `IN` operator, `BETWEEN` operator, and `is_prefix` function. Once the ranges are output, some of the conditions from the predicate can be discarded if the conditions appear to always be true on the data read from the ranges.

The output ranges are a set of lower and upper read limits. The read limit is the prefix of the main table key.

If there are no computed columns in the table schema, the ranges are output as follows. The key columns are considered in their order in the schema as long as there are upper-level conditions in the predicate (connected by the `AND`, `OR`, and `NOT` logical operators) that limit the values of these columns. Column conditions can be point (`=` operator, `IN` operator) and range ( `!=`, `<`, `<=`, `>`, `>=`, `is_prefix`). The resulting range is built as long as the point conditions are met and ends at the first range condition.

**Examples:**

Let the table have key columns: `a, b, c`.

- For the `a = 1 and b in (3, 4)` predicate, the following ranges will be output:
   ```
   [[1, 3] .. [1, 3]], [[1, 4] .. [1, 4]]
   ```
- For the `a = 1 and b between 20 and 30 and c between 40 and 60` predicate, the following range will be output:
   ```
   [[1, 20] .. [1, 30]]
   ```

- For the `a = 1 and b = 2 and c between 40 and 60` predicate, the following range will be output:
   ```
   [[1, 2, 40] .. [1, 2, 60]]
   ```
- For the `a in (1, 2, 3) and b in (5, 6) and c between 40 and 60` predicate, the following ranges will be output:
   ```
   [[1, 5, 40] .. [1, 5, 60]], [[1, 6, 40] .. [1, 6, 60]], [[2, 5, 40] .. [2, 5, 60]], [[2, 6, 40] .. [2, 6, 60]], [[3, 5, 40] .. [3, 5, 60]], [[3, 6, 40] .. [3, 6, 60]]
   ```
- For the `a = 1 and b = 2 and c between 40 and 60 or a between 10 and 20` predicate, the following ranges will be output:
   ```
   [[1, 2, 40] .. [1, 2, 60]], [[10] .. [20]]
   ```


An important feature of the output ranges is that the lower and upper limits have a _common prefix_ and differ only by the last component.

If there are computed columns in the table schema, the ranges are first output without the computed columns and then are either supplemented with computed columns or trimmed to the first computed column that cannot be computed. For each range, the computed column can be computed when placing in the expression only the columns belonging to the common prefix of the lower and upper limit.

**Examples:**

Let the table have key columns: `h = hash(a, b), a, b, c`.

- For the `a = 1 and b in (3, 4)` predicate, the following ranges will be output:
   ```
   [[hash(1, 3), 1, 3] .. [hash(1, 3), 1, 3]], [[hash(1, 4), 1, 4] .. [hash(1, 4), 1, 4]]
   ```
- For the `a = in (1, 2)` predicate, the range cannot be output, because the computed column depends not only on the `a` column, but also on the `b` column.

- Special consideration is given to the case when the computed column is an expression taken by the module: `h = hash(a, b) % 3, a, b, c`. In that case, there is no need to compute the column value, because the set of values is already limited by the module. When the ranges are output, all possible values of the computed column with the expression taken by the module are considered. For the `a = in (1, 2)` predicate, the following ranges will be output:
   ```
   [[0, 1] .. [0, 1]], [[0, 2] .. [0, 2]], [[1, 1] .. [1, 1]], [[1, 2] .. [1, 2]], [[2, 1] .. [2, 1]], [[2, 2] .. [2, 2]]
   ```
- For the `a between 10 and 20` predicate:
   ```
   [[0, 10] .. [0, 20]], [[1, 10] .. [1, 20]], [[2, 10] .. [2, 20]]
   ```

The number of output ranges is limited by the `range_expansion_limit` option.

For efficient query execution, it is important to use the information from the primary table key as much as possible in order to cut off as much data as possible when coordinating the query.

If the ranges could not be output, the table is read as a whole. Such reading is called _full scan_. A rather large table (triple digit gigabytes) cannot be read within the time limit (by default, `query_timeout` is one minute) allocated for query execution. To prohibit _full scan_, you can set the `allow_full_scan` query option to `false`.


### JOINS { #joins }
You can also specify additional reference tables in the query to compute the final response or for additional filtering. **Only lookup joins** are currently supported, i.e. a JOIN execution schema where the master data handler explicitly queries the additional table by key.

In the query itself, this means that the condition for JOIN must contain such a set of key columns of the reference table that the ranges for reading the data can be derived from the limit on these columns. If JOIN cannot be executed using the reference table index, the "Foreign table key is not used in the join clause" error will be returned. To disable such behavior, use the `allow_join_without_index` parameter.

In general, JOIN is executed as follows. On each cluster node, the data is read from the main table shard, accumulated in memory, and multiple JOIN keys for the reference table are computed. This is followed by a query to the reference table with the read ranges and optional predicate obtained based on the JOIN keys. Then JOIN is executed in memory and execution continues as usual.

When using multiple JOINs in a query, it would take a lot of time to execute them consecutively. Individual JOINs are therefore grouped into sets that can be executed concurrently. So when a JOIN group is executed while reading data from the main table, a set of JOIN keys is built for each reference in the group. Next, queries to read data from references are sent concurrently and once the data is received, JOIN is executed which generates the resulting rows. The complete resulting set of rows is not materialized in memory, but as JOIN is executed, the rows are passed to the next operator (for example, grouping).

JOIN is executed in a special way when the master table and the reference table share a common key prefix to within a JOIN condition. In that case, the ranges for reading the reference table are already known before reading the main table during the coordination phase of the query. Therefore, the query to read the reference table is sent immediately after coordination.

## Debugging a query { #query_debug }

### Execution statistics { #execution_stat }
You can specify the `--print-statistics` option in the CLI. The statistics will be printed as follows:
```yson
{
  "incomplete_output" = %false;
  "async_time" = 20458u;
  "memory_usage" = 0u;
  "inner_statistics" = [
    {
      "incomplete_output" = %false;
      "async_time" = 12098u;
      "memory_usage" = 0u;
      "inner_statistics" = [
        {
          "incomplete_output" = %false;
          "async_time" = 4547u;
          "memory_usage" = 0u;
          "sync_time" = 23u;
          "codegen_time" = 8u;
          "rows_read" = 2164;
          "execute_time" = 0u;
          "read_time" = 13u;
          "incomplete_input" = %false;
          "write_time" = 0u;
          "rows_written" = 2164;
          "data_weight_read" = 77352;
        };
        ...
      ];
      "codegen_time" = 5u;
      "sync_time" = 13u;
      "rows_read" = 26908;
      "execute_time" = 5u;
      "read_time" = 0u;
      "incomplete_input" = %false;
      "write_time" = 0u;
      "rows_written" = 26908;
      "data_weight_read" = 1176824;
    };
    ...
  ];
  "codegen_time" = 11u;
  "sync_time" = 64u;
  "rows_read" = 188520;
  "execute_time" = 16u;
  "read_time" = 27u;
  "incomplete_input" = %false;
  "write_time" = 8u;
  "rows_written" = 188520;
  "data_weight_read" = 8245048;
}
```

The statistics have a hierarchical structure and correspond to the query execution structure.
- The first level is aggregation on the coordinator (http or rpc proxy).
- The second level is aggregation on nodes.
- The third level is executing a part of the subquery to the thread pool on nodes.

- `rows_read`: Number of rows read.
- `data_weight_read`: The amount of read data in bytes in uncompressed form.
- `codegen_time`: Code generation time. After the first code generation, the query template (query without literals) gets into the cache.
- `execute_time`: Query execution time excluding read time (decoding data from the chunk format).
- `rows_read`: Number of rows read.
- `read_time`: Synchronous read time.
- `wait_on_ready_event_time`: Reader data waiting time.
- `async_time`: Total waiting time. If it is very different from wait_on_ready_event_time, this means that it is waiting for the CPU to allocate time in the fair-share scheduler.

### The explain-query command { #explain_query }
The explain rows command can be used to debug a query. The command outputs the result in a structured form.
Example:
```
{
    "udf_registry_path" = "//sys/udfs";
    "query" = {
        "where_expression" = "((((oh.ff_status = ?) AND (o.fake_order_type = ?)) AND (NOT is_null(o.external_id))) AND (NOT is_null(oh.order_update_time))) AND (oh.id > ?)";
        "is_ordered_scan" = %false;
        "joins" = [
            [
                {
                    "lookup_type" = "source ranges";
                    "common_key_prefix" = 0u;
                    "foreign_key_prefix" = 1u;
                };
            ];
        ];
        "common_prefix_with_primary_key" = ...;
        "key_trie" = "(key0, { (0#0:0#<Max>] })";
        "ranges" = ...;
    };
    "subqueries" = {
        // Part of the query to be executed on the nodes.
    };
    "top_query" = {
        // Part of the query to be executed on the proxy.
    };
}

```

- `where_expression`: Syntactic representation of the where condition from which read ranges are derived (`ranges`).
- `is_ordered_scan`: Defines whether the order of the table rows will be retained when reading the data. This mode may be needed when implementing pagination. If you need to read the entire set of rows that meet the WHERE condition (no LIMIT), it is faster to read concurrently without preserving the order of rows in the table.
- `joins`: Contains a list of join groups. Join within a group is executed concurrently for multiple tables.
- `lookup_type`: The way of specifying read ranges when executing join. Possible options: source ranges, prefix ranges, and IN clause. The source ranges mode is the most optimal one. In prefix ranges mode, join keys are copied. This occurs if the join condition contains expressions or table fields that are not a secondary table key prefix. In IN clause mode, join is executed as a subquery with an IN expression. This mode is used if the read ranges of the secondary table cannot be derived from the join condition. If there is an additional AND condition in join, an attempt is made to output the read ranges using both the join condition and the additional AND condition.
- `common_key_prefix`: The size of the common key prefix of the primary table and the secondary table.
- `foreign_key_prefix` The size of the secondary table key prefix that is derived from the join condition.
- `common_prefix_with_primary_key`: The size of the table primary key prefix contained in the grouping condition. This enables you not to make a common hash table for all keys (grouping keys with different prefixes cannot match). When using group by and order by concurrently, a non-zero common prefix with the primary key enables order by + limit computation to be transferred to the nodes and reduces the amount of data transferred to the proxy (coordinator).
- `ranges`: A list of table read ranges obtained from the where condition.
- `key_trie`: An intermediate representation of the key column limits generated from the where condition. The read ranges (`ranges`) are then derived from the key trie.
- `top_query`, `subqueries`: Parts of the query running on different cluster nodes. For more information about executing a query, see [Executing a query](#query_execution).

## Extensions and settings

### Query complexity limits (Options)
Option in the driver configuration:
`query_timeout`

Options of individual queries:

- `input_row_limit`: The limit on the number of rows that are read on one cluster node in one thread.
- `output_row_limit`: The limit on the number of rows that are output on one cluster node in one thread. This value also limits the number of rows in grouping on one cluster node.
- `range_expansion_limit`: The maximum number of ranges that will be output from the WHERE condition.
- `fail_on_incomplete_result`: Defines the behavior in case of exceeding `input_row_limit` or `output_row_limit`.
- `memory_limit_per_node`: The limit on the amount of memory available to a query on one cluster node.


<!---

### UDF
Для языка запросов имеется возможность задания пользовательских функций. Их можно писать на C/C++ и загружать на кластер в виде файлов.
Пользовательская функция создается и загружается в систему следующим способом:

Из репозитория {{product-name}} потребуются следующие заголовочные файлы:

- `yt/ytlib/query_client/udf/yt_udf.h`;
- `yt/ytlib/query_client/udf/yt_udf_cpp.h`;
- `yt/ytlib/query_client/function_context.h`.

Для функции на C нужно будет добавить в файл с кодом функции заголовочный файл `yt_udf.h`, а для С++ `yt_udf_cpp.h`. Функции на С++ должны идти с ключевым словом extern "C".

Для пользовательских функций доступны два вида соглашений о вызове (calling convention) — `simple` и `unversioned_value`. Первым параметром в обоих случаях передается указатель на `TExpressionContext`. `TExpressionContext` является непрозрачным объектом и передается в некоторые служебные функции, например для аллокации памяти. Вторым параметром опционально может передаваться указатель на `TFunctionContext`. Контекст функции является разделяемым объектом для вызовов пользовательской функции в пределах одного экземпляра исполнения запроса в одном потоке. Он позволяет произвести инициализацию некоторых данных один раз и использовать их при последующих вызовах пользовательской функции.

- В случае `simple` сигнатура функции описывается следующим образом. В случае если тип результата скалярный, он и является типом результата функции. Соответствие по типам следующее: `Boolean` => `char`, `Int64` => `int64_t`, `Uint64` => `u_int64_t`, `Double` => `double`. В случае если тип результата — строка или Any, результат передается двумя параметрами после контекста — указателем на массив с данными и указателем на размер массива.

```c
char is_prefix(TExpressionContext* context, char* pattern_begin, int pattern_length, char* data_begin, int data_length)
    void lower(TExpressionContext* context, char** result, int* result_len, char* s, int s_len)
```

- В случае `unversioned_value` тип функции должен быть `void`, первым параметром идет указатель на `TExpressionContext`, далее результат передается указателем на `TUnversionedValue`, и далее следуют аргументы функции, которые тоже передаются указателями на `TUnversionedValue`. Для функций с переменным числом аргументов последними параметрами следуют указатель на массив `TUnversionedValue` и количество элементов в массиве. Для данного соглашения о вызове имеется возможность описывать полиморфные функции, задавая множество допустимых типов (тег `union_type`).

```c
void regex_extract(TExpressionContext* expressionContext, NYT::NQueryClient::TFunctionContext* functionContext, TUnversionedValue* result, TUnversionedValue* pattern, TUnversionedValue* input, TUnversionedValue* rewrite)
    void farm_hash(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* args, int args_len)
```

Файл с UDF нужно скомпилировать с помощью clang в llvm байткод.
```
clang++-3.7 -c -emit-llvm -std=c++1y -Wglobal-constructors udf.cpp
```

В случае если udf состоит из нескольких файлов, нужно скомпилировать каждый файл, а потом произвести компоновку файлов с байткодом.
```
llvm-link-3.7 -o result.bc udf.bc additional_file.bc
```

После этого необходимо выполнить оптимизацию получившегося байткода:
```
llvm-opt-3.7 -O2 -internalize -internalize-public-api-list=<список экспортируемых функций> -globalopt -globaldce -o optimized_udf.bc result.bc
```

Получившийся байткод необходимо загрузить на кластер в директорию, которая указана в конфигурации клиента в поле `udf_registry_path` (по умолчанию `//tmp/udfs`). У загруженного файла необходимо выставить атрибут `function_descriptor`, который представляет собой структуру со следующими полями, перечисленными в таблице 2. Обязательными являются: `name`, `result_type`, `argument_types` и `calling_convention`.

<small>Таблица 2 — Структура `function_descriptor`</small>

| **Имя** | **Описание** |
| ------- | ------------ |
| `name` |Имя функции в файле с байткодом. Полезно когда имя файла не совпадает с именем функции. |
| `argument_types` | Массив декрипторов типов, который состоит из полей `tag` и `value`. ||
| `result_type` | Дескриптор типа |
| `repeated_argument_type` | Дескриптор типа |
| `calling_convention` | Соглашение о вызове: `simple | unversioned_value` |
| `use_function_context` | Передавать ли контекст функции. По умолчанию значение %false. |


Дескриптор типа состоит из полей `tag` и `value`. `tag` принимает значения `type_argument | union_type | concrete_type`. Для UDF используются только `union_type` и `concrete_type`. В случае `union_type` `value` — массив имен возможных типов, в случае `concrete_type` — имя типа.

{% note info "Примечание" %}

Префикс `_yt_` зарезервирован для имён внутренних служебных UDF, поэтому не стоит использовать его в именах своих функций.

{% endnote %}

-->
