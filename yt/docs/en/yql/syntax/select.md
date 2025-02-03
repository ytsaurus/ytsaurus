# SELECT syntax


## SELECT

Returns the result of evaluating the expressions specified after `SELECT`.

It can be used in combination with other operations to obtain other effect.

**Examples:**

```yql
SELECT "Hello, world!";
```

```yql
SELECT 2 + 2;
```


<!-- This is all for RTMR, all content below is excluded -->


## FROM {#from}

Data source for `SELECT`. The argument can accept the table name, the result of another `SELECT`, or a [named expression](expressions.md#named-nodes). Between `SELECT` and `FROM`, list the comma-separated column names from the source (or `*` to select all columns).

Search for the table by name in the database specified by the [USE](use.md) operator.

**Examples**

```yql
SELECT key FROM my_table;
```

```yql
SELECT * FROM
  (SELECT value FROM my_table);
```

```yql
$table_name = "my_table";
SELECT * FROM $table_name;
```





## WITH

It's set after the data source in `FROM` and is used for additional hints for tables. You can't use hints for subqueries and [named expressions](expressions.md#named-nodes).

The following values are supported:

* `INFER_SCHEMA`: Sets the flag for output of the table schema. Behaves similarly to the definition of a [yt.InferSchema pragma](pragma.md#inferschema), but only for a specific data source. The number of rows to be output (1 to 1,000) can be specified.
* `FORCE_INFER_SCHEMA` — defines a table schema output flag. Behaves similarly to the definition of a [yt.ForceInferSchema pragma](pragma.md#inferschema), but only for a specific data source. The number of rows to be output (1 to 1,000) can be specified.
* `DIRECT_READ`: Suppresses certain optimizers and enforces accessing table contents as is. Behaves similarly to the definition of a debugging [DirectRead pragma](pragma.md#debug), but only for a specific data source.
* `INLINE`: Hints that the table contents is small and you need to use its in-memory view to process the query. The actual size of the table is not controlled in this case, and if it's large, the query might fail with an out-of-memory error.
* `UNORDERED`: Suppresses original table sorting.
* `XLOCK`: Hints that you need to lock the table exclusively. It's useful when a table is read at the stage of [query metaprogram](action.md) processing. Table content is then refreshed in the main query. Avoids data loss if an external process managed to change the table between executing a metaprogram phase and the main part of the query.
* `SCHEMA` type: Hints that the specified table schema must be used entirely, ignoring the schema in the metadata.
* `COLUMNS` type: Hints that the specified types should be used for columns whose names match the table's column names in the metadata, as well as which columns are additionally present in the table.
* `IGNORETYPEV3`, `IGNORE_TYPE_V3`: Sets the flag to ignore type_v3 types in the table. Behaves similarly to the definition of a [yt.IgnoreTypeV3 pragma](pragma.md#ignoretypev3), but only for a specific data source.

When setting the `SCHEMA` and `COLUMNS` hints, the type must be a [structure](../types/containers.md).


If a `SCHEMA` hint is defined, then when using [EACH](#each), [RANGE](#range), [LIKE](#like), [REGEXP](#regexp), and [FILTER](#filter) table functions, an empty list of tables is permitted that is processed as an empty table with columns described in `SCHEMA`.


**Examples:**

```yql
SELECT key FROM my_table WITH INFER_SCHEMA;
SELECT key FROM my_table WITH FORCE_INFER_SCHEMA="42";
```

```yql
$s = (SELECT COUNT(*) FROM my_table WITH XLOCK);

INSERT INTO my_table WITH TRUNCATE
SELECT EvaluateExpr($s) AS a;
```

```yql
SELECT key, value FROM my_table WITH SCHEMA Struct<key:String, value:Int32>;
```

```yql
SELECT key, value FROM my_table WITH COLUMNS Struct<value:Int32?>;
```

```yql
SELECT key, value FROM EACH($my_tables) WITH SCHEMA Struct<key:String, value:List<Int32>>;
```




## WHERE {#where}

Filtering rows in the `SELECT` result based on a condition.

**Example**

```yql
SELECT key FROM my_table
WHERE value > 0;
```



## ORDER BY {#order-by}

Sorting the `SELECT` result using a comma-separated list of sorting criteria. As a criteria, you can use a column value or an expression on columns. Ordering by column sequence number is not supported (`ORDER BY N`where `N` is a number).

Each criteria can be followed by the sorting direction:
- `ASC`: Sorting in the ascending order. Applied by default.
- `DESC`: Sorting in the descending order.

Multiple sorting criteria will be applied left-to-right.

**Example**

```yql
SELECT key, string_column
FROM my_table
ORDER BY key DESC, LENGTH(string_column) ASC;
```
`ORDER BY` keyword can be used in the [window function](window.md) mechanism as well.


## LIMIT and OFFSET {#limit-offset}

`LIMIT` limits the output to the specified number of rows. If the limit value is `NULL` or no `LIMIT` is specified, the output is unlimited.

`OFFSET`: specifies the offset from the beginning (in rows). If the offset value is `NULL` or no `OFFSET` is specified, the null value is used.

**Examples**

```yql
SELECT key FROM my_table
LIMIT 7;
```

```yql
SELECT key FROM my_table
LIMIT 7 OFFSET 3;
```

```yql
SELECT key FROM my_table
LIMIT 3, 7; -- equivalent to previous example
```

```yql
SELECT key FROM my_table
LIMIT NULL OFFSET NULL; -- equivalent to SELECT key FROM my_table
```



## ASSUME ORDER BY

Checking that the `SELECT` result is sorted by the value in the specified column or multiple columns. The result of such a `SELECT` statement is treated as sorted, but without actually running a sort. Sort check is performed at the query execution stage.

As in case of `ORDER BY`, it supports setting the sort order using the keywords `ASC` (ascending order) and `DESC` (descending order). Expressions are not supported in `ASSUME ORDER BY`.

**Examples:**

```yql
SELECT key || "suffix" as key, -CAST(subkey as Int32) as subkey
FROM my_table
ASSUME ORDER BY key, subkey DESC;
```



## TABLESAMPLE and SAMPLE {#tablesample} {#sample}

Building a random sample from the data source specified in `FROM`.

`TABLESAMPLE` is part of the SQL standard and works as follows:

* The operating mode is specified:
   * `BERNOULLI` means "slowly, straightforwardly going through all the data, but in a truly random way".
   * `SYSTEM` uses knowledge about the physical data storage of data to avoid full data scans, but somewhat sacrificing randomness of the sample.
      The data is split into sufficiently large blocks, and the whole data blocks are sampled. For applied calculations on sufficiently large tables, the result may well be consistent.
* The size of the random sample is indicated as a percentage after the operating mode, in parentheses.
* To manage the block size in the `SYSTEM` mode, use the `yt.SamplingIoBlockSize` pragma
* Optionally, it can be followed by the `REPEATABLE` keyword and an integer in parentheses to be used as a seed for a pseudorandom number generator.

`SAMPLE` is a shorter alias without sophisticated settings and sample size specified as a fraction. It currently corresponds to the `BERNOULLI` mode.

{% note info "Note" %}

In the `BERNOULLI` mode, if the `REPEATABLE` keyword is added, the seed is mixed with the chunk ID for each chunk in the table. That's why sampling from different tables with the same content might produce different results.

{% endnote %}

**Examples:**

```yql
SELECT *
FROM my_table
TABLESAMPLE BERNOULLI(1.0) REPEATABLE(123); -- one percent of a table.
```

```yql
SELECT *
FROM my_table
TABLESAMPLE SYSTEM(1.0); -- approximately one percent of a table.
```

```yql
SELECT *
FROM my_table
SAMPLE 1.0 / 3; -- one third of a table.
```


<!--[Example in tutorial](https://cluster-name.yql/Tutorial/yt_14_Sampling)-->


## DISTINCT {#distinct}

Selecting unique rows.

{% note info %}

Applying `DISTINCT` to calculated values is not currently implemented. For this purpose, use a subquery or the clause [`GROUP BY ... AS ...`](group_by.md).

{% endnote %}

**Example**

```yql
SELECT DISTINCT value -- only unique values from the table
FROM my_table.
```

Also, the `DISTINCT` keyword can be used to apply aggregate functions only to unique values. For more information, see the documentation for [GROUP BY](group_by.md).


## SELECT execution procedure {#selectexec}

The `SELECT` query result is calculated as follows:

* a set of input tables is defined and expressions after [FROM](#from) are calculated.
* [SAMPLE](#sample)/[TABLESAMPLE](#sample) is applied to input tables.
* [FLATTEN COLUMNS](flatten.md#flatten-columns) or [FLATTEN BY](flatten.md) is executed. Aliases set in `FLATTEN BY` become visible after this point.
* all [JOIN](join.md)s are executed.
* Add to (or replace in) the data the columns listed in [GROUP BY ... AS ...](group_by.md);
* [WHERE](#where) &mdash; is executed. All data that don't match the predicate are filtered out.
* [GROUP BY](group_by.md) is executed and aggregate function values are calculated.
* [HAVING](group_by.md#having) is filtered.
* [window function](window.md) values are calculated.
* Evaluate expressions in `SELECT`;
* Assign names set by aliases to expressions in `SELECT`.
* top-level [DISTINCT](#distinct) is applied to columns obtained this way.
* all subqueries in [UNION ALL](#unionall) are calculated the same way, then they get joined (see [PRAGMA AnsiOrderByLimitInUnionAll](pragma.md#pragmas)).
* sorting based on [ORDER BY](#order-by) is performed.
* [OFFSET and LIMIT](#limit-offset) are applied to the obtained result.



## Column order in YQL {#orderedcolumns}
The standard SQL is sensitive to the order of columns in projections (that is, in `SELECT`). While the order of columns must be preserved in the query results or when writing data to a new table, some SQL constructs use this order.
This is also true for [UNION ALL](#unionall) and positional [ORDER BY](#orderby) (ORDER BY ordinal).

The column order is ignored in YQL by default:
* The order of columns in the output tables and query results is undefined
* The data scheme of the `UNION ALL` result is output by column names rather than positions

If you enable `PRAGMA OrderedColumns;`, the order of columns is preserved in the query results and is derived from the order of columns in the input tables using the following rules:
* `SELECT`: an explicit column enumeration dictates the result order.
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input.
* the column order after [JOIN](join.md): first, the columns from the left side, then the columns from the right side. If the order for one of the sides included in the `JOIN` output is not defined, the order of output columns is not defined either.
* `UNION ALL` order depends on [UNION ALL](#unionall) execution mode.
* the column order for [AS_TABLE](#as_table) is not defined.



{% note warning "Attention!" %}

Key columns always go before non-key columns in table schema {{product-name}}. The order of key columns is determined by the order of the composite key.
When `PRAGMA OrderedColumns;` is enabled, non-key columns preserve their output order.

{% endnote %}

When `PRAGMA OrderedColumns;` is used, the sequence of columns after [PROCESS](process.md)/[REDUCE](reduce.md) is not defined.


## UNION ALL {#union-all}

Concatenating results of multiple `SELECT` statements (or subqueries).

Two `UNION ALL` modes are supported: by column names (the default mode) and by column positions (corresponds to the ANSI SQL standard and is enabled by the [PRAGMA](pragma.md#positionalunionall)).

In the "by name" mode, the output of the resulting data schema uses the following rules:


* The resulting table includes all columns that were found in at least one of the input tables.
* if a column wasn't present in any input table, it's automatically assigned [optional data type](../types/optional.md) (that permits `NULL` value);
* If a column in different input tables had different types, then the shared type (the broadest one) is output.
* If a column in different input tables had a heterogeneous type, for example, string and numeric, an error is raised.


The order of output columns in this mode is equal to the largest common prefix of the order of inputs, followed by all other columns in the alphabetic order.
If the largest common prefix is empty (for example, if the order isn't specified for one of the inputs), then the output order is undefined.

In the "by position" mode, the output of the resulting data schema uses the following rules:
* All inputs must have equal number of columns
* The order of columns must be defined for all inputs
* The names of the resulting columns must match the names of columns in the first table
* The type of the resulting columns is output as a common (widest) type of input column types having the same positions

The order of the output columns in this mode is the same as the order of columns in the first input.

If `ORDER BY/LIMIT/DISCARD/INTO RESULT` is present in the combined subqueries, the following rules apply:
* `ORDER BY/LIMIT/INTO RESULT` is only permitted after the last subquery.
* `DISCARD` is allowed only before the first subquery.
* The specified operators influence the result of `UNION ALL` rather than the subquery.
* To apply an operator to a subquery, enclose the subquery in parentheses.

**Examples**

```yql
SELECT 1 AS x
UNION ALL
SELECT 2 AS y
UNION ALL
SELECT 3 AS z;
```

In default mode, the result of executing this query will be a selection with three columns: x, y, and z. When `PRAGMA PositionalUnionAll;` is enabled, the selection will have one column: x.


```yql
PRAGMA PositionalUnionAll;

SELECT 1 AS x, 2 as y
UNION ALL
SELECT * FROM AS_TABLE([<|x:3, y:4|>]); -- error. The order of columns in AS_TABLE is not defined.
```

```yql
SELECT * FROM T1
UNION ALL
(SELECT * FROM T2 ORDER BY key LIMIT 100); -- if there are no parentheses, ORDER BY/LIMIT will apply to the aggregate result of UNION ALL.
```


{% note warning "Attention!" %}

`UNION ALL` doesn't physically merge subquery results. In `UNION ALL` output results, each subselect can be presented as an individual link to the full result table if the aggregate result exceeds the sample limit.

If you need to see all results as one link to the full result table, they must be explicitly combined in a [temporary table](#temporary-tables) with the following record:

```yql
INSERT INTO @tmp
SELECT 1 AS x
UNION ALL
SELECT 2 AS y
UNION ALL
SELECT 3 AS z;

COMMIT;

SELET * FROM @tmp;
```

{% endnote %}





## COMMIT {#commit}

By default, the entire YQL query is executed within a single transaction, and independent parts inside it are executed in parallel, if possible.
Using the `COMMIT;` keyword you can add a barrier to the execution process to delay execution of expressions that follow until all the preceding expressions have completed.

To commit in the same way automatically after each expression in the query, you can use `PRAGMA autocommit;`.

**Examples:**

```yql
INSERT INTO result1 SELECT * FROM my_table;
INSERT INTO result2 SELECT * FROM my_table;
COMMIT;
-- The content of SELECT from the second row will already be in result2:
INSERT INTO result3 SELECT * FROM result2;
```




## Accessing multiple tables in one query {#concat} {#each} {#range} {#like} {#filter} {#regexp}

In standard SQL, [UNION ALL](#unionall) is used to execute a query across tables that bundle outputs of two or more `SELECT`s. This is not very convenient for the use case of running the same query on multiple tables (for example, if they contain data for different dates). To make this more convenient, in YQL `SELECT` statements, after `FROM`, you can specify not only a table or a subquery, but also call built-in functions letting you combine data from multiple tables.

The following functions are defined for these purposes:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` combines all the tables listed in the arguments.

```EACH($list_of_strings) or EACH($list_of_strings VIEW view_name)``` combines all tables whose names are listed in the list of strings. Optionally, you can pass multiple lists in separate arguments similar to `CONCAT`.

```RANGE(`prefix`, `min`, `max`, `suffix`, `view`)``` combines a range of tables. Arguments:

* The prefix is the folder to search tables in. It's specified without a trailing slash. This is the only required argument. If it's the only one specified, all tables in the folder are used.
* min, max: These two arguments specify a range of table names included. The range is inclusive on both ends. If no range is specified, all tables in the prefix folder are used. The names of tables or folders located in the folder specified in prefix are lexicographically matched with the `[min, max]` range rather than concatenated, so make sure to specify the range without leading slashes.
* suffix is the name of the table. It's expected without a leading slash. If no suffix is specified, then the `[min, max]` arguments specify a range of table names. If the suffix is specified, then the `[min, max]` arguments specify a range of folders hosting a table with the name specified in the suffix argument.

```LIKE(`prefix`, `pattern`, `suffix`, `view`)``` and ```REGEXP(`prefix`, `pattern`, `suffix`, `view`)``` — the pattern argument is set in a format similar to the same-name binary operator: [LIKE](expressions.md#like) and [REGEXP](expressions.md#regexp).

```FILTER(`prefix`, `callable`, `suffix`, `view`)```: The `callable` argument must be a callable expression with the `(String)->Bool` signature that will be called for each table/subdirectory in the prefix folder. The query will only include those tables for which the callable value returned `true`. [Lambda functions](expressions.md#lambda) are most convenient to use as callable values.

{% note warning "Attention!" %}

All of the above functions don't guarantee the order of the table union.

The list of tables is calculated **before** executing the query. Therefore, the tables created during the query execution won't be included in the function results.

{% endnote %}

By default, the schemas of all participating tables are bundled based on [UNION ALL](#unionall) rules. If you don't want to combine schemas, then you can use functions with the `_STRICT` suffix, for example `CONCAT_STRICT` or `RANGE_STRICT` that are totally similar to the original functions, but treat any difference in table schemas as an error.

To specify a cluster of unioned tables, add it before the function name.

All arguments of the functions described above can be declared separately using [named expressions](expressions.md#named-nodes). In this case, you can also use  simple expressions in them by implicitly calling `EvaluateExpr`.

To get the name of the source table from which you originally obtained each row, use `TablePath()`.


**Examples:**

```yql
USE some_cluster;
SELECT * FROM CONCAT(
  `table1`,
  `table2`,
  `table3`);
```

```yql
USE some_cluster;
$indices = ListFromRange(1, 4);
$tables = ListMap($indices, ($index) -> {
    RETURN "table" || CAST($index AS String);
});
SELECT * FROM EACH($tables); -- identical to the previous example.
```

```yql
USE some_cluster;
SELECT * FROM RANGE(`my_folder`);
```

```yql
SELECT * FROM some_cluster.RANGE( -- You can specify the cluster before the name of the
  `my_folder`,
  `from_table`,
  `to_table`) function;
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_folder`,
  `to_folder`,
  `my_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_table`,
  `to_table`,
  ``,
  `my_view`);
```

```yql
USE some_cluster;
SELECT * FROM LIKE(
  `my_folder`,
  "2017-03-%"
);
```

```yql
USE some_cluster;
SELECT * FROM REGEXP(
  `my_folder`,
  "2017-03-1[2-4]?"
);
```

```yql
$callable = ($table_name) -> {
    return $table_name > "2017-03-13";
};

USE some_cluster;
SELECT * FROM FILTER(
  `my_folder`,
  $callable
);
```


## Listing the contents of a directory in a cluster {#folder}

Specified as the `FOLDER` function in [FROM](#from).

Arguments:

1. The path to the directory.
2. An optional string with a list of meta attributes separated by a semicolon.

The result is a table with three fixed columns:

1. **Path** (`String`): The full name of the table.
2. **Type** (`String`): The node type (table, map_node, file, document, and others).
3. **Attributes** (`Yson`) is a Yson dictionary with meta attributes ordered in the second argument.

Recommendations for use:

* To get only the list of tables, you must add `...WHERE Type == "table"`. Then you can add more conditions if you want using the `AGGREGATE_LIST` aggregate function from the Path column to get only the list of paths and pass them to [EACH](#each).
* Because the Path column is returned in the same format as the `TablePath()` function's output, you can use it for the table's JOIN attributes for its rows.
* We recommend that you work with the Attributes column using [Yson UDF](../udf/list/yson.md).

{% note warning "Attention!" %}

Use FOLDER with attributes containing large values with caution (`schema` could be one of those). The query containing FOLDER in a folder with a large number of tables and a heavy attribute may cause a severe load on the wizard{{product-name}}.

{% endnote %}

**Examples:**

```yql
USE hahn;

$table_paths = (
    SELECT AGGREGATE_LIST(Path)
    FROM FOLDER("my_folder", "schema;row_count")
    WHERE
        Type = "table" AND
        Yson::GetLength(Attributes.schema) > 0 AND
        Yson::LookupInt64(Attributes, "row_count") > 0
);

SELECT COUNT(*) FROM EACH($table_paths);
```




## WITHOUT {#without}

Excluding columns from the result of `SELECT *`.

**Examples**

```yql
SELECT * WITHOUT foo, bar FROM my_table;
```

```yql
PRAGMA simplecolumns;
SELECT * WITHOUT t.foo FROM my_table AS t
CROSS JOIN (SELECT 1 AS foo) AS v;
```


## FROM ... SELECT ... {#from-select}

An inverted format, first specifying the data source and then the operation.

**Examples**

```yql
FROM my_table SELECT key, value;
```

```yql
FROM a_table AS a
JOIN b_table AS b
USING (key)
SELECT *;
```




## Data views (VIEW) {#view}

YQL supports two types of data views:

* Linked to specific tables.
* Independent, that might use an arbitrary number of tables within the cluster.
   Both of the views are non-materialized. It means that they are substituted into the calculation graph at each use.

Accessing a `VIEW`:

* Views linked to a table require a special syntax: ```[cluster.]`path/to/table` VIEW view_name```;
* Views that aren't linked to tables, look like regular tables from the user perspective.

If the meta attributes of the table specify an automatic UDF call to convert raw data into a structured set of columns, you can access raw data using a special `raw` view like this: ```[cluster.]`path/to/table` VIEW raw```.

**Examples:**

```yql
USE some_cluster;
SELECT *
FROM my_table VIEW my_view;
```


<!--[Learn more about view creation](../misc/schema.md#view).-->

## Explicitly created temporary (anonymous) tables {#temporary-tables}

In complex multiphase queries, it can be useful to explicitly create a physical temporary table in order to manually affect the query process. To do this, you can use the table name starting with `@`. Such tables are called anonymous to distinguish them from temporary tables created by {{product-name}} operation.

Each such name within the query is replaced, at execution time, by a globally unique path to a table in a temporary directory. Such temporary tables are automatically deleted upon completion of the query, following the same rules as implicitly created tables.

This feature lets you ignore conflicts in paths to temporary tables between parallel operations, and also avoid deleting them explicitly when the query completes.

**Examples:**

```yql
INSERT INTO @my_temp_table
SELECT * FROM my_input_table ORDER BY value;

COMMIT;

SELECT * FROM @my_temp_table WHERE value = "123"
UNION ALL
SELECT * FROM @my_temp_table WHERE value = "456";
```

Temporary table names can use [named expressions](expressions.md#named-nodes).

```yql
$tmp_name = "my_temp_table";

INSERT INTO @$tmp_name
SELECT 1 AS one, 2 AS two, 3 AS three;

COMMIT;

SELECT * FROM @$tmp_name;
```




## FROM AS_TABLE {#as-table}

Accessing named expressions as tables using the `AS_TABLE` function.

`AS_TABLE($variable)` lets you use the value of `$variable` as the data source for the query. In this case, the `$variable` variable may be `List<Struct<...>>` type.

**Example**

```yql
$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value),
    AsStruct(2u AS Key, "v2" AS Value),
    AsStruct(3u AS Key, "v3" AS Value));

SELECT Key, Value FROM AS_TABLE($data);
```

