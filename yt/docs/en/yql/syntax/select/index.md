# SELECT overview

## Definition

Returns the result of evaluating the expressions specified after `SELECT`. It can be used in combination with other operations to obtain other effect.

Examples:

```yql
SELECT "Hello, world!";
```

```yql
SELECT 2 + 2;
```


## SELECT execution procedure {#selectexec}

The `SELECT` query result is calculated as follows:

* Determine the set of input tables by evaluating the [FROM](from.md) clauses.
* Apply [SAMPLE](sample.md)/[TABLESAMPLE](sample.md) to input tables.
* Execute [FLATTEN COLUMNS](../flatten.md#flatten-columns) or [FLATTEN BY](../flatten.md); aliases set in `FLATTEN BY` become visible after this point.
* Execute every [JOIN](../join.md).
* Add to (or replace in) the data the columns listed in [GROUP BY ... AS ...](../group_by.md) (executed after `WHERE` starting with version [2025.02](../../changelog/2025.02.md#group-by-expr-alias-where)).
* [WHERE](where.md) is executed. All data that don't match the predicate are filtered out.
* [GROUP BY](../group_by.md) is executed and aggregate function values are calculated.
* [HAVING](../group_by.md#having) is filtered.
* [window function](../window.md) values are calculated.
* Evaluate expressions in `SELECT`.
* Assign names set by aliases to expressions in `SELECT`.
* top-level [DISTINCT](distinct.md) is applied to columns obtained this way.
* All subqueries in the [UNION [ALL]](operators.md#union), [INTERSECT [ALL]](operators.md#intersect), and [EXCEPT [ALL]](operators.md#except) operators are processed following the same logic and then joined, intersected, or excluded, respectively. The current implementation doesn't support combining `INTERSECT [ALL]`, `EXCEPT [ALL]`, and `UNION [ALL]`, nor does it allow using more than one `INTERSECT [ALL]` or `EXCEPT [ALL]` operator within a single `SELECT` statement.
* sorting based on [ORDER BY](order_by.md) is performed.
* [OFFSET and LIMIT](limit_offset.md) are applied to the obtained result.

## Column order in YQL {#orderedcolumns}

The standard SQL is sensitive to the order of columns in projections (that is, in `SELECT`). While the order of columns must be preserved in the query results or when writing data to a new table, some SQL constructs use this order.
This is also true for the [UNION [ALL]](operators.md#union), [INTERSECT [ALL]](operators.md#intersect), and [EXCEPT [ALL]](operators.md#except) operators and the positional [ORDER BY](order_by.md) (ORDER BY ordinal).

The column order is ignored in YQL by default:

* The order of columns in the output tables and query results is undefined
* The data schema of `UNION [ALL]`, `INTERSECT [ALL]`, and `EXCEPT [ALL]` results is output by column names rather than positions

If you enable `PRAGMA OrderedColumns;`, the order of columns is preserved in the query results and is derived from the order of columns in the input tables using the following rules:

* `SELECT`: an explicit column enumeration dictates the result order.
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input.
* the column order after [JOIN](../join.md): first, the columns from the left side, then the columns from the right side. If the order for one of the sides included in the `JOIN` output is not defined, the order of output columns is not defined either.
* Column order of `UNION [ALL]`, `INTERSECT [ALL]`, and `EXCEPT [ALL]` depends on [execution mode](operators.md#positional-mode).
* the column order for [AS_TABLE](from_as_table.md) is not defined.


## Combining queries {#combining-queries}

You can join the results of multiple `SELECT` queries (or subqueries) using the `UNION` and `UNION ALL` keywords.

```yql
query1 UNION [ALL] query2 (UNION [ALL] query3 ...)
```

Joining more than two queries is interpreted as a left-associative operation, meaning

```yql
query1 UNION query2 UNION ALL query3
```

is interpreted as

```yql
(query1 UNION query2) UNION ALL query3
```

You can find the intersection of two `SELECT` queries (or subqueries) using the `INTERSECT` and `INTERSECT ALL` keywords.

```yql
query1 INTERSECT query2
```

Queries containing multiple `INTERSECT [ALL]` operators aren't allowed.

```yql
query1 INTERSECT query2 INTERSECT query3 ... -- error

query1 INTERSECT query2 INTERSECT ALL query3 ... -- error
```

You can exclude one `SELECT` query (or subquery) from another using the `EXCEPT` and `EXCEPT ALL` keywords.

```yql
query1 EXCEPT query2
```

Queries containing multiple `EXCEPT [ALL]` operators aren't allowed.

```yql
query1 EXCEPT query2 EXCEPT query3 -- error

query1 EXCEPT query2 EXCEPT ALL query3 -- error
```

Combining different `UNION`, `INTERSECT`, or `EXCEPT` operators within a single query isn't allowed.

```yql
query1 UNION query2 INTERSECT query3 -- error

query1 UNION query2 EXCEPT query3 -- error

query1 INTERSECT query2 EXCEPT query3 -- error
```

This behavior is independent of whether `DISTINCT` / `ALL` is present.

```yql
query1 UNION ALL query2 INTERSECT ALL query3 -- error

query1 UNION ALL query2 EXCEPT ALL query3 -- error

query1 INTERSECT ALL query2 EXCEPT ALL query3 -- error
```

{% note warning %}

Only `UNION` and `UNION ALL` can be used multiple times. Using more than one `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, or `EXCEPT ALL` operator isn't allowed.

{% endnote %}

If `ORDER BY/LIMIT/DISCARD/INTO RESULT` is present in the combined subqueries, the following rules apply:

* `ORDER BY/LIMIT/INTO RESULT` is only permitted after the last subquery.
* `DISCARD` is allowed only before the first subquery.
* The specified operators influence the result of `UNION [ALL]`, `INTERSECT [ALL]`, or `EXCEPT [ALL]` rather than the subquery.
* To apply an operator to a subquery, enclose the subquery in parentheses.


## Accessing multiple tables in one query

In standard SQL, [UNION ALL](operators.md#union-all) is used to execute a query across tables that bundle outputs of two or more `SELECT`s. This is not very convenient for the use case of running the same query on multiple tables (for example, if they contain data for different dates). To make this more convenient, in YQL `SELECT` statements, after `FROM`, you can specify not only a table or a subquery, but also call built-in functions letting you combine data from multiple tables.

The following functions are defined for these purposes:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` combines all the tables listed in the arguments.

`EACH($list_of_strings)` or `EACH($list_of_strings VIEW view_name)` combines all tables whose names are listed in the list of strings. Optionally, you can pass multiple lists in separate arguments similar to `CONCAT`.

{% note warning %}

All of the above functions don't guarantee the order of the table union.

The list of tables is calculated **before** executing the query. Therefore, the tables created during the query execution won't be included in the function results.

{% endnote %}

By default, the schemas of all participating tables are merged based on [UNION ALL](operators.md#union-all) rules. If you don't want to merge schemas, use functions with the `_STRICT` suffix (for example, `CONCAT_STRICT`) — they will work the same but treat any difference in table schemas as an error.

All arguments of the functions described above can be declared separately using [named expressions](../expressions.md#named-nodes). In this case, you can also use  simple expressions in them by implicitly calling [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

To get the name of the source table from which you originally obtained each row, use T[ablePath()](../../builtins/basic.md#tablepath).

#### Examples

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

## Clauses supported in SELECT queries

* [FROM](from.md)
* [FROM AS_TABLE](from_as_table.md)
* [FROM SELECT](from_select.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique_distinct_hints.md)
* [UNION INTERSECT EXCEPT](operators.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
* [CONCAT](concat.md)
