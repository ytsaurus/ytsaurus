---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/union_all.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/union_all.md
---
## UNION ALL {#union-all}

Concatenating results of multiple `SELECT` statements (or subqueries).

Two `UNION ALL` modes are supported: by column names (the default mode) and by column positions (corresponds to the ANSI SQL standard and is enabled by the [PRAGMA](../../pragma.md#positionalunionall)).

In the "by name" mode, the output of the resulting data schema uses the following rules:

{% include [union all rules](union_all_rules.md) %}

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

If you need to see all results as one link to the full result table, they must be explicitly combined in a [temporary table](temporary_table.md) with the following record:

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


