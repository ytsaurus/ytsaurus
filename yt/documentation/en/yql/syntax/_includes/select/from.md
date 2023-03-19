---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/from.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/from.md
---
## FROM {#from}

Data source for `SELECT`. The argument can accept the table name, the result of another `SELECT`, or a [named expression](../../expressions.md#named-nodes). Between `SELECT` and `FROM`, list the comma-separated column names from the source (or `*` to select all columns).

Search for the table by name in the database specified by the [USE](../../use.md) operator.

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
