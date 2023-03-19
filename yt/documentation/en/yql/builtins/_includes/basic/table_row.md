---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/table_row.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/table_row.md
---
## TableRow, JoinTableRow {#tablerow}

Getting the entire table row as a structure. No arguments. `JoinTableRow` in case of `JOIN` always returns a structure with table prefixes.

**Signature**
```
TableRow()->Struct
```

**Example**
```yql
SELECT TableRow() FROM my_table;
```
