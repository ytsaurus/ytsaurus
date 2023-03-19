---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/is-null.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/is-null.md
---
## IS \[NOT\] NULL {#is-null}

Matching an empty value (`NULL`). Since `NULL` is a special value that [equals nothing](../../../types/optional.md#null_expr), regular [comparison operators](#comparison-operators) are not suitable for this task.

**Examples**

```yql
SELECT key FROM my_table
WHERE value IS NOT NULL;
```
