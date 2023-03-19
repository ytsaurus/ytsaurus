---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/action/begin.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/action/begin.md
---
## BEGIN .. END DO {#begin}

Performing an action without declaring it (anonymous action).

**Syntax**
1. `BEGIN`;
1. List of top-level expressions.
1. `END DO`.

An anonymous action can't include any parameters.

**Example**

```
DO BEGIN
    SELECT 1;
    SELECT 2  -- here and in the previous example of ';' you can omit
END DO before END
```
