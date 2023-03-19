---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/discard.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/discard.md
---
# DISCARD

Calculates [`SELECT`](../select.md), [`REDUCE,`](../reduce.md) or [`PROCESS`](../process.md), but returns no result to the client or to the table. It can't be set at the same time as [INTO RESULT](../into_result.md).

It's good to combine it with `Ensure` to check the final calculation result against the user's criteria.

**Examples**

```yql
DISCARD SELECT 1;
```

```yql
INSERT INTO result_table WITH TRUNCATE
SELECT * FROM
my_table
WHERE value % 2 == 0;

COMMIT;

DISCARD SELECT Ensure(
    0, -- will discard result anyway
    COUNT(*) > 1000,
    "Too small result table, got only " || CAST(COUNT(*) AS String) || " rows"
) FROM result_table;

```
