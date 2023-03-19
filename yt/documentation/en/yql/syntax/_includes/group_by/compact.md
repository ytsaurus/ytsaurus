---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/group_by/compact.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/group_by/compact.md
---
## COMPACT

The presence of [SQL hint](../../lexer.md#sql-hints) `COMPACT` right after the `GROUP` keyword allows for more effective aggregation in cases where the query author knows beforehand that no aggregation key produces large amounts of data (more than a gigabyte or millions of rows). If this assumption fails to materialize, then the operation may fail with Out of Memory error or start running much slower compared to the non-COMPACT version.

Unlike regular GROUP BY, Map-side combiner and additional Reduce for each field with [DISTINCT](#distinct) aggregation are disabled.

**Example:**
```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- top 3 keys based on the number of unique values
FROM my_table
GROUP /*+ COMPACT() */ BY key
ORDER BY count DESC
LIMIT 3;
```
