---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/from_as_table.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/from_as_table.md
---
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
