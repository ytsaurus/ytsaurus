---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/aggregate_by.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/aggregate_by.md
---

## AGGREGATE_BY and MULTI_AGGREGATE_BY {#aggregate-by}
Applying the [aggregate function factory](../../basic.md#aggregationfactory) to all column or expression values. The `MULTI_AGGREGATE_BY` function requires a structure, tuple, or list in a column or expression value and applies the factory on an item-by-item basis, placing the result in a container of the same form. If different column or expression values contain lists of different lengths, the resulting list will have the smallest of the lengths of those lists.

1. Column, `DISTINCT` column, or expression.
2. Factory.

**Examples:**
```yql
$count_factory = AggregationFactory("COUNT");

SELECT
    AGGREGATE_BY(DISTINCT column, $count_factory) as uniq_count
FROM my_table;

SELECT
    MULTI_AGGREGATE_BY(nums, AggregationFactory("count")) as count,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("min")) as min,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("max")) as max,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("percentile", 0.9)) as p90
FROM my_table;
```
