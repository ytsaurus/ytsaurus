---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/count_distinct_estimate.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/count_distinct_estimate.md
---
## CountDistinctEstimate, HyperLogLog and HLL {#countdistinctestimate}

**Signature**
```
CountDistinctEstimate(T)->Uint64?
HyperLogLog(T)->Uint64?
HLL(T)->Uint64?
```

Approximate estimate of the number of unique values using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm. Logically does the same as [COUNT(DISTINCT ...)](#count), but works much faster and with a small margin of error.

Arguments:

1. Value for estimation.
2. Accuracy (from 4 to 18 inclusive, the default value is 14).

The choice of accuracy enables you to trade off the additional consumption of computing resources and RAM for error reduction.

All three functions are currently aliases, but `CountDistinctEstimate` may start using a different algorithm in the future.

**Examples**
```yql
SELECT
  CountDistinctEstimate(my_column)
FROM my_table;
```

```yql
SELECT
  HyperLogLog(my_column, 4)
FROM my_table;
```

