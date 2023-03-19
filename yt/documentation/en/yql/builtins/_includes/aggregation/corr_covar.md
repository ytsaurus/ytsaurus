---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/corr_covar.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/corr_covar.md
---
## CORRELATION and COVARIANCE {#correlation-covariance}

**Signature**
```
CORRELATION(Double?, Double?)->Double?
COVARIANCE(Double?, Double?)->Double?
COVARIANCE_SAMPLE(Double?, Double?)->Double?
COVARIANCE_POPULATION(Double?, Double?)->Double?
```

Correlation and covariance of two columns.

Abbreviated versions `CORR` or `COVAR` are also available. For covariance, versions with the `SAMPLE`/`POPULATION` suffix similar to [VARIANCE](#variance) described above are available.

Unlike the majority of other aggregate functions, they do not skip `NULL` and count it as 0.

When the [aggregate function factory](../../basic.md#aggregationfactory) is used, `Tuple` of two values is passed as the first [AGGREGATE_BY](#aggregateby) argument.

**Examples**
```yql
SELECT
  CORRELATION(numeric_column, another_numeric_column),
  COVARIANCE(numeric_column, another_numeric_column)
FROM my_table;
```

```yql
$corr_factory = AggregationFactory("CORRELATION");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, another_numeric_column), $corr_factory)
FROM my_table;
```
