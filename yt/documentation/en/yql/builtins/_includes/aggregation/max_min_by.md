---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/max_min_by.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/max_min_by.md
---
## MAX_BY and MIN_BY {#max-min-by}

**Signature**
```
MAX_BY(T1?, T2)->T1?
MAX_BY(T1, T2)->T1?
MAX_BY(T1, T2, limit:Uint64)->List<T1>?

MIN_BY(T1?, T2)->T1?
MIN_BY(T1, T2)->T1?
MIN_BY(T1, T2, limit:Uint64)->List<T1>?
```

Return the value of the first argument for the table row in which the second argument was minimum/maximum.

You can optionally specify the third argument N which affects the behavior if the table has multiple rows with the same minimum or maximum value:

* If N is not specified, the value of one of the rows will be returned and the rest are discarded.
* If N is specified, the list with all values will be returned, but no more than N, all values are discarded after reaching the specified number.

When choosing the N value, do not exceed hundreds or thousands to avoid problems with the limited available memory on {{product-name}} clusters.

If the job necessarily needs all values and their number can be measured in tens of thousands or more, then instead of these aggregate functions use `JOIN` of the original table with a subquery where `GROUP BY + MIN/MAX` is performed on the columns you are interested in.

{% note warning "Attention!" %}

If the second argument is always NULL, the aggregation result is NULL.

{% endnote %}

When the [aggregate function factory](../../basic.md#aggregationfactory) is used, `Tuple` of the value and key is passed as the first [AGGREGATE_BY](#aggregateby) argument.

**Examples**
```yql
SELECT
  MIN_BY(value, LENGTH(value)),
  MAX_BY(value, key, 100)
FROM my_table;
```

```yql
$min_by_factory = AggregationFactory("MIN_BY");
$max_by_factory = AggregationFactory("MAX_BY", 100);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $min_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $max_by_factory)
FROM my_table;
```
