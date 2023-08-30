
## TOP and BOTTOM {#top-bottom}

**Signature**
```
TOP(T?, limit:Uint32)->List<T>
TOP(T, limit:Uint32)->List<T>
BOTTOM(T?, limit:Uint32)->List<T>
BOTTOM(T, limit:Uint32)->List<T>
```

Return the list of maximum/minimum expression values. The first argument is an expression, the second one is a limit on the number of items.

**Examples**
```yql
SELECT
    TOP(key, 3),
    BOTTOM(value, 3)
FROM my_table;
```

```yql
$top_factory = AggregationFactory("TOP", 3);
$bottom_factory = AggregationFactory("BOTTOM", 3);

SELECT
    AGGREGATE_BY(key, $top_factory),
    AGGREGATE_BY(value, $bottom_factory)
FROM my_table;
```

## TOP_BY and BOTTOM_BY {#top-bottom-by}

**Signature**
```
TOP_BY(T1?, T2, limit:Uint32)->List<T1>
TOP_BY(T1, T2, limit:Uint32)->List<T1>
BOTTOM_BY(T1?, T2, limit:Uint32)->List<T1>
BOTTOM_BY(T1, T2, limit:Uint32)->List<T1>
```

Return the list of values of the first argument for strings with maximum/minimum values of the second argument. The third argument is the limit on the number of items in the list.

When the [aggregate function factory](../../basic.md#aggregationfactory) is used, `Tuple` of the value and key is passed as the first [AGGREGATE_BY](#aggregateby) argument. In this case, the limit on the number of items is passed as the second argument when creating the factory.

**Examples**
```yql
SELECT
    TOP_BY(value, LENGTH(value), 3),
    BOTTOM_BY(value, key, 3)
FROM my_table;
```

```yql
$top_by_factory = AggregationFactory("TOP_BY", 3);
$bottom_by_factory = AggregationFactory("BOTTOM_BY", 3);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $top_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $bottom_by_factory)
FROM my_table;
```
