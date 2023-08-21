
## AggregationFactory {#aggregationfactory}

Create the [aggregate function](../../aggregation.md) factory to separate the process of describing how to aggregate data and which data to apply it to.

Arguments:

1. A string in quotation marks that is the name of an aggregate function, for example, ["MIN"](../../aggregation.md#min).
2. Optional aggregate function parameters that do not depend on the data. For example, the percentile value in [PERCENTILE](../../aggregation.md#percentile).

The resulting factory can be used as a second parameter of the [AGGREGATE_BY](../../aggregation.md#aggregateby) function.
If the aggregate function works on two columns instead of one, for example, [MIN_BY](../../aggregation.md#minby), `Tuple` of two values is passed as a first argument in [AGGREGATE_BY](../../aggregation.md#aggregateby). For more information, see the description of this aggregate function.

**Examples:**
```yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY(value, $factory) AS min_value -- apply MIN aggregation to value column
FROM my_table;
```

## AggregateTransform... {#aggregatetransform}

`AggregateTransformInput()` transforms the [aggregate function](../../aggregation.md) factory, for example, the one obtained via the [AggregationFactory](#aggregationfactory) function, into another factory in which the specified transformation of input items is performed before the aggregation starts.

Arguments:

1. Aggregate function factory.
2. Lambda function with one argument which transforms an input item.

**Examples:**
```yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate(["1","2","3"], $g); -- 6
select ListAggregate([1,2,3], $h); -- 12
```

`AggregateTransformOutput()` transforms the [aggregate function](../../aggregation.md) factory, for example, the one obtained via the [AggregationFactory](#aggregationfactory) function, into another factory in which the specified transformation of the result is performed after the aggregation is completed.

Arguments:

1. Aggregate function factory.
2. Lambda function with one argument which transforms the result.

**Examples:**
```yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate([1,2,3], $g); -- 12
```

## AggregateFlatten {#aggregateflatten}

Adapts the [aggregate function](../../aggregation.md) factory, for example, the one obtained via the [AggregationFactory](#aggregationfactory) function, so that it performs aggregation on input items — lists. This operation is similar to [FLATTEN LIST BY](../../../syntax/flatten.md): each item in the list is aggregated.

Arguments:

1. Aggregate function factory.

**Examples:**
```yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
select AggregateBy(x, $j) from (
   select [1,2] as x
   union all
   select [2,3] as x
); -- [1, 2, 3]

```
