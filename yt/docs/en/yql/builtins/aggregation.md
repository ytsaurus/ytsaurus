

## COUNT {#count}

**Signature**
```
COUNT(*)->Uint64
COUNT(T)->Uint64
COUNT(T?)->Uint64
```

Counting the number of rows in the table (if `*` or constant is specified as the argument) or non-empty values in a table column (if the column name is specified as an argument).

Like other aggregate functions, it can be combined with [GROUP BY](../syntax/group_by.md) to get statistics on the parts of the table that correspond to the values in the columns being grouped. Use the [DISTINCT](../syntax/group_by.md#distinct) modifier to count the number of unique values.

**Examples**
```yql
SELECT COUNT(*) FROM my_table;
```

```yql
SELECT key, COUNT(value) FROM my_table GROUP BY key;
```

```yql
SELECT COUNT(DISTINCT value) FROM my_table;
```

## MIN and MAX {#min-max}

**Signature**
```
MIN(T?)->T?
MIN(T)->T?
MAX(T?)->T?
MAX(T)->T?
```

Minimum or maximum value.

As an argument, you may use an arbitrary computable expression with a comparable result.

**Examples**
```yql
SELECT MIN(value), MAX(value) FROM my_table;
```

## SUM {#sum}

**Signature**
```
SUM(Unsigned?)->Uint64?
SUM(Signed?)->Int64?
SUM(Interval?)->Interval?
SUM(Decimal(N, M)?)->Decimal(35, M)?
```

Sum of the numbers.

As an argument, you may use an arbitrary computable expression with a numeric result or `Interval` type.

Integers are automatically expanded to 64 bits to reduce the risk of overflow.

```yql
SELECT SUM(value) FROM my_table;
```

## AVG {#avg}

**Signature**
```
AVG(Double?)->Double?
AVG(Interval?)->Interval?
AVG(Decimal(N, M)?)->Decimal(N, M)?
```

Arithmetic average.

As an argument, you may use an arbitrary computable expression with a numeric result or `Interval` type.

Integer values and time intervals are automatically converted to Double.

**Examples**
```yql
SELECT AVG(value) FROM my_table;
```

## COUNT_IF {#count-if}

**Signature**
```
COUNT_IF(Bool?)->Uint64?
```

Number of rows for which the expression specified as the argument is true (the expression's calculation result is true).

The value `NULL` is equated to `false` (if the argument type is `Bool?`).

The function *does not* do the implicit type casting to Boolean for strings and numbers.

**Examples**
```yql
SELECT
  COUNT_IF(value % 2 == 1) AS odd_count
```

{% note info %}

If you need to count the number of unique values on the rows that meet the condition, then unlike other aggregate functions, you can't use the [DISTINCT](../syntax/group_by.md#distinct) modifier, since the arguments contain no values. To get this result, use, inside a subquery, the built-in `IF` function with two arguments (to get `NULL` in ELSE), and apply an outer [COUNT(DISTINCT ...)](#count) to its results.

{% endnote %}

## SUM_IF and AVG_IF {#sum-if}

**Signature**
```
SUM_IF(Unsigned?, Bool?)->Uint64?
SUM_IF(Signed?, Bool?)->Int64?
SUM_IF(Interval?, Bool?)->Interval?

AVG_IF(Double?, Bool?)->Double?
```

Sum or arithmetic average, but only for the rows that satisfy the condition passed by the second argument.

Therefore, `SUM_IF(value, condition)` is a slightly shorter notation for `SUM(IF(condition, value))`, same for `AVG`. The argument's data type expansion is similar to the same-name functions without a suffix.

**Examples**
```yql
SELECT
    SUM_IF(value, value % 2 == 1) AS odd_sum,
    AVG_IF(value, value % 2 == 1) AS odd_avg,
FROM my_table;
```

When the [aggregate function factory](basic.md#aggregationfactory) is used, [Tuple](#aggregateby) of the value and predicate is passed as the first `AGGREGATE_BY` argument.

**Examples**

```yql
$sum_if_factory = AggregationFactory("SUM_IF");
$avg_if_factory = AggregationFactory("AVG_IF");

SELECT
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $sum_if_factory) AS odd_sum,
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $avg_if_factory) AS odd_avg
FROM my_table;
```

## SOME {#some}

**Signature**
```
SOME(T?)->T?
SOME(T)->T?
```

Get the value for an expression specified as an argument, for one of the table rows. Gives no guarantee of which row is used. This function is a counterpart for [any()](https://clickhouse.tech/docs/ru/sql-reference/aggregate-functions/reference/any/) in ClickHouse.

Because of no guarantee, `SOME` is computationally cheaper than [MIN](#min)/[MAX](#max) (that are often used in similar situations).

**Examples**
```yql
SELECT
  SOME(value)
FROM my_table;
```

{% note alert %}

When the aggregate function `SOME` is called multiple times, it's **not** guaranteed that all the resulting values are taken from the same row of the source table. To get this guarantee, pack the values into any container and pass it to `SOME`. For example, for a structure you can do this using `AsStruct`.

{% endnote %}


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




## AGGREGATE_LIST {#agg-list}

**Signature**
```
AGGREGATE_LIST(T? [, limit:Uint64])->List<T>
AGGREGATE_LIST(T [, limit:Uint64])->List<T>
AGGREGATE_LIST_DISTINCT(T? [, limit:Uint64])->List<T>
AGGREGATE_LIST_DISTINCT(T [, limit:Uint64])->List<T>
```

Get all column values as a list. In combination with `DISTINCT`, returns only unique values. The optional second parameter sets the maximum number of obtained values.

If you know in advance that there are not many unique values, better use the `AGGREGATE_LIST_DISTINCT` aggregate function that builds the same result in memory (which may not be enough for a large number of unique values).

The order of elements in the resulting list depends on the implementation and is not set externally. To get an ordered list, sort the result, for example, using [ListSort](list.md#listsort).

To get a list of several values from one string, *DO NOT* use the `AGGREGATE_LIST` function  several times, but place all the desired values in a container, for example, via [AsList](basic.md#aslist) or [AsTuple](basic.md#astuple) and pass this container in a single `AGGREGATE_LIST` invocation.

For example, you can use it in combination with `DISTINCT` and the [String::JoinFromList](../udf/list/string.md) function (analog of `','.join(list)` from Python) to print to a string all the values that were seen in the column after applying [GROUP BY](../syntax/group_by.md).

**Examples**

```yql
SELECT
   AGGREGATE_LIST( region ),
   AGGREGATE_LIST( region, 5 ),
   AGGREGATE_LIST( DISTINCT region ),
   AGGREGATE_LIST_DISTINCT( region ),
   AGGREGATE_LIST_DISTINCT( region, 5 )
FROM users
```

```yql
-- Analog of GROUP_CONCAT from MySQL
SELECT
    String::JoinFromList(CAST(AGGREGATE_LIST(region, 2) AS List<String>), ",")
FROM users
```
There is also a short form of these functions: `AGG_LIST` and `AGG_LIST_DISTINCT`.

{% note alert %}

Executed **NOT** in a lazy way, so when you use it, you need to make sure that you get a list of reasonable size, around a thousand items. To be on the safe side, you can use a second optional numeric argument that includes a limit on the number of items in the list.

{% endnote %}

If the number of items in the list is exceeded, the `Memory limit exceeded` error is returned.

<!--[Example in tutorial](https://cluster-name.yql/)-->


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

When the [aggregate function factory](basic.md#aggregationfactory) is used, `Tuple` of the value and key is passed as the first [AGGREGATE_BY](#aggregateby) argument.

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

When the [aggregate function factory](basic.md#aggregationfactory) is used, `Tuple` of the value and key is passed as the first [AGGREGATE_BY](#aggregateby) argument. In this case, the limit on the number of items is passed as the second argument when creating the factory.

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



## TOPFREQ and MODE {#topfreq-mode}

**Signature**
```
TOPFREQ(T [, num:Uint32 [, bufSize:Uint32]])->List<Struct<Frequency:Uint64, Value:T>>
MODE(T [, num:Uint32 [, bufSize:Uint32]])->List<Struct<Frequency:Uint64, Value:T>>
```

Getting an approximate list of the most frequent column values with an estimate of their number. Return a list of structures with two fields:

* `Value`: The found frequent value.
* `Frequency`: Estimate of the number of mentions in the table.

Mandatory argument: the value itself.

Optional arguments:

1. For `TOPFREQ`: The desired number of items in the result. `MODE` is an alias to `TOPFREQ` with 1 in this argument. For `TOPFREQ`, the default value is also 1.
2. The number of items in the used buffer, which enables you to trade off memory consumption for accuracy. The default value is 100.

**Examples**
```yql
SELECT
    MODE(my_column),
    TOPFREQ(my_column, 5, 1000)
FROM my_table;
```



## STDDEV and VARIANCE {#stddev-variance}

**Signature**
```
STDDEV(Double?)->Double?
STDDEV_POPULATION(Double?)->Double?
POPULATION_STDDEV(Double?)->Double?
STDDEV_SAMPLE(Double?)->Double?
STDDEVSAMP(Double?)->Double?

VARIANCE(Double?)->Double?
VARIANCE_POPULATION(Double?)->Double?
POPULATION_VARIANCE(Double?)->Double?
VARPOP(Double?)->Double?
VARIANCE_SAMPLE(Double?)->Double?
```

Standard variance and dispersion by column. A [one-pass parallel algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm) is used the result of which may differ from that obtained by more common methods that require two passes over the data.

By default, sample dispersion and standard variance are calculated. Several write methods are available:

* With the `POPULATION` suffix/prefix, for example, `VARIANCE_POPULATION` and `POPULATION_VARIANCE`: Calculates dispersion/standard variance for the general population.
* With the `SAMPLE` suffix or without a suffix, for example, `VARIANCE_SAMPLE`, `SAMPLE_VARIANCE`, and `VARIANCE`: Calculates sample dispersion and standard variance.

There are also several abbreviated aliases, for example, `VARPOP` or `STDDEVSAMP`.

If all passed values are `NULL`, `NULL` is returned.

**Examples**
```yql
SELECT
  STDDEV(numeric_column),
  VARIANCE(numeric_column)
FROM my_table;
```


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

When the [aggregate function factory](basic.md#aggregationfactory) is used, `Tuple` of two values is passed as the first [AGGREGATE_BY](#aggregateby) argument.

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



## PERCENTILE and MEDIAN {#percentile-median}

**Signature**
```
PERCENTILE(Double?, Double)->Double?
PERCENTILE(Interval?, Double)->Interval?

MEDIAN(Double? [, Double])->Double?
MEDIAN(Interval? [, Double])->Interval?
```

Calculating percentiles according to the amortized version of the [TDigest](https://github.com/tdunning/t-digest) algorithm. `MEDIAN`: Alias for `PERCENTILE(N, 0.5)`.

{% note info "Limitation" %}

The first argument (N) must be the name of the table column. If you need to bypass this limitation, you can use a subquery. The limitation is introduced to simplify computations, because several invocations with the same first argument (N) are merged into one pass in the implementation.

{% endnote %}

```yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99)
FROM my_table;
```




## HISTOGRAM {#histogram}

**Signature**
```
HISTOGRAM(Double?)->HistogramStruct?
HISTOGRAM(Double?, weight:Double)->HistogramStruct?
HISTOGRAM(Double?, intervals:Uint32)->HistogramStruct?
HISTOGRAM(Double?, weight:Double, intervals:Uint32)->HistogramStruct?
```
In the description of signatures, HistogramStruct means the result of an aggregate function that is a structure of a certain form.

Building an approximate histogram by a numerical expression with automatic selection of baskets.

[Auxiliary functions](../udf/list/histogram.md)

### Basic settings

The limit on the number of baskets can be set using the optional argument, the default value is 100. Note that the additional accuracy costs additional computing resources and may have a negative impact on the query execution time and in extreme cases — on its success.

### Weight support

You can specify "weight" for each value involved in building the histogram. To do this, you must pass a weight calculation expression to the aggregate function as the second argument. The default weight value is always `1.0`. If non-standard weights are used, the limit on the number of baskets can be set by the third argument.

If two arguments are passed, the meaning of the second argument is determined by its type (integer literal — the limit on the number of baskets, otherwise — weight).


### If you need an accurate histogram

1. You can use the aggregate functions described below with fixed basket grids: [LinearHistogram](#linearhistogram) or [LogarithmicHistogram](#logarithmichistogram).
2. You can calculate the basket number for each row and perform [GROUP BY](../syntax/group_by.md) on it.

When the [aggregate function factory](basic.md#aggregationfactory) is used, [Tuple](#aggregateby) of the value and weight is passed as the first `AGGREGATE_BY` argument.

**Examples**
```yql
SELECT
    HISTOGRAM(numeric_column)
FROM my_table;
```

```yql
SELECT
    Histogram::Print(
        HISTOGRAM(numeric_column, 10),
        50
    )
FROM my_table;
```

```yql
$hist_factory = AggregationFactory("HISTOGRAM");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, 1.0), $hist_factory)
FROM my_table;
```

## LinearHistogram, LogarithmicHistogram and LogHistogram {#linearhistogram}

Building a histogram based on an explicitly specified fixed scale of baskets.

**Signature**
```
LinearHistogram(Double?)->HistogramStruct?
LinearHistogram(Double? [, binSize:Double [, min:Double [, max:Double]]])->HistogramStruct?

LogarithmicHistogram(Double?)->HistogramStruct?
LogarithmicHistogram(Double? [, logBase:Double [, min:Double [, max:Double]]])->HistogramStruct?
LogHistogram(Double?)->HistogramStruct?
LogHistogram(Double? [, logBase:Double [, min:Double [, max:Double]]])->HistogramStruct?
```

Arguments:

1. An expression whose value is used to build a histogram. All subsequent ones are optional.
2. Distance between baskets for `LinearHistogram` or a logarithm base for `LogarithmicHistogram`/`LogHistogram` (these are aliases). In both cases, the default value is 10.
3. Minimum value. The default value is minus infinity.
4. Maximum value. The default value is plus infinity.

The result format is completely similar to [adaptive histograms](#histogram), which enables you to use the same [set of auxiliary functions](../udf/list/histogram.md).

If the variation of input values is uncontrollably large, we recommend specifying a minimum and a maximum value to prevent potential drops due to high memory consumption.

**Examples**
```yql
SELECT
    LogarithmicHistogram(numeric_column, 2)
FROM my_table;
```
## CDF (cumulative distribution function) {#histogramcdf}

You can assign a CDF suffix to each Histogram function type to build a cumulative distribution function. Constructions
```yql
SELECT
    Histogram::ToCumulativeDistributionFunction(Histogram::Normalize(<function_type>Histogram(numeric_column)))
FROM my_table;
```
and
```yql
SELECT
    <function_type>HistogramCDF(numeric_column)
FROM my_table;
```
are completely equivalent.

{% if audience == internal %}

* [The used library with implementation in Arcadia](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/histogram/adaptive).
* [Distance, Weight, Ward](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/histogram/adaptive/common.cpp).

{% endif %}


## BOOL_AND, BOOL_OR and BOOL_XOR {#bool-and-or-xor}

**Signature**
```
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Applying the appropriate logical operation (`AND`/`OR`/`XOR`) to all values of a boolean column or expression.

These functions **do not skip** the `NULL` value during aggregation, a single `NULL` value will turn the result into  `NULL`. The `MIN`/`MAX` or `BIT_AND`/`BIT_OR`/`BIT_XOR` functions can be used for aggregation with `NULL` skips.

**Examples**
```yql
SELECT
  BOOL_AND(bool_column),
  BOOL_OR(bool_column),
  BOOL_XOR(bool_column)
FROM my_table;
```

## BIT_AND, BIT_OR, and BIT_XOR {#bit-and-or-xor}

Applying the appropriate bitwise operation to all values of a numeric column or expression.

**Examples**
```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```




## SessionStart {#session-start}

Without arguments. Only allowed if there is [SessionWindow](../syntax/group_by.md#session-window) in
[GROUP BY](../syntax/group_by.md)/[PARTITION BY](../syntax/window.md#partition).
Returns the value of the `SessionWindow` key column. In case of `SessionWindow` with two arguments — the minimum value of the first argument within a group/partition.
In case of the extended variant of `SessionWindoow` — the value of the second tuple item returned by `<calculate_lambda>` when the first tuple item is `True`.



## AGGREGATE_BY and MULTI_AGGREGATE_BY {#aggregate-by}
Applying the [aggregate function factory](basic.md#aggregationfactory) to all column or expression values. The `MULTI_AGGREGATE_BY` function requires a structure, tuple, or list in a column or expression value and applies the factory on an item-by-item basis, placing the result in a container of the same form. If different column or expression values contain lists of different lengths, the resulting list will have the smallest of the lengths of those lists.

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


## UDAF

If the above aggregate functions were not enough for some reason, YQL has a mechanism for describing custom aggregate functions.
