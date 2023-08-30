
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

[Auxiliary functions](../../../udf/list/histogram.md)

### Basic settings

The limit on the number of baskets can be set using the optional argument, the default value is 100. Note that the additional accuracy costs additional computing resources and may have a negative impact on the query execution time and in extreme cases — on its success.

### Weight support

You can specify "weight" for each value involved in building the histogram. To do this, you must pass a weight calculation expression to the aggregate function as the second argument. The default weight value is always `1.0`. If non-standard weights are used, the limit on the number of baskets can be set by the third argument.

If two arguments are passed, the meaning of the second argument is determined by its type (integer literal — the limit on the number of baskets, otherwise — weight).


### If you need an accurate histogram

1. You can use the aggregate functions described below with fixed basket grids: [LinearHistogram](#linearhistogram) or [LogarithmicHistogram](#logarithmichistogram).
2. You can calculate the basket number for each row and perform [GROUP BY](../../../syntax/group_by.md) on it.

When the [aggregate function factory](../../basic.md#aggregationfactory) is used, [Tuple](#aggregateby) of the value and weight is passed as the first `AGGREGATE_BY` argument.

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

The result format is completely similar to [adaptive histograms](#histogram), which enables you to use the same [set of auxiliary functions](../../../udf/list/histogram.md).

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