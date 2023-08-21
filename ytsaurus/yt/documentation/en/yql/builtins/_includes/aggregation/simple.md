
## COUNT {#count}

**Signature**
```
COUNT(*)->Uint64
COUNT(T)->Uint64
COUNT(T?)->Uint64
```

Counting the number of rows in the table (if `*` or constant is specified as the argument) or non-empty values in a table column (if the column name is specified as an argument).

Like other aggregate functions, it can be combined with [GROUP BY](../../../syntax/group_by.md) to get statistics on the parts of the table that correspond to the values in the columns being grouped. Use the [DISTINCT](../../../syntax/group_by.md#distinct) modifier to count the number of unique values.

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

If you need to count the number of unique values on the rows that meet the condition, then unlike other aggregate functions, you can't use the [DISTINCT](../../../syntax/group_by.md#distinct) modifier, since the arguments contain no values. To get this result, use, inside a subquery, the built-in `IF` function with two arguments (to get `NULL` in ELSE), and apply an outer [COUNT(DISTINCT ...)](#count) to its results.

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

When the [aggregate function factory](../../basic.md#aggregationfactory) is used, [Tuple](#aggregateby) of the value and predicate is passed as the first `AGGREGATE_BY` argument.

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