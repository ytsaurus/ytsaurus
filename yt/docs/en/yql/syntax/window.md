# OVER, PARTITION BY, and WINDOW

Window functions were introduced in the SQL:2003 standard and expanded in the SQL:2011 standard. They let you run calculations on a set of table rows that are related to the current row in some way.

Unlike [aggregate functions](../builtins/aggregation.md), using window functions doesn't cause rows to become grouped into a single output row: the number of rows in the results table always matches that in the source table.

If a query contains both aggregate and window functions, grouping is performed and aggregate function values are calculated first. The calculated values of aggregate functions can be used as window function arguments (but not the other way around). The order for calculating window functions in relation to other query elements is described in the [SELECT](select/index.md) section.

## Syntax {#syntax}

General syntax for calling a window function is as follows

```yql
function_name([expression [, expression ...]]) OVER (window_definition)
```

or

```yql
function_name([expression [, expression ...]]) OVER window_name
```

Here, `window_name` (*window name*) is an arbitrary ID that is unique within the query and `expression` is an arbitrary expression that contains no window function calls.

In the query, each window name must be mapped to the *window definition* (`window_definition`):

```yql
SELECT
    F0(...) OVER (window_definition_0),
    F1(...) OVER w1,
    F2(...) OVER w2,
    ...
FROM my_table
WINDOW
    w1 AS (window_definition_1),
    ...
    w2 AS (window_definition_2)
;
```

Here, the `window_definition` is written as

```antlr
[ PARTITION BY (expression AS column_identifier | column_identifier) [, ...] ]
[ ORDER BY expression [ASC | DESC] ]
[ frame_definition ]
```

You can set an optional *frame definition* (`frame_definition`) in one of the following ways:

* ```ROWS frame_begin```
* ```ROWS BETWEEN frame_begin AND frame_end```
* ```RANGE frame_begin```
* ```RANGE BETWEEN frame_begin AND frame_end```

{% note info %}

`RANGE` mode is available starting with language version 2026.01.

In Query Tracker, select language version 2026.01 or later to use `RANGE` frames.

{% endnote %}

The *frame start* (`frame_begin`) and *frame end* (`frame_end`) are set one of the following ways:

* ```UNBOUNDED PRECEDING```
* ```offset PRECEDING```
* ```CURRENT ROW```
* ```offset FOLLOWING```
* ```UNBOUNDED FOLLOWING```

Here, *frame offset* (`offset`) is a non-negative literal. If the frame end isn't set, `CURRENT ROW` is assumed.

In `ROWS` mode, the offset is always an integer. In `RANGE` mode with an offset, `ORDER BY` must contain exactly one column; the offset type is determined by that column and must support addition, subtraction, and comparison with it. Numeric columns, including `Decimal`, use a numeric offset; date and time columns use `Interval` or `Interval64`. PostgreSQL types are supported similarly. `NaN` and `Inf` values are not allowed.

There should be no window function calls in any of the expressions inside the window definition.

## Calculation algorithm

### Partitioning {#partition}

If `PARTITION BY` is set, the source table rows are grouped into *partitions*, which are then handled independently of each other. If `PARTITION BY` isn't set, all rows in the source table are put in the same partition. If `ORDER BY` is set, it determines the order of rows in a partition.

Both in `PARTITION BY` and [GROUP BY](group_by.md) you can use aliases and [SessionWindow](group_by.md#session-window).

If `ORDER BY` is omitted, the order of rows in the partition is undefined.

### Frame {#frame}

The `frame_definition` specifies a set of partition rows that fall into the *window frame* associated with the current row.

In `ROWS` mode, the window frame contains rows with the specified offsets relative to the current row in the partition. For example, `ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING` contains three preceding rows, the current row, and five following rows.

In `RANGE` mode with an offset, the range is based on values of the single `ORDER BY` column rather than a number of rows. For example, `RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING` includes rows whose value is between “current minus 3” and “current plus 5”. `RANGE CURRENT ROW` includes all rows with the same sort value. `UNBOUNDED PRECEDING` and `UNBOUNDED FOLLOWING` have the same meaning as in `ROWS` mode.

The set of rows in the window frame may change depending on which row is the current one. For example, for the first row in the partition, the ROWS `BETWEEN 3 PRECEDING AND 1 PRECEDING` window frame will have no rows.

Setting `UNBOUNDED PRECEDING` as the frame start means "from the first partition row" and `UNBOUNDED FOLLOWING` as the frame end — "up to the last partition row". Setting `CURRENT ROW` means "from/to the current row".

If no `frame_definition` is specified, a set of rows to be included in the window frame depends on whether there is `ORDER BY` in the `window_definition`.
Namely, if there is `ORDER BY`, then `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is implicitly assumed. If none, then `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Further, depending on the specific window function, it's calculated either based on the set of rows in the partition or the set of rows in the window frame.

[List of available window functions](../builtins/window.md)

```yql
SELECT
    ts,
    AVG(value) OVER w AS moving_avg
FROM my_table
WINDOW w AS (
    ORDER BY ts
    RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
);
```

#### Examples

```yql
SELECT
    COUNT(*) OVER w AS rows_count_in_window,
    some_other_value -- access to the current row
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
);
```

```yql
SELECT
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
);
```

```yql
SELECT
    -- AVG (like all aggregate functions used as window functions)
    -- calculated on a window frame
    AVG(some_value) OVER w AS avg_of_prev_current_next,
    some_other_value -- access to the current row
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
);
```

```yql
SELECT
    -- LAG doesn't depend on the window frame position
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY my_column
);
```

## Implementation specifics

* Functions on `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` or `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` run in O(partition size) without extra memory. `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` requires memory to buffer rows with equal `ORDER BY` values.

* For a `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame, a `COMPACT` [hint](lexer.md#sql-hints) can be specified after `PARTITION` to select the in-memory strategy.

  For example: `PARTITION /*+ COMPACT() */ BY key` or `PARTITION /*+ COMPACT() */ BY ()` (if `PARTITION BY` was initially absent).

  If the `COMPACT` hint is specified, this requires additional memory equal to O(partition size), but then no extra `JOIN` operation is made.

* If the window frame doesn't start with `UNBOUNDED PRECEDING`, calculating window functions on this window requires additional memory equal to O(the maximum number of rows from the window boundaries to the current row), while the computation time is equal to O(number_of_partition_rows * window_size).

* For a frame starting with `UNBOUNDED PRECEDING` and ending with `N`, where `N` is neither `CURRENT ROW` nor `UNBOUNDED FOLLOWING`, O(N) extra memory is required and computation takes O(partition size).

* The `LEAD(expr, N)` and `LAG(expr, N)` functions always require O(N) of RAM.

If possible, change `ROWS/RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` to `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` by reversing the `ORDER BY` sorting order.

In terms of MapReduce, window functions are physically executed through Reduce, with `PARTITION BY` keys, which can mean lengthy execution for big sections, as well as a strict 200 GB per section limit for the main clusters {{product-name}}.
