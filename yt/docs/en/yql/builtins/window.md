# List of window functions in YQL
The syntax for calling window functions is detailed in a [separate article](../syntax/window.md).



## Aggregate functions {#aggregate-functions}

All the [aggregate functions](aggregation.md) can also be used as window functions.
In this case, each row includes an aggregation result obtained on a set of rows from the [window frame](../syntax/window.md#frame).

**Examples:**
```yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```



## ROW_NUMBER {#row_number}

Row number within a [partition](../syntax/window.md#partition). Without arguments.

**Signature**
```
ROW_NUMBER()->Uint64
```


**Examples**
```yql
SELECT
    ROW_NUMBER() OVER w AS row_num
FROM my_table
WINDOW w AS (ORDER BY key);
```



## LAG / LEAD {#lag-lead}

Accessing a value from the [partition](../syntax/window.md#partition) row that is behind (`LAG`) or ahead (`LEAD`) of the current partition row by a fixed number of rows. The first argument specifies the expression to be accessed, and the second argument specifies the offset in rows. You may omit the offset. By default, the neighbor row is used: the previous or next, respectively (hence, 1 is assumed by default). For the rows having no neighbors at a given distance (for example, `LAG(expr, 3)` in the first and second rows of the partition), `NULL` is returned.

**Signature**
```
LEAD(T[,Int32])->T?
LAG(T[,Int32])->T?
```

**Examples**
```yql
SELECT
   int_value - LAG(int_value) OVER w AS int_value_diff
FROM my_table
WINDOW w AS (ORDER BY key);
```


<!--[Example of an operation](https://https://cluster-name.yql/Operations/X5sOE2im9cB5qXp-fKJ_fQMs3k-T4IJIfu6M0-pgovI=)-->


## FIRST_VALUE / LAST_VALUE

Accessing the values from the first and last rows of the [window border](../syntax/window.md#frame) (in the order of the window's `ORDER BY`). The only argument is the expression that you need to access.

Optionally, `OVER` can be preceded by the additional modifier `IGNORE NULLS`. It changes the behavior of functions to the first or last __non-empty__ (i.e., non-`NULL`) value among the window frame rows. The antonym of this modifier is `RESPECT NULLS`: it's the default behavior that can be omitted.

**Signature**
```
FIRST_VALUE(T)->T?
LAST_VALUE(T)->T?
```

**Examples**
```yql
SELECT
   FIRST_VALUE(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   LAST_VALUE(my_column) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```



## RANK / DENSE_RANK {#rank}

Number the groups of neighboring [partition](../syntax/window.md#partition) rows having the same expression value in the argument. `DENSE_RANK` numbers the groups one-by-one, and `RANK` skips `(N - 1)` values, with `N` being the number of rows in the previous group.

If there is no argument, it uses the order specified in the `ORDER BY` section in the window definition.
If the argument is omitted and `ORDER BY` is not specified, then all rows are considered equal to each other.

{% note info %}

Passing an argument to `RANK`/`DENSE_RANK` is a non-standard extension in YQL.

{% endnote %}

**Signature**
```
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
```

**Examples**
```yql
SELECT
   RANK(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```
```yql
SELECT
   RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);




## SessionState() {#session-state}

A non-standard window function `SessionState()` (without arguments) lets you get the session calculation status from [SessionWindow](../syntax/group_by.md#session-window) for the current row.
It's allowed only if `SessionWindow()` is present in the `PARTITION BY` section in the window definition.


