
## GROUP BY

Group the `SELECT` results by the values of the specified columns or expressions. The aggregate functions (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) are often used together with `GROUP BY` to perform calculations in each group.

**Syntax**
```sql
SELECT                             -- In SELECT, you can use:
    column1,                       -- key columns specified in GROUP BY
    key_n,                         -- named expressions specified in GROUP BY
    column1 + key_n,               -- random non-aggregate derived functions
    Aggr_Func1( column2 ),         -- aggregate functions containing any columns in arguments,
    Aggr_Func2( key_n + column2 ), --   including named expressions specified in GROUP BY
    ...
FROM table
GROUP BY
    column1, column2, ...,
    <expr> AS key_n           -- When grouping by expression, it can be assigned a name via AS,
                              -- that may be used in SELECT
```

Querying the `SELECT * FROM table GROUP BY k1, k2, ...` type returns all the columns listed in GROUP BY, which is equivalent to the `SELECT DISTINCT k1, k2, ... query FROM table`.

An asterisk can also be used as an argument for the `COUNT` aggregate function. `COUNT(*)` means "the count of rows in the group".


{% note info %}

Aggregate functions ignore `NULL` in their arguments, except for `COUNT`.

{% endnote %}

YQL also supports factories of aggregate functions implemented using the `AGGREGATION_FACTORY` and `AGGREGATE_BY` functions.

**Examples**

```sql
SELECT key, COUNT(*) FROM my_table
GROUP BY key;
```

```sql
SELECT double_key, COUNT(*) FROM my_table
GROUP BY key + key AS double_key;
```

```sql
SELECT
   double_key,                           -- OK: key column
   COUNT(*) AS group_size,               -- OK: COUNT(*)
   SUM(key + subkey) AS sum1,            -- OK: aggregate function
   CAST(SUM(1 + 2) AS String) AS sum2,   -- OK: aggregate function with constant argument
   SUM(SUM(1) + key) AS sum3,            -- ERROR: nested aggregations are not allowed
   key AS k1,                            -- ERROR: use of non-key Key column without aggregation
   key * 2 AS dk1,                       -- ERROR in YQL: use of non-key Key column without aggregation
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey as sk,

```


{% note warning "Attention!" %}

Specifying a name for a column or expression in `GROUP BY .. AS foo` it is an extension on top of YQL. This name becomes visible in `WHERE` despite the fact that filtration by `WHERE` is performed[before](../../select.md#selectexec) grouping. For example, if the `T` table includes two columns, `foo` and `bar`, then the query `SELECT` foo FROM T WHERE foo > 0 GROUP BY bar AS foo would actually filter data by the `bar` column from the source table.

{% endnote %}
