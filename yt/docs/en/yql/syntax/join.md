
# JOIN

It lets you combine multiple data sources (subqueries or tables) by equality of values in the specified columns or expressions (the `JOIN` keys).

**Syntax**

```sql
SELECT ...    FROM table_1
-- first JOIN step:
  <Join_Type> JOIN table_2 <Join_Condition>
  -- left subquery -- entries in table_1
  -- right subquery -- entries in table_2
-- next JOIN step:
  <Join_Type> JOIN table_n <Join_Condition>
  -- left subquery -- JOIN result in the previous step
  -- right subquery -- entries in table_n
-- JOIN can include the following steps
...
WHERE  ...
```

At each JOIN step, rules are used to establish correspondences between rows in the left and right data subqueries, creating a new subquery that includes every combination of rows that meet the JOIN conditions.

{% note warning "Attention!" %}

Since columns in YQL are identified by their names, and you can't have two columns with the same name in the subquery, `SELECT * FROM ... JOIN ...` can't be executed if there are columns with identical names in the joined tables.

{% endnote %}

## Types of join (Join_Type)

* `INNER` <span style="color: gray;">(default)</span>: Rows from joined subqueries that don't match any rows on the other side won't be included in the result.
* `LEFT`: If there's no value in the right subquery, it adds a row to the result with column values from the left subquery, using `NULL` in columns from the right subquery
* `RIGHT`: If there's no value in the left subquery, it adds the row to the result, including column values from the right subquery, but using `NULL` in columns from the left subquery
* `FULL` = `LEFT` + `RIGHT`
* `LEFT/RIGHT SEMI`: One side of the subquery is a whitelist of keys, its values are not available. The result includes columns from one table only, no Cartesian product is created.
* `LEFT/RIGHT ONLY`: Subtracting the sets by keys (blacklist). It's almost the same as adding `IS NULL` to the key on the opposite side in the regular `LEFT/RIGHT` JOIN, but with no access to values: the same as `SEMI` JOIN.
* `CROSS`: A full Cartesian product of two tables without specifying key columns and no explicit `ON/USING`.
* `EXCLUSION`: Both sides minus the intersection.

![](_assets/join-YQL-06.png)

{% note info %}

`NULL` is a special value to denote nothing. Hence, `NULL` values on both sides are not treated as equal to each other. This eliminates ambiguity in some types of `JOIN` and avoids a giant Cartesian product otherwise created.

{% endnote %}

## Conditions for joining (Join_Condition)

For `CROSS JOIN`, no join condition is specified. The result includes the Cartesian product of the left and right subquery, meaning it combines everything with everything. The number of rows in the resulting subquery is the product of the number of rows in the left and right subqueries.

For any other JOIN types, specify the condition using one of the two methods:

1. `USING (column_name)`. Used if both the left and right subqueries share a column whose equality of values is a join condition.
2. `ON (equality_conditions)`. Lets you set a condition of equality for column values or expressions over columns in the left and right subqueries or use several such conditions combined by `and`.

**Examples:**

```sql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
FULL JOIN b_table AS b USING (key);
```

```sql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
FULL JOIN b_table AS b ON a.key = b.key;
```

```sql
SELECT     a.value as a_value, b.value as b_value, c.column2
FROM       a_table AS a
CROSS JOIN b_table AS b
LEFT  JOIN c_table AS c ON c.ref = a.key and c.column1 = b.value;
```

To make sure no full scan of the right joined table is required, a secondary index can be applied to the columns included in the Join condition. Accessing a secondary index should be specified explicitly in `JOIN table_name VIEW index_name AS table_alias` format.

For example, creating an index to use in the Join condition:

```yql
ALTER TABLE b_table ADD INDEX b_index_ref GLOBAL ON(ref);
```

Using the created index:

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
INNER JOIN b_table VIEW b_index_ref AS b ON a.ref = b.ref;
```


If the statement filters data in addition to `JOIN`, we recommend wrapping the criteria that would return `true` for most of the rows in the `LIKELY(...)` function call. If your assumption that positive values prevail for the criteria is correct, such a hint might speed up your subquery. `LIKELY` can be useful when the predicate calculation is a resource-intensive operation and JOIN significantly reduces the number of rows.

In front of any data source for `JOIN`, you can add the `ANY` keyword to suppress duplicate `JOIN` keys on the given side. In this case, only one row is left from the set of rows with the same `JOIN` key value (no matter which one, that's why the keyword is called `ANY`).
This syntax is different from the one accepted in [ClickHouse], where `ANY` is placed before `JOIN` type and only functions for the right side.

Request

```yql
$t1 = AsList(
    AsStruct("1" AS key, "v111" AS value),
    AsStruct("2" AS key, "v121" AS value),
    AsStruct("2" AS key, "v122" AS value),
    AsStruct("3" AS key, "v131" AS value),
    AsStruct("3" AS key, "v132" AS value));

$t2 = AsList(
    AsStruct("2" AS key, "v221" AS value),
    AsStruct("2" AS key, "v222" AS value),
    AsStruct("3" AS key, "v231" AS value),
    AsStruct("3" AS key, "v232" AS value),
    AsStruct("4" AS key, "v241" AS value));

SELECT
  a.key, a.value, b.value
FROM ANY AS_TABLE($t1) AS a
JOIN ANY AS_TABLE($t2) AS b
ON a.key == b.key;
```

results in:

| a.key | a.value | b.value |
| --- | --- | --- |
| "3" | "v131" | "v231" |
| "2" | "v121" | "v221" |

а без `ANY` выдал бы:

| a.key | a.value | b.value |
| --- | --- | --- |
| "3" | "v131" | "v231" |
| "3" | "v131" | "v232" |
| "3" | "v132" | "v231" |
| "3" | "v132" | "v232" |
| "2" | "v121" | "v221" |
| "2" | "v121" | "v222" |
| "2" | "v122" | "v221" |
| "2" | "v122" | "v222" |




## JOIN execution strategies in {{product-name}}

### Introduction


The following `JOIN` syntax is supported in standard SQL:
```yql
SELECT
  ...
FROM T1 <Join_Type> JOIN T2
ON F(T1, T2);
```

where `F(T1, T2)` is a custom predicate depending on columns of both tables `T1, T2`.
YQL only supports a special case where the `F` predicate is separable, i.e. looks as follows:

```yql
SELECT
  ...
FROM T1 <Join_Type> JOIN T2
ON F1(T1) = G1(T2) AND F2(T1) = G2(T2) AND ...;
```

Such predicate structure allows for effective `JOIN` implementation within the map-reduce concept.

Just like in standard SQL, several `JOIN`s can be created in a single SELECT in YQL:
```yql
SELECT
  ...
FROM
T1 <Join_Type> JOIN T2 ON F1(T1) = G1(T2) AND F2(T1) = G2(T2) AND ...
   <Join_Type> JOIN T3 ON H1(T1,T2) = J1(T3) AND H2(T1,T2) = J2(T3) AND ...;
```

Currently, such `JOIN`s are performed consecutively, strictly in the order specified in the query.
The only exception is the StarJoin strategy.

### Calculating JOIN keys

`JOIN` execution starts when the keys are calculated and their values are saved to a separate column.
This transformation takes place as early as the SQL parser level and is common for all backends (YDB, {{product-name}}, DQ, etc.)

In practice, the query
```yql
SELECT
  ...
FROM T1 <Join_Type> JOIN T2
ON F1(T1) = G1(T2) AND F2(T1) = G2(T2) AND ...;
```

is converted to
```yql
SELECT
...
FROM (
    SELECT t.*,
           F1(...) as _yql_join_key1,
           F2(...) as _yql_join_key2, ...
    FROM T1 as t
) as t1
<Join_Type> JOIN (
     SELECT t.*,
            G1(...) as _yql_join_key1,
            G2(...) as _yql_join_key2, ...
    FROM T2 as t
) as t2
ON t1._yql_join_key1 = t2._yql_join_key1 AND t1._yql_join_key2 = t2._yql_join_key2 AND ...;
```

### Converting keys to a simple common type

This stage is already specific for {{product-name}}. It converts key columns from both sides to an identical simple type.

Request
```yql
SELECT
  ...
FROM T1 as t1 <Join_Type> JOIN T2 as t2
ON t1.key1 = t2.key1 AND t1.key2 = t2.key2 AND ...;
```

is converted to
```yql
SELECT
...
FROM (
    SELECT t.*,
           CastToCommonKeyType(key1) as _yql_join_key1,
           CastToCommonKeyType(key2) as _yql_join_key2,
    FROM T1 as t
) as t1
<Join_Type> JOIN (
     SELECT t.*,
           CastToCommonKeyType(key1) as _yql_join_key1,
           CastToCommonKeyType(key2) as _yql_join_key2,
    FROM T2 as t
) as t2
ON t1._yql_join_key1 = t2._yql_join_key1 AND t1._yql_join_key2 = t2._yql_join_key2 AND ...;
```

Conversion to a common type is necessary for the correct map-reduce operation with related but different key types.
For example, `Optional<Int32>` will be the common type for `Int32` and `Uint32` key types.
If no conversion into a common type is performed, and initial columns are left as key for map-reduce operations,
then Ytsaurus{{product-name}} will deem -1 and 4294967295 keys to be identical.

This conversion is not always necessary. For example, Int32 and Optional key types<Int32> operate correctly.

Additionally, composite key types (everything more sophisticated than Optional from [simple type](../types/primitive.md)))
after changing to common type are also converted to a string:

```yql

if (
    YQL::HasNulls(casted_key), -- if null
    null occurs somewhere in a key,                      -- then the value is converted to a string-type null (null in SQL is not equal to any value, including itself)
    StablePickle(casted_key),  -- otherwise the value is serialized to string presentation
)

```

This conversion is needed because composite key types aren't supported as reduce operation keys in {{product-name}}.

Therefore, after all conversions we obtain keys of pair-wise identical simple type (with precision down to Optional) at both `JOIN` sides.

### Basic JOIN strategy (aka CommonJoin)

The basic `JOIN` strategy is selected when other `JOIN` strategies cannot be applied due to some other reason.
This strategy supports all `JOIN` types, including `CROSS JOIN`, and is implemented through a single MapReduce operation.

The following happens at the map stage:

1) converting keys to a simple common type
2) if the ANY modifier is present, identical keys are "thinned" by a separate window filter. Strings with identical keys are detected at a window of a certain size (hundreds of megabytes) and duplicates are filtered out
3) nulls are processed in keys. For `INNER JOIN`, the nulls from both sides are filtered out,
   and for `LEFT/RIGHT/FULL JOIN` strings with null keys, they are passed to a separate output table immediately from the map stage

From the map stage, strings with an identical key are passed to a single {{product-name}} reduce job where `JOIN` actually takes place.
If necessary, the resulting table from the reduce stage is merged with output tables from the map stage by using a separate {{product-name}} Merge operation.

To perform `CROSS JOIN` (where there are no keys), an identical 0 key is assigned to all strings of both output tables at the map stage.

### LookupJoin strategy

This strategy is triggered when one of the tables is sorted by `JOIN` keys and the second one has a very small size (less than ~900 strings).

{% note info %}

Hereinafter, a table is called _sorted by JOIN keys_ if the list of `JOIN` keys acts as a sorting keys prefix for a certain order of `JOIN` keys.
For example, a table with `JOIN` `b, a` keys and sorting by `a, b, c` is sorted by the `JOIN` keys.

{% endnote %}


LookupJoin is supported for the following JOIN types:
* `INNER` (small table can be from any side)
* `LEFT SEMI` (small table from the left)
* `RIGHT SEMI` (small table from the right)

The LookupJoin strategy is implemented through a single Map operation on a big table, with a small table loaded into the memory.
Key types don't have to match. The small table's keys are casted into the big table's key type.

Unlike the MapJoin strategy (see below), in LookupJoin `JOIN` keys' values from the small table are passed to the
ranges `setting` in the big table's YPath.<!--(../user-guide/storage/ypath.md).--> Thus, only the strings with `JOIN` keys that are present in the small table are read out from the big table.

LookupJoin is the most effective `JOIN` strategy. However, it imposes the strictest conditions on `JOIN`
 types (they must be "filtering" based on the big table) and on the small table's size (keys must "fit in" the maximum permitted number of `ranges` in YPath).
Moreover, `ANY` on the big table's side is not supported in LookupJoin.

(PRAGMA) settings for the strategy:

| Name | Description |
| --- | --- |
| [`yt.LookupJoinLimit`](pragma.md#lookupjoinlimit) | Small table's maximum size in bytes (up to 10M) |
| [`yt.LookupJoinMaxRows`](pragma.md#lookupjoinmaxrows) | Small table's maximum size in strings (up to 1,000) |

The LookupJoin strategy gets disabled when any of these values is set to 0.

### SortedJoin strategy (aka MergeJoin)

This strategy is triggered when both tables are sorted by `JOIN` keys.
Here, `JOIN` keys must match in terms of types with precision down to Optional at the upper level.

If only one table is sorted and the other's size is under `yt.JoinMergeUnsortedFactor * <size of sorted table>`,
 the SortedJoin strategy is selected as well and the unsorted table is sorted out a separate {{product-name}} operation.
[`yt.JoinMergeUnsortedFactor`](pragma.md#ytjoinmergeunsortedfactor) is set to `0.2` by default.

The SortedJoin strategy supports all `JOIN` types except `CROSS JOIN` and is implemented through a single Reduce operation.
Here, reduce with foreign tables mode is used where possible.<!--(../user-guide/data-processing/operations/reduce.md)-->
Moreover, when `JOIN` keys are unique, the `enable_key_guarantee = false` setting is additionally enabled. <!--(../user-guide/data-processing/operations/reduce.md)-->

The SortedJoin strategy can be forcibly selected through an [SQL hint](lexer.md#sql-hints):

```yql
SELECT * FROM T1 AS a JOIN /*+ merge() */ T2 AS b ON a.key = b.key;
```

In this case (where necessary)
1) `JOIN` keys will be converted to common type
2) both tables will be sorted by `JOIN` keys

(PRAGMA) settings for the strategy:

| Name | Description |
| --- | --- |
| [`yt.JoinMergeUnsortedFactor`](pragma.md#ytjoinmergeunsortedfactor) | See above |
| [`yt.JoinMergeTablesLimit`](pragma.md#ytjoinmergetableslimit) | Maximum number of `JOIN` input tables (when using [RANGE, CONCAT](select/concat.md), etc.) |
| [`yt.JoinMergeUseSmallAsPrimary`](pragma.md#ytjoinmergeusesmallasprimary) | Influences the primary table selection when executing a Reduce operation |
| [`yt.JoinMergeForce`](pragma.md#ytjoinmergeforce) | Forces the selection of the SortedJoin strategy for all `JOIN` in a query |

`yt.JoinMergeTablesLimit` set to 0 disables the SortedJoin strategy.

### MapJoin strategy

This strategy triggers when one of the input tables is sufficiently small (its size doesn't exceed [`yt.MapJoinLimit`](pragma.md#ytmapjoinlimit)).
In this case, a smaller table is loaded into the memory (as a dictionary with `JOIN` keys), and Map is executed thereafter for the big table.

This strategy supports all `JOIN` types (including `CROSS`). However, it isn't selected if `ANY` is on the bigger side.

An unique feature of MapJoin is that this strategy is available for earlier selection.
In other words, when a smaller input table is already calculated and subject to size limitations and the big table isn't yet ready.
In this case, MapJoin can be selected immediately,
and there's a chance that the Map operation for the big table will "glue to" the Map operation (for example, filter) preparing this big table.



There is also a sharded MapJoin variant: the small table is split into [`yt.MapJoinShardCount`](pragma.md#ytmapjoinshardcount) parts
(each one cannot exceed `yt.MapJoinLimit`), and each part is in parallel and on its own `JOIN`with the big table through a Map operation.
Then all obtained parts are joined through {{product-name}} Merge.

Sharded MapJoin is only possible for certain `JOIN` types (`INNER`,`CROSS`, and `LEFT SEMI`, provided that the small table to the right is unique).

(PRAGMA) settings for the strategy:

| Name | Description |
| --- | --- |
| [`yt.MapJoinLimit`](pragma.md#ytmapjoinlimit) | Maximum size of the smaller `JOIN` side presentation in the memory for which the MapJoin strategy is selected |
| [`yt.MapJoinShardCount`](pragma.md#ytmapjoinshardcount) | Maximum number of shards |
| [`yt.MapJoinShardMinRows`](pragma.md#ytmapjoinshardminrows) | Minimum number of strings in one shard |

`yt.MapJoinLimit` set to 0 disables the MapJoin strategy.

### StarJoin strategy

This strategy makes it possible to perform several subsequent `JOIN`s at once through a single Reduce operation.

This strategy is possible when dictionary tables are subsequently appended to one ("main") table through `INNER JOIN` or `LEFT JOIN`, provided that:
1) identical keys are used in all `JOIN`s from the main table
2) all tables are sorted by `JOIN` keys
3) dictionary tables are also unique in terms of `JOIN` keys


(PRAGMA) settings for the strategy:

| Name | Description |
| --- | --- |
| [`yt.JoinEnableStarJoin`](pragma.md#ytjoinenablestarjoin) | Enables/disables StarJoin strategy selection (enabled by default) |

### Strategy selection sequence

When `JOIN` is executed, strategies are tried in a certain sequence, and the first suitable one is selected.
The sequence is as follows:

1) StarJoin
2) LookupJoin
3) OrderedJoin
4) MapJoin
5) CommonJoin (always possible)


