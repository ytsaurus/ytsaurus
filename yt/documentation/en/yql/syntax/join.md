---
vcsPath: yql/docs_yfm/docs/ru/yql-product/syntax/join.md
sourcePath: yql-product/syntax/join.md
---
{% include [x](_includes/join.md) %}


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
| [`yt.JoinMergeTablesLimit`](pragma.md#ytjoinmergetableslimit) | Maximum number of `JOIN` input tables (when using [RANGE, CONCAT](select.md#concat), etc.) |
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


