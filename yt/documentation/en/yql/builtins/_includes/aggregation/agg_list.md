---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/agg_list.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/agg_list.md
---
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

The order of elements in the resulting list depends on the implementation and is not set externally. To get an ordered list, sort the result, for example, using [ListSort](../../list.md#listsort).

To get a list of several values from one string, *DO NOT* use the `AGGREGATE_LIST` function  several times, but place all the desired values in a container, for example, via [AsList](../../basic.md#aslist) or [AsTuple](../../basic.md#astuple) and pass this container in a single `AGGREGATE_LIST` invocation.

For example, you can use it in combination with `DISTINCT` and the [String::JoinFromList](../../../udf/list/string.md) function (analog of `','.join(list)` from Python) to print to a string all the values that were seen in the column after applying [GROUP BY](../../../syntax/group_by.md).

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