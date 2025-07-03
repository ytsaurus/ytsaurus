# Query execution stages

YQL is a strongly typed programming language. This specifically means that before a YQL query is executed, it must be typed (the type for each node must be computed in the computation graph). Clicking the **Validate** button in the web interface triggers typing.

For example, the query

```yql
SELECT key + 1 AS new_key FROM T;
```
is typed as follows (with some simplifications):

1. The metadata of table `T` is loaded, revealing that the `key` column is of the Uint32 type.
2. The literal type is always known: type 1 is Int32.
3. The `+` operation type is computed based on the argument types. For Int32 and Uint32 arguments, the result is Int32.
4. As a result, the query type is a list of structs with one Int32 element, namely `List<Struct<new_key:Int32>>`.

However, YQL also supports more complex typing cases where the query type depends on the *values* of certain expressions, and these values may depend on the table *contents*. Typical example:

```yql
$from = "2022-02-01";
$to = SELECT MAX(dt) FROM T1;

INSERT INTO Result
SELECT * FROM RANGE(Dir, $from, $to);
```

To infer the [RANGE](../syntax/select/concat.md) type (that is, to infer the common type for tables), compute the `$to` value.

Another example:

```yql
$cols = SELECT AGGREGATE_LIST(DISTINCT name) FROM T1 WHERE <condition>; -- dynamically generate the list of columns

 -- select the columns from the list
SELECT * FROM (SELECT ChooseMembers(TableRow(), $cols) FROM T2) FLATTEN COLUMNS;
```

To type [ChooseMembers](../builtins/struct.md#choosemembers), compute the *value* (not type) of `$cols`.

We can draw the following conclusions:

1. Some YQL queries require computations for typing.
2. Sometimes these computations require table reads.

YQL has a dedicated Evaluation stage for typing such queries, which is where all the necessary computations are performed.

Please note that this stage is subject to certain limitations. The main one is that the total size of tables that the Evaluation stage depends on is limited. The default limit is 1 MB, but it can be expanded to 10 MB using the [yt.EvaluationTableSizeLimit](../syntax/pragma/yt.md#ytevaluationtablesizelimit) pragma.

These limitations are designed to avoid a lengthy Evaluation stage during which the query plan cannot yet be displayed, and after which user typing errors can still occur. This prevents scenarios where you run a computation for several hours only to encounter a "No such column" error and a failed query.

The only way to get around this limitation is to split the query into two. The first query computes the evaluation part and writes data to the table, while the second one depends on that (small-sized) table.
