# Inserting from operations into dynamic tables

## General information { #common }

{{product-name}} enables you to specify [sorted dynamic tables](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) as output tables of [operations](../../../user-guide/dynamic-tables/operations.md) (bulk insert).

Application:
- Data delivery to tables.
- `DELETE WHERE` client implementation.
- Background process of cleaning up rows with a non-trivial TTL.

{% note warning %}

If you run the operation carelessly, it can easily cause a dynamic table to become inoperative and it cannot be rescued without administrative intervention. Before running the operation on production clusters, view the relevant [section](#prod).

{% endnote %}

## Features { #features }

Operations that contain dynamic output tables have a number of features.
* The operation cannot be run under a user transaction.
* Only sorted dynamic tables can be output.
* A dynamic table can be written to either in "append" or "overwrite" mode. This is regulated by the `<append=%true>` attribute on the path. Unlike sorted static tables, adding data to a dynamic table takes a shared lock on it, so multiple operations can write to the table at the same time. Note that writing in "append" mode is actually upsert, not append (data does not have to be written to the end of the table).
* You can only write to a mounted or frozen dynamic table. Writing to a frozen table is strongly not recommended, because compaction does not work in frozen tables. Without it, reading soon becomes inefficient and writing is ([locked](#compaction)).
* Only sorted data with `unique_keys=%true` can be written to a dynamic table. If user jobs do not have this feature, use an intermediate static table, which can then be sorted into a dynamic one using the sort operation.
* An operation transaction is not like classic tablet transactions. In particular, you can write to the output table using `insert_rows` while the operation is running, and if the operation affects the same rows, they will be overwritten.
* Data can be written from the operation in extended format that enables you to delete rows and specify "aggregate" mode.

## Transactional model { #trasactional_model }

As with ordinary operations, the bulk insert commit is atomic: the changes will either fall into all output tables or none.

While the operation is being committed, all dynamic tables in which the insertion takes place are locked. The lock applies to any writes, as well as reads with a timestamp greater than the lock start time and SyncLastCommittedTimestamp. Reads with AsyncLastCommittedTimestamp will not be locked. If the operation commit is completed successfully, the table is unlocked and the data appears in it. The timestamp of all inserted rows is equal to the timestamp of the operation commit. In particular, the timestamp of the inserted rows in all affected tables will be the same.

Bulk insert only conflicts with those tablet transactions that affect the moment of commit (locking). If a tablet transaction started before locking, it will not be able to commit afterwards. There will either (most likely) be an abort, or the operation commit will wait for the transaction commit.

Several bulk insert operations with ``<append=%true>`` can run and even be committed in parallel. There are no row-by-row locks: the system enables different operations to edit the same row.


## Deletions and extended write format

The extended format enables you to delete rows from the table, set "aggregate" (`aggregate` in insert_rows) and "overwrite" mode (`update` in insert_rows). To use it, you need to specify the `schema_modification=unversioned_update` attribute on the path to the output table and write from the operation in the special schema:

* Key columns remain unchanged.
* A mandatory column with the `$change_type` name is added, indicating whether the row is a write or a deletion.
* Each column with a value `name` is replaced by two columns: `$value:name` and `$flags:name`. The first one will contain the value and the second one will manage "aggregate" and "overwrite" modes.

The service column names can be taken from the [api](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/api/public.h?rev=r6838179#L112).

`$change_type` takes values from the [ERowModificationType](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/api/public.h?rev=r7538031#L42) enum. Only `Write` and `Delete` values are allowed.


Name | Value
:- | :-
`ERowModificationType::Write`| 0|
`ERowModificationType::Delete`| 1|

The value flags take the bit mask of enum values [EUnversionedUpdateDataFlags](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt/client/tablet_client/public.h
?rev=r6838179#L102). The `missing | aggregate` combination makes no sense, but it is acceptable. If no flags are specified for a column, they are considered equal to 0.

Name | Value | Clarification
:- | :- | :-
`EUnversionedUpdateDataFlags::Missing`| 1| If the bit is set, the value will be ignored, otherwise it will be overwritten (if there is no value, Null will be written to the table).
`EUnversionedUpdateDataFlags::Aggregate`| 2| If the bit is set, aggregate will be applied, otherwise the value will be updated.

{% cut Пример %}

Let's assume that we have a table with the _user_name_ key column (string), the _age_ column (uint64), and the _balance_ aggregate column (int64). The extended schema will then look like this:

```
{name="user_name"; type=string; sort_order=ascending}
{name="$change_type"; type=uint64; required=%true}
{name="$value:age"; type=uint64}
{name="$flags:age"; type=uint64}
{name="$value:balance"; type=int64}
{name="$flags:balance"; type=uint64}
```

Then to delete the string, you need the following write
```
{
  "user_name"="vasya";
  "$change_type"=1; // delete
}
```

And to update the balance (and maintain the age), you need the following write

```
{
  "user_name"="vasya";
  "$change_type"=0; // write
  "$flags:age"=1; // missing
  "$value:balance"=100500;
  "$flags:balance"=2; // aggregate
}
```

{% endcut %}

In extended format, you can also create intermediate static tables which will then be inserted into dynamic tables using sort or merge. This may be needed very rarely.

### DELETE WHERE via input query

With bulk insert, you can effectively delete multiple rows from a table by condition using [input query](../../../user-guide/dynamic-tables/dyn-query-language). An SQL-like query language can be used to filter the rows supplied to the input of an operation. The same syntax as in select-rows is used. For example, you can delete all rows with an even key from a table this way.

```python
input_query = "1u as [$change_type], key1, key2 where key1 % 2 = 0"
yt.run_merge(
    table,
    yt.TablePath(
        table,
        append=True,
        attributes={"schema_modification": "unversioned_update"}),
    mode="ordered",
    spec={"input_query": input_query})
```

The operation will read all rows from the table, leave those matching the condition, convert them into a deletion format, and write them back to the table.

{% note warning %}

As with delete_rows, deletion does not delete rows from the table, but writes tombstones that will later be compacted. If you delete much data from a table, queries can start slowing down in a random way, because for every useful row you have to read a lot of deleted rows.

For regular deletions, in most scenarios compaction will run automatically, but in some cases you may need to configure the table.

{% endnote %}

## Additional fields in the specification { #additional_fields }

The operation specification may contain the same fields as usual, with a few exceptions.

* To specify the [table writer](../storage/io_configuration#table_writer) configuration, use the `dynamic_table_writer` option indicated in the same place as the ordinary `table_writer` (in `*_job_io`). Chunks and blocks of dynamic tables must be smaller than chunks of static tables. The default value is `{desired_chunk_size=100MB; block_size=256KB}`.

## Compaction features { #compaction }
This section describes the use of bulk insert for large amounts of data.

Sorted dynamic tables resemble an [LSM tree](https://ru.wikipedia.org/wiki/LSM-%D0%B4%D0%B5%D1%80%D0%B5%D0%B2%D0%BE). The used data structure is designed to maintain an even, albeit quite large, flow of writes. Written data is compacted in background mode and stacked in chunks in a predictable manner.

When inserted from an operation, many new chunks are added to the table, at least one per job. These chunks are not aligned to the table structure, but "piled up" in an unpredictable manner. After a few operations, access to the data will become inefficient, so it must be repaired.

There are two issues with compaction. First, resources are needed. A single node can compact at the speed of about 100 Mb/s, with not only fresh data but also old data being processed. Therefore, if there is only one node in the bundle, bulk insert for tens of gigabytes per minute is unlikely to work, because the structure will not have time to recover.

Second, compaction heuristics are not perfect. If there are a lot of very small chunks in the table, the algorithm will become less efficient. The same happens if there is a very large chunk with a wide range of keys in the table. In the worst case, the table will get stuck and can only be repaired manually.

Bulk insert can lead to a number of unusual scenarios. We have prepared the proper heuristics for many of them, but we have hardly taken everything into account???. Therefore, a test run on real volumes is an important next step.

