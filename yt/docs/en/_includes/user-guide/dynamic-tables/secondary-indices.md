# Secondary indexes

In database theory, a secondary index is a data structure that allows for faster table sampling by non-key columns. An indexed tuple of non-key columns is called a *secondary key*.

In {{product-name}}, these structures are implemented for sorted dynamic tables as separate tables, hereafter referred to as index tables, and special objects linking the indexed table to the index one. In {{product-name}} terminology, these objects are called secondary indexes. They do not have their own paths in Cypress, are immutable, and exist as long as both the indexed and index tables exist.

Secondary indexes are also supported for replicated tables.

## Usage { #usage }

{% if audience == "internal" %}
{% note warning %}

Secondary indexes have not been thoroughly tested, and they are currently available to everyone only on Zeno, Hume, Freud, and Pythia test clusters. If you want to use them for production processes, please contact yt-admin@ and describe the details of your process.

{% endnote %}
{% endif %}

By default, indexing is performed by a tuple of one or more non-key table columns, and there is a bijective correspondence between table rows — this behavior is set by the `kind=full_sync` secondary index attribute.

The index table schema determines which data can be sampled faster with the use of the index. For efficient read range selection, the secondary key must be a prefix of the index table's key. Fast access to data in an indexed table is achieved by storing its full key (called a *primary* key) in the index table. Since rows of sorted dynamic tables must have unique keys, the index table key must contain the primary key, representing a concatenation of the secondary and primary keys (there is an [exception](../../../user-guide/dynamic-tables/secondary-indices#unique) to this rule). An index table may contain one or more non-key columns of the indexed table as its own non-key columns. It may also contain no significant non-key values, but in this case you will have to add to the index table schema a dummy column `$empty` of the `int64` type, which will always have a null value.

Example of creating tables and a linking secondary index:

```bash
yt create table //path/to/table --attributes '{dynamic=true; schema=[{name=key; type=int64; sort_order=ascending}; {name=value; type=string}]}'
yt create table //path/to/index_table --attributes '{dynamic=true; schema=[{name=value; type=string; sort_order=ascending}; {name=key; type=int64; sort_order=ascending}; {name="$empty"; type=int64}]}'
yt create secondary_index --attributes '{table_path="//path/to/table"; index_table_path="//path/to/index/table"; kind=full_sync}'
```

{% note warning "Note" %}

Note that the column names and types in the tables are exactly the same. If the index table contains columns that are not in the indexed table, or their types do not match, a secondary index creation error occurs (except for the dummy column `$empty`, which is not required in the indexed table).

{% endnote %}

A secondary index can be created only if both tables are unmounted. Once the secondary index is created and the tables are mounted, *further* writes to the indexed table are automatically displayed in the index table. You can also execute efficient select queries with filtering by secondary key using the `WITH INDEX` keyword. A query with an index runs efficiently if the predicate can be used to [infer](../../../user-guide/dynamic-tables/dyn-query-language#input_data_cut) relevant read ranges from the index table.

```bash
yt select-rows "key, value FROM [//path/to/table] WITH INDEX [//path/to/index/table] where value BETWEEN 0 and 10"
```

When executing a query with this syntax, the system first reads the rows matching the `where value BETWEEN 0 and 10` predicate from the index table and then performs an inner join with the indexed table rows by common columns. Such a query is equivalent to the following one:

```bash
yt select-rows "key, value FROM [//path/to/index/table] AS IndexTable JOIN [//path/to/table] ON (IndexTable.key, IndexTable.value) = (key, value) WHERE IndexTable.value BETWEEN 0 and 10"
```

A current list of secondary indexes connected to the table is specified in the `secondary_indices` table attribute. A secondary index for which this particular table is an index one is specified in the `index_to` attribute.

You can also exclude some rows from the index, create an index over a column with a list, and create a special index that enforces uniqueness of the secondary keys in the table.

### Partial indexing { #predicate }

An index table may contain not all rows, but only those that satisfy a certain predicate. This predicate is set by the `predicate` secondary index attribute. It must be a valid boolean expression over the indexed table schema (the predicate can use columns that don't exist in the index table). The `predicate` attribute is compatible with any of the secondary index types.

This can be helpful if the queries the index is useful for are related only to a particular isolated portion of the data. For example, the table schema is `[{name=id; sort_order=ascending; type=uint64}; {name=name; type=string}; {name=age; type=int64}; {name=loyalty; type=boolean}; ...]`, and in queries you perform sampling by age only for the rows with a non-null `loyalty` value. In this case, a secondary index with the `not is_null(loyalty)` predicate and an index table with the schema `[{name=age; type=int64; sort_order=ascending}; {name=id; sort_order=ascending; type=uint64}; {name=$empty, type=int64}]` is an optimal solution: the rows with `loyalty=#` will not appear in the index table.

### Index over a list { #unfolding }

A `kind=unfolding` index unfolds rows by a column with a list of values of an elementary type. It enables you to perform sampling of rows where the indexed list contains particular values. For example, the row `{id=0; child_ids=[1, 4, 7]}` will correspond to the rows `[{child_ids=1; id=0}; {child_ids=4; id=0}; {child_ids=7; id=0}]` in the index table. Table schemas must contain such identically named columns whose types correspond as `list<T>` or `optional<list<T>>` in the indexed table and `T` in the index table, where `T` is an elementary type. In select queries, you can set a filter on the indexed value using the `list_contains` function. Example: `id FROM [//path/to/table] WITH INDEX [//path/to/index/table] where list_contains(child_ids, 1)`.

{% note warning "Note" %}

When multiple allowable "child_ids" values are specified in the predicate, such as `list_contains(child_ids, 1) or list_contains(child_ids, 4)`, the select query may return multiple rows corresponding to the same primary key in the indexed table if both values are contained in the indexed list. In order for uniqueness to be restored, result must be grouped by the table’s primary key.

A `list_contains(child_ids, 1) OR list_contains(child_ids, 2) OR ...` query cannot contain a large number of conditions because of the limitation on the syntax tree depth in all query expressions. To mitigate this limitation, you can write the predicate in the following form: `child_ids IN (1, 2, ...)`. In this case, the system will interpret the reference to the "child_ids" column as a reference to a column in the index table.

{% endnote %}

For example, let the rows in a table with the schema `[{name=id; type=int64; sort_order=ascending}; {name=book_name; type=string}; {name=genres; type_v3={type_name=list; item=string}}; ...]` describe books. To perform quick sampling by genre, you can use the `unfolding` secondary index over the index table with the schema `[{name=genres; sort_order=ascending; type=string};{name=id; type=int64; sort_order=ascending}; {name=book_name; type=string}]` — the `book_name` column in the index table schema will enable you to use the index table without joining it to the indexed one for queries where only the `genres`, `book_name`, and `id` columns are needed, further reducing the number of reads and speeding up query execution.

### Unique index { #unique }

A `kind=unique` index maintains the uniqueness of the secondary key in the indexed table. A key in an index table schema for a secondary index of this kind is the secondary key, and non-key values are the primary key. Should user attempt a write which would result in two rows with the same secondary key being present in the indexed table, the `UniqueIndexConflict` error will occur. Should two transactions with row writes that have different primary keys and identical secondary keys conflict, one of them will be aborted with the `TransactionLockConflict` error.

### Secondary index sharding { #sharding }

For the purposes of data sharding [computed key columns](../../../user-guide/dynamic-tables/resharding#expression) are often employed. For the sake of convenience they are independent between the index table and the indexed table — they can have the same or different names, types, and expressions so that sharding can be controlled independently. Please note that, just as with regular tables, using a computed column in an index table will make queries with a range by indexed column inefficient.

### Copying and moving { #copy-move }

{% note warning "Note" %}

If you copy or move an index or indexed table or both tables at the same time, the secondary index that links them will disappear.

{% endnote %}

## Limitations { #limitations }

* Since a select query using an index performs a table join, reads must be performed within a transaction to obtain a globally consistent slice (by default, select queries are executed with SyncLastCommittedTimestamp; in this mode, the system generally cannot guarantee consistency between two data structures).

* Indexing by aggregate columns and writing to an indexed table with a `shared write` lock are prohibited.

* Insertions to an indexed table via a [map-reduce operation](../../../user-guide/dynamic-tables/bulk-insert) are not displayed in the index table.

* Changing the schema of a table with a secondary index has its specifics as well. When adding a key column, it must first be added to the index tables, and only then to the indexed table. When adding a non-key column, the opposite is true (or it may be omitted from index table).

* All secondary key columns, as well as columns used in the predicate, must have the same `lock` [group](../../../user-guide/dynamic-tables/transactions#conflicts) in the indexed table.

### Limitations imposed on replicated tables

Consistent reads from replicated tables when using secondary indexes are possible only if the index and indexed tables have an overlapping set of clusters with synchronous replicas. Before creating a secondary index, make sure that the indexed table and all index tables are combined into a single [collocation](../../../user-guide/dynamic-tables/replicated-dynamic-tables#replication_collocation).

## Performance { #performance }

A disadvantage of using of secondary indexes is increased latency of writes to the indexed table. Writing to the indexed table will also entail transparent reads from it and transparent writes to all index tables for the user, and in case of unique indexes, transparent reads from the index table as well. These reads are performed at the end of the transaction. As a consequence, the write latency is increased by the read latency.

{% if audience == "internal"%}
## Building an index { #building }

There is a script that helps correctly create a secondary index over the existing data. It enables you to build a secondary index with the minimum downtime required to take a snapshot of the indexed table and remount it (unless you are building a unique index; to maintain strict uniqueness, no writes must be made to the table during the entire building process). After the script acquires the necessary lock, it mounts the indexed table, which can then be used for reading and writing. Once the script finishes running, the index table can be used for queries. The user must ensure that no queries are made with the use of the index until it is fully built.

The script supports replicated tables, but the downtime will be a bit longer: you will also have to wait for the replicas to synchronize. Its use stipulates that the indexed and index tables already exist.

{% note warning "Note" %}

After a secondary index is built using this script, the index table may contain extra rows that do not correspond to any entries in the indexed table. This may be caused by background compaction and does not affect correctness when using the secondary index in select queries, because these rows will be omitted when joining the tables. Building a strictly bijective index is possible only with a full downtime of both tables. There is a dedicated flag in the script for this mode.

{% endnote %}

{% endif %}
