# Dynamic tables

Dynamic tables are a type of table in {{product-name}} that implement an interface to read and write data by key. They support transactions and their own SQL dialect.

Dynamic tables add the capabilities of typical key-value storage to the {{product-name}} system.
Elementary operations with such tables are writing, deleting, and reading rows by key. You can also execute SQL queries to work with row ranges.

Dynamic tables support [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) for transactions. Working with them does not require communication with the {{product-name}} master server, which provides additional scalability.

The closest analogs to dynamic tables are:

* [Apache HBase](http://hbase.apache.org): In terms of data storage organization.
* [Spanner](https://ai.google/research/pubs/pub39966): In terms of transaction implementation.
* [Impala](http://impala.apache.org): In terms of the query calculation model.

The similarities with these systems will be described below.

## Types of dynamic tables { #types }

Dynamic tables can be [sorted](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) and [ordered](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md). In addition, a replication mechanism is implemented for each table type. For more information about replication, see [replicated dynamic tables](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md).

## Architecture { #architecture }

Architecturally, the way dynamic tables are stored in {{product-name}} uses the [Log-structured merge tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) idea. This makes it similar to the [BigTable](http://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf) and [HBase](https://hbase.apache.org/) systems. To ensure fault tolerance and consistency, `Hydra` state replication library is used (analog of [Raft](https://raft.github.io/raft.pdf)). To support distributed transactions, the [two-phase commit](https://en.wikipedia.org/wiki/Two-phase_commit_protocol) that is similar to [Google Spanner](https://research.google/pubs/pub39966/) is used.

For more information about the data architecture and model, see separate sections about [sorted](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) and [ordered](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md) dynamic tables.


## Attributes { #attributes }

Besides the attributes inherent to all tables, dynamic tables have a number of unique attributes, some of which are represented in the table:

| **Name** | **Type** | **Description** |
| ---------- | -------- | ------------- |
| chunk_row_count | Int | Total number of non-combined rows in all table chunks. |
| tablets[*](**) | TabletInfo | Tablet description. |
| atomicity | Atomicity | [Atomicity mode](../../../user-guide/dynamic-tables/transactions.md#atomicity). By default: `full`. |
| in_memory_mode | InMemoryMode | [In-memory mode](overview.md#in_memory) for tables. By default: `none`. |

The `chunk_row_count` attribute can be used to evaluate the table size, but the evaluation is approximate for the following reasons:
- It does not take into account the rows that were recently written and have not yet been flushed into chunks, residing only in nodes memory.
- Different versions of a single row can be counted multiple times until corresponding chunks are combined.
- Rows may be logically marked as deleted or obsolete but will still be counted by the attribute until physically deleted.

Individual attributes of a dynamic table affect its behavior. In particular, you can use individual attributes to:
- [Set up background compaction](../../../user-guide/dynamic-tables/compaction.md#attributes).
- [Set TTL](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#remove_old_data): Time to live for individual values.
- [Manage automatic sharding](../../../user-guide/dynamic-tables/resharding.md#auto).

## Limitations { #limitations }

A number of size and content type limitations are imposed on rows and the schema of a dynamic table:

- The number of columns in a dynamic table cannot exceed 1024, of which no more than 32 can be key columns.
- The maximum length of the `String` value in a dynamic table is 16 megabytes.

Besides that, dynamic tables inherit the [limitations](../../../user-guide/storage/static-tables.md#limitations) of static tables.


## In-memory tables (in-memory) { #in_memory }

Accessing data in RAM is much faster than accessing data on the disk. In the {{product-name}} system, you can set up a table so that its data is permanently stored in memory. This drastically increases the speed of table operations. This method has certain limitations:

- The RAM capacity on all cluster nodes is limited, so this option should only be used when really needed.

- Any sorted dynamic table can be switched to in-memory mode, as well as taken out of this mode. Run `remount_table` to make the changes take effect.

- The use of dynamic in-memory tables does not affect the levels of isolation, consistency, or fault tolerance. The data of such tables is still stored in chunks of the distributed storage, in-memory mode only means that in addition to the disk the table data is also stored in RAM.

- When in-memory mode is enabled, data is uploaded into the cluster node memory. This may take some time.

In-memory mode is configured using the `in_memory_mode` attribute for the table. Possible attribute values:

| **Mode** | **In-memory mode** | **Description** |
| --------- | ------------------- | ------------ |
| None | Disabled. | Table data is written to chunks and their blocks, compressed and uncompressed, and cached on a common basis. This mode is suitable for large tables that cannot be fixed in memory. |
| Compressed | Enabled. | The compressed table data — the contents of the chunk blocks — is permanently present in memory, but the data blocks must be unpacked for read accesses. Unpacked blocks are cached on a common basis. This mode is a good combination in terms of speed-to-memory capacity ratio. |
| Uncompressed | Enabled. | Uncompressed table data is permanently present in memory, no disk accesses or data unpacking are needed for read accesses. This mode provides maximum performance at the cost of RAM. |


### Hash tables to read by key

The read by key operation searches for the table data keys using a binary search. You can achieve some acceleration by building hash tables for the keys. To do this, set the `enable_lookup_hash_table = %true` attribute for the table.

Hash tables only make sense in `uncompressed` mode and they are not allowed in other modes.

## Table storage formats { #formats }

The {{product-name}} system offers several ways of table data storage in chunks. Switching between them is managed by the `optimize_for` attribute on the table. This attribute can be changed on an existing table, but the effect will only be noticeable for newly created chunks. Taking into account compaction, old data can also be recompressed in background mode.

If the table is already mounted, run the `remount_table` command to inform the tablets of the format change.

Possible values of the `optimize_for` attribute:

- `lookup`: The format, which enables rows to be efficiently read from a table by individual keys.
- `scan`: The `scan` format uses column-by-column storage, as well as adaptive column-by-column compression algorithms. The vertical data partitioning method is less suitable for random reads by keys, because more blocks will need to be read and more CPU performance will be required.

## Access permissions { #acl }

The access control system applies to dynamic tables:

- To read data from a dynamic table (using the `select_rows` and `lookup_rows` queries), the user must have the `read` permission for the table.
- To write data to a dynamic table (using the `insert_rows` and `delete_rows` commands), the user must have the `write` permission for the table.
- To run the `mount_table`, `unmount_table`, `remount_table`, and `reshard_table` commands, the user must have the `mount` permission for the table.
- Similar to accounts, in order to create tables linked to a bundle, the user must have the `use` permission for the bundle. The same rule is required to change the table bundle.

[**]: The parameter appears in the answer several times.
