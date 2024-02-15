# Sorted dynamic tables

This section describes the structure of sorted dynamic tables and the actions that can be performed with these tables.

## Data model { #model }

Each sorted dynamic table is a set of rows ordered by key. As with static tables, the key can be composite: it can consist of several columns. Unlike static tables, the key in a sorted dynamic table is unique.

Sorted dynamic tables must be strictly schematic: all column names and types must be specified in the schema in advance.

## Supported operations { #methods }

### Creating

To create a sorted dynamic table, run the `create table` command and specify the schema and `dynamic=True` setting in the attributes. The schema must match the sorted table.

CLI
```bash
yt create table //path/to/table --attributes \
'{dynamic=%true;schema=[{name=key;type=string;sort_order=ascending}; {name=value;type=string}]}'
```

{% note warning "Attention!" %}

You must specify at least one key column in the table schema. If this is not done, the table will be successfully created, but it will [ordered](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md) rather than sorted. Most types of select queries will work for the table. But all such queries will come down to a full scan of the data.

{% endnote %}

### Changing a table schema and type

You can change the schema of an existing dynamic table using the `alter-table` command. For the command to be successful, the table must be [mounted](../../../user-guide/dynamic-tables/overview.md#mount_table) and the new schema must be compatible with the old one. There are no changes to the data written to the disk, because the old data is suitable for the new schema.

Use `alter-table` to convert a dynamic table into a static table. For more information, see [MapReduce for dynamic tables](../../../user-guide/dynamic-tables/mapreduce.md#convert_table).

### Reading rows

The client can read rows by a given key using the `lookup` method. To do this, you need to specify the table name, a timestamp defining the slice of data to be read (`<= t`), and the names of the columns that interest you. This is a point query: it requires all key components to be specified. There is a type of API call that allows multiple rows to be read in a single query by different keys.

Reads within an atomic transaction will use the transaction start timestamp. You can also indicate a specific numeric value. For example, approximately matching the physical moment in time, as well as one of two special values:

- `sync_last_committed`: Read the latest version guaranteed to contain all changes made to already committed transactions.
- `async_last_committed`: Read the latest version if possible, but returning data with an unspecified, small delay (typically tens of milliseconds) is allowed.

The `async_last_committed` label can be used when read consistency is not required. This mode may work faster when there is competition between reads and two-phase commits. With a two-phase commit, the table rows are locked in a special way until the second stage starts for the transaction and the commit label is selected. Readers wanting the most recent data have to wait for the first phase to end, because before then it is not known whether the transaction will be successful.

The `sync_last_committed` and `async_last_committed` labels do not guarantee a consistent data slice. The data seen by the query reader can correspond to different points in time, either at the level of entire rows or at the level of individual row columns. A specific timestamp must be indicated for consistent reading across the entire table or for a set of tables.

For a table with non-atomic changes, `sync_last_committed` and `async_last_committed` modes are equivalent, since no two-phase commit occurs.

### Running a script

The system understands an SQL-like dialect, which can be used to select and aggregate volumes of data in millions of rows in real time. As with the row read operation by key, a timestamp can be specified in the query, relative to which the query must be executed.

Since the data in the system is actually sorted by a set of key columns, the system uses this property to reduce the amount of reading at runtime. Specifically, the user's query is analyzed and `key ranges`, ranges in key space whose combination covers the entire search area, are inferred from it. This enables you to execute `range scan` with a single select query. For more information, see [Query language](../../../user-guide/dynamic-tables/dyn-query-language.md).

When building the query, remember that if the system fails to infer non-trivial `key ranges`, `full scan` of the data will occur. `Full scan` occurs, for example, if you specify `key1, key2` as key columns and set filtering only by `key2`.

### Writing rows { #insert_rows }

The client can write using the `insert_rows` method within the active transaction. To do this, the rows to be written must be reported. All key fields must be present in each such row. Some of the data fields from those specified in the schema may be missing.

Semantically, if a row with the specified key is not in the table, it appears. If there is already a row with this key, some of the columns are overwritten.

There are 2 modes when some of the fields are specified:

- `overwrite` (default): All unspecified fields update their values to `null`.
- `update`: Enabled by the `update == true` option. The previous value will then be retained. In this mode, all columns marked with the `required` attribute must be transmitted.

{% note info "Note" %}

When reading and executing an SQL query, the data is visible only when the transaction starts. Changes written within the same transaction are not available for reading.

{% endnote %}

### Deleting rows

Within the transaction, the client can delete a row or a set of rows by reporting the appropriate keys.

Semantically, if a row with the specified key was in the table, it will be deleted. If there was no row, then no changes will occur.

As with most MVCC systems, deletion in {{product-name}} comes down to writing a row without data, but with a special `tombstone` marker that signals deletion. This means that freeing up disk space from deleted rows does not happen immediately, but in a delayed manner. Deleting rows also does not cause an immediate acceleration in reading. Since data combining occurs during reading, it slows down.

### Locking rows

The client can lock rows within a transaction. Locking guarantees that the row will not be changed in other transactions during the current transaction. A single row can be locked from multiple transactions. You can specify individual `lock` column groups to be locked and a `weak` or `strong` lock mode.

### Deleting old data (TTL) { #remove_old_data }

In the process of combining the tablet chunks, some of the data may be identified as obsolete and deleted.

{% note info "Note" %}

Deleting rows in a transaction actually only writes a special marker, but does not free up memory.

{% endnote %}

Values of the `min_data_versions`, `max_data_versions`, `min_data_ttl`, and `max_data_ttl` attributes indicate whether data can be deleted. The value can be deleted if two conditions are met at the same time:

- There is no prohibition to delete this value.
- There is at least one permission to delete this value.

To understand which values have permissions and prohibitions and what types they are, you can mentally sort all the values in this table row and column : (`t1, v1`), (`t2, v2`), ..., (`tn, vn`) where `ti` are timestamps and `vi` are values themselves. Timestamps are considered ordered in descending order. In addition, the table row delete command generates a special value for all its columns: semantically, it does not equal `null`, because once `null` is written to the rows, they cannot be deleted. Then the deletion rules are as follows:

- The first `min_data_versions` values cannot be deleted for version number reasons.
- Values written less than `min_data_ttl` before the current moment cannot be deleted for time reasons.
- Values following the first `max_data_versions` can be deleted for version number reasons.
- Values written earlier than `max_data_ttl` from the current moment can be deleted for time reasons.

Default settings:

- min_data_versions = 1;
- max_data_versions = 1;
- min_data_ttl = 1800000 (30 min);
- max_data_ttl = 1800000 (30 min).

By default, at least one value - the last one - will always be stored, as well as all values written in the last 30 minutes. The time during which a transaction can remain consistent is limited; the system does not allow long transactions.

By using the listed parameters, flexible storage policies can be built. For example, `min_data_versions = 0`, `max_data_versions = 1`, `min_data_ttl = 0`, and `max_data_ttl = 86400000 (1 day)` allow any data older than one day to be deleted, saving only one version from the last day.

{% note info "Note" %}

The specified parameters enable the system to delete data, but do not force it to. Combining chunks and deleting data is a background operation.

{% endnote %}

If you need to force data cleaning, use the `forced_compaction_revision` attribute:

```bash
yt set //table/@forced_compaction_revision 1; yt remount-table //table
```

The given set of commands starts the compaction of all data written up to this moment. In this way, both redundant duplicate versions and logically deleted data will be deleted. This operation creates an immediate load on the cluster, which depends on the volume of multi-version data, so this operation is considered an administrative intervention.

{% note warning "Attention!" %}

The `forced_compaction_revision` setting causes a heavy load on the cluster. We do not recommend using this attribute without a specific need and understanding of the consequences.

{% endnote %}

When fresh writes are added to a table by keys that are ascending on average, the old data is also deleted by keys that are  ascending on average. At the end of the table, there are partitions with data for which there are tombstones. These partitions will not be compressed as long as the number of chunks in them is small. The table size stored on the disk is constantly increasing, although the amount of data in it will remain constant. A possible solution is to specify the `auto_compaction_period` parameter that sets the periodicity at which partitions will be forced to compact.

## Aggregation columns { #aggr_columns }

If the data scenario involves the constant addition of deltas to values already written in the table, use aggregation columns. In the schema, specify the `aggregate=sum` attribute or another aggregate function for the column. You can then make a write to such a column with additive semantics to the value already written in the table.

No old value is read, only the delta is written to the table. The actual summing up occurs when reading. The values in the tables are stored together with a timestamp. When reading from an aggregation column, values corresponding to the same key and the same column, but with different timestamps, are summed up. To perform optimization, the old data in the aggregation column is summed up at the compaction stage, and only one value corresponding to their sum remains in the table.

The following aggregate functions are supported: `sum`, `min`, `max`, or `first`.

The default setting is to overwrite what is in the table. To write a delta, specify the `aggregate=true` option in the write command.

## Row-by-row cache { #lookup_cache }

Row-by-row cache is a feature similar to memcached. It will be useful if the table does not fit in RAM, but there is a profile for reading data by keys (lookup queries) getting into some rarely changing working set that fits in your memory).

To use the cache,  in the table attributes, you need to specify the number of rows per tablet to be cached; in queries that satisfy the read profile, you need to specify an option that allows the cache to be used. The option in the query is needed to separate queries that can read the entire table and flush data from the cache.

The table attribute: `lookup_cache_rows_per_tablet`

The lookup query option: `use_lookup_cache`

The following ratios can be used to determine the number of cached rows per tablet:
- If you know the size of the working set which is mostly read from the table, divide its size by the number of tablets.
- If you know how many tablets there are on average per node and how much RAM is available, then `lookup_cache_rows_per_tablet` equals the amount of memory that can be allocated per node/number of tablets per node/average row size (Data weight of the table/number of rows in the table).

[*first]: Selects the first written value, ignores all subsequent values.

