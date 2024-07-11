# Sorted dynamic tables

This section describes the structure of sorted dynamic tables and the actions that can be performed with these tables.

## Data model { #model }

Each sorted dynamic table is a set of rows ordered by key. As with static tables, the key can be composite: it can consist of several columns. Unlike static tables, the key in a sorted dynamic table is unique.

Sorted dynamic tables must be strictly schematic: all column names and types must be specified in the schema in advance.

## Supported operations { #methods }

### Creating

To create a sorted dynamic table, run the `create table` command and specify the schema and `dynamic=True` setting in the attributes. The schema must correspond to the schema of the sorted table.

CLI
```bash
yt create table //path/to/table --attributes \
'{dynamic=%true;schema=[{name=key;type=string;sort_order=ascending}; {name=value;type=string}]}'
```

{% note warning "Attention!" %}

You must specify at least one key column in the table schema. If this is not done, the table will be successfully created, but it will [ordered](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md) rather than sorted. Most types of select queries will work for the table. But all such queries will come down to a full scan of the data.

{% endnote %}

### Changing a table schema and type

You can change the schema of an existing dynamic table using the `alter-table` command. For the command to be successful, the table must be [unmounted](../../../user-guide/dynamic-tables/overview.md#mount_table), and the new schema must be compatible with the old one. There are no changes to the data written to the disk, because the old data is suitable for the new schema.

Use `alter-table` to convert a static table into a dynamic table. For more information, see [MapReduce for dynamic tables](../../../user-guide/dynamic-tables/mapreduce.md#convert_table).

### Reading rows

The client can read rows by a given key using the `lookup` method. To do this, you need to specify the table name, a timestamp defining the slice of data to be read (`<= t`), and the names of the columns that interest you. This is a point query: it requires all key components to be specified. There is a type of API call that allows multiple rows to be read in a single query by different keys.

Reads within an atomic transaction will use the transaction start timestamp. You can also indicate a specific numeric value, such as an approximation of the physical moment in time, as well as one of two special values:

- `sync_last_committed`: Read the latest version guaranteed to contain all modifications of already committed transactions.
- `async_last_committed`: Read the latest version if possible, but returning data with an unspecified, small delay (typically tens of milliseconds) is allowed.

The `async_last_committed` label can be used when read consistency is not required. This mode may work faster when there is competition between reads and two-phase commits. With a two-phase commit, the table rows are locked in a special way until the second stage starts for the transaction and the commit label is selected. Readers wanting the most recent data have to wait for the first phase to end, because before then it is not known whether the transaction will be successful.

The `sync_last_committed` and `async_last_committed` labels do not guarantee a consistent data slice. The data seen by the query reader can correspond to different points in time, either at the level of entire rows or at the level of individual row columns. A specific timestamp must be indicated for consistent reading across the entire table or for a set of tables.

For a table with non-atomic changes, `sync_last_committed` and `async_last_committed` modes are equivalent, since no two-phase commit occurs.

### Running a script

The system understands an [SQL-like dialect](../../../user-guide/dynamic-tables/dyn-query-language.md), which can be used to select and aggregate volumes of data in millions of rows in real time. As with the row read operation by key, a timestamp can be specified in the query, relative to which the query must be executed.

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

As with most MVCC systems, deletion in {{product-name}} comes down to writing a row without data, but with a special `tombstone` marker that signals deletion. This means that freeing up disk space from deleted rows does not happen immediately, but in a delayed manner. Deleting rows also does not instantly increase reading speed. On the contrary, it causes a slowdown, because during read operations data is merged.

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

With default settings, at least one value (the last one) is always stored, along with all values written in the last 30 minutes. The time during which a transaction can remain consistent is limited: the system does not allow prolonged transactions.

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

Another popular scenario involves adding fresh writes to a table by keys that are (on average) ascending, while simultaneously deleting old data by keys that are also, on average, ascending. In this case, the table is appended with partitions containing logically deleted data that is marked by tombstones. These partitions will not be compressed as long as the number of chunks in them is small. The size of the table on the disk keeps increasing, even though the amount of undeleted data in the table remains constant. A possible solution is to specify the `auto_compaction_period` parameter, which sets the periodicity at which partitions are forced to compact.

## Aggregation columns { #aggr_columns }

If the data scenario involves the constant addition of deltas to values already written in the table, use aggregation columns. In the schema, specify the `aggregate=sum` attribute or another aggregate function for the column. You can then make a write to such a column with additive semantics to the value already written in the table.

No old value is read, only the delta is written to the table. The actual summing up occurs when reading. The values in the tables are stored together with a timestamp. When reading from an aggregation column, values corresponding to the same key and the same column, but with different timestamps, are summed up. To perform optimization, the old data in the aggregation column is summed up at the compaction stage, and only one value corresponding to their sum remains in the table.

Supported aggregate functions are: `sum`, `min`, `max`, and `first`.

The default setting is to overwrite what is in the table. To write a delta, specify the `aggregate=true` option in the write command.

## Row-by-row cache { #lookup_cache }

For point reads of rows (reading specific rows by their full keys) from a dynamic table, there's a lookup rows API call.

In a basic scenario, the table is stored on a disk (HDD, SSD, NVMe). Data read from disk or compressed by a codec (LZ, gzip, zstd) is measured in blocks. When reading blocks from disk, the data is cached at multiple levels: at the OS level and at the level of the {{product-name}} node process. In the {{product-name}} node process, blocks are cached in both compressed and uncompressed form.

In general, if the caches do not contain a block with a given key, the block is read from disk, transferred over the network, uncompressed, and then read according to the format of the table chunks (unversioned/versioned, scan/lookup). These operations may take a fair bit of time. Furthermore, this creates additional load on the disk, network, and CPU.

Usually, point reads only read a fairly small fraction of the data contained in a block (less than 1%). So if there are no blocks with your lookup keys, a large chunk of work in the cache of uncompressed blocks will go to waste.

In latency-critical scenarios involving reads by full keys from a dynamic table, the only option used to be to load the entire data into memory in uncompressed form (table attribute `@in_memory_mode=uncompressed`). The downside of this approach is that it requires a lot of RAM when working with large tables.

A fairly common scenario involves working with a table that contains a certain limited amount of frequently read data. In other words, the probability of reads by any given key at any given time is not equal for all keys. If your table contains such a working set with hot data, you can use a lookup cache.

A lookup cache is an LRU cache that stores individual rows from the table. You could theoretically cache individual rows using a separate tool. For instance, memcached: you send a request to memcached, and if the data is not present in the cache, read it from {{product-name}}, then add it to the cache.

The problem is that the table data can change at any time: a table row that is stored in the cache may at any given point of time be modified or deleted by a currently running transaction. For this reason, to ensure data consistency and isolation during reads, the cache must be synchronized with any data modifications in the table. And that is why the cache is integrated with {{product-name}}.

To use the cache, make sure you do the following:
- Request a quota for the `lookup_rows_cache` memory category (similar to the `tablet_static` quota for in-memory tables).
- Set the `@lookup_cache_rows_ratio` attribute for the tables. This attribute specifies the fraction of rows to be cached for each tablet. You can start at fractions of a percent (0.0005) and gradually increase the value all the way up to a few percent (0.03).
- Set the `@enable_lookup_cache_by_default` attribute for the table to `%true`.
- Run the remount-table command.

Once this is done, all lookup queries to the table will be processed using the cache.

If you do not want to use the cache for some queries, you can specify the `use_lookup_cache` option in the lookup rows query itself. This can be particularly useful if there's a background process that periodically modifies data across the entire table (performs a lookup followed by an insert). Using the cache for these lookups could otherwise cause data to be flushed from it. On the other hand, if this process only updates a small fraction of the table data, using the cache could be useful for all lookup queries, as this would speed up the execution of the periodic process.

By default, the `use_lookup_cache` option is set to `null`. If `use_lookup_cache` is `null`, the cache's usage is determined by the table's `@enable_lookup_cache_by_default` attribute value (cache size must also be configured). If the passed `use_lookup_cache` value is `true` or `false`, the cache's usage for the lookup query is determined by this option instead.

The `@lookup_cache_rows_per_tablet` table attribute is deprecated, so we do not recommend using it. The problem with this attribute is that different tablets can contain different amounts of data. The cache's operation may not be efficient with fixed tablet sizes. Furthermore, a fixed cache size can often become suboptimal as sharding settings change and the table grows in size.

If you are using hunks, it may be beneficial to enable the lookup cache and set the `@in_memory_mode=uncompressed` mode for the table. In this case, the in-memory data contains references to hunks. The amount of this data is typically not very large. The lookup cache allows storing hunks and reducing the frequency of disk reads and network consumption.

### Diagnostic metrics for lookup cache performance

When the cache operates efficiently, the frequency of block reads from disk or from the block cache should gradually decrease.

Common lookup metrics include:

Metrics for disk reads and network transfers:
- `yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate`
- `yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_cache.rate`
- `yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_transmitted.rate`

If hunks are used:
- `yt.tablet_node.lookup.hunks.chunk_reader_statistics.data_bytes_read_from_disk.rate`
- `yt.tablet_node.lookup.hunks.chunk_reader_statistics.data_bytes_transmitted.rate`

Response time metric for lookup rows queries on a tablet node:
- `yt.tablet_node.lookup.duration`

Lookup cache–specific metrics:
- `yt.tablet_node.lookup.cache_hits.rate`: Number of cache hits per second.
- `yt.tablet_node.lookup.cache_misses.rate`: Number of cache misses per second.
- `yt.tablet_node.lookup.cache_inserts.rate`: Number of cache inserts per second. This value may be different from "cache_misses" if the keys aren't anywhere in the table, or if the cache runs out of memory (`lookup_rows_cache` memory categories). For the number of queried keys that are absent from the table, use the metric "yt.tablet_node.lookup.missing_row_count.rate". If the table is not loaded into memory, any attempts to read missing keys always result in queries to the disk. To prevent this, enable the key xor filter.
- `yt.tablet_node.lookup.cache_outdated.rate`: Number of keys per second that present in the cache but are outdated because the cache ran out of memory and could not update them in time. Once the cache reaches its memory quota ("lookup_rows_cache" memory category), it can no longer update existing rows. In this case, the old value remains cached but can no longer be read.

As the cache is processed, the system performs garbage collection. The following metrics allow tracking the actual amount of memory being used.
- `yt.tablet_node.row_cache.slab_allocator.lookup.alive_items`: Number of current allocations.
- `yt.tablet_node.row_cache.slab_allocator.lookup.allocated_items.rate`: Number of allocations per second.
- `yt.tablet_node.row_cache.slab_allocator.lookup.freed_items.rate`: Number of deallocations per second.
- `yt.tablet_node.row_cache.slab_allocator.lookup.arena_size`: Sizes of arenas (sets of allocations of the same size) in bytes.

These metrics have a "rank" tag, which represents the allocation size in bytes: 16, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1022, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384, 22576, 32768, large. "Large" is used for allocation sizes that are greater than 32768 bytes.

## Filtering out non-existent keys { #key_filter }

Often, tables have a load pattern where reads by non-existent keys make up a significant portion of all queries. To quickly identify that a key is absent from the table, dynamic tables implement a key filtering mechanism. This is achieved using a [xor filter](https://arxiv.org/abs/1912.08258), which functions similarly to the Bloom filter.

This filter can be useful in two scenarios:
- The lookup key is not present in the table.
- The lookup key is rarely written to. In this case, we read all chunks within the partition, even though we already know that most of them don't have the key that we need.

Filtering is supported for lookup queries as well as for select queries that include a key prefix condition.

### Enabling the filter

To enable filtering:
- Allocate memory for the key filter block cache within the bundle (recommended value is 500 MB). You can do this yourself by navigating to the Edit Bundle -> Resources menu. For bundles that are still using the legacy resource model, {% if audience == "internal" %}contact the administrator.{% else %}use the [dynamic node config](../../../admin-guide/cluster-operations#ytdyncfgen) to set the "data_node/block_cache/xor_filter/capacity" field value. This action is available to the cluster administrator.{% endif %}
- Enable saving filters during write operations. To do this, set the `@chunk_writer/key_filter/enable = %true` attribute for the table. If the table does not have the `@chunk_writer` attribute, set it to `{key_filter={enable=%true}}`.
- If you need filtering for select queries, use the `@chunk_writer/key_filter_prefix` attribute to enable writes and specify the "prefix_lengths" parameter for this attribute.
- Set `@mount_config/enable_key_filter_for_lookup = %true`. This option enables filtering for both lookup and select queries.
- Perform the remount-table command on the table.
- To add the filter for all existing chunks, you can use [forced compaction](../../../user-guide/dynamic-tables/compaction#forced_compaction).

### Filter parameters

Filter write parameters are specified in `@chunk_writer/key_filter` for lookup queries and in `@chunk_writer/key_filter_prefix` for select queries:
- enable: Whether a filter should be created for the chunk's blocks.
- false_positive_rate: Percentage of incorrect filter responses (the filter did not filter out a key that was, in fact, missing). The default value is 1 / 256. The higher the precision, the more space the filter will take up.
- bits_per_key: Number of bits that a single key takes up in the filter.
- prefix_lengths: List of key prefix lengths for which the filter should be created. This parameter is only valid for `@chunk_writer/key_filter_prefix`.

You cannot set both "false_positive_rate" and "bits_per_key" at the same time. If "false_positive_rate" is set, the "bits_per_key" value is derived from it. We recommend using the "false_positive_rate" parameter.

### Diagnostic metrics for filter performance

When the filter is running efficiently, the number of block reads from disk or from the block cache should decrease.

To make monitoring the filter's performance easier, use the ready-made Key Filter (table-based) and Bundle UI Key Filter (bundle-based, available in the bundle interface) dashboards.

#### Common metrics

To determine whether you should enable filtering, it is useful to calculate the ratio of the total number of chunks where the key was looked up to the number of successfully read keys. We recommend that you consider enabling the filter if this ratio is greater than 3:
- yt.tablet_node.lookup.unmerged_missing_row_count.rate / yt.tablet_node.lookup.row_count.rate

#### Metrics for lookup operations

Metrics for disk or cache reads:

- yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_cache.rate: Number of bytes read by lookup queries from the block cache.
- yt.tablet_node.lookup.chunk_reader_statistics.data_bytes_read_from_disk.rate: Number of bytes read by lookup queries from disk.

Filter-specific metrics:

- yt.tablet_node.lookup.key_filter.input_key_count.rate: Number of keys that the filter received as input.
- yt.tablet_node.lookup.key_filter.filtered_out_key_count.rate: Number of keys filtered out by the filter.
- yt.tablet_node.lookup.key_filter.false_positive_key_count.rate: Number of keys that were not filtered out by the filter, even though they did not actually exist.

#### Metrics for select operations

Metrics for disk or cache reads:

- yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_cache.rate: Number of bytes read by select queries from the block cache.
- yt.tablet_node.select.chunk_reader_statistics.data_bytes_read_from_disk.rate: Number of bytes read by select queries from disk.

Filter-specific metrics:

- yt.tablet_node.select.range_filter.input_range_count.rate: Number of ranges that the filter received as input.
- yt.tablet_node.select.range_filter.filtered_out_range_count.rate: Number of ranges filtered out by the filter.
- yt.tablet_node.select.range_filter.false_positive_range_count.rate: Number of ranges that were not filtered out by the filter, even though they did not actually exist.
