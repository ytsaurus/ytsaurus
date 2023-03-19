# Running operations on dynamic tables

This section describes how to run MapReduce operations on dynamic tables and tells you about data consistency. Examples of converting a static table into a dynamic table and vice versa are given.

Everything described applies equally to all types of operations — map, merge, etc., and not only directly to map-reduce, and the read-table command.

## Overview

{{product-name}} can run MapReduce operations on top of [sorted and ordered](../../../user-guide/dynamic-tables/overview.md#types) dynamic tables. You can also quickly (at the metadata level) convert a static table into a dynamic table.

Since static and dynamic tables were originally created for different work scenarios, there are several fundamental differences between them (both sorted and ordered).

1. When inserted into a dynamic table, the data first gets into the log and a special in-memory storage (the dynamic store), and only after that it is written to chunks on the disks in the form of versioned rows (until the data is written to chunks, fault tolerance is guaranteed by the log). Data from static tables is always written to chunks on the disks.
2. All keys in a _sorted_ dynamic table are unique. Table rows are uniquely determined by their key.
3. All data in a _sorted_ dynamic table is versioned: when inserted into a sorted dynamic table, the commit timestamp is assigned to it. When reading, you can specify a timestamp to get the table row version for the specified time slice.
4. In _sorted_ dynamic tables, compaction occurs periodically: old data versions are deleted, chunks are overwritten.
5. Neither _sorted_ nor _ordered_ dynamic tables allow pass-through indexing by row number: in sorted dynamic tables, it is completely impossible, in ordered dynamic tables, it is possible within each tablet separately.

## Running operations on dynamic tables { #running_operations_over_dyn_tables }

_This section applies to both sorted and ordered tables._

A dynamic table can be specified as an input one (or one of the input tables) for a MapReduce operation. The MapReduce operation interface is the same as static tables have, except that you specify the path to the dynamic table instead of the path to the static table. The dynamic table can be either mounted or unmounted. A [separate article](../../../user-guide/dynamic-tables/bulk-insert.md) is devoted to the use of dynamic tables as output tables for the operation.

Since ordinary MapReduce operations work with chunks and dynamic tables store some data in memory in dynamic stores, there is some specificity in running operations on dynamic tables. By default, data from dynamic stores is not visible from operations. There are several ways to bypass this restriction.

A suitable method in most cases is to first unmount the table and then specify the `@enable_dynamic_store_read=%true` attribute on it, after which all data will be visible from operations. This is a popular approach if you want fresh data, but there are side effects:

- The results of successive runs of the same operation on top of the same table (even under snapshot lock) may differ, because the second run will read more fresh rows. For this reason, caching in YQL is disabled for tables with`@enable_dynamic_store_read`. For idempotency, you must [explicitly specify a timestamp](#dyntable_mr_timestamp) or [a set of row indexes](#dyntable_mr_ordered).
- If the `@merge_rows_on_flush` attribute is set on the table, you cannot ensure consistent read by timestamp when `@enable_dynamic_store_read` is enabled.
- If one tablet is read by hundreds of jobs, there is a risk of overloading the tablet nodes.

If you don't want to use this mode for any reason, there are some other ways:

1. Unmount the table. When the dynamic table is unmounted, all of its data is stored in chunks.
2. Freeze the table using the `freeze-table` command and wait until all tablets are in `frozen` state. In this state all data is written to the chunks, no new data can be written, but the table can be read using the `lookup-rows` and `select-rows` commands. You can execute MapReduce operations.
3. Order the table state at a point in time in the past so that all data is already in the chunks.
4. Do nothing and get the data from the chunks as in clause 3, but with no guarantee that all keys will refer to the same table version (data is written to the disk at different times in different tablets, there is no synchronization between them).

{% note info "Note" %}

You can run operations that ignore dynamic stores and read only from chunks on top of tables with enabled `@enable_dynamic_store_read`. To do this, explicitly specify `enable_dynamic_store_read = %false` in the spec root.

{% endnote %}

## Running operations on the state of a sorted dynamic table at a particular point in time { #dyntable_mr_timestamp }

By default, the most recent row version contained in the chunks gets into the MapReduce operation. If the table is mounted, the versions in the chunks may be inconsistent: it may be for two different rows from the same transaction that one row is already in the chunk and the other is still in RAM. As a result, the MapReduce operation will only display some of the transaction rows. If `@enable_dynamic_store_read` is enabled, inconsistency is also possible because the jobs read different parts of the table at different times, and writing may have occurred between reads. In cases where it is important to have a consistent table state in the MapReduce operation, you must specify a timestamp in the special ypath attribute: `<timestamp=123456789>//path/to/dynamic/sorted/table`.

There is a lower and upper limit on `timestamp`:
- Since the data is periodically compacted, `timestamp` cannot be less than the value of the `@retained_timestamp` attribute. Compaction clears old data, but only the one preceding `@retained_timestamp`.
- If the `@enable_dynamic_store_read` attribute is specified on the table, there is no upper limit. However, if you specify `timestamp` from the future, consistency is not ensured.
- If `@enable_dynamic_store_read` is not specified, `timestamp` must be smaller than the timestamp of those rows that have not yet been written to the disk. You can find out the timestamp via the `unflushed_timestamp` table attribute.

Thus, if reading from dynamic stores is enabled, we recommend taking the current timestamp (it can be obtained using the [generate_timestamp](../../../api/commands#generate_timestamp) command). When reading from a replica table, `timestamp` must be generated on the metacluster. If reading from dynamic stores is not enabled, use `unflushed_timestamp - 1`.

When specifying `timestamp`, the scheduler will check that the timestamp value falls within the permitted interval.

The `unflushed_timestamp` and `retained_timestamp` attribute values of a mounted table are constantly changing, so you must use snapshot lock when working with the table. There are no guarantees that `retained_timestamp` is always less than `unflushed_timestamp`. The listed values depend on the compaction settings and the process of writing data to the chunks, and if this condition is violated too often, the table settings will have to be changed.

Pay attention to the following attributes:

- `dynamic_store_auto_flush_period`: The frequency with which data is flushed to the disk. It makes sense if `@enable_dynamic_store_read` is disabled. The default value is 15 minutes.
- `min_data_ttl`: Determines to what extent old data must not be compacted. For more information, see [Sorted dynamic tables](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#remove_old_data).

{% note info "Note" %}

The current implementation does not support operations with timestamp indication on tables mounted in non-atomic mode.

{% endnote %}

## Running operations on ordered tables { #dyntable_mr_ordered }

Ordered dynamic tables are quite similar to static tables. The differences in terms of operations are that indexing by row numbers is performed separately within each tablet (and indexing may not start at zero because of [trim](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md#trim)) and fresh data in each tablet is in the dynamic store.

{% note info "Note" %}

The effect of trim may not be immediately seen in the operation. The situation in which rows deleted via trim are included in the operation is considered allowable.

{% endnote %}

### Indexing


The `enable_tablet_index` [control attribute](../../../user-guide/storage/io-configuration.md#control_attributes) is available in operations on ordered tables.

The `enable_row_index` attribute has a different semantics than in static tables: the returned row index is counted relative to the beginning of the tablet. Row numbering within the tablet is logical, i.e. even if trim is performed, the existing row numbers will not change.

You can also specify row index and tablet index in ypath ranges. Each row in the ordered table is indexed in a pair `(tablet_index, row_index)`. The `lower_limit = {tablet_index = a; row_index = b}` lower limit allows rows from the `a` tablet with numbers `b` and greater and all rows from tablets with numbers greater than `a`. The `upper_limit = {tablet_index = c; row_index = d}` upper limit allows rows from tablets with numbers less than `c` and rows from the `c` tablet with a number strictly less than `d`.

You can specify only `tablet_index` within the limit. The lower limit of the `{tablet_index = e}` type allows all rows of tablets with numbers not less than `e` and the upper limit of this type allows all rows of tablets with numbers strictly less than `e`.

{% note warning "Attention!" %}

There is a limit on the number of ranges in YPath. If you plan to specify one range per each tablet of an ordered table in the operation and the number of tablets exceeds 100, notify yt@.

{% endnote %}

### Allowable index limits
As with timestamp for sorted tables, row index in the ordered table has an upper and lower limit within the tablet. The lower limit has to do with deleting rows via trim. The upper one is related to storing rows in the dynamic store and makes sense if [enable_dynamic_store_read](#running_operations_over_dyn_tables) does not work for you and you want to run an operation on a mounted table.

Limits can be obtained from the `@tablets` table attribute. The value corresponding to each tablet has the `trimmed_row_count` and `flushed_row_count` fields.

Unlike timestamp, the correctness of indexes for ordered tables is __not validated__: if you specify indexes outside the specified limits, the operation will simply read less data than expected.

## Converting a static table into a dynamic table

To convert a static table into a dynamic table, run the `alter-table` command:

{% list tabs %}

- CLI

   ```bash
   yt alter-table --dynamic //tmp/t
   ```

- Python

   ```python
   yt.alter_table("//tmp/t", dynamic=True)
   ```

{% endlist %}

There are a few conversion peculiarities that are worth paying attention to.

- The `alter-table` command will be executed for an unsorted table as well. The result will be an [ordered dynamic table](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md). For such tables, the `insert-rows` command works differently than it does for sorted tables and the `select-rows` command will be much slower on the same queries.

- All keys in a sorted dynamic table are unique, so a static table must also contain only unique keys. The system identifies this fact by the `@unique_keys` attribute in the schema. You cannot set this attribute to the schema of an existing non-empty table using the `alter-table` command. You need to specifically create a table with the specified `unique_keys=true` attribute in the schema. The uniqueness of keys will be validated when writing. If the table has already been created and all keys in it are unique, but the schema has the `unique_keys=false` attribute, you need to create a new table with the corrected schema and copy the data into it using the Merge operation.

- Typical chunk and block sizes for dynamic tables are smaller than for static tables. If the table is to be converted into a dynamic table, set certain parameters in the spec of the operation generating the table:

   ```python
   "job_io": {"table_writer": {"block_size": 256 * 2**10, "desired_chunk_size": 100 * 2**20}}
   ```

   If an operation has more than one job type, the config only needs to be specified for `job_io` of the last job type in the operation (`merge_job_io` for the Sort operation, `reduce_job_io` for the MapReduce operation). If the operation uses automatic merging of chunks, a similar config must be specified in the `"automerge": {"job_io": {...}}` section.

   For more information about `job_io` for different types of operations, see Operation settings.

- You can check the sizes of chunks and blocks of the resulting table using the following table attributes:
   - Chunks: `@compressed_data_size` / `@chunk_count`
   - Blocks: `@chunk_format_statistics/*/max_block_size` (attribute value is a dict containing the max_block_size value for each chunk type in the table).

{% cut "Learn more about chunk and block sizes" %}

Dynamic tables are usually used for a quick search by key. Data in chunks is split into blocks, and when read from the disk, the entire block is read at a time. By default, the table is compressed with a specific codec, decompression will also be applied to the entire block. To avoid applying too many calculations to search for a single row, reduce the block size. By default, static table blocks have a default size of 16 megabytes, while dynamic table blocks are 256 kilobytes. But if a static table is converted into a dynamic table, the block size remains the same as that of the original static table. Therefore, when creating a static table, you must force the block size to be reduced by specifying the `block_size` setting in the specification.

{% note warning "Attention!" %}

The current {{product-name}} implementation checks that the maximum block size does not exceed 512 kilobytes when attempting to query a dynamic table. Otherwise the query will end with the "Maximum block size limit violated" error. This is done to avoid slow reads from tables. The same error will occur with a dynamic table loaded into memory when trying to load a chunk with large blocks. To solve this problem, you can either compact the table or increase the block size limit to 32 megabytes using the `max_unversioned_block_size` attribute (you must set the attribute on the table and then call `remount_table`).

{% endnote %}

Tablets in dynamic tables are divided into partitions. A typical partition size is several hundred megabytes. If the chunks become larger than the partition size, a background process is started that forces the chunks to be split into smaller chunks. It is also useful to specify the `desired_chunk_size` setting in the specification when creating a dynamic table.

The chunks will then be quite small (100 megabytes each). Otherwise, the chunks will be large by default (512 MB or more). When a table is sharded into shards (tablets), these chunks can fall into multiple tables and will then be counted multiple times when calculating a table size. As a result, the dynamic table size may be several times larger than the size of the original static table.

{% endcut %}

If the table is intended to be stored in memory (managed by the `@in_memory_mode`attribute), we recommend performing forced compaction to achieve the best performance. The need for this is caused by the following:

- The same chunk gets into different tablets, and the chunks of different tablets are loaded into memory independently (the tablets may be on different cluster nodes). As a result, more memory is consumed than necessary.
- Readers for static table chunks are not optimized for cases when the data is already in memory.

If the dynamic table is to be read-only, it must be mounted in "frozen" state (managed by the `--freeze` flag of the `mount-table` command). This will prohibit writing, disable compaction, and reduce the table maintenance overhead costs. Do it only if the the chunk and block sizes are as recommended.

## Using computed columns

To distribute the load across the cluster more evenly, dynamic tables need to be sharded (split into tablets).  To distribute the load evenly across the tablets, it may be convenient to add an extra column prior to all the key columns in which to write hash from the key part by which it makes sense to perform sharding - for example, hash from the first column. The result is a table whose keys are evenly distributed in the `[0, 2^64-1]` range.

The column added in this way is a computed column. The calculation formula must be specified in the column schema in the `expression` field. Support for computed columns was added to all operations except Sort. When writing to the table, the value of such columns will be computed. To prepare a static table to be converted into a dynamic table, you must first make the table in an unsorted schema (but with computed columns), then sort it using the Sort operation.

Below is an example of creating a dynamic table with computed columns from a static table. Instead of `write_rows`, the table could be written to using the Map, Reduce, MapReduce, or Merge operations. In the latter case, you must specify `{'schema_inference_mode': 'from_output'}` in the specification so that the data is validated using the output table schema.

{% cut "Creating a dynamic table with computed columns" %}

```python
import yt.wrapper as yt
import yt.yson as yson
import time

# Unsorted schema.
schema = [
    {"name": "hash", "type": "uint64", "expression": "farm_hash(key)"},
    {"name": "key", "type": "uint64"},
    {"name": "value", "type": "string"}]

# Sorted schema with unique keys attribute.
sorted_schema = yson.YsonList([
    {"name": "hash", "type": "uint64", "expression": "farm_hash(key)", "sort_order": "ascending"},
    {"name": "key", "type": "uint64", "sort_order": "ascending"},
    {"name": "value", "type": "string"}])
sorted_schema.attributes["unique_keys"] = True

# Create table
table = "//tmp/table"
yt.remove(table, force=True)
yt.create_table(table, attributes={"schema": schema})

# Write rows into table. Computed columns are ommited: {{product-name}} will evaluate them.
yt.write_table(table, [{"key": 2, "value": "2"}, {"key": 1, "value": "1"}])

# Sort table. Resulting table schema has unique_keys=True.
yt.run_sort(table, yt.TablePath(table, attributes={"schema": sorted_schema}), sort_by=["hash", "key"], spec={
    "partition_job_io": {"table_writer": {"block_size": 256 * 2**10}},
    "merge_job_io": {"table_writer": {"block_size": 256 * 2**10}},
    "sort_job_io": {"table_writer": {"block_size": 256 * 2**10}}})

# Alter table into dynamic.
yt.alter_table(table, dynamic=True)

# Mount table and wait until all tablets are mounted.
yt.mount_table(table, sync=True)

# Print rows.
for row in yt.select_rows("* from [{}]".format(table)):
    print row
```

{% endcut %}

## Converting a dynamic table into a static table and vice versa { #convert_table }

Below is an example of the script that demonstrates how to save a dynamic table into a static table and then restore the dynamic table back from the static table. This operation can be performed to back up dynamic tables.

{% cut "Converting tables" %}

```python
#!/usr/bin/python
import yt.wrapper as yt
import argparse

def dump(src, dst):
    with yt.Transaction():
        # Take snapshot lock for source table.
        yt.lock(src, mode="snapshot")
        # Get timestamp of flushed data.
        ts = yt.get_attribute(src, "unflushed_timestamp") - 1
        # Create table and save vital attributes.
        yt.create("table", dst, attributes={
            "optimize_for": yt.get_attribute(src, "optimize_for", default="lookup"),
            "schema": yt.get_attribute(src, "schema"),
            "_yt_dump_restore_pivot_keys": yt.get_attribute(src, "pivot_keys")
        })
        # Dump table contents.
        yt.run_merge(yt.TablePath(src, attributes={"timestamp": ts}), dst, mode="ordered")

def restore(src, dst):
    # Create destination table.
    yt.create("table", dst, attributes={
        "optimize_for": yt.get_attribute(src, "optimize_for", default="lookup"),
        "schema": yt.get_attribute(src, "schema")
    })
    # Make blocks smaller (required for real-time lookups).
    yt.run_merge(src, dst, mode="ordered", spec={
        "job_io": {"table_writer": {"block_size": 256 * 2**10, "desired_chunk_size": 100 * 2**20}},
        "force_transform": True
    })
    # Make table dynamic.
    yt.alter_table(dst, dynamic=True)
    # Restore tablet structure.
    yt.reshard_table(dst, yt.get_attribute(src, "_yt_dump_restore_pivot_keys"), sync=True)

# Compact table to make reported size closer to expected (not necessary step, really).
def force_compact(table):
    yt.mount_table(table, sync=True)
    yt.set(table + "/@forced_compaction_revision", 1)
    yt.remount_table(table)

if __name__ ==  "__main__":
    parser = argparse.ArgumentParser(description="Dynamic tables dump-restore tool (require {{product-name}} 19.4 or larger).")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dump", nargs=2, metavar=("SOURCE", "DESTINATION"), help="Dump dynamic table to static")
    mode.add_argument("--restore", nargs=2, metavar=("SOURCE", "DESTINATION"), help="Restore dynamic table from static")
    args = parser.parse_args()
    if args.dump:
        dump(*args.dump)
    if args.restore:
        restore(*args.restore)
        force_compact(args.restore[1])
```

{% endcut %}


## Examples of running the operations


```bash
yt map 'grep some_value' --src //path/to/dynamic/table --dst //tmp/output --spec '{pool = "project-pool"; job_count = 100; }'
```


```bash
yt reduce 'python my_reducer.py' --src //path/to/dynamic/table --dst '<sorted_by = ["key"]>'//tmp/output
```

