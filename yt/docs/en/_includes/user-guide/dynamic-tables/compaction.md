# Background compaction

A tablet of a sorted dynamic table is an [LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). When a row is written, it enters the dynamic store. As the dynamic store gets filled with data, it is flushed to the chunks on the disk. A background compaction process periodically combines several chunks. It is needed to:
- Apply [delete tombstones](../../../user-guide/dynamic-tables/sorted-dynamic-tables#udalenie-stroki) and physically delete rows.
- Delete old versions.
- Reduce overlapping store count, the number of chunks in which a fixed key may need to be searched.

Compaction relies mainly on heuristics. However, depending on the table write scenario, the optimal approaches may differ. For example:
- If a small max_data_ttl is set on the table, it makes sense to periodically compact the old chunks and free up space.
- If rows are regularly deleted, we recommend examining the chunk that contains insertions and deletions during compaction in order to physically delete the rows.
- If the table write stream is large, you can sacrifice read optimality and loosen the overlapping store count settings in favor of reducing write amplification — the ratio of the data amount processed by compaction to the amount of written data, and vice versa.

## Glossary { #glossary }
- **Dynamic store**: A structure for storing freshly written rows; it is located in RAM. Analog of MemTable.
- **Chunk******: An immutable structure for storing rows flushed to the disk. Analog of SSTable.
- **Store******: A common name for chunks and dynamic stores. Technically speaking, chunk stores (and dynamic stores), not chunks are stored in the tablet, but the chunk and chunk store terms are usually interchangeable.
- **Partition******: A part of the tablet's subdivision. Similarly to tables that are split into tablets bounded by pivot keys, tablets are split into partitions. Chunks within a partition do not cross its boundaries.
- **Eden******: A special partition containing chunks that cannot be placed in any other partition because they cross their boundaries. Chunks typically do not live long in Eden, quickly undergoing partitioning.
- **Overlapping store count**, **OSC**: Maximum overlap of chunks; maximum number of chunks covering a particular key. Limits fan-in from above, i.e. the number of chunks that actually have to be read in order to get the actual value by key.
- **Flush******: The process of flushing data from a dynamic store to a chunk on the disk.
- **Compaction**********: The process of merging chunks that involves discarding old versions of data, removing delete tombstones, and combining small chunks.
- **Partitioning******: A process that is similar to compaction, except its purpose is not to combine chunks, but to split chunks located in Eden into different partitions.
- **Background processes**: A collective term for flush+compaction+partitioning.
- **Write amplification**, **WA**: The ratio of the amount of data handled by background processes (compaction/partitioning) to the amount of data written to the table. It is an important effectiveness indicator of the selected parameters.
- **Row version**: In the [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) model, a single key can be associated with multiple values, each with its own timestamp. An individual value is called a version. Generally speaking, writes to each column can be versioned independently, so it makes sense to refer to versions of the *values*, not a *row* as a whole. In terms of setting up compaction, this is usually irrelevant.

## List of attributes { #attributes }

All values with the `Duration` type are specified in milliseconds. All values indicating the amount of data are specified in bytes. Attributes for which there is no description have a complex semantics and we do not recommend changing them.

{% note warning "Warning" %}

Many attributes influence the behavior of background compaction processes. Careless setting of attributes can create an unexpected load on a bundle or cluster.

Please use only those attributes whose meaning you understand.

{% endnote %}

{% note info "Note" %}

Setting an attribute on a mounted table does not apply the settings. To apply settings, use the `remount-table` command:

CLI
```bash
yt set //path/to/table/@auto_compaction_period 86400000
yt remount-table //path/to/table
```

You can find out the tablet's current settings at `//sys/tablets/x-x-x-x/orchid/config`.

{% endnote %}

### Flush { #flush_attributes }

The following attributes regulate flush behavior.

| Name | Type | Default | Description |
|--|--|--|--|
| dynamic_store_auto_flush_period | Duration* | 900,000 (15 min) | Frequency of forced flushes, when the dynamic store is flushed to the disk straight away, even if it hasn't reached its overflow threshold yet. |
| dynamic_store_flush_period_splay | Duration | 60 000 (1 min) | Random shift for the period to avoid synchronization of different tablets. Real flush will come after `period + random(0, splay)`. |
| merge_rows_on_flush | bool | false | Allows version merging and deletion of rows by TTL at flush. |
| merge_deletions_on_flush | bool | false | Allows consecutive deletions to be merged into one at flush. |
| max_dynamic_store_row_count | int | 1 000 000 | Maximum number of rows in the dynamic store. |
| max_dynamic_store_pool_size | int | 1 073 741 824 (1 GB) | Maximum dynamic store size. |
| dynamic_store_overflow_threshold | double | 0.7 | The share of filling of the dynamic store relative to max_dynamic_store_row_count and max_dynamic_store_pool_size at which flush starts. |

### Compaction { #compaction_attributes }

#### Basic options

| Name | Type | Default | Description |
|--|--|--|--|
| auto_compaction_period | int*| - | Frequency of periodic compaction in ms. Compaction will affect every table chunk at least once per auto_compaction_period. |
| auto_compaction_period_splay_ratio | double | 0.3 | Random shift for the period to avoid synchronization. Compaction will come after `period * (1 + random(0, splay_ratio))`. |
| periodic_compaction_mode | `store`, `partition` | `store` | For more information, see [Periodic compaction](#periodic_compaction). |
| forced_compaction_revision | - | - | For more information, see [Forced compaction](#forced_compaction). |
| max_overlapping_store_count | int | 30 | Maximum number of chunks that potentially contain a key. When the threshold is reached, writing to the table will be locked until compaction optimizes the structure. |
| critical_overlapping_store_count | int* | - | Sets a threshold beyond which compaction ignores the size [limits](#compaction_size_policy) for chunks within a single partition. By setting a small value (5–10) for this option, you can reduce the OSC and data access speed at the cost of a noticeable increase in write amplification. |
| enable_compaction_and_partitioning | bool | true | Completely disables compaction. Writing to the table in this state will quickly lead to exceeding overlapping store count. If no writing is intended, then instead of this attribute,mount the table in a frozen state. |

#### Sizes and constants

| Name | Type | Default | Description |
|--|--|--|--|
| min_partition_data_size | int | 96 MB | Minimum, desired, and maximum partition size. |
| desired_partition_data_size | int | 256 MB | ↑ |
| max_partition_data_size | int | 320 MB | ↑ |
| min_partitioning_data_size | int | 64 MB | Minimum and maximum data size for a single partitioning. Increasing it reduces write amplification at the cost of increasing the number of chunks in Eden and therefore increasing overlapping store count. |
| max_partitioning_data_size | int | 1 GB | ↑ |
| min_partitioning_store_count | int | 1 | Minimum and maximum number of chunks for a single partitioning. |
| max_partitioning_store_count | int | 5 | ↑ |
| min_compaction_store_count | int | 3 | Minimum and maximum number of chunks for a single compaction. Periodic and forced compactions ignore the lower estimate, but take into account the upper one. |
| max_compaction_store_count | int | 5 | ↑ |
| compaction_data_size_base | int | 16 MB | |
| compaction_data_size_ratio | double | 2.0 |  |

### Deleting old data { #ttl_attributes }
For more information about these attributes, see [Deleting old data](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#remove_old_data).

| Name | Type | Default |
|--|--|--|
| min_data_ttl | int | 1 800 000 (30 min) |
| max_data_ttl | int | 1 800 000 (30 min) |
| min_data_versions | int | 1 |
| max_data_versions | int | 1 |

## Forced compaction { #forced_compaction }
To trigger the forced compaction of all table chunks, set the `forced_compaction_revision` attribute value to `1`.

If you execute `remount-table`, the setting will be immediately applied to all table tablets. When working with tables of a terabyte or more, this can create a load surge both on the table bundle and on the entire cluster. Therefore, we recommend executing `remount` of different tablets at different times. To do this, run the following command: `yt remount-table --first-tablet-index X --last-tablet-index Y`. To estimate the recommended time for remounting the table, use the formula `table_size / bundle_node_count / (100 Mb/s)`.

{% if audience == "internal" %}
To automatically and evenly remount the table, run the [gradual_remount](https://a.yandex-team.ru/arc_vcs/yt/yt/scripts/gradual_remount) script.

To keep track of the number of processed chunks, see the [store_compactor.out_store_count.rate](https://nda.ya.ru/t/QTBvO-Wf5Xoyr8) graph.
{% endif %}

To abort forced compaction, delete the `forced_compaction_revision` attribute from the table and execute `remount-table`.

{% note info "Note" %}

If the table size is more than 20 TB or 100,000 chunks, get administrator permission before starting forced compaction.

{% endnote %}

## Periodic compaction { #periodic_compaction }
To ensure that all table chunks are periodically compacted, regardless of whether the table is being written to or not, use the `auto_compaction_period` attribute. It has two modes regulated by the `periodic_compaction_mode` attribute:

- `store` (default value): The decision to compact each chunk created earlier than `now - auto_compaction_period` is made independently.
- `partition`: If the partition contains at least one chunk created earlier than `now - auto_compaction_period`, all partition chunks are compacted at the same time.

To avoid synchronization and even out the load, a random shift is added to `auto_compaction_period` when calculating the compaction time of each specific chunk. This shift is defined by the `auto_compaction_period_splay_ratio` attribute.

Setting only the `periodic_compaction_mode` attribute is not enough to enable periodic compaction — you need to explicitly set the `auto_compaction_period` attribute.

#### Selecting a mode { #periodic_compaction_mode }
- **You need to delete data by TTL**: `store` mode (default).
- **You need to clear the rows deleted via delete-rows**: `partition` mode.

`Partition` mode is better suitable for clearing rows deleted via `delete-rows`, because in case of `store`, write by a key and the corresponding delete tombstone can get into different chunks which will be independently compacted one after another, and tombstone will never be deleted.

`Store` mode reviews chunks independently, and when the oldest chunk is reviewed, the obsolete data will be deleted.

#### Selecting a period { #periodic_compaction_period }
The longer the period is, the less load there is on the bundle. Each bundle node is capable of compacting about 100-200 MB/s maximum. The load from periodic compaction usually does not exceed several units of MB/s. The period is often commensurate with `max_data_ttl` or is several times less than it.

For example, there is a table of 500 GB and a compaction period of one day and two nodes in the bundle. The load per node would then be 500 GB/86,400 sec/2 ≃ 3.1 MB/s, which is allowable.

{% note warning "Warning" %}

Initially setting `auto_compaction_period` on the table with a lot of old chunks can cause all chunks to start being compacted at the same time. In this case, follow the same recommendations as for forced compaction.

{% endnote %}

## Scenarios { #scenarios }

#### Deleting data by TTL { #scenario_ttl }
**Scenario**: TTL is set for the table, but the size continues to increase as if no TTL is applied.

**Solution**: Use [periodic compaction](#periodic_compaction).

#### Deleting short-lived keys { #write_delete }
**Scenario**: The row is written and deleted after a while. It is required that delete tombstones do not pile up and space is cleared.

**Solution**: Use [periodic compaction](#periodic_compaction) in `partition` mode.

#### Many writes by one key { #woodpecker }
**Scenario**: There may be more than a few thousand writes by one key.

**Problem**: There is a limit on the number of versions of one key in the system. It is about 30,000 versions. If this is exceeded, background processes will end up with the "Too many write timestamps in a versioned row" error.

**Solution**: Set the `True` value of the `merge_rows_on_flush` attribute and reduce TTL via the `min_data_ttl` attribute so that the number of versions within TTL is not more that a few thousand. If many deletions are made per key, use `merge_deletions_on_flush`.

## Tablets and background processes under the hood { #tablet_inside }

This section details the structure of tablets. It's not mandatory for reading, but it may be helpful to those who want to tweak some of the settings on their own.

{% if audience == "internal" %}
To learn more, see page 73 onward in the [presentation](https://docs.yandex.ru/docs/view?url=ya-disk-public%3A%2F%2Fi847DzRWoKk%2Fi0Xox19Rn1ITfuoLOyZvivqdJcyuW%2Bc%3D&name=dyntables.pdf&nosw=1) [here](dynamic_tables_internals#2018:-arhitektura-dinamicheskih-tablic).
{% endif %}

### Structure { #tablet_description }

A tablet is a part of the table that is responsible for the data between two pivot keys. Typically, tablet size ranges from 100 MB (for in-memory tables) to a few gigabytes, sometimes reaching tens of gigabytes for especially large tables. From the master server's point of view, a tablet is a set of chunks. All operations described below occur on the tablet node directly serving the tablet.

Similarly to tables that are split into tablets bounded by pivot keys, tablets are split into partitions. The partition size is about 200 MB (compressed size). In addition, there is a special partition, Eden. Its boundaries coincide with those of the tablet. Each chunk belongs either to Eden or to one of the partitions. If a chunk falls entirely between the boundary keys of a particular partition, it belongs to it. Otherwise it belongs to Eden.

### Dynamic stores and flushing data to the disk { #flush }
There is always at least one dynamic store in the table — an active one. When writing, the data first gets into the dynamic store. When the dynamic store becomes too large (hundreds of megabytes) or the node runs out of tablet dynamic memory, the store is *rotated*. That is, it becomes passive and is replaced by a new active store. This activates the flush process, and the passive store is flushed to the disk over time.

If flush does not work for some reason, the data is accumulated in memory, and after overflow, writing starts failing with the "Node is out of tablet memory, all writes disabled" error.

By default, all row versions are saved to the chunk during flush. To apply cleanup by TTL, use the `merge_rows_on_flush` attribute. It should be done when the TTL is less than the typical dynamic store lifetime (about 15 minutes), and there are many writes by one key.

{% if audience == "internal" %}
Relevant graphs:
- [yt.tablet_node.store_flusher.dynamic_memory_usage](https://nda.ya.ru/t/TR0ubGeZ4s8eGT): Memory breakdown by category.
- [yt.tablet_node.store_rotator.rotation_count.rate](https://nda.ya.ru/t/xyqhxAMn4s8f2E): Rotation count breakdown by category and reason.
   - forced: Shortage of memory on the node.
   - overflow: Dynamic store size over the threshold.
   - periodic: Occurs with a frequency of `dynamic_store_auto_flush_period` unless triggered by another condition.
- [yt.tablet_node.store_rotator.rotated_memory_usage.max](https://nda.ya.ru/t/sEz1KuY94s8fVH): Dynamic store size at the time of rotation.
   {% endif %}

### Partitioning { #partitioning }

The chunk flushed to the disk ends up in Eden. It usually contains keys from the entire tablet range and can't be attributed to a particular partition. The partitioning process takes one or more chunks from Eden, merges them, splits the data into partitions, and places one chunk into each partition. If there are many small chunks in Eden, it will first run compaction and merge them.

Small chunks make the system less efficient. The first reason for this is overhead costs. The second reason is write amplification. Suppose a node serves several tablets of a large table. Since there are many tablets and the memory is shared, the dynamic store size in each tablet will be tens of megabytes, which is rather small. If there are 100 partitions in a tablet, the chunk size in each of them will be less than one megabyte after partitioning. If you increase the minimum allowable Eden size at which partitioning starts, larger chunks will be flushed to partitions at the cost of the increased overlapping store count.

### Compaction { #compaction }

The compaction process reads a batch of chunks in a single partition and merges them into one (or several, in rare cases), physically deleting rows and old versions. Compaction can be started for several reasons:

- forced: The `forced_compaction_revision` attribute is set on the table. In this case, all tablet chunks will be compacted and the batch size will only be limited by the `max_compaction_store_count` value.
- periodic: The `auto_compaction_period` attribute is set on the table. Chunks created earlier than `now - auto_compaction_period` will be compacted.
- regular: Regular mode started without any external influence.

#### Picking chunks based on size { #compaction_size_policy }

In regular mode, the system selects chunks taking into account write amplification. If there is a 100 MB chunk in the partition and a 1 MB chunk periodically appears, then if you compact them together each time, you will get x100 amplification. The following rules are used:

- There must be from `min_compaction_store_count` to `max_compaction_store_count` chunks in a batch, but the more the better.
- Chunks must be sorted by size. Each successive chunk must be by no more than `compaction_data_size_ratio` times larger than the sum of the sizes of the previous chunks.
- The previous rule does not apply as long as the total size of the chunks is less than `compaction_data_size_base`.

For example, ratio = 2, base = 16 MB. Then:

- A set of 1 KB, 1 MB, 10 MB chunks is allowed: the sum is not exceeding 16 MB.
- A set of 10 MB, 20 MB, 50 MB, 150 MB chunks is allowed: 50 < 2 × (10 + 20), 150 < 2 × (10 + 20 + 50).
- A set of 1 MB, 10 MB, 100 MB chunks is not allowed: 100 > 2 × (10 + 1).

Reducing the base and ratio values improves the WA at the cost of increasing the OSC. In the limiting case where base = 1 but no data is deleted, it can be demonstrated that the write amplification is logarithmically dependent on the amount of data. Each time a row participates in compaction, the size of the chunk that contains it increases by at least (1 + 1 / ratio) times. Consequently, this row participates in a total of no more than log(tablet_size, 1 + 1 / ratio) compactions. In practice, this estimation is inaccurate not only because of the deletions, but also because the partition splits into two when the threshold size is reached.

#### Mechanism for cleaning up old versions { #major_timestamp }

When compaction merges another batch of chunks, it can remove old versions of some rows and apply deletions. However, this may not always be possible, because compaction considers only some of the chunks within a partition, not all of them.

Let's consider a table with min/max_data_ttl = 0, min/max_data_versions = 1 (only the most recent version must be stored by each key). For example, there were two writes by a key: {value = 1; timestamp = 10}, {value = 2; timestamp = 20}, and deletion with timestamp = 30.
Suppose these versions are stored in three different chunks. If compaction considers only the first and third chunks, the following data is processed:

```
delete tombstone {timestamp = 30}
{value = 1; timestamp = 10}
```

If you apply deletions and remove the row completely without writing it to a new chunk, it will result in an incorrect read: further reads will read the {value = 2; timestamp = 20} value, although the row was deleted.

To avoid this problem, we calculate the *major timestamp*, which is the earliest timestamp across all data in all the chunks of a given partition and Eden to not be included in the current compaction batch. Compaction then has the right to delete only those versions where timestamp < major timestamp.

This logic can lead to obsolete versions not actually being deleted. First, if the tablet or partition is not written to, many chunks within the partition stabilize, and compaction no longer initiates since it has no way of detecting if any of the rows are aged. Second, if there is one large chunk in the partition and writing is not very intensive, then the newly appearing small chunks will be compacted with one another without affecting the large one. The large chunk imposes a limit on major timestamp, so even if ttl = 0, repeated versions in fresh chunks will not be deleted. You can deal with this using `auto_compaction_period`.
