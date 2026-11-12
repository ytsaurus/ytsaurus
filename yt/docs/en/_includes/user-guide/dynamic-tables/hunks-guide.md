# Hunks usage in dynamic tables

## Introduction

Hunk chunks are a mechanism for storing large string values separately from table rows. If a string value in a chunk exceeds a set limit, it is replaced by a light link that leads to a new type of chunk — a hunk chunk. Unlike regular chunks, hunk chunks store raw unstructured data: to read them, you just need to know the offset relative to the beginning of the chunk and the length. This construct opens up additional opportunities to optimize the use of dynamic tables:

- Enables you to store only a small part of the table in memory, preserving low lookup latency.
- Reduces write amplification, because hunk chunks are compacted less often than regular chunks.
- Reduces disk throughput for tables not stored in memory.
- Allows erasure coding with low overhead costs for on-the-fly hunk value erasure repair.

For more information about optimization, see [Use cases](#use-scenarios).

## When should I use hunks?

Hunks may prove useful for tables containing many large string values. However, they might not be the best choice if one of the following applies:
- If your table resides on an HDD, reading chunks from the disk involves many concurrent, fine-grained accesses to the medium — HDDs aren't exactly optimized for this.
- The table data has a good compression ratio (table attribute `compression_ratio` is quite low). Currently, blocks in hunk chunks are not compressed to allow granular reads of hunks from disk, so migrating to hunks may significantly increase the table's disk space usage. This is partially offset by setting a hunk erasure codec. You can also try dictionary compression of hunk values, which is currently available in test mode and should fully resolve this issue.


If the size of a string value does not exceed `max_inline_hunk_size`, the value is called `inline value`; otherwise, it is called `ref value`. Hunk chunks consist of `ref values`.

To roughly estimate the share of `ref values` in your table accounting for a varying `max_inline_hunk_size`, you can use a script: `yt/yt/tools/hunk_chunk_advisor/hunk_chunk_advisor compute_weight`.

{% cut "Example" %}

```bash
./hunk_chunk_advisor compute_weight --proxy zeno --table-path //home/table --sampling-rate 0.1 --result-file result_file --computation-path //home/hunk_chunk_advisor
```

Then, it will compute the ratio of data that constitute `ref values` for different `max_inline_hunk_size`.

| Script parameter | Description |
| --- | --- |
| `proxy` | {{product-name}} cluster name. |
| `table-path` | Cypress path to the table. |
| `sampling-rate` | The share of table rows for calculating statistics (required if the table is too large to run the operation on it). |
| `result-file` | Local path to where you want to create a PNG file with detailed statistics. |
| `computation-path` | Cypress path to the directory where the tables with intermediate calculations will be located. We recommend creating a new subdirectory within the project directory to avoid violating the ACL and cluttering the working directory. |

{% endcut %}

## Enabling hunks

To enable hunks, you need to change the table schema by setting the `max_inline_hunk_size` attribute on the desired string columns. To modify the schema, the table needs to be unmounted.

{#slim_format}

Using the alternative chunk format is also recommended. The default chunk format (`@optimize_for=lookup`) can be inefficient in terms of uncompressed data size. You should either select the slim chunk format (`@optimize_for=lookup` and `@chunk_format=table_versioned_slim`) or the scan format (`@optimize_for=scan` and `@chunk_format=table_versioned_columnar`). When using the scan format, you should also set the same column group for all columns in the schema. For more information about column groups, see the [documentation](../storage/chunks#columns).

To enable hunks on your table, you can use the script located at `yt/yt/experiments/public/hunkifier`.

{% cut "More about hunkifier" %}

Example:

```bash
./hunkifier --proxy zeno  --table-path //home/replica_table --max-inline-hunk-size 128`
```

The script unmounts the table, alters its schema by adding the necessary attribute, sets additional settings, and then re-mounts the table.

Basic parameters:

| Script parameter | Description |
| --- | --- |
| `proxy` | {{product-name}} cluster name. |
| `table-path` | Cypress path to the table. |
| `max-inline-hunk-size` | Maximum size of a string value in bytes. |

Additional parameters:
| Script parameter | Description |
| --- | --- |
| `primary-medium` | A medium for storing the chunks (regular and hunk). |
| `enable-crp` | A flag for enabling [consistent replica placement](#consistent-replica-placement) on the table. |
| `max-hunk-compaction-garbage-ratio` | The [max_hunk_compaction_garbage_ratio](#max_hunk_compaction_garbage_ratio) attribute. |
| `hunk-erasure-codec` | The [erasure codec](#hunk_erasure_codec) for hunk chunks. |
| `fragment-read-hedging-delay` | Fixed delay before hedging fragment reads. The delay before sending a [hedged request](#hunk_hedging) for fragment reads. |
| `enable-slim-format` | A flag for enabling the [slim chunk format](#slim_format) on the table. |
| `enable-columnar-format` | Use the column-by-column chunk format on the table. |
| `columns` | A list of columns the `max_inline_hunk_size` attribute will be set on. If the option is not specified, the attribute is set on all string columns of the table. |

{% endcut %}


Once hunks are enabled and the table is mounted, flush and compaction will also generate hunk chunks with `ref values` along with regular chunks.

To quickly change the structure of all chunks in the table, you can perform a forced compaction taking into account all the recommendations described in the [documentation](./compaction#forced_compaction). You can also use the `forced_store_compaction_revision` and `forced_hunk_compaction_revision` options for forced compaction of regular-only or hunk-only chunks.

{% note info %}

The `forced_hunk_compaction_revision` option does not compact all of the hunk chunks: the compaction algorithm unconditionally compacts only those hunk chunks that are referenced by regular chunks falling under the compaction criteria.

{% endnote %}

## Use cases {#use-scenarios}

There are several use cases where various hunk table configurations may prove useful. Each configuration is set by a specific set of options with their own trade-offs.

- <b>An in-memory table takes up a lot of tablet static memory.</b>
    In such cases, having a low latency for table reads is often critical. If you put a table with enabled hunks in memory, only non-hunk chunks will end up there. This can save you a lot of tablet static memory without significantly increasing the latency of read requests.

    If a user reads a row that does not have `ref values`, reading will still be performed from memory. If the row does have `ref values`, reading them will require additional resources (to reach the data node and read these values from the disk). Accordingly, decreasing the `max_inline_hunk_size` will also reduce the number of memory reads and the amount of tablet static memory required.

    {% note info %}

    High-load tables may require additional optimization efforts, so we recommend reading the [Further optimization](#optimization) section below.

    {% endnote %}

{#max_hunk_compaction_garbage_ratio}

- <b>Table writes cause high write amplification.</b>
    As you may know, the LSM tree data organization of a dynamic table implies that the write throughput to the disk is higher than the initial user write throughput. High flow and write amplification can affect the number of disks required to maintain the table (with DWPD, the allowed write throughput to the disk ends up being relatively small).

    When regular chunks that reference hunk chunks are compacted, it's not mandatory to read and modify the latter. Because of that, after compaction hunk chunks may partially consist of outdated values if some of the `ref values` a left without references. This makes it possible to skip reading and overwriting `ref values`, thereby reducing the read and write throughput during compaction.

    The option that adjusts the maximum allowable percentage of garbage in hunk chunks is `max_hunk_compaction_garbage_ratio`. The higher this value, the lower the write amplification, but the higher the table's disk space (because hunk chunks will contain more outdated values).

- <b>Table reads cause high disk load.</b>
    When reading regular chunks from the disk, you have to read entire blocks that usually take up a few hundred kilobytes. Large flows may overload the disks, and the block cache may be underutilized. Hunks are read with a granularity of 4 KB.

- <b>The table disk space is too high.</b>

    If some parts of regular chunks are overloaded or unavailable, erasure recovery may be inefficient and resource-intensive. Optimizations in the hunk chunk read layer make this process less disruptive. You can enable erasure coding for hunk chunks by setting the `@hunk_erasure_codec` attribute on the table. To enable on-the-fly part recovery, see [Further optimization](#optimization). After changing the table's erasure codec, you can use the table's `@erasure_statistics` attribute to determine how many chunks were written with each codec.

{#hunk_erasure_codec}

## Further optimization {#optimization}

From the Data Node's perspective, `ref values` reads become fragment reads, so this section uses the latter term.

{#hunk_hedging}

- <b>Hedged requests for fragment reads or on-the-fly erasure recovery.</b>
    If a read request from a Data Node takes too long, it can be hedged. We recommend setting the desired ratio of hedging requests to primary requests in the table's `@hunk_chunk_reader/hedging_manager/secondary_request_ratio` attribute. The hedging delay is selected automatically.

    This setting also applies to erasure-coded hunks: if a Data Node does not respond in time, hedging initiates data recovery from other parts. For regular chunk reads, a similar setting is configured separately in `@chunk_reader/hedging_manager/secondary_request_ratio`.

- <b>Fragment read request batching.</b>
    By default, three random data nodes are selected for each table chunk to store its replicas. Even a single key lookup tends to generate reads from multiple chunks, not to mention lookups of multiple keys. Since reading each `ref value` is reading from the disk from a remote data node, there is a high fan-out of requests to the storage subsystem. This can be performance-critical if the flow of requests to the table is rather high. In this case, you can set the `@enable_consistent_chunk_replica_placement` boolean attribute on the table.

    This will make it so that chunk replicas of a single tablet will be assigned to the same data nodes in best-effort mode. Fragment read requests to data nodes are batched. And disk requests are batched on data nodes. In some cases, this substantially reduces the fan-out.

    {% note info %}

    This option can significantly affect the storage layer due to a particular chunk replication load. Set it only after consulting with the {{product-name}} team.

    {% endnote %}

{#consistent-replica-placement}

- <b>More efficient disk reads.</b>
    Fragment reads cause many granular disk reads. To keep them efficient and low-latency, use a medium with io_uring to store the table's chunks. You can also bypass the OS cache by using direct I/O. To do this, set the Boolean `@hunk_chunk_reader/use_direct_io` attribute on the table. On some media, direct I/O reads are disabled globally, in which case this attribute has no effect.

## Diagnostics

To determine the number of hunk chunks, the amount of data referenced by hunk links, and disk space usage, use the table's `@hunk_reference_statistics` attribute:

```bash
yt get "<table_cypress_path>/@hunk_reference_statistics"
```

This attribute uses precomputed statistics and does not require scanning all chunks.

| Field | Description |
| --- | --- |
| `hunk_chunk_count` | The number of unique sealed hunk chunks in the table. Each shared chunk is counted once. |
| `local_referenced_data_weight` | The total length of the hunks referenced by this table's chunks. |
| `local_referenced_data_size` | The size of the same data, including hunk headers. |
| `local_referenced_regular_disk_space`, `local_referenced_erasure_disk_space` | Disk space corresponding to this table's references, reported separately for regular and erasure-coded hunk chunks. |
| `global_referenced_regular_disk_space`, `global_referenced_erasure_disk_space` | Disk space corresponding to all references to this table's hunk chunks, including references from other tables (relevant when using hunk storage). |
| `regular_disk_space`, `erasure_disk_space` | Total disk space occupied by this table's hunk chunks. Each shared chunk is counted once. |

Note: When journal hunk chunks (hunk storage) are used, only sealed hunk chunks are included in the statistics. This does not apply to `local_referenced_*` statistics because they are derived from the table's regular chunks.

Note: The `@hunk_statistics` and `@chunk_format_statistics` attributes for calculating hunk statistics are deprecated. Use `@hunk_reference_statistics` instead. It is much easier to calculate and deduplicates shared hunk chunks.

Chunks also have several useful attributes:

- All chunks: `@consistent_replica_placement_hash` and `@consistent_replica_placement`.
- Regular chunks: `@hunk_chunk_refs`.
- Hunk chunks: `@hunk_count` and `@total_hunk_length`.

For more information about hunk chunk refs, see the node's Orchid: `tablet_cells/<cell_id>/tablets/<tablet_id>/hunk_chunks/<chunk_id>`

### How can I determine the actual fraction of disk space occupied by garbage in hunk chunks?

To reduce write amplification during compaction, hunk chunks may accumulate garbage. Its relative amount is controlled by the `max_hunk_compaction_garbage_ratio` parameter. However, this parameter cannot be used to accurately calculate the amount of garbage in hunk chunks. The situation is further complicated when hunk chunks are referenced by different tablets or even tables, as expected when using hunk storage.

To accurately calculate the size and fraction of garbage, read the table's `@hunk_reference_statistics` attribute and perform the following calculations:

```text
total_disk_space = regular_disk_space + erasure_disk_space
referenced_disk_space = global_referenced_regular_disk_space + global_referenced_erasure_disk_space
garbage_disk_space = total_disk_space - referenced_disk_space
```

In this case, the estimated garbage fraction is `garbage_disk_space / total_disk_space`.

## FAQ {#faq}

#### What should I do if disk space usage increases significantly after enabling hunks?

You can adjust the `@max_hunk_compaction_garbage_ratio` table attribute. When this threshold is exceeded, the hunk chunk is compacted. The default value is `0.5`. For some tables, you may want to change it to `0.15` or `0.3` depending on the load. The downside is that decreasing this value increases compaction write amplification.

#### What should I do if the chunk count increases significantly after enabling hunks?

When the dynamic store gets flushed to disk, this can create many small chunks. In this case, you can adjust the `@max_hunk_compaction_size` table attribute. All hunk chunks below the specified value will be compacted. The default value is `8MB`. For some tables, depending on the workload, it may be useful to set `16MB` to `32MB`. The downside is that increasing this value also increases the compaction write amplification.


