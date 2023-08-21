# Chunks

This section contains information about chunks — parts of a table or file that store data.

## General information { #common }

Data stored in [tables](../../../user-guide/storage/static-tables.md), [files](../../../user-guide/storage/files.md), and logs is split into parts that are called chunks. A chunk stores a continuous range of data.
Specifically, each chunk of the table contains a range of its rows.
A chunk is an unmodifiable entity: once a chunk is created, the data in it cannot be changed.

The chunk data is physically stored on the cluster nodes. {{product-name}} master servers monitor the location of the chunks and keep them in [replicated](../../../user-guide/storage/replication.md) state.

{{product-name}} master servers store only metadata. However, metadata has a significant size, so in order to limit the memory consumption by the master servers, the number of chunks available to the account is [quoted](../../../user-guide/storage/quotas.md).

### Chunk size { #chunk-size }

Chunks are not monolithic, they consist of blocks so that you can read and write chunks in parts.
By default, static table blocks are 16 megabytes and dynamic table blocks are 256 kilobytes.

You can change the block size in the [`table_writer`](../../../user-guide/storage/io-configuration.md) settings, the `block_size` attribute.

A chunk usually occupies hundreds of megabytes or several gigabytes on a cluster node.
The size of the chunks to be created can be configured via the [`table_writer`](../../../user-guide/storage/io-configuration.md) section, the `desired_chunk_size` attribute.


We do not recommend making chunks too small. This leads to an increase in their number, which consumes the master server memory. Small chunks also slow down reading: the number of requests to the master and the disk that need to be made to read all the data increases.

### Reading data { #reading }

When the client calls the procedure for reading data from a static table, the following steps are performed on the proxy server:

1. Getting a list of chunks that make up the table from the master server.
2. Getting a list of cluster nodes where chunk replicas are located from the master server.
3. Downloading data from cluster nodes and transmitting it to the client.

### Writing data { #writing }

Writing is performed in a similar way:

1. Splitting the input data stream into parts.
2. Designing each part in the form of a chunk.
3. Transferring data to cluster nodes and binding it to a table.


## Chunk format { #optimize_for }

There are two chunk formats in {{product-name}}: row-by-row and column-by-column.
The chunk format is set by the `optimize_for` parameter. For row-by-row format, `optimize_for` has the `lookup` value, for column-by-column format — the `scan` value.

### Row-by-row format: optimize_for=lookup { #lines }

In row-by-row format, each row is stored entirely in one block.
The row-by-row format is suitable for [dynamic tables](../../../user-guide/dynamic-tables/overview.md) from which data is selected by key.

### Column-by-column format: optimize_for=scan { #columns }

The column-by-column format does not store information about the type next to each value and uses lightweight column compression techniques.
By default, the data of each column specified in the schema gets into a separate sequence of blocks within a chunk. This speeds up the reading of a small number of columns.

If you need to read one row or a small range of rows by key, you have to make as many disk accesses as there are columns in the table. To minimize disk accesses, you can use the `group` attribute in the description of columns in the schema, which will allow you to store related data in one block. For more information, see the [Data schema](../../../user-guide/storage/static-schema.md) section.

{% note info "Note" %}

Using a column-by-column format without a schema makes no sense.

{% endnote %}

### Example of specifying a format { #format-example }

CLI
```bash
yt set //tmp/table/@optimize_for scan
```

Setting the attribute affects only future chunks and does not convert tables (similar to setting the `@erasure_codec` and `@compression_codec` attributes).

To convert already written data into a new format, perform the Merge operation:

CLI
```bash
yt merge --mode ordered --src //tmp/table --dst //tmp/table --spec '{force_transform = %true}'
```

## Chunk owners { #attributes }

Cypress nodes consisting of chunks — tables, files, and logs — are called chunk owners.
A set of chunks belonging to the chunk owners is organized as a tree-like data structure. The structure leaves are chunks and the intermediate nodes are chunk lists (`chunk_list`).

Besides the attributes inherent to all Cypress nodes, chunk owners have the following additional attributes:

| **Name** | **Type** | **Description** | **Mandatory** |
| ------------------------- | ----------------------- | ------------------------------------------------------------ | ---------------- |
| `chunk_list_id` | `Guid` | Root chunk list ID. | Yes |
| `chunk_ids` | `array<Guid>` | List of IDs of all chunks. |
| `chunk_count` | `int` | Number of chunks. |
| `compression_statistics` | `CompressionStatistics` | Statistics on the types of used [compression codecs](../../../user-guide/storage/compression.md) and data sizes. | Yes |
| `erasure_statistics` | `ErasureStatistics` | Statistics on the types of used [erasure codecs](../../../user-guide/storage/replication.md) and data sizes. | Yes |
| `optimize_for_statistics` | `OptimizeForStatistics` | Statistics on the types of used [chunk types](#optimize_for) (`optimize_for`). | Yes |
| `multicell_statistics` | `MulticellStatistics` | Statistics on the distribution of chunks on master servers. | Yes |
| `uncompressed_data_size` | `int` | Total uncompressed data volume of all chunks (not including replication and erasure coding). | Yes |
| `compressed_data_size` | `int` | Total compressed data volume of all chunks (not including replication and erasure coding). | Yes |
| `compression_ratio` | `double` | Compression ratio, the ratio of compressed volume to uncompressed volume. | Yes |
| `update_mode` | `string` | The way to change data under a transaction: `none`, `append`, or `overwrite`. For nodes outside a transaction, it is `none`. | Yes |
| `replication_factor` | `integer` | [Replication](../../../user-guide/storage/replication.md) factor (equals 1 for erasure coding). | Yes |
| `compression_codec` | `string` | [Compression codec](../../../user-guide/storage/compression.md) name. | Yes |
| `erasure_codec` | `string` | [Erasure codec](../../../user-guide/storage/replication.md) name. | Yes |
| `vital` | `bool` | Whether the chunks of this node are [vital](#vitality) | No |
| `media` | `Media` | Replication factors and other [medium](../../../user-guide/storage/media.md) settings. | Yes |
| `primary_medium` | `string` | [Primary medium](../../../user-guide/storage/media.md#primary). | Yes |

## Data vitality { #vitality }

The {{product-name}} system divides chunks into two types: vital and non-vital. By default, data in the system is vital (the `vital` attribute is `true`) and its loss is a serious incident. Data loss can occur due to failure of cluster nodes.

Non-vital data is data whose loss is not critical. {{product-name}} cluster settings are such that inaccessibility of non-vital chunks does not trigger monitorings and exploitation intervention. Typical examples of non-vital data are intermediate results of operations: the scheduler will notice losses and perform recomputation of stderr jobs. For non-vital data, the `vital` attribute is `false`.

{% note info "Note" %}

The system does not consider chunks written without erasure coding and having a single replication factor to be vital.

{% endnote %}

Data may be lost when cluster nodes fail. To avoid this, three copies of data are stored in {{product-name}} by default.
However, if multiple hosts fail, the system may lose access to all copies. If computations were interrupted when vital data was lost, restart them.







