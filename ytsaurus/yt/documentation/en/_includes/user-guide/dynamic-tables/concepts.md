# Concepts

## Tablets { #tablets }

For each table, the key space is divided by a set of boundary keys into non-overlapping ranges — tablets. Table tablets are listed in the `tablets` attribute. Each tablet is described by the `TabletInfo` structure:

| **Name** | **Type** | **Description** | **Mandatory** |
| -------- | --------- | -------------- | -----------------|
| tablet_id | Guid | Tablet ID | Yes |
| statistics | TabletStatistics | Tablet statistics | Yes |
| state | String | State (mounted/unmounted/frozen/mounting/unmounting/freezing/unfreezing/frozen_mounting) | Yes |
| pivot_key | List | Boundary key where the tablet starts (for sorted tables only) | Yes |
| cell_id | Guid | ID of the cell to which the tablet is mounted | No |

{% cut "Example of tablet statistics" %}

```json
{
    "chunk_count" = 1;
    "compressed_data_size" = 324;
    "disk_space" = 2502;
    "disk_space_per_medium" = {
        "default" = 2502;
    };
    "dynamic_memory_pool_size" = 0;
    "hunk_compressed_data_size" = 0;
    "hunk_uncompressed_data_size" = 0;
    "memory_size" = 0;
    "overlapping_store_count" = 1;
    "partition_count" = 1;
    "preload_completed_store_count" = 0;
    "preload_failed_store_count" = 0;
    "preload_pending_store_count" = 0;
    "store_count" = 1;
    "tablet_count" = 1;
    "tablet_count_per_memory_mode" = {
        "none" = 1;
        "compressed" = 0;
        "uncompressed" = 0;
    };
    "uncompressed_data_size" = 538;
    "unmerged_row_count" = 2;
}
```

{% endcut %}

Each tablet logically consists of a set of sorted data chunks and a special memory area called `dynamic store`. Chunks are stored in a replicated way in a blob repository ^[storage of large amounts of unstructured data] and overlap in key ranges.  When data is written, it goes into the `dynamic store` and is stored there. When the data reaches the amounts specified in the configuration, it is written to the disk as a chunk. When the number of chunks within a tablet becomes large enough, some of them are combined — compaction is performed. At this moment, old and irrelevant data may be deleted. For more information, see [Compaction](../../../user-guide/dynamic-tables/compaction.md).

A list of all tablets of all dynamic system tables is available at `//sys/tablets`.

## Boundary keys { #keys }

To change a table tablet set, use the `reshard_table` command. When changing the number of tables of sorted dynamic tables, boundary keys must be specified. Tablet boundary keys are random sequences of values, not necessarily of the same length as the key columns.

One of the options is to take a hash of some subsequent key components in order to distribute the data evenly across the key space. Then the boundary keys can be sequences of length 1, consisting of numbers that evenly divide the entire interval of hash function values.

The boundary key of the first table tablet always equals the empty sequence — effective minus infinity — and cannot be changed.

## Partitions { #partitions }

Tables in dynamic tables are divided into partitions to ensure parallel processing of queries within the cluster node. A typical partition size is several hundred megabytes. If the chunks become larger than the partition size, a background process is started that forces the chunks to be split into smaller chunks.

## Tablet cells { #tablet_cells }

Tablets are maintained by entities called tablet cells. Each cell is a complex automaton whose state is replicated by the `Hydra` subsystem. Each cluster node has so-called tablet slots — virtual positions into which cells can fall.

Cells in the cluster are created by the administrator during initial setup. The total number of cells must be close to the total number of tablet slots on the cluster nodes, but with a certain margin to account for the possibility of a certain number of cluster nodes failing.

Each tablet cell belongs to a certain bundle. The name of the bundle in which the cell is created is specified in the `tablet_cell_bundle` attribute of a specific table or one of the parent directories. By default, tablet cells are created in the `default` bundle.

A list of all cells is available at `//sys/tablet_cells`. Each cell maintains changelogs (mutation logs) and sometimes records state snapshots. The logs go into the `//sys/tablet_cells/<cell_id>/changelogs` directory and the snapshots go into the `//sys/tablet_cells/<cell_id>/snapshots` directory as files.

The master server automatically selects a specific cluster node and an available tablet slot on that node to serve the tablet cell. If the cluster node serving the cell fails, after some time the master server will select a free slot on another node and start the cell on that node.

Tablet cells have the attributes indicated in the table:

| **Name** | **Type** | **Description** |
| ---------- | ---------- | ------------------ |
| id | Guid | Cell ID. |
| peers | Dict | Dict containing the name of the cluster node serving the cell and the status of the cell. |
| tablet_cell_bundle | String | Name of the tablet cell bundle to which the cell belongs. |
| status | Dict | Cell state, decommission flag. |
| total_statistics | Dict | Various tablet cell statistics. |

{% cut "Cell state example" %}

```json
{
    "health" = "good";
    "decommissioned" = %false;
}
```

{% endcut %}

## Tablet cell bundles { #tablet_cell_bundles }

Cells are usually created in groups with the same settings. Bundles are needed to combine cells on the basis of common settings. In the current implementation, bundles are the only reliable way to isolate the load, provided that the bundles are properly distributed across the cluster nodes.

Bundles are identified by string names. The list of bundles in the system is available at `//sys/tablet_cell_bundles`. In the system, there is always a built-in and non-removable bundle with the `default` name. Bundle settings are specified in the `options` attribute when it is created and can also be changed later. Changing the settings in the `options` attribute requires unmounting all the bundle tables and recreating all the cells.

Each table, including a static one, has the `tablet_cell_bundle` attribute where the name of the bundle to which cells this table will be mounted is stored. For more information about mounting, see [Table mounting](../../../user-guide/dynamic-tables/operations.md#mount_table).

Each bundle has the `node_tag_filter` attribute which is used to select the cluster nodes for tablet cells in the given bundle. The filter is a logical expression with the operators: `&` — logical "and", `|` — logical "or", `!` — logical "no" and round brackets.
For example, `%foo | (bar & !baz)`.
The variables in the expression are the possible tag values. If the cluster node has the specified tag, the variable in the formula takes the `true` value; if not, it takes the `false` value. Tablet cells will only fall onto those cluster nodes for which the entire expression takes the `true` value.

Tablet cell bundles have the attributes indicated in the table:

| **Name** | **Type** | **Description** |
| ------------------- | ------------------- | -------------------------------------- |
| name | String | Bundle name. |
| options | TabletCellOptions | Settings of all tablets of this bundle. |
| tablet_cell_count | Int | Number of cells in a bundle. |
| tablet_cell_ids[*](**) | Guid | IDs of cells in a bundle. |

The state of the bundle, the state of the tablet cells, and the list of cluster nodes included in the bundle are displayed on the Tablet cells tab.

[**]: The parameter appears in the answer several times.
