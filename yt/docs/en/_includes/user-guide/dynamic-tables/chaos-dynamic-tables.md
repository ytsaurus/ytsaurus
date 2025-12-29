## Chaos replicated tables

Chaos replicated dynamic tables in {{product-name}} are functionally similar to regular [replicated tables](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md) but are based on the new *chaos replication* protocol. Much like regular replicated tables, they allow multiple copies of the same table to be stored across different clusters updated in real time. These copies are strictly consistent with each other and together represent a continuously available dynamic table with full transaction support.

Compared to regular replicated tables, chaos dynamic tables are always available for both reading and writing, thanks to the chaos replication protocol. Chaos replication has the following distinct features:
- Each cluster must contain a special type of replica: a replication queue.
- Replication is performed using a pull model, where any available queue with sufficiently recent data can serve as a replication source.
- Replica status data is stored in the form of a *replication card* on separate *chaos metadata* clusters.
- To ensure compatibility with regular replicated tables, chaos replication relies on the special `chaos_replicated_table` object. Unlike a `replicated_table`, it doesn't contain any data: it's simply a proxy node for chaos metadata and replicas.
- Chaos metadata is stored in a dedicated chaos cell bundle, and the `chaos_replicated_table` is linked to this bundle.

Chaos replicated dynamic tables can be both [sorted](../../../user-guide/dynamic-tables/overview.md) and [ordered](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md). For ordered dynamic tables, each replica is used both to hold data and as a replication queue for other replicas. For sorted dynamic tables, replicas containing data and replicas serving as replication queues are fundamentally different and have distinct types.


## Creating a chaos replicated dynamic table

This section describes the steps for quickly creating a chaos replicated dynamic table and highlights their main interface differences compared to regular replicated tables. For more in-depth information about chaos dynamic tables, read the sections that follow.

1. Creating a chaos dynamic table. First, create a `chaos_replicated_table` object. Even though it only functions as a proxy, it provides a convenient entry point that greatly simplifies working with chaos replicated dynamic tables. Furthermore, for any operation, `chaos_replicated_table` requires only read access to the cluster on which it resides. It remains available during updates performed in read access mode.

When creating this object, make sure to specify the `chaos_cell_bundle` attribute, which defines the chaos bundle to link the table to and hold all necessary metadata. In addition, we recommend specifying the `schema` attribute: when it's present, you can specify the `chaos_replicated_table` directly for selects and lookups.

```bash
yt create chaos_replicated_table //path/to/replicated_table --attr '{
  chaos_cell_bundle=...;
  schema=[...];
}'
```

2. Creating a replica with data. This replica stores the data in regular format. If you unlink this replica from the replication, it becomes a regular dynamic table and retains all its data. To link a replica table to a `chaos_replicated_table`, first create a `chaos_table_replica` object, which is part of the metadata describing their relationship. It's a complete analog of the `table_replica` object used for replicated dynamic tables. When creating a `chaos_table_replica`, make sure to specify the `chaos_replicated_table` it belongs to and the replica table it describes. Note the `"content_type" = "data"`: this indicates that the replica contains data and can't be used as a replication source. To create a replica of an ordered dynamic table, specify `"content_type" = "queue"`: this way, the replica will both hold data and serve as a replication source.

```bash
$ yt create chaos_table_replica --attr '
{
     "table_path" = "//path/to/replicated_table";
     "cluster_name" = replica_cluster;
     "replica_path" = "//path/to/data";
     "content_type" = "data";
     "mode" = "sync";
     "enabled" = %true;
}'
1-2-3-4
```

The operation that creates the `chaos_table_replica` returns the ID of the newly created replica. You'll need it as the `upstream_replica_id` for the replica table. You can either specify the `upstream_replica_id` when creating the table or change it later using `alter_table`.

```bash
$ yt --proxy replica_cluster create table //path/to/data --attr '{dynamic=%true; schema=[...]; upstream_replica_id="1-2-3-4"}'
# Or
$ yt --proxy replica_cluster alter-table --upstream-replica-id 1-2-3-4 //path/to/data
```

3. Creating a replica with a replication queue (only for sorted chaos dynamic tables). This step is similar to creating a replica with data, but you need to specify `"content_type" = "queue"` when creating the `chaos_table_replica` and use the `replicated_log_table` type.

When creating a queue, you can specify `pivot_keys` to split it into multiple tablets. Sharding can only be done once, when you're create the queue. If you end up needing more shards later, you'll have to create a new replication queue and delete the old one (chaos replication allows performing these operations on live tables).

```bash
$ yt create chaos_table_replica --attr '
{
     "table_path" = "//path/to/replicated_table";
     "cluster_name" = replica_cluster;
     "replica_path" = "//path/to/replication_queue";
     "content_type" = "queue";
     "mode" = "sync";
     "enabled" = %true;
}'
5-6-7-8
$ yt --proxy replica_cluster create replication_log_table //path/to/replication_queue --attr '{dynamic=%true; schema=[...]; upstream_replica_id="5-6-7-8"}'
```

## Working with chaos replicated dynamic tables

The simplest approach is to always use `chaos_replicated_table` for both writes and reads. This ensures that all transactional guarantees are maintained.

However, chaos replication also supports accessing the entire chaos replicated table through any replica:
- When writing to a `chaos_replicated_table` or any of its replicas, the changes apply across the entire chaos replicated table according to its current state. For example, when formally writing to an asynchronous replica, the data in the transaction is actually written only to synchronous replicas.
- Reads from a replica retrieve data only from that replica. Note that this behavior may change in the future.
- Reads from a replica with the `replica_consistency=sync` option retrieve data from a synchronous replica (or a sufficiently recent asynchronous replica, not necessarily the one specified in the operation).
- Reads from a `chaos_replicated_table` are always performed in `replica_consistency=sync` mode, meaning they only return data from a sufficiently recent replica.

## Adding a new replica

When working with a table over an extended period of time, you may occasionally need to add a new replica: for example, in order to move it to a new cluster or to simply recreate an existing replica. You can add a new replica using the familiar `chaos_table_replica` command. However, executing it on a table that has been running for a while will most likely lead to issues: replication to the newly created replica might fail, and the buildup of data in queues could potentially cause problems across the entire bundle. This happens because the replication history is regularly cleaned up, and the original data has long since been deleted.

There are several ways to add a new replica that don't require replication of lifetime data:
- Specify `catchup=%false` when creating the replica. In this case, it's assumed that the replica doesn't need the history, so it receives data only starting from a specific timestamp after the table's creation. For a replica with data, only the latest records are replicated, meaning its contents will differ from other replicas. However, `catchup=%false` mode may be a reasonable default approach for adding a new replication queue: typically, replication queues store only a small portion of recent data that hasn't yet been propagated to other replicas.
- Specify a `replication_progress` when creating the replica. In this case, replication to the replica starts at the specified [progress point](#progress-replikacii). With this method, you can explicitly control the starting point for data replication. For more information about replication progress, read below.
- Specify the progress on the replica table. Similar to the previous approach, replication continues from the specified progress point. This is the recommended method because the progress is explicitly stored as an attribute of the table. **Note:** the progress on the table must be greater than or equal to the value specified in the `chaos_table_replica`. If the table's progress suddenly drops below this value, replication stops.

When adding a new replica with an explicitly specified progress, make sure that your current replication queues contain the necessary data. You can usually achieve this by unmounting an existing replica with data. This ensures that data can no longer be replicated to this table and will start to accumulate in the queues.

### Copying an existing replica

Let's take a closer look at a common scenario when you need to copy an existing replica with a data to a new cluster. In this case, we need the new replica to be completely identical to the existing one and to initiate replication to it. You can achieve this by following these steps:

1. Unmount the donor replica. We recommend switching it to asynchronous mode before doing this and wait till status becomes `unmounted`. Unmounting is necessary to record the replication progress.
2. Read the progress from the `@replication_progress` attribute of the donor replica table and remember it.
3. Create a new replica using the `create chaos_table_replica` command and specify the previous step progress. This will save the data in the replication queues until the replica starts working. You need to remember the resulting ID for later substitution into `upstream_replica_id`.
4. Copy the donor replica to a new cluster. This is a potentially time-consuming step and can be optimized: first, copy the replica locally (using the `yt copy` command) — this will quickly create the necessary data snapshot, then copy it in the background and move on to the next step. When copying to another cluster, you need to do a remote copy for the dynamic table — this will preserve all timestamps (if you convert the table to static during copying, the timestamp information will be lost, which may lead to data loss after replication is enabled).
5. Mount the donor replica. Replication will go to it, but the data in the copy will no longer change.
6. After copying is complete, set the correct `replication_progress` and `upstream_replica_id` on the new table. (Although a newly created replica with an empty progress will pull the progress from the card, it is always worth explicitly setting the progress on the replica table at this step — this way you can be sure that it will be exactly what is expected.)
7. Mount the new replica. It should update the progress in the replication card according to the specified value and begin replication.

This is how you can copy a replica with data for a sorted chaos dynamic table. You cannot copy a queue replica, but you can simply create a new one by specifying `catchup=%false` — replication of subsequent records will use the new queue replica as well.

### Moving replicas

You can move replicas to a new location while updating the path in the replication card. Since this operation is not atomic, it is recommended to switch the replica being moved to async mode in advance to avoid downtime.

1. `yt unmount-table <old path>`
2. `yt move <old path> <new path>`
3. `yt alter-table-replica --params '{replica_path="<new path>"}' <replica_id>`
4. `yt mount-table <new path>`

### Resharding of queue replicas

Queue replicas are queues, so you cannot directly change the number of tablets in such replicas. Resharding of queue replicas is only possible through recreating them. This can be done in the following way:

1. Create new queue replicas with the desired number of shards in sync mode and specify `catchup=%false` and `replicated_table_tracker_enabled=%false` with a new path. The latter is necessary to prevent all new replicas from becoming async.
2. Make sure that writing has started in them: the replica lag in the interface should remain close to zero, and the `replication_lag_time` attribute should increase.
3. Generate a timestamp `yt generate-timestamp` and remember it.
4. Wait until the `replication_lag_timestamp` value of _all_ replicas is greater than the timestamp generated in the previous step. You can view the `replication_lag_timestamp` of table replicas using the command:
```bash
$ yt get "<path to chaos_replicated_table>/@replicas" | grep replication_lag_timestamp
```
5. Delete the old queue replicas.
6. If necessary, move the new replicas to the place of the old replicas.
7. If necessary, enable automatic replica switching via `alter-table-replica`.

### Creating a new replica for an ordered dynamic table

Replicas of an ordered dynamic table are queues, and the simplest way is to create a new replica by specifying `catchup=%false`, which will create a replica with working replication. However, in such a replica, row numbers will differ from those in other replicas, which may lead to unexpected results when reading, especially if `$row_index` is specified explicitly.

To ensure that rows in the new replica have the same numbers as in previous replicas, you need to specify explicit [offsets](../../../user-guide/dynamic-tables/ordered-dynamic-tables#creating) and progress corresponding to these offsets when creating the replica.

Here is a way to do it:

1. Unmount the donor replica and wait for the `unmounted` status.
2. Save the replication progress (from the `@replication_progress` attribute of the replica table) and use it both when creating the replica (pass it in the attribute when creating `chaos_table_replica`) and set it explicitly on the new replica table (using `alter-table`).
3. For each tablet of the donor replica, get the number of rows in it — to do this, you need to iterate through the list in the `/@tablets` attribute and collect the `flushed_row_count` fields. Important: the donor replica must still be unmounted.
4. Create a new replica, specifying the saved progress for it.
5. After the new replica is created, you can mount the donor replica back.
6. Create a new replica table, specifying the number of tablets and the required offsets during creation using the `tablet_count` and `trimmed_row_counts` attributes. You also need to specify `upstream_replica_id` and `replication_progress`.
7. The new replica can be mounted — new rows will be replicated to it as well, and `$row_index` for them will be the same as for other replicas.

This method allows you to create a new replica that is consistent with existing ones. It will only contain new data. We do not consider full copying for two reasons: 1) there is no remote copy for ordered dynamic tables, 2) the queue scenario generally assumes cleaning up old data, and their absence in the new replica is supposed to be a big problem.

## Required number of replication queues

While the chaos replication protocol can work with any number of replication queues, in scenarios where queues are distributed across multiple data centers, you should have at least three queues, with at least two being synchronous at any given time.

If, even temporarily, write are made only to one synchronous queue, replication will break if that queue suddenly becomes unavailable. This configuration may be acceptable only if the queue is located on a cross–data center cluster.

## Chaos cells

Chaos tables achieve fault tolerance and continuous availability through a separate layer of chaos metadata stored in *chaos cells*. Chaos cells are located on cross–data center clusters. In addition, you can migrate information between chaos cells when temporarily shutting down some of them. For example, this can be useful during updates.

Like tablet cells, chaos cells combine into chaos cell bundles. Bundles are required for isolation and help account for resources among different consumers. Unlike tablet cell bundles, chaos cell bundles are known to all clusters. You can view a full list of chaos cells directly in the UI, including those physically located on other clusters.

To perform chaos replication, you need a chaos cell bundle and the appropriate permissions to use it.

{%if audience == "internal" %}
Currently, only administrators can create chaos cell bundles. If you want to use chaos replication, contact support.
{%endif%}

## Replication progress

During replication, it's important to know which data has been transmitted and which hasn't. In the transactional data model, updates to {{product-name}} dynamic tables are uniquely identified by a key-timestamp pair, with timestamps strictly increasing. This way:
- For each individual key, updates must be strictly ordered by timestamps.
- Updates don't have to be ordered across different keys.

Replication progress is represented as a step line chart in a key-time coordinate system: for each key range, the specified timestamp indicates the point where replication stopped for that range, inclusively. This approach allows separate data read requests for specific ranges, and these ranges don't necessarily have to match the sharding keys of the replication queues.

Progress is represented by a sequence of segments (lower key, timestamp). The last key is set using a separate upper_key field. You can check a table's progress using the `@replication_progress` attribute. We recommend doing this only on unmounted tables, because the actual progress on mounted tables may be significantly greater. You can change the progress value using the `alter-table` command. This can be useful if you need to connect a new replica with prepared data to an existing table.

Note that for a mounted replica, only the tablet node reliably knows the progress of its tablets. Neither the master that responds to reads of the `@replication_progress` attribute nor the chaos cell from which you can retrieve the replication card (more on that below) has reliable information about the actual progress. The information available to them lags behind, and that's by design.

## Replication card

A replication card contains the basic metadata for the chaos replication protocol. While it isn't explicitly used in typical user scenarios, it can be helpful for troubleshooting.

The card provides information about replicas, including their associated clusters and tables, replica modes, and (potentially lagging) progress.

In addition, replication cards include the `history` and `era` fields. The era field tracks chaos replication epochs. A new epoch begins with each change in the cumulative state of replicas, such as additions, deletions, and mode switches. The `history` field records the epochs in which the state of a specific replica changed. The `history` is periodically cleaned up in the background.

In order to find out the unique identifier of the card, you need to read the `@replication_card_id` attribute from the chaos replicated dynamic table or from the replica table. To get the card itself, it is enough to do a get by the identifier (don't forget to add `#`).

The user never specifies `@replication_card_id` for replica tables. It is enough to specify `@upstream_replica_id` — the card identifier is unambiguously derived from the replica identifier.

When creating a `chaos_replicated_table`, a new replication card is automatically created. To create a `chaos_replicated_table` that points to an existing replication card, you need to explicitly pass it as the `replication_card_id` attribute during creation.

By default, the existence of a replication card is tied to a chaos replicated dynamic table — when the `chaos_replicated_table` object is deleted, the card will be automatically deleted as well (this is true for both new cards and attached existing ones). You can change this behavior using the `@owns_replication_card` attribute — if you set it to `false`, the card will continue to exist even after the `chaos_replicated_table` is deleted. Important: this feature should be used with extreme caution, and you should carefully monitor that there is always a chaos replicated dynamic table that owns the card. Otherwise, you can fill up the memory of chaos metadata with irrelevant cards — they will continue to exist.

## Switching replica modes

To switch a replica's mode, use the `alter-table-replica` command, just like you would do with a regular ordered table. You can either toggle a replica using the enable/disable flags or change the mode from synchronous to asynchronous and vice versa using the `mode` option.

Switching modes causes a brief shutdown of the replica while the system waits for any transactions writing to the old synchronous replicas to complete and starts new ones that write to the updated synchronous replicas. This ensures that the replication accurately reflects the data stored on the synchronous replicas. A similar barrier is implemented for replicated dynamic tables; in practice the downtime is usually about a few seconds for both replicated and chaos tables.

## Switching replica modes automatically

Like regular replicated tables, chaos replicated tables support [automatic replica switching](../../../user-guide/dynamic-tables/in practice the downtime is usually about a few seconds).

To set up automatic switching, set the `ReplicatedTableOptions` for the replication card and specify for each replica whether its mode can change automatically.

When creating a `chaos_replicated_table`, you can specify the `replicated_table_options` attribute where you want to pass the `ReplicatedTableOptions`. For more information about the `ReplicatedTableOptions`, see the dedicated [section](../../../user-guide/dynamic-tables/replicated-dynamic-tables#attributes).

For an existing chaos replicated dynamic table, you can change the options with the `alter_replication_card` command by passing it the replication card ID (you can find it in the `@replication_card_id` attribute of the chaos replicated table and its replicas):
```python
yt.alter_replication_card(replication_card_id, replicated_table_options=replicated_table_options)
yt.alter_replication_card(replication_card_id, enable_replicated_table_tracker=True)
```

To control automatic switching for individual replicas, each replica has the `enable_replicated_table_tracker` flag that you can either set when creating a new replica or change for an existing replica using the `alter-table-replica` command:
```bash
$ yt alter-table-replica [--enable-replicated-table-tracker | --disable-replicated-table-tracker] replica_id
```

## Attributes

`chaos_table_replica` attributes

| Name | Type | Value |
|----|-----|----------|
| table_path | string | Path to the chaos_replicated_table. |
| replication_card_id | TReplicationCardId | Explicitly specifies the replication card. You don't need to specify it if you already specified the table_path. |
| cluster_name | string | Cluster with the replica table. |
| replica_path | string | Path to the replica table. |
| content_type | ETableReplicaContentType | `data` or `queue`, depending on whether it's data or a replication queue. For replicas of ordered dynamic tables, specify `queue`. |
| mode | ETableReplicaMode | The replica's mode at creation time: `sync` or `async`. |
| enabled | bool | Whether the replica should be enabled: `true` or `false`. |
| catchup | bool | `true` if data should be copied to the replica; `false` if the replica should be blank. |
| replication_progress | TReplicationProgress | Progress at the replica's creation time. You can omit this if the progress is already specified for the replica table. |
| enable_replicated_table_tracker | bool | `true` if automatic mode switching should be enabled for the replica. |
