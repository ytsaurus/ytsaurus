# Replicated tables

## General information { #common }

Replicated dynamic tables are a type of tables in {{product-name}} that does not store a full set of written data, but is a replication queue. As the data is successfully written to replicas, it is gradually deleted from the replicated dynamic table.

Copies (replicas) of a replicated table can be located both on the same cluster as the source table (intra-cluster replication) and on other clusters often located in different DCs (inter-cluster replication). Replication between dynamic tables enables you to have multiple copies of the same table in independent clusters and organize automatic data replication between them.

Using replicated dynamic tables enables you to:

- Survive DC level failures, both read and write.
- Increase fault tolerance of the user service by isolating table replicas.
- Update {{product-name}} clusters with less or no downtime for the user.

Both [sorted](../../../user-guide/dynamic-tables/overview.md) and [ordered](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md) dynamic tables can be replicated. Replicas can be **synchronous** and **asynchronous**. A successful write transaction commit ensures that the data is in synchronous replication and in the replication queue. Data will be written to asynchronous replicas in the background. For more information, see [Replication](#replication).

Each replicated dynamic table can have replicas of both types. Data consistency when writing to a replicated dynamic table is the same as when using an unreplicated dynamic table (sorted or ordered, respectively).

Replication requires one **meta cluster** and one or more **replica clusters**. Setting up replication consists of several steps:

1. Creating a special `replicated_table` object on the meta cluster. This object stores information about replicas and replication status.
2. Create one dynamic table (sorted or ordered depending on the task) on each of the replica clusters.
3. Setting up replicas on the meta cluster and enabling replication.

Once the above steps are complete, data can be written to or deleted from the replicated table on the meta cluster using the `insert-rows` and `delete-rows` commands. The changes will get into the replica clusters.

A replicated dynamic table has a _schema_ and _tablets_ defined by the boundary keys. The schema of the replicated table must be no wider than that of the replicas (differences are possible during migration of all replicas to a wider schema).

Transactions apply to a write to a replicated dynamic table: the actual writing of rows into it occurs when a write transaction is committed. This transaction must be performed exactly on the meta cluster. Other dynamic (not necessarily replicated) meta cluster tables can still be used within such a transaction.

## Writing data { #write }

Writing from the client to the replicated dynamic table results in a distributed commit. As part of the commit, rows are added to the replication queue on the meta cluster. The client also writes changes to each synchronous replica (if any) as part of this transaction. The data flows from the client to the meta cluster, as well as to each cluster with a synchronous replica directly, i.e. the flows from the client are multiplied (at least doubled with one synchronous replica). This distributed transaction is coordinated by the meta cluster. If data cannot be written to at least one synchronous replica, the client will get an error.

Writing to a replicated dynamic table with at least one synchronous replica semantically does not differ from writing to a regular dynamic table. You can write to a replicated dynamic table even if there are no synchronous replicas.

{% note info "Note" %}

When writing to a replicated dynamic table without synchronous replicas, the system is unable to check for conflicts between transactions, so semantically such a write differs from a regular one.

{% endnote %}

A successful write to a replicated dynamic table with only asynchronous replicas means that the changes are accepted by the system and are guaranteed (as quickly as possible) to be delivered to the replicas. The asynchronous replicas themselves are always lagging behind the source table.

To allow a client to write to a replicated dynamic table that has no synchronous replica, the write commands (`insert-rows`, `delete-rows`) have a special `require_sync_replica` flag (the default value is `true`). Setting this flag to `false` enables a write to a replicated dynamic table without a synchronous replica.

If data cannot be written to the asynchronous replica, it will be accumulated in the replicated table. Once the connection with the replica is re-established, all unreplicated data will be written to the replica.

## Reading data { #read }

The main task of a replicated dynamic table is to write user data to the replication queue as quickly as possible. As was mentioned above, a replicated dynamic table does not store a full set of data. Therefore, the {{product-name}} system seeks to reduce the read load from the replicated dynamic table and will redirect all read queries to the synchronous replica if there is one (defined by a subquery to the meta cluster). Such reading is guaranteed to be semantically indistinguishable from reading from a regular dynamic table.

{% note warning "Attention!" %}

The ability to read from a replicated dynamic table causes the additional overhead costs of interacting with the meta cluster. This feature cannot be used in a situation when low latency or high RPS is required.

{% endnote %}

{% note warning "Attention!" %}

With asynchronous replicas only, the ability to read from the replicated dynamic table remains, but is undesirable as it creates additional load on the replicated dynamic table. Reading should be performed from replicas.

{% endnote %}

Reading from a replicated dynamic table can be performed using the `select-rows` and `lookup-rows` commands. When using `select-rows`, several meta cluster tables can participate in a single query. If at least one of the tables in a query is replicated, all other tables must also be replicated, and there must be a single cluster on which there are synchronous replicas of all the specified tables. In other words, inter-cluster queries are not supported.

## Replication { #replication }
The asynchronous replication process works according to the push schema. There is a replicator for each tablet of the replicated dynamic table and each replica there, which reads fresh changes from the replication queue and sends the data to the asynchronous replicas. The sending process is a distributed (between clusters) transaction, in which data is written to the replica table and data portion replication is recorded in the replicated dynamic table metadata.

### Guarantees

For asynchronous replicas, replication transactions (delivering data to replicas) are in no way coordinated with write transactions to the replicated table. Changes in a single write transaction to a replicated table can be broken down into multiple replication transactions. Guarantees:

- There are no guarantees of atomicity for the replica reader - they may catch the replica in an intermediate state when only part of the transaction changes have been applied to it.
- Exactly-once semantics is guaranteed (each write to the replicated table will be delivered to all replicas exactly once).
- The correct order of change delivery to the replica tables is guaranteed — all transactions in the tablet of the replicated table are ordered relative to the commit timestamp. This is the order in which the changes are sent to the replicas.

In case of synchronous mode, the replicator is not involved in the appearance of data in the replica. Instead, a client writing to a replicated table within the same transaction writes (automatically, implicitly) to the synchronous replica table as well. In this sense, the way the data gets into the synchronous replica is not much different from an ordinary table write. In particular, for a synchronous replica, the consistency of its state is guaranteed at exactly the same level as for a regular dynamic table. However, it is important to realize that what is written is only true if the replica remains synchronous at all times. When replication mode switches to asynchronous, the data starts getting into the table with a lag, and when communicating directly with the replica, there is not much chance for the client to realize that such a switch occurred.

The replica tables can be accessed directly through the clusters where these replicas are physically located. The replica tables are special. When replicated, the replica tables contain data with the timestamp generated on the meta cluster. You cannot write data to replica tables directly (bypassing the meta cluster), the system makes every effort to prohibit such writing (such writing can be performed by setting `upstream_replica_id` to zero. The risks of this action are described below). You can read data from replica tables. This is a welcome use case when the replica tables serve a realtime service hosted in the same DC, which gives data to an external user.

Limitations on the consistency of the data that can be read:

- If the replica is synchronous, reading without specifying a timestamp will give the most recent state (specifically, there is no snapshot isolation at the level of individual values of individual rows: different rows and different columns of the same row may correspond to different points in time; visibility of data from all committed transactions is guaranteed).
- Reading from a replica with a timestamp is possible, but we recommend getting the timestamp from the meta cluster, because this timestamp is actually invalid for the replica cluster (because it came from the meta cluster).
- For asynchronous replicas, a lag is always possible (and it exists), so any reading will return lagged data; consistency is not guaranteed in replication (a transaction in the replicated table can atomically change `A` and `B` keys, read the new value for `A` from the replica one, and the old value for `B` after that).

### Replicas

Each replicated table is associated with a set of replicas (`table_replica` objects) created as follows:

Creating a replica:

```bash
yt create replicated_table //tmp/replicated --attr '{dynamic=%true; schema=[{name=k;type=string;sort_order=ascending};{name=v;type=string}]}'

yt create table_replica --attr '{table_path="//tmp/replicated"; cluster_name="replica_cluster";replica_path="//tmp/replica"}'
```

This set of commands creates a replicated table on the meta cluster `//tmp/replicated` and configures an (asynchronous) replica for it on the `replica_cluster` cluster at `//tmp/replica`.

The specified commands **do not create** the table itself `//tmp/replica` on the replica cluster, but only configure the replica for `//tmp/replicated`. Replica tables must be created individually.

Note that after a replica is created, it is by default in a **disabled state** (`state = "disabled"`), in which replication is disabled. To run a replica, use the `alter-table-replica --enable` command. For more information, read [below](#replica-state).

Each created replica (like any master server object) has an ID. The following commands can be used to view replica information:

Viewing a replica:

```bash
yt get //tmp/replicated/@replicas
yt get '#replica-id/@'
```

You can delete a replica like other objects using the `remove` command:

Deleting a replica:

```bash
yt remove '#replica-id'
```

#### Adding a replica to an existing replicated table {#add-new-replica}

If you want to add a replica by fully copying the data of an existing replica, use the {% if audience == "public" %}replica copy tool{% else %}[replica copy tool](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md#add-table-replica-script){% endif %}.

If you want to add a new replica or you already have a replica table that stores the required data state, use one of the two attributes (`start_replication_timestamp` or `start_replication_row_indexes`) when creating a `table_replica` object. With `start_replication_timestamp`, all changes that have a commit ts strictly greater than the specified value (meaning the meta cluster timestamp) will be replicated. If you need to initiate replication from a specific moment, the easiest way to do that is to specify a timestamp obtained by calling generate-timestamp for the meta cluster. With `start_replication_row_indexes`, you need to specify the row index (and do so for each tablet of the replicated table) from where you want data to replicate. You can obtain relevant row indexes from the get-tablet-infos query.

### Creating replica tables

A replica table is a regular (dynamic) table on the target cluster. But when creating it, you must specify the `upstream_replica_id` attribute and the ID of the replica object on the meta cluster. Specifying `upstream_replica_id` allows the replicator to write from the metacluster to the replica cluster and prohibits direct writing to the replica table (which would bypass the replicator). Specifying this ID protects against configuration errors when the same table appears as a replica for multiple replicated tables.

Creating a replica table:

```bash
yt create table //tmp/replica --attr '{dynamic=%true; upstream_replica_id=replica-id;schema=[{name=k;type=string;sort_order=ascending};{name=v;type=string}]}'
```

The `upstream_replica_id` table attribute can be changed after the table is created using the `alter-table` command. All table tablets must be unmounted.

### Replica settings {#replica-state}

Immediately after a replica is created, it is in a `disabled` state (the `state` attribute). No replication is performed in this state. To enable a replica, use the `alter-table-replica` command:

Enabling a replica:

```bash
yt alter-table-replica replica-id --enable
```

The replica will switch to `enabling` state and after it is successfully enabled, it will switch to `enabled` state.

The replica can be disabled, thereby interrupting replication. To disable a replica, use the same `alter-table-replica` command. Disabling is also asynchronous, the replica will switch to `disabling` and then to `disabled` state:

Disabling a replica:

```bash
yt alter-table-replica replica-id --disable
```

### Manual synchronization

In some cases it is sufficient to have only asynchronous replicas (they provide the maximum possible write speed to the replicated table, since all actions are performed in the background), but still be able to explicitly wait for replication to finish in any replica. The `get-in-sync-replicas` command is used for that. For it to work, you need to specify a timestamp and the system will find all replicas that are guaranteed to be reached by all changes with a timestamp no greater than the specified one. For example, you can generate the current timestamp and then wait in the cycle for the specified replica to be in the list returned by the command. This replica can then be queried and timestamped. This will guarantee read consistency.

### Replication modes

By default, replicas are created in asynchronous mode. The user can specify a synchronous mode at creation (via the `mode` attribute) and change the mode on the fly (using the `alter_table_replica` command). This switch can result in write downtime: if a replica is switched from asynchronous to synchronous state, queue tail replication must be fully complete before the client can write to that replica. Therefore, it is not a good idea to consider a replica with a large lag to be synchronous (the replicated table cannot be written to until the lag disappears).

Asynchronous replication is possible without saving the start `timestamps`. To use this mode, create a replica with the `{"mode": "async", "preserve_timestamps": "false"}` attributes, and the replica table must be created without specifying the `upstream_replica_id` attribute. This feature can be useful in a number of cases:

1. You want to replicate data from multiple sources into one replica table.
2. You want to manually change data in the replica table (patch, insert new data), bypassing the replication mechanism.

### Automatic switching of the **synchronous** replica

If the cluster on which the synchronous replica is located is unavailable, writing to the replicated dynamic table is stopped. To continue writing, the synchronous replica must be switched to another cluster. The {{product-name}} system can automatically switch a synchronous replica. To activate this feature, either create a replicated dynamic table with the `replicated_table_options={enable_replicated_table_tracker=%true}` attribute or add the specified attribute later using the `yt set` call. The {{product-name}} system will then monitor the table replicas and switch the synchronous replica to another cluster if necessary.

There are two settings to specify the number of synchronous replicas: `min_sync_replica_count` and `max_sync_replica_count`. The automated system switches available replicas to synchronous mode, ensuring that there are no more than `max_sync_replica_count` synchronous replicas. The automated system also monitors that the number of synchronous replicas does not fall below `min_sync_replica_count`.
If `min_sync_replica_count` and `max_sync_replica_count` are not specified, then both fields are equal to 1 by default. If only `min_sync_replica_count` is not specified, it will be equal to `max_sync_replica_count`. If only `max_sync_replica_count` is not specified, it will be equal to the total number of replicas. Thus, by default, the automated system supports exactly one synchronous replica.
In case of chaos replication, there are special parameters (`min_sync_queue_replica_count` and `max_sync_queue_replica_count`) for replicas of type `queue`.

There is a mode when the {{product-name}} system stops monitoring any table replica, but continues monitoring the remaining replicas. To enable this mode for a replica (`table_replica`), use the `yt set` call and set the `enable_replicated_table_tracker=%false` attribute.

You can view the current mode for the replica table using the `replicated_table_tracker_enabled` attribute of the meta cluster replica object:

```bash
$ yt --proxy meta-cluster get //home/some_meta_table/@replicas

"table-replica-id" = {
     "cluster_name" = "some_name";
     "replica_path" = "//home/some_table";
     "state" = "enabled";
     "mode" = "async";
     "replicated_table_tracker_enabled" = %false;
 };
```

#### Concurrent switching of synchronous table group replicas { #replication_collocation }

You can combine replicated tables into groups (collocations) so that the system monitors that synchronous replicas of these tables are on the same clusters.
To do this, create a `table_collocation` object that stores a list of collocated replicated tables.

Working with replicated table collocations:
```bash
# Create a collocation with a list of tables.
yt create table_collocation --attributes '{collocation_type=replication; table_paths=["//tmp/replicated_table_1"; "//tmp/replicated_table_2"]}'

# Find out the list of tables in a collocation.
yt get "#collocation-id/@table_ids"
yt get "#collocation-id/@table_paths"

# Find out the ID of the collocation to which the table belongs.
yt get //tmp/replicated_table_1/@replication_collocation_id

# Find out the list of all tables in the collocation to which the table belongs.
yt get //tmp/replicated_table_1/@replication_collocation_table_paths

# Add a table to the collocation.
yt set //tmp/replicated_table_3/@replication_collocation_id collocation-id

# Delete a table from the collocation.
yt remove //tmp/replicated_table_3/@replication_collocation_id
```

#### Specifying preferred clusters for synchronous replicas

When a replicated table on different clusters has multiple replicas that can become synchronous, you can specify which of them should be given preference in case of automatic switching. To do this, the following attribute must be set on the replicated table:
```bash
yt set //tmp/replicated_table/@replicated_table_options/preferred_sync_replica_clusters '["cluster-name"]'
```

This attribute respects [collocations](#replication_collocation) of replicated tables.
To specify preferred clusters for all tables in a collocation at once, just set this attribute in the collocation options once.
```bash
yt set #collocation-id/@replicated_table_options/preferred_sync_replica_clusters '["cluster-name"]'
```
If this attribute is not set for the collocation, its value will be derived from the replicated tables' options. In this case, make sure that it is set **for all tables** of the collocation.

### Limiting replication speed

Asynchronous replication can lead to a substantial load on the network connecting the data centers as well as on the replica table cluster, so we recommend setting up limits for write-intensive tables. The system supports two types of settings. In most scenarios, we recommend using relative throttling.

{% if audience == "internal" %}
{% note info %}

For main production clusters that are used for replication, relative throttling is enabled by default for all tables. If needed, you can explicitly disable it by setting the `mount_config/relative_replication_throttler/enable` attribute to `%false`. We do not recommend disabling throttling for large tables because this may result in increased loads on the replica cluster.

{% endnote %}
{% endif %}


#### Relative replication throttling

Relative throttling can be configured via the `mount_config/relative_replication_throttler` attribute. This attribute is a dict that can contain the following values.

| Name | Type | Description | Default value |
| -------------------------- | ------------------------ | ------------------------------------------------------------ | ---|
| enable | bool | Throttling flag | `%false` (may depend on the cluster) |
| ratio | double | Ratio of the replication speed to the write speed | 2 |


Relative throttling limits the speed of asynchronous replication relative to the write speed. For example, if there's a data set that took an hour to write, and the asynchronous replica was unavailable during that time, then with ratio = 3 that data will be replicated in 20 minutes once the replica becomes available.

If the write speed is constant, the default value (ratio = 2) ensures that the catch-up time is roughly the same as the lag time. In this case, it is assumed that the replica cluster has sufficient resources to handle a doubled load for the duration of the catch-up period. By decreasing the ratio value, you can reduce the required resource guarantees at the cost of increasing the replica catch-up time.

Setup example:

```
yt set //tmp/replicated_table/@mount_config/relative_replication_throttler '{ratio=1.5}'
```

#### Absolute replication throttling


This type of throttling can be configured via the `mount_config/replication_throttler` attribute, which can accept two values:

- limit: The limit for the average bandwidth value in bytes/sec.
- period: The time interval in ms during which the average bandwidth value is measured.

For example, the following command sets a replication limit of one megabyte per second:

```bash
yt set //tmp/replicated_table/@mount_config/replication_throttler '{period=1000;limit=1000000}'
```

{% note info "Note" %}

The limit you set does not apply to the entire table, but to each table tablet individually.

{% endnote %}

### Maintaining the tablet index during ordered table replication

By default, asynchronous replication of an ordered dynamic table writes rows to an arbitrary replica tablet. If you need to maintain the tablet index (this is important if you want the same tablet row to appear under the same index across different replicas), set the `preserve_tablet_index` attribute of the ordered dynamic table itself to `%true`. When using this attribute, you need to manually track that the replicated table and the replica tables have the exact same number of tablets.

## Changing schemas { #alter_schema }

To perform a compatible schema change to both a replica table and the replicated table, run the `alter-table` command. This command must be called for each table individually. Ensure that the table is unmounted before executing the command.

The replica table schemas do not have to match the replicated table schema, but must be no narrower than the replicated table schema for replication to work correctly. You need to start the schema change with the replicas and then move on to the replicated table.

## Resistance to downtimes and updates { #downtimes }

Each replica cluster exists independently. When a replica cluster is disabled, there is no way to read data from it. Replicas can be updated independently.

The replica table schemas do not have to match the replicated table schema, but must be no narrower than the replicated table schema for replication to work correctly. You need to start the schema change with the replicas and then move on to the replicated table.

In order to write data to a replicated table, two conditions must be met, and are sufficient:

1. The meta cluster is available.
2. All synchronous replicas of the replicated table are available. It is assumed that the meta cluster is geo-distributed, updated infrequently and with a short downtime during which the table cannot be written to (but can still be read directly from replicas). If a replicated table has no synchronous replicas, it can be written to whenever the meta cluster is available. If a table has a synchronous replica, it needs to be kept available (which can be helped by automated synchronous replica switching).

## Attributes { #attributes }

All replicated tables are tables and they have a corresponding set of attributes. Any replicated table is additionally characterized by these parameters:

| Name | Type | Description |
| -------------------------- | ------------------------ | ------------------------------------------------------------ |
| replicas | Guid->ReplicaInfo | Description of replicas (keys are the replica IDs, and values are some of their important attributes) |
| replicated_table_options | ReplicatedTableOptions | Automated replica switching settings |
| preserve_tablet_index | bool | Whether to save the tablet index during replication of ordered dynamic tables. The default value is `%false` |

`ReplicatedTableOptions` here have the following form:


| Name | Type | Description |
| --------------------------------- | --------- | ------------------------------------------------------------ |
| enable_replicated_table_tracker | bool | Whether automated synchronous replica switching is enabled. The default value is `%false` |
| max_sync_replica_count | integer | Maximum and preferred number of synchronous replicas to be supported by the automated system |
| min_sync_replica_count | integer | Minimum number of synchronous replicas to be supported by the automated system |
| max_sync_queue_replica_count | integer | Similar to `max_sync_replica_count`, but for `queue` replicas (used in chaos replication). |
| min_sync_queue_replica_count | integer | Similar to `min_sync_replica_count`, but for `queue` replicas (used in chaos replication). | |
| preferred_sync_replica_clusters | list | List of preferred clusters for synchronous replicas |
| sync_replica_lag_threshold | Duration | Maximum lag value up to which the replica is considered available. The default value is 10 minutes. This value may sometimes substantially prolong replication, in which case you should decrease it. |
| enable_preload_state_check | bool | Whether to check the preload state of tables mounted in memory to see if they are available. The default value is `%false`. |
| incomplete_preload_grace_period | Duration | When checking replica availability in terms of the preload state, the replica is considered available if the last preload was successfully completed no later than the value specified in this parameter. The default value is 5 minutes. This parameter is used to ensure that the short-term preload during tablet rebalancing does not interfere with the algorithm. |

Each replica is described by the `ReplicaInfo` dict with the following form:

| Name | Type | Description |
| ---------------------- | ------------------- | ------------------------------------------------------------ |
| cluster_name | string | Replica cluster name |
| replica_path | string | The path to the replica table on the replica cluster |
| state | TableReplicaState | Replica state |
| mode | TableReplicaMode | Replica mode: `async` or `sync` |
| replication_lag_time | Duration | Replica lag estimate |
| errors | Error[*](*error) | List of replication errors for this replica |
| preserve_timestamps | bool | Save start `timestamps`? The default value is `true`. Makes sense only for asynchronous replicas |
| atomicity | EAtomicity | `full` or `none`. Makes sense only for asynchronous replicas |

The `replication_lag_time` gives a rough estimate of the replication lag (to within tens of seconds).

The replica object itself, in addition to the listed attributes (as well as those inherent in all objects), also has a number of additional attributes (all of which can be found out by running a query of the `yt get '#replica-id/@'` type):

| Name | Type | Description |
| --------------------------------- | -------------------- | ------------------------------------------------------------ |
| mode | ReplicaMode | Replica mode: `sync` or `async` |
| start_replication_timestamp | Timestamp | Starting timestamp for the replica |
| table_path | string | Path to the replicated table on the meta cluster |
| tablets | TabletReplicaInfo[*](**) | Description of tablet states. The position in the list corresponds to the tablet index in the table itself |
| enable_replicated_table_tracker | bool | Enables automated synchronous replica switching (see above). The default value is `%true` |

The `TabletReplicaInfo` structure has the following form:

| Name | Type | Description |
| ------------------------------- | ------------------- | ------------------------------------------------------------ |
| tablet_id | Guid | Tablet ID |
| state | TableReplicaState | State of this tablet replica |
| current_replication_row_index | integer | Replication boundary in the queue by write index (writes with index >= the specified index are subject to replication) |
| current_replication_timestamp | Timestamp | Replication boundary in the queue by timestamp (writes with timestamp > the specified timestamp are subject to replication) |
| replication_lag_time | Duration | Replica lag estimate for this tablet |
| flushed_row_count | integer | The number of this tablet's rows written to the disk. If the replicated table is frozen, you can compare this value against `current_replication_row_index` to keep track of the replication |
| trimmed_row_count` | integer | The number of initial rows in this tablet that have been successfully replicated and whose chunks were deleted (log truncation) |
| replication_error | Error | Describes a replication error. If there is no error, this tag is also missing |

The `current_replication_row_index` and `current_replication_timestamp` parameters increase monotonically as the replicator runs, marking the successfully replicated writes.

## Checking a replication queue { #queue_check }

A replication queue check is useful for re-creating a replicated table without losing changes. The procedure is as follows:

1. Run the `freeze-table` command for the replicated table.
2. Wait (by querying the `tablet_state` table attribute) for all tablets to go into `frozen` state.
3. Get information about the state of the tablets of the table of interest by making a `get #replica-id/@tablets` query. Each response list item describes the replication state for a specified tablet of the replicated table to the specified replica. For each tablet, view the `flushed_row_count` and `current_replication_row_index` values and wait until they are the same.

## Use cases { #examples }

### Creating, setting up replicas

The examples work from version ytsaurus-client >= 0.8.19. .

Examples of using replicated tables:

```bash
export YT_PROXY=cluster-name

# Creating a replicated table.
yt create replicated_table //tmp/replicated --attr '{
  dynamic=%true;
  schema=[{name=k;type=int64;sort_order=ascending};{name=v;type=int64}]
}'
730e-68d3e-3ff01a9-325bbdcd

# Creating a replica associated with first-replica-cluster.
yt create table_replica --attr '{
  table_path="//tmp/replicated";
  cluster_name="first-replica-cluster";
  replica_path="//tmp/replica"
}'
730e-7bcd8-3ff02c5-fd0b36ee

# Creating a second replica on second-replica-cluster.
yt create table_replica --attr '{
  table_path="//tmp/replicated";
  cluster_name="second-replica-cluster";
  replica_path="//tmp/replica"
}'
730e-8611b-3ff02c5-f647333f

# Creating a replica table on first-replica-cluster.
YT_PROXY=first-replica-cluster yt create table //tmp/replica --attr '{
  dynamic=%true;
  upstream_replica_id="730e-7bcd8-3ff02c5-fd0b36ee";
  schema=[{name=k;type=int64;sort_order=ascending};{name=v;type=int64}]
}'
6cd9-66770-3ee0191-980d9f6

# Creating a replica table on second-replica-cluster.
YT_PROXY=second-replica-cluster yt create table //tmp/replica --attr '{
  dynamic=%true;
  upstream_replica_id="730e-8611b-3ff02c5-f647333f";
  schema=[{name=k;type=int64;sort_order=ascending};{name=v;type=int64}]
}'
78b6-1dacd9-3f40191-2057d1df

# Mounting a replica table and the replicated table.
yt mount-table //tmp/replicated
YT_PROXY=first-replica-cluster yt mount-table //tmp/replica
YT_PROXY=second-replica-cluster yt mount-table //tmp/replica

# Writing data to the replicated table.
echo '{k=1;v=100}' | yt insert-rows //tmp/replicated --format yson
Table //tmp/replicated has no synchronous replicas

# Setting the no-require-sync-replica flag to force a write.
echo '{k=1;v=100}' | yt insert-rows //tmp/replicated --format yson --no-require-sync-replica

# Write was successful, but there is no data in the replica.
YT_PROXY=first-replica-cluster yt select-rows '* from [//tmp/replica]' --format yson

# Viewing replica state.
yt get '#730e-7bcd8-3ff02c5-fd0b36ee/@state'
"disabled"

# Replica was not enabled. Enabling a replica.
yt alter-table-replica 730e-8611b-3ff02c5-f647333f --enable
yt alter-table-replica 730e-7bcd8-3ff02c5-fd0b36ee --enable

# Checking replication progress.
YT_PROXY=first-replica-cluster yt select-rows '* from [//tmp/replica]' --format json
{"k":1,"v":100}

# Attempting to read data directly from the replicated table.
yt select-rows '* from [//tmp/replicated]' --format json
No in-sync replicas found for table //tmp/replicated

# Reading is not possible without synchronous replicas.
# Disabling the second replica and rewriting.
yt alter-table-replica 730e-8611b-3ff02c5-f647333f --disable
echo '{k=2;v=200}' | yt insert-rows //tmp/replicated --format yson --no-require-sync-replica

# Reading from replicas
YT_PROXY=first-replica-cluster yt select-rows '* from [//tmp/replica]' --format json
{"k":1,"v":100}
{"k":2,"v":200}

YT_PROXY=second-replica-cluster yt select-rows '* from [//tmp/replica]' --format json
{"k":1,"v":100}

# You can see that first-replica-cluster received the data, but second-replica-cluster did not.
# Failed replication can be noticed by a growing lag.
yt get '#730e-8611b-3ff02c5-f647333f/@replication_lag_time'
141000

# Changing the replication type on first-replica-cluster and enabling the replica.
yt alter-table-replica 730e-8611b-3ff02c5-f647333f --enable --mode sync

# Reading data from first-replica-cluster and checking if the data has appeared.
YT_PROXY=first-replica-cluster yt select-rows '* from [//tmp/replica]' --format json
{"k":1,"v":100}
{"k":2,"v":200}

# Having a synchronous replica allows us to read directly from the replicated table.
yt select-rows '* from [//tmp/replicated]' --format json
{"k":1,"v":100}
{"k":2,"v":200}

# Deleting the first row, this time in synchronous mode.
echo '{k=1}' | yt delete-rows //tmp/replicated --format yson

# Checking the command execution status.
YT_PROXY=first-replica-cluster yt select-rows '* from [//tmp/replica]' --format json
{"k":2,"v":200}

YT_PROXY=second-replica-cluster yt select-rows '* from [//tmp/replica]' --format json
{"k":2,"v":200}

# Disabling writes to the replicated table.
yt freeze-table //tmp/replicated

# Checking if the tablet is frozen (if all the data has been written to the disk).
yt get //tmp/replicated/@tablet_state
"frozen"

# Checking that all rows were successfully replicated in each of the replicas.
# To do this, view the flushed_row_count and replicated_row_index attributes.
yt get '#730e-7bcd8-3ff02c5-fd0b36ee/@tablets'
[
    {
        "flushed_row_count" = 5;
        "tablet_id" = "730e-68d3e-3ff02be-ee882e4a";
        "trimmed_row_count" = 2;
        "replication_lag_time" = 0;
        "current_replication_timestamp" = 1610012496066773010u;
        "current_replication_row_index" = 5;
        "state" = "enabled";
    };
]

yt get '#730e-8611b-3ff02c5-f647333f/@tablets'
[
    {
        "flushed_row_count" = 5;
        "tablet_id" = "730e-68d3e-3ff02be-ee882e4a";
        "trimmed_row_count" = 2;
        "replication_lag_time" = 0;
        "current_replication_timestamp" = 1610012496066773010u;
        "current_replication_row_index" = 5;
        "state" = "enabled";
    };
]

# You can see that the flushed_row_count and current_replication_row_index values are the same, which means that both replicas have been fully replicated.
```

[*error]: The parameter appears in the answer several times.
