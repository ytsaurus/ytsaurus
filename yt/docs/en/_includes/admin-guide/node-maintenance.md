## Node administration

This section describes some of the features available for administering cluster nodes.

### Pending restart

The primary use case for this flag is to optimize master replication operations during maintenance of cluster data nodes, where the nods are assumed to be down for a short time. Before initiating such maintenance, we recommend running the following command:

```bash
$ yt add-maintenance --component="cluster_node" --type="pending_restart" --address="my-node.yandex.net"  --comment="my comment"
```

To make sure that the _restart pending_ flag is set for the node, use the following command
```bash
$ yt get //sys/cluster_nodes/my-node.yandex.net/@pending_restart
> true
```

#### How it works

Setting the _restart pending_ flag for a node object significantly extends its _lease_ transaction timeout. This means that the node is still considered _online_ for an extended period even after it stops making periodic requests to the master server to confirm its availability. You can adjust the duration of this interval in the dynamic config of the master server at `//sys/@config/node_tracker/pending_restart_lease_timeout` (the default value is 10 minutes).

Optimization of the master chunk replicator is achieved through special handling of replicas that are located on nodes with this flag. We'll refer to these nodes as _temporarily unavailable_ replicas. Replicas located on regular nodes will be called _available_ replicas.

For instance, a chunk in a regular format is considered _underreplicated_ if at least one of the following conditions is met:
* The total number of _available_ + _temporarily unavailable_ replicas is less than the _replication factor_.
* The number of _available_ replicas is less than 1 + _max replicas per rack_.
* There are more than one _temporarily unavailable_ replicas.

In turn, _erasure_ chunks are considered _parity missing_ or _data missing_ if:
* One of the replicas is unavailable, and it isn't _temporarily unavailable_.
* The difference between the minimum number of replicas required for the chunk to be repairable and the number of _temporarily unavailable_ replicas is less than _max erasure replicas per rack_.

In a typical cluster configuration with _replication factor = 3_ and _max replicas per rack = 1_, a practical application of _pending restart_ assumes that simultaneous maintenance will be limited to nodes within a single rack.

#### Removing the flag

```bash
$ yt remove-maintenance --component="cluster_node" --address="my-node.yandex.net" --id="<maintenance-id>"
```

Prolonged use of this flag can be dangerous, so its validity is intentionally limited by a configurable interval, the previously mentioned `pending_restart_lease_timeout`, after which the flag is removed automatically. The deadline is calculated from the last time the flag was set.
