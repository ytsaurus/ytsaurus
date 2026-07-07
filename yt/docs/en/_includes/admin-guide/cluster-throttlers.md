# Inter-cluster network bandwidth throttling

Cluster throttlers limit incoming inter-cluster traffic on {{product-name}} MR clusters. Throttling applies to [RemoteCopy](../../user-guide/data-processing/operations/remote-copy.md) operations and MapReduce operations that read input tables from remote clusters.

For more information about how throttling affects user operations, see [Inter-cluster Network Bandwidth Throttling](../../user-guide/data-processing/operations/cluster-throttlers.md).

## Distributed throttler mechanism { #distributed-throttler }

Each `exec` node runs a distributed throttler factory. Each throttler limits the incoming data stream from a specific remote cluster. For example, the `bandwidth_remote` throttler on the `local` cluster limits the incoming stream from the `remote` cluster.

A leader is elected among all `exec` nodes using a gossip-like protocol. The leader:
- Collects throttler state from all `exec` nodes.
- Tracks the total inter-cluster bandwidth usage.
- Distributes individual limits to `exec` nodes.
- Sends controller agents information about inter-cluster bandwidth availability.

If the leader becomes unavailable, a new leader is elected using the gossip-like protocol.

## Configuration { #configuration }

The cluster throttlers configuration is stored in the `//sys/cluster_throttlers` node of the cluster metadata tree. To limit the remote read rate from a specific external cluster, add that cluster to the configuration.

### Configuration fields { #config-fields }

- `enabled` — enables or disables throttling. When set to `%false`, throttlers are not applied.
- `update_period` — interval (in ms) for polling the `//sys/cluster_throttlers` node to update the configuration.
- `distributed_throttler` — distributed throttler settings:
  - `limit_update_period` — how often (in ms) an `exec` node sends its state to the leader and receives its local quota.
  - `leader_update_period` — how often (in ms) an `exec` node updates information about the current leader.
  - `local_throttlers_attribute_update_period` — how often (in ms) the `local_throttlers` attribute in the discovery service is updated (used for introspection).
  - `throttler_expiration_time` — time (in ms) after which the leader considers a throttler inactive if no updates have been received from it.
- `cluster_limits` — quotas for each remote cluster, keyed by cluster name:
  - `bandwidth` — incoming bandwidth limit:
    - `limit` — maximum throughput in bytes per second.
  - `rps` — read request rate limit:
    - `limit` — maximum number of requests per second.

### Configuration example { #config-example }

```
{
    "enabled" = %true;
    "update_period" = 5000;
    "distributed_throttler" = {
        "leader_update_period" = 5000;
        "throttler_expiration_time" = 60000;
        "limit_update_period" = 1000;
        "local_throttlers_attribute_update_period" = 5000;
    };
    "cluster_limits" = {
        "remote_1" = {
            "bandwidth" = {
                "limit" = 549755813888;  // 512 GB/s
            };
        };
        "remote_2" = {
            "bandwidth" = {
                "limit" = 4294967296;  // 4 GB/s
            };
        };
    };
}
```

## Managing configuration with the CLI { #cli }

To manage the cluster throttlers configuration using the CLI:

1. Create a configuration file with the required settings (see the example above).
2. Create the `//sys/cluster_throttlers` node in the cluster metadata tree:
   ```bash
   yt create document //sys/cluster_throttlers
   ```
3. Write the configuration to the created node:
   ```bash
   yt set //sys/cluster_throttlers < config_file
   ```

{% if audience == "internal" %}
## Managing configuration via ytdyncfgen { #ytdyncfgen }

Configuration templates are stored in the repository at `yt/admin/ytdyncfgen/templates/components/cluster_throttlers/` — one YAML file per cluster.

To change the throttler configuration on a cluster (for example, `arnold`):

1. Edit the file `yt/admin/ytdyncfgen/templates/components/cluster_throttlers/arnold.yaml`.
2. Build `ytdyncfgen` and apply the config:
   ```bash
   ya make yt/admin/ytdyncfgen/bin -r
   ./yt/admin/ytdyncfgen/bin/ytdyncfgen -c arnold -s cluster_throttlers -p
   ```
3. Re-canonize the tests:
   ```bash
   ya make yt/admin/ytdyncfgen/tests -tAZ
   ```
4. Commit the changes and create a PR:
   ```bash
   arc commit -m "Update cluster_throttlers on arnold" yt
   arc pr c
   ```
{% endif %}

## Introspection { #introspection }

### Throttler state on nodes { #throttler-state }

Throttler state for `exec` nodes is published under the `remote_cluster_throttlers_group` group in the discovery service. Each node publishes a `local_throttlers` attribute with the following fields:

- `rate` — current throughput in bytes per second.
- `limit` — quota in bytes per second assigned to this node by the leader.
- `queue_byte_size` — queue size in bytes.
- `quota_exceeded` — whether the quota has been exceeded.
- `period` — limit update period in milliseconds.

To view throttler state on the `local` cluster, run the following command on that cluster:

```bash
yt --proxy local list \
  "//sys/discovery_servers/<discovery-server>/orchid/discovery_server/remote_cluster_throttlers_group/@members" \
  --attribute local_throttlers --format json | jq -r '.[] | .["$attributes"]["local_throttlers"]'
```

The command outputs throttler state for all `exec` nodes in the cluster:

```json
{
  "bandwidth_remote_1": {
    "rate": 19324406.5,
    "limit": 21496801.3,
    "queue_byte_size": 0,
    "quota_exceeded": false,
    "period": 1000
  },
  "bandwidth_remote_2": {
    "rate": 1832910.1,
    "limit": 1935680.6,
    "queue_byte_size": 0,
    "quota_exceeded": false,
    "period": 1000
  }
}
```

To view utilization of the network channel from the `remote` cluster to the `local` cluster, run the following command on the `local` cluster:

```
yt --proxy local list \
  "//sys/discovery_servers/<discovery-server>/orchid/discovery_server/remote_cluster_throttlers_group/@members" \
  --attribute local_throttlers --format json | jq '[.[] | ."$attributes"."local_throttlers"."bandwidth_remote"."rate"] | add'
```

### Verifying the leader { #leader-check }

To verify that a single leader has been elected in the throttling group, run:

```bash
yt list \
  "//sys/discovery_servers/<discovery-server>/orchid/discovery_server/remote_cluster_throttlers_group/@members" \
  --attribute leader_id --attribute address --format json \
  | jq -r '.[] | [.["$attributes"]["leader_id"], .["$attributes"]["address"]] | @tsv' \
  | cut -f 1 | sort -u
```

If the command outputs more than one unique `leader_id`, this indicates a split-brain condition. Investigate the root cause, for example, network isolation of some nodes.

{% if audience == "internal" %}
### Monitoring { #monitoring }

The monitoring system has a [cluster-throttlers](https://monitoring.yandex-team.ru/projects/yt/dashboards/cluster-throttlers) dashboard covering all clusters.

Key throttler metrics:
- `queue_byte_size` — throttler queue size on the `exec` node. Growth in this metric indicates that the node is accumulating requests faster than the leader can issue quota.
- `queue_estimated_overrun_duration` — estimated time, in seconds, required to process the current throttler queue at the current quota.
{% endif %}
