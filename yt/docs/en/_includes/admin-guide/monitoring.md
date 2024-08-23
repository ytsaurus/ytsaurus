# Monitoring

## Setting up Prometheus
### Prerequisites
- [Follow the instructions](https://github.com/prometheus-operator/prometheus-operator#quickstart) to install the Prometheus operator.

### Launch
Monitoring services are created by the {{product-name}} operator automatically. To collect metrics, you need to:
1. Create [ServiceMonitor](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_monitor.yaml).
2. Create a [service account](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_account.yaml).
3. Give the account you created a [role](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus_role_binding.yaml).
4. [Create Prometheus](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/prometheus/prometheus.yaml).

## Monitoring methods and important {{product-name}} metrics

The monitoring of technical systems can be roughly classified into two general approaches: let's call them qualitative monitoring and quantitative monitoring.

### Qualitative monitoring

Qualitative monitoring verifies whether a system can perform its basic functions. This is done by running test tasks, or checks. These checks return a qualitative description of the system state:

- The system runs normally (OK).
- The system doesn't work (CRITICAL).
- The system works, but with deviations from the normal mode (WARNING).

A list of the most important qualitative checks in {{product-name}}:

- `sort_result`: Starts a sort operation on a small test table, waits for its completion, and checks the sorting result.
- `map_result`: Starts a map operation with a small test table as an input, waits for its completion, and checks the map result.
- `dynamic_table_commands`: Checks the operability of dynamic tables by going over their entire life cycle: creates a test table, mounts the table, writes data to the table and reads data from it, then unmounts and deletes the table.

There are checks of the self-diagnosis messages of the {{product-name}} cluster's subsystems:

- `master_alerts`: Checks the `//sys/@master_alerts` attribute value.
- `scheduler_alerts`: Checks the `//sys/scheduler/@alerts` attribute value.
- `controller_agent_alerts`: Reads the alerts attribute from all nodes at `//sys/controller_agents/instances`.
- `lost_vital_chunks`: Checks that the lost_vital_chunks counter is zero at `//sys/lost_vital_chunks/@count`. Learn more about vitality [here](../../user-guide/storage/chunks.md#vitality).
- `tablet_cells`: Checks that dynamic tables don't have any tablet cells in failed status.
- `quorum_health`: Checks that all masters are operational and in quorum.

The implementation of these checks can be viewed in [Odin](https://github.com/ytsaurus/ytsaurus/tree/main/yt/odin).

In its current implementation, Odin doesn't support alerts: it can only run checks, save the results to a dynamic table, and display them in the UI.

### Quantitative monitoring

Quantitative monitoring evaluates metrics collected in the form of time series from various cluster's subsystems. They can be used to assess load and resource consumption trends and pre-empt cluster problems that would be noticeable to the user.

Some important quantitative metrics in Prometheus:

- `yt_resource_tracker_total_cpu{service="yt-master", thread="Automaton"}`: Load on Automaton, the master's main thread of execution. The load shouldn't exceed 90%.
- `yt_resource_tracker_memory_usage_rss{service="yt-master"}`: Memory consumption by the master. The threshold must be just under half of the container's memory. The master performs fork to write a snapshot, so there must be a double memory margin.
- `yt_resource_tracker_memory_usage_rss`: A metric that is also important for other cluster components. It enables you to assess the memory consumption trend to avoid Out of Memory events.
- `yt_changelogs_available_space{service="yt-master"}`: Free space for changelogs. There is a changelogs storage setting in the master's configuration file. The amount of free space must exceed the value of this setting.
- `yt_snapshots_available_space{service="yt-master"}`: The same for shapshots.
- `yt_logging_min_log_storage_available_space{service="yt-master"}`: The same for logs.

In addition, you should monitor increases in chunks in bad states:

- `yt_chunk_server_underreplicated_chunk_count`: The number of chunks that have fewer replicas on cluster disks than specified in the replication_factor attribute. A continual growth in the number of underreplicated chunks may indicate that your [media](../../user-guide/storage/media.md) settings prevent replication_factor conditions from being met in the current cluster configuration. You can view the number of underreplicated chunks at //sys/underreplicated_chunks/@count.
- `yt_chunk_server_parity_missing_chunk_count` and `yt_chunk_server_data_missing_chunk_count`: A continual growth of parity_missing_chunk or data_missing_chunk attribute values can be caused by the erasure repair process not keeping up with the breakdown when hosts or disks fail. You can view the number of parity_missing and data_missing chunks at //sys/parity_missing_chunks/@count and //sys/data_missing_chunks/@count.

It's important to monitor [quotas](../../user-guide/storage/quotas.md) of system accounts such as sys, tmp, and intermediate, as well as user quotas of product processes. Example for a sys account:

- `yt_accounts_chunk_count{account="sys"} / yt_accounts_chunk_count_limit{account="sys"}`: Percentage of the chunk quota usage.
- `yt_accounts_node_count{account="sys"} / yt_accounts_node_count_limit{account="sys"}`: Percentage of the quota usage per number of Cypress nodes.
- `yt_accounts_disk_space_in_gb{account="sys"} / yt_accounts_disk_space_limit_in_gb{account="sys"}`: Percentage of the disk quota usage.
