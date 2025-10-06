# Monitoring

## Setting up Prometheus
### Prerequisites
- A running {{product-name}} cluster;
- Installed Odin [following the instructions](../../admin-guide/install-odin.md);
- Installed prometheus-operator [following the instructions](https://github.com/prometheus-operator/prometheus-operator#quickstart).

### Installation

Services for monitoring are created automatically by the {{product-name}} operator and Odin Helm charts.

To collect metrics do the following:
1. Create a [ServiceMonitor](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_monitor.yaml) for collecting metrics from {{product-name}} components.
2. Create a ServiceMonitor for collecting Odin metrics by setting `metrics.serviceMonitor.enable: true` in the Helm chart values (it is not created by default).
3. Create a [service account](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_account.yaml).
4. Grant the created account a [role](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus_role_binding.yaml).
5. [Create Prometheus](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus.yaml).

## Monitoring methods and important {{product-name}} metrics

The monitoring of technical systems can be roughly classified into two general approaches: let's call them qualitative monitoring and quantitative monitoring.

### Qualitative monitoring { #odin }

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
- `clock_quorum_health` (disabled by default) – checks that all clock servers are running and in quorum.
- `suspicious_jobs` – checks that there are no jobs on the cluster that are stalling without making progress.
- `tablet_cell_snapshots` – checks that tablet cell snapshots are being created on time.
- `scheduler_uptime` – checks that the scheduler is running stably and not reconnecting to the master.
- `controller_agent_count` – checks that a sufficient number of controller agents are connected.
- `controller_agent_uptime` – checks that controller agents are running stably and not reconnecting to the scheduler.
- `operations_snapshots` – checks that snapshots are being regularly built for operations.
- `operations_count` – checks that the number of operations in Cypress has not grown excessively.
- `dynamic_table_replication` (disabled by default) – checks that replication for replicated dynamic tables is working in the cluster.
- `register_watcher` – checks that cluster nodes are running stably and not reconnecting to the master too frequently.
- `tmp_node_count` – checks that the number of objects in the `//tmp` directories does not exceed a reasonable threshold.
- `destroyed_replicas_size` – checks that the system is cleaning up replicas of obsolete (deleted) chunks in time.
- `query_tracker_yql_liveness` – checks the health of YQL queries through Query Tracker.
- `query_tracker_chyt_liveness` (disabled by default) – checks the health of CHYT queries through Query Tracker.
- `query_tracker_ql_liveness` (disabled by default) – checks the health of YT QL queries through Query Tracker.
- `query_tracker_dq_liveness` (disabled by default) – checks the liveness of DQ.
- `controller_agent_operation_memory_consumption` – checks memory consumption by operations in controller agents.
- `discovery` (disabled by default) – checks that a sufficient number of discovery server instances are running.
- `master` – checks that a `get` operation for the Cypress root executes successfully.
- `medium_balancer_alerts` – checks that the Medium Balancer is running without errors.
- `oauth_health` – checks that the Odin token is successfully authorized.
- `operations_archive_tablet_store_preload` – checks that system tables required for the operations archive are successfully preloaded into memory.
- `proxy` – checks that HTTP proxies for heavy query execution are running and operational.
- `queue_api` (disabled by default) – creates a queue and a consumer, attempts to write/read data, i.e. checks that the Queue API is functioning correctly.
- `queue_agent_alerts` (disabled by default) – checks that a sufficient number of Queue Agent instances are running and that there are no alerts on them.
- `query_tracker_alerts` – checks that a sufficient number of Query Tracker instances are running and that there are no alerts on them.
- `scheduler` – checks that the scheduler is running (its orchid is available).
- `scheduler_alerts_jobs_archivation` – checks that there are no errors archiving jobs on nodes.
- `scheduler_alerts_update_fair_share` – checks that there are enough compute resources to satisfy guarantees.
- `stuck_missing_part_chunks`, `missing_part_chunks` – check that there are no erasure-coded data parts that have been unavailable for a long time and cannot be restored.
- `unaware_nodes` (disabled by default) – checks that there are no nodes in the cluster that are not assigned to any rack.
- `operations_satisfaction` – checks that operations running in the cluster receive their fair share of resources.

The implementation of these checks can be viewed in [Odin](https://github.com/ytsaurus/ytsaurus/tree/main/yt/odin).

### Quantitative monitoring

Quantitative monitoring evaluates metrics collected in the form of time series from various cluster's subsystems. They can be used to assess load and resource consumption trends and pre-empt cluster problems that would be noticeable to the user.

Some important quantitative metrics in Prometheus:

- `yt_resource_tracker_total_cpu{service="yt-master", thread="Automaton"}`: Load on Automaton, the master's main thread of execution. The load shouldn't exceed 90%.
- `yt_resource_tracker_memory_usage_rss{service="yt-master"}`: Memory consumption by the master. The threshold must be just under half of the container's memory. The master performs fork to write a snapshot, so there must be a double memory margin.
- `yt_resource_tracker_memory_usage_rss`: A metric that is also important for other cluster components. It enables you to assess the memory consumption trend to avoid Out of Memory events.
- `yt_changelogs_available_space{service="yt-master"}`: Free space for changelogs. There is a changelogs storage setting in the master's configuration file. The amount of free space must exceed the value of this setting.
- `yt_snapshots_available_space{service="yt-master"}`: The same for snapshots.
- `yt_logging_min_log_storage_available_space{service="yt-master"}`: The same for logs.

In addition, you should monitor increases in chunks in bad states:

- `yt_chunk_server_underreplicated_chunk_count`: The number of chunks that have fewer replicas on cluster disks than specified in the replication_factor attribute. A continual growth in the number of underreplicated chunks may indicate that your [media](../../user-guide/storage/media.md) settings prevent replication_factor conditions from being met in the current cluster configuration. You can view the number of underreplicated chunks at //sys/underreplicated_chunks/@count.
- `yt_chunk_server_parity_missing_chunk_count` and `yt_chunk_server_data_missing_chunk_count`: A continual growth of parity_missing_chunk or data_missing_chunk attribute values can be caused by the erasure repair process not keeping up with the breakdown when hosts or disks fail. You can view the number of parity_missing and data_missing chunks at //sys/parity_missing_chunks/@count and //sys/data_missing_chunks/@count.

It's important to monitor [quotas](../../user-guide/storage/quotas.md) of system accounts such as sys, tmp, and intermediate, as well as user quotas of product processes. Example for a sys account:

- `yt_accounts_chunk_count{account="sys"} / yt_accounts_chunk_count_limit{account="sys"}`: Percentage of the chunk quota usage.
- `yt_accounts_node_count{account="sys"} / yt_accounts_node_count_limit{account="sys"}`: Percentage of the quota usage per number of Cypress nodes.
- `yt_accounts_disk_space_in_gb{account="sys"} / yt_accounts_disk_space_limit_in_gb{account="sys"}`: Percentage of the disk quota usage.
