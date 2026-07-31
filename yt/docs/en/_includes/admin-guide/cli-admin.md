# CLI admin commands (`yt admin`)

The `yt admin` command group combines the administrative and diagnostic utilities for working with a {{product-name}} cluster. The commands are available through the standard `yt` CLI client (`ytsaurus-client` package).

Some commands require the additional dependencies (`kubernetes>=31.0.0` for `logs k8s`, `docker>=7.1.0` for `metrics replay`). These are installed together with the [extra `admin` set](../../api/python/start.md#extras):

{% note warning "Attention" %}

Most of the `yt admin` commands (`describe`, `logs`, `metrics`) are **experimental** and their interface may change in a future versions. Running such a command displays a warning. To disable it, set the environment variable:

```bash
export YT_SUPPRESS_ADMIN_EXPERIMENTAL_WARNING=1
```

{% endnote %}

## Common interface { #interface }

```bash
yt admin <admin_command> [<sub_command>] [options]
```

List of available subcommands:

| Command | Purpose |
| --- | --- |
| `switch-leader` | Switch the leader of a master cell |
| `describe` | Get a description of a cluster or dynamic table (attributes, configs, status) |
| `logs` | Export cluster component logs through the Kubernetes API |
| `metrics` | Export and local visualization of Prometheus metrics |
| `remove-master-unrecognized-options` | Remove unrecognized options from the master's dynamic config |

Get help for any command with `--help`:

```bash
yt admin --help
yt admin describe --help
yt admin describe cluster --help
```

---

## Switch the leader of a master cell (`switch-leader`) { #switch-leader }

Switches the leader of the specified master cell to a given address.

```bash
yt admin switch-leader --cell-id <cell-id> --new-leader-address <address>
```

| Parameter | Description |
| --- | --- |
| `--cell-id` | Master cell ID |
| `--new-leader-address` | Address of the master that should become the new leader |

### Example

```bash
yt admin switch-leader \
  --cell-id 65726e65-ad6b7562-10259-79747361 \
  --new-leader-address ms-1.masters.ytsaurus-dev.svc.cluster.local:9010
```

---

## Get a description of a cluster or dynamic table (`describe`) { #describe }

Collects a description of the cluster or the individual dynamic tables (attributes, configurations, current state) and saves it to a file.

The result is saved to a file with a timestamp in its name, for example `describe/cluster_2026-06-03T12-00-00Z.yson`.

Common output options for both subcommands:

| Parameter | Default | Description |
| --- | --- | --- |
| `--format {yson,json,yaml}` | `yson` | Output file format |
| `-o, --output <dir>` | `describe` | Directory for saving the result |

### Cluster description (`cluster`) { #describe-cluster }

Collects general information about cluster components and their configurations.

```bash
yt admin describe cluster [--nodes] [--bundles] [--dynamic-configs] [--static-configs] [-o dir] [--format ...]
```

| Parameter | Description |
| --- | --- |
| `--nodes` | Information about cluster nodes (`//sys/cluster_nodes`) |
| `--bundles` | Information about dynamic table bundles (`//sys/tablet_cell_bundles`) |
| `--dynamic-configs` | Dynamic configurations of the master, scheduler, controller agents, nodes, and proxies |
| `--static-configs` | Static configurations of nodes (one per flavor) |

If none of the `--nodes`, `--bundles`, `--dynamic-configs`, `--static-configs` flags are specified, **all** information is collected at once.

#### Examples

Collect cluster information in YSON:

```bash
yt admin describe cluster
```

Collect only node and bundle information in JSON into the `debug` directory:

```bash
yt admin describe cluster --nodes --bundles --format json -o debug
```

### Dynamic table description (`table`) { #describe-table }

Collects a detailed set of the attributes for one or more dynamic tables, including "heavy" attributes (`tablets`, chunk statistics, and the compression statistics). One or more paths to the dynamic tables are passed as arguments.

```bash
yt admin describe table <path> [<path> ...] [-o dir] [--format ...]
```

#### Example

```bash
yt admin describe table //home/project/table1 //home/project/table2 --format yaml -o tables-dump
```

---

## Exporting logs of cluster components (`logs`) { #logs }

Downloads logs of {{product-name}} cluster components deployed in Kubernetes. Only one backend is available — `k8s`, which operates through the Kubernetes API.

```bash
yt admin logs k8s [options] <cluster_name> <component_name> [<group_name>]
```

The utility retrieves pod names and directory paths from the cluster specification, then finds the required logs in the `ytserver` container. It then does one of two things:
- Downloads entire log files.
- Runs `grep` on the server side and saves only the lines that match the query.

### Requirements

- Access to a Kubernetes cluster (`kubeconfig` or in-cluster configuration).
- RBAC permissions for `get` on CR `ytsaurus` and `exec` on pods in the required namespace.
- Installed Python package `kubernetes` (included in `ytsaurus-client[admin]`).

### Positional arguments

| Argument | Default | Description |
| --- | --- | --- |
| `cluster_name` | — | Name of the {{product-name}} cluster (CR name, see `kubectl get ytsaurus`) |
| `component_name` | — | Component type (see the table below) |
| `group_name` | `default` | Name of the component instance group |

### Options

| Parameter | Default | Description |
| --- | --- | --- |
| `-n, --namespace <ns>` | `default` | Kubernetes namespace where the cluster is located |
| `-p, --pods <pod>` | — | Specific pod (can be specified multiple times) |
| `--exec-slot-index <N>` | — | Slot index for job-proxy logs (for `exec_nodes` only) |
| `--from-ts <ISO8601>` | — | Logs modified after the specified time, in the format `2026-06-15T14:00:00Z` |
| `--to-ts <ISO8601>` | — | Logs created before the specified time, in the format `2026-06-15T14:00:00Z` |
| `-w, --writer <name>` | — | Filter by writer name from the config (can be specified multiple times). The method for getting the list of writers is shown below |
| `--writer-force <regex>` | — | Force regex for filtering log file names |
| `-o, --output <dir>` | `logs` | Directory for saving logs |
| `--grep <regex>` | — | Server-side grep on content (with auto-decompression of `.zstd`/`.gz`) |
| `-y, --yes` | — | Skip the initial confirmation prompt for download. Does not disable the prompt about overwriting existing local files |

### Supported components

| Component key | Short name | CR field | Group field |
| --- | --- | --- | --- |
| `discovery` | `ds` | discovery | — |
| `primary_masters` | `ms` | primaryMasters | — |
| `master_caches` | `msc` | masterCaches | — |
| `http_proxies` | `hp` | httpProxies | `role` |
| `rpc_proxies` | `rp` | rpcProxies | `role` |
| `data_nodes` | `dnd` | dataNodes | `name` |
| `exec_nodes` | `end` | execNodes | `name` |
| `tablet_nodes` | `tnd` | tabletNodes | `name` |
| `queue_agents` | `qa` | queueAgents | — |
| `tcp_proxies` | `tp` | tcpProxies | `role` |
| `kafka_proxies` | `kp` | kafkaProxies | `role` |
| `schedulers` | `sch` | schedulers | — |
| `controller_agents` | `ca` | controllerAgents | — |
| `query_trackers` | `qt` | queryTrackers | — |
| `yql_agents` | `yqla` | yqlAgents | — |
| `cypress_proxies` | `cyp` | cypressProxies | — |
| `bundle_controllers` | `bc` | bundleController | — |

Pod names follow the pattern `<short_name>-<group>-<index>` (the `-<group>-` part is omitted for the `default` group). Examples: `ms-0`, `rp-internal-0`.

### Examples

Master logs for a time interval with two writers:

```bash
yt admin logs k8s ytbench primary_masters \
  -w access -w debug \
  --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
  -o case-1
```

Only specific pods:

```bash
yt admin logs k8s ytbench primary_masters -w debug \
  --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
  -p ms-3 -p ms-4 -o case-1
```

Job-proxy slot logs on an exec node:

```bash
yt admin logs k8s ytbench exec_nodes -w debug \
  --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
  --exec-slot-index 17 -p end-6 -o case-2
```

Server-side grep on content:

```bash
yt admin logs k8s ytbench primary_masters -w debug --grep "Transaction aborted" -o case-3
```

To get the list of available writers for a component, check its configuration in the CR. For example, for `primary_masters`:

```bash
kubectl -n ytsaurus-dev get ytsaurus -o json | jq -r '
  .items[0].spec.primaryMasters |
  (.loggers[]? | select(.writerType == "file") | .name),
  (.structuredLoggers[]? | .name)
'
```

### Output structure

```
<output_dir>/
  <pod_name>/
    <log_file>
```

For exec node slots:

```
<output_dir>/
  <pod_name>/
    slot/
      <slot_id>/
        <log_file>
```

{% note info "Note" %}

The utility uses inode-based file identification to correctly download logs even if they are rotated between the listing and download stages. When using `--grep`, compressed files (`.zstd`, `.gz`) are automatically decompressed, and the archive extension is removed from the output file name.

{% endnote %}

---

## Export and local visualization of Prometheus metrics (`metrics`) { #metrics }

Tools for working with metrics: validating the export specification, exporting metrics from Prometheus to an archive, and replaying the archive locally using Prometheus + Grafana, which are deployed locally in Docker.

```bash
yt admin metrics <validate|dump|replay> [options]
```

#### `spec.yaml` format { #metrics-spec }

The specification describes which metrics to export. File structure:

- `defaults.step` — default sampling step (can be overridden with the `--step` option).
- `targets` — list of targets, each with its own `type`:
  - `type: metric` — specifies a PromQL query in the `query` field; selectors are extracted from the query.
  - `type: dashboard` — path to a Grafana dashboard JSON file in the `path` field (relative to `spec.yaml`); `expr` values are extracted from all dashboard panels. Dashboard variable values are taken from its `templating` and can be overridden with a `params` dict.

Example `spec.yaml`:

```yaml
defaults:
  step: 30s

targets:
  - type: metric
    query: yt_resource_tracker_total_cpu{service="master"}

  - type: dashboard
    path: master-accounts.json
    params:
      cluster: clusrer-name
      account: sys
      left_medium: default
      right_medium: ssd_blobs
```

The `params` field specifies dashboard variable values. If a variable is not set, its default value from the dashboard is used. You can [get the dashboard file (the `path` field) from the `monitoring` image or build it yourself](#dashboards).

Selectors from multiple targets are merged and deduplicated; you can view the resulting list with the `metrics validate` command.

#### Dashboard for a `type: dashboard` target { #dashboards }

The `path` field of a `type: dashboard` target specifies a Grafana dashboard JSON file. Pre-built {{product-name}} dashboards are stored in the repository at [`yt/admin/dashboards/yt_dashboards/tests_os/canondata`](https://github.com/ytsaurus/ytsaurus/tree/main/yt/admin/dashboards/yt_dashboards/tests_os/canondata). You can download the required file directly from there and specify its path in `path`. For example, for the `master-accounts` dashboard:

```bash
curl -O https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/admin/dashboards/yt_dashboards/tests_os/canondata/test_dashboards.test_master_accounts/master-accounts.json
```

### Validate the metrics specification (`validate`) { #metrics-validate }

Validates the metrics specification (`spec.yaml`) and prints a list of unique selectors.

```bash
yt admin metrics validate --spec spec.yaml [--target '<json>' ...]
```

| Parameter | Description |
| --- | --- |
| `--spec <path>` | Path to `spec.yaml` (required) |
| `--target <json>` | Additional inline target in JSON, for example `'{"type":"metric","query":"yt_x"}'` |

### Export metrics to an archive (`dump`) { #metrics-dump }

Exports metrics from Prometheus for a specified time range to a zip archive.

```bash
yt admin metrics dump --spec spec.yaml \
  --from-ts <ISO8601> --to-ts <ISO8601> \
  --prometheus-url <url> [--step <step>] [--output metrics.zip] [--max-series N] [--force]
```

| Parameter | Default | Description |
| --- | --- | --- |
| `--spec <path>` | — | Path to `spec.yaml` (required) |
| `--from-ts <ISO8601>` | — | Start of the time range (required) |
| `--to-ts <ISO8601>` | — | End of the time range (required) |
| `--prometheus-url <url>` | — | Base URL of Prometheus (required) |
| `--step <step>` | from spec | Sampling step |
| `--target <json>` | — | Additional inline target |
| `--output <path>` | `metrics.zip` | Path to the output archive |
| `--max-series <N>` | `100000` | If the total number of time series to export exceeds this value, confirmation is requested |
| `--force` | — | Skip confirmation when `--max-series` is exceeded |

{% note info "Note" %}

If the `PROMETHEUS_USER` and `PROMETHEUS_PASSWORD` environment variables are set, Basic Auth with these values is used for requests to Prometheus. Otherwise, requests are made without authentication.

{% endnote %}

### Local replay of metrics (`replay`) { #metrics-replay }

Imports metrics from a previously exported archive into a temporary directory and starts local Prometheus and Grafana containers on top of it for viewing.

```bash
yt admin metrics replay <archive.zip> [--prometheus-port N] [--grafana-port N]
```

| Parameter | Default | Description |
| --- | --- | --- |
| `archive` | — | Path to the metrics archive (`.zip`) |
| `--prometheus-port <N>` | free port | Port for Prometheus |
| `--grafana-port <N>` | free port | Port for Grafana |

#### Requirements

- Access to the docker daemon (Prometheus and Grafana containers are launched through it).
- The `docker` Python package installed (included in `ytsaurus-client[admin]`).

### Example scenario

```bash
# 1. Validate the specification
yt admin metrics validate --spec spec.yaml

# 2. Export metrics for a time range (defaults to metrics.zip)
yt admin metrics dump --spec spec.yaml \
  --from-ts 2026-06-03T10:00:00Z --to-ts 2026-06-03T11:00:00Z \
  --prometheus-url https://prometheus.example.com

# 3. Replay locally
yt admin metrics replay metrics.zip
```

---

## Remove unrecognized options from the master config (`remove-master-unrecognized-options`) { #remove-master-unrecognized-options }

Removes unrecognized options from the master dynamic config (`//sys/@config`).

The command reads `//sys/@master_alerts`, finds the alert `Found unrecognized options in dynamic cluster config`, and removes from `//sys/@config` all options listed in it. After removing nested options, empty intermediate nodes (`{}`) are also removed. If no such alert exists, the command does nothing.

This is most often needed [after updating a cluster](../../admin-guide/update-ytsaurus.md#operator): in a new version of {{product-name}}, some config fields may become obsolete and remain in `//sys/@config` as unrecognized.

```bash
yt admin remove-master-unrecognized-options [--dry] [--do-not-print-config]
```

| Parameter | Default | Description |
| --- | --- | --- |
| `--dry` | `False` | Only print the unrecognized options without removing them |
| `--do-not-print-config` | `False` | Do not print `//sys/@config` before removal (the config is printed by default) |

### Example

```bash
# View which options will be removed without making any changes
yt admin remove-master-unrecognized-options --dry

# Remove unrecognized options
yt admin remove-master-unrecognized-options
```

---

## Running utilities as standalone scripts { #standalone-scripts }

Some utilities are additionally packaged as standalone programs in the [`yt/yt/scripts/`](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/scripts) directory. They reuse the same `yt.admin.*` modules as `yt admin` and can be called without installing the full CLI — for example, in environments where it is more convenient to run a module directly.

| Script | `yt admin` equivalent | Description |
| --- | --- | --- |
| `fetch_cluster_info` | `describe cluster` / `describe table` | Describes the cluster and dynamic tables |
| `fetch_cluster_logs` | `logs k8s` | Exports logs from Kubernetes |
| `fetch_cluster_metrics` | `metrics validate/dump/replay` | Works with metrics |
| `remove_master_unrecognized_options` | `remove-master-unrecognized-options` | Removes unrecognized options from the master dynamic config |
