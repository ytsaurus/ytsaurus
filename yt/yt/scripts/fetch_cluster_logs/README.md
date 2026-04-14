# Fetch cluster logs

## Overview

> **NOTE**: This tool is experimental and its interface may change.

`fetch_cluster_logs` is a Python CLI tool that collects YTsaurus pod logs from a Kubernetes cluster over a given time window.
It can either **copy** matching log files or **grep** them in-place on the pod and save results locally.

Typical uses:
- Pull all YTsaurus server logs modified within a time range for incident analysis.
- Run a server-side grep across compressed logs and collect only matching lines.
- Filter logs by specific writers.

---

## What it does (high-level)

### Phase 1: Discovery
1. Reads the YTsaurus **Custom Resource** (CR) via Kubernetes API and derives:
   - component groups, replica counts, and log directories from the CR spec.
2. Constructs expected **pod names** from the component type, group name, and index.
3. Lists log files in the container using `kubectl exec` and filters them by:
   - modification/creation time (`--from-ts`, `--to-ts`)
   - writer name patterns (`--writer`)
   - custom regex patterns (`--writer-force`)
4. Displays statistics about found files and prompts for confirmation (unless `--yes` is used).

### Phase 2: Download
1. Creates output directory structure.
2. Streams file content through `kubectl exec` and saves locally.
3. For each matching file:
   - **Copy mode**: downloads the file directly to local directory.
   - **Grep mode** (`--grep`): runs grep (with decompression for `.zstd`/`.gz` files) inside the container and saves only matching lines.
4. Uses inode-based file identification to handle log rotation safely.

---

## Requirements

- Python 3.8+ with:
  - `python-dateutil`
  - `kubernetes`
  - `ytsaurus-client`
- Access:
  - Kubernetes cluster access with kubeconfig or in-cluster configuration
  - RBAC permissions allowing `get` on YTsaurus CRs and `exec` on pods in the target namespace

---

## Installation

```bash
# optional: create venv
python3 -m venv .venv && source .venv/bin/activate

pip install python-dateutil kubernetes ytsaurus-client
# place the script on your PATH or call via `python -m`
```

Run the module from the repository root (or any location that resolves the module path):

```bash
python -m yt.yt.scripts.fetch_cluster_logs --help
```

---

## Supported components

| Component key        | Short name | CRD field         | Config suffix      | Group field |
|----------------------|------------|-------------------|--------------------|-------------|
| discovery            | `ds`       | discovery         | discovery          | —           |
| primary_masters      | `ms`       | primaryMasters    | master             | —           |
| master_caches        | `msc`      | masterCaches      | master-cache       | —           |
| http_proxies         | `hp`       | httpProxies       | http-proxy         | `role`      |
| rpc_proxies          | `rp`       | rpcProxies        | rpc-proxy          | `role`      |
| data_nodes           | `dnd`      | dataNodes         | data-node          | `name`      |
| exec_nodes           | `end`      | execNodes         | exec-node          | `name`      |
| tablet_nodes         | `tnd`      | tabletNodes       | tablet-node        | `name`      |
| query_trackers       | `qt`       | queryTrackers     | query-tracker      | —           |
| queue_agents         | `qa`       | queueAgents       | queue-agent        | —           |
| tcp_proxies          | `tp`       | tcpProxies        | tcp-proxy          | `role`      |
| kafka_proxies        | `kp`       | kafkaProxies      | kafka-proxy        | `role`      |
| controller_agents    | `ca`       | controllerAgents  | controller-agent   | —           |
| schedulers           | `sch`      | schedulers        | scheduler          | —           |
| yql_agents           | `yqla`     | yqlAgents         | yql-agent          | —           |
| cypress_proxies      | `cyp`      | cypressProxies    | cypress-proxy      | —           |
| bundle_controllers   | `bc`       | bundleController  | bundle-controller  | —           |

**Pod naming:** `<short>-<group>-<index>` (the `-<group>-` part is omitted if the group name is `default`).

Examples: `rp-internal-0`, `end-compute-a-3`, `ms-0`.

---

## Log directory detection

For each component/group the script inspects the CR spec's `locations`:
- If `--exec-slot-index` is **unset**: picks the location with `locationType: "Logs"`.
- If `--exec-slot-index N` is set (only valid for `exec_nodes`): uses `locationType: "Slots"` and appends `/<N>`.

Fallback if not found: `/var/log`.

---

## Usage

```bash
python -m yt.yt.scripts.fetch_cluster_logs [-h] [-n NAMESPACE] [-p PODS] [--exec-slot-index EXEC_SLOT_INDEX]
  [--from-ts ISO8601] [--to-ts ISO8601] [-w WRITER]
  [--writer-force regex] [-o dir] [--grep regex] [--yes]
  cluster_name component_name [group_name]
```

### Positional Arguments

- `cluster_name` - YTsaurus cluster name (CR name in Kubernetes)
- `component_name` - Component type (see supported components table)
- `group_name` - Component group name (default: `default`)

### Options

- `-h, --help` - Show help message and exit
- `-n NAMESPACE, --namespace NAMESPACE` - Kubernetes namespace (default: `default`)
- `-p PODS, --pods PODS` - Specific pod name to fetch logs from (can be specified multiple times)
- `--exec-slot-index EXEC_SLOT_INDEX` - Slot index for job proxy logs (only for `exec_nodes`)
- `--from-ts ISO8601` - Filter logs modified after this time (e.g., `2026-02-07T14:47:40Z`)
- `--to-ts ISO8601` - Filter logs created before this time (e.g., `2026-02-07T14:47:40Z`)
- `-w WRITER, --writer WRITER` - Filter by writer name from config (can be specified multiple times)
- `--writer-force regex` - Force add regex pattern for filename filtering (can be specified multiple times)
- `-o dir, --output dir` - Output directory path (default: `logs`)
- `--grep regex` - Filter log content by regex (decompresses `.zstd`/`.gz` files automatically)
- `--yes, -y` - Skip confirmation prompt

---

## Examples

### Example 1: Fetch primary masters logs with multiple writers

```bash
$ python -m yt.yt.scripts.fetch_cluster_logs ytbench primary_masters --writer access -w debug \
    --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z --output case-1
```

### Example 2: Target specific pods

```bash
$ yt admin logs-via-k8s ytbench primary_masters --writer access -w debug --from-ts 2026-02-07T14:38:01Z \
    --to-ts 2026-02-07T14:48:01Z --output case-1 --pod ms-3 -p ms-4
```

### Example 3: Fetch logs from a specific group

```bash
$ python -m yt.yt.scripts.fetch_cluster_logs ytbench data_nodes second -w debug \
    --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z --output case-2
```

### Example 4: Fetch exec node slot logs

```bash
$ python -m yt.yt.scripts.fetch_cluster_logs ytbench exec_nodes -w debug \
    --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
    --output case-2 --exec-slot-index 17 --pod end-6
```

---

## Output Structure

Files are organized as:
```
<output_dir>/
  <pod_name>/
    <log_file_1>
    <log_file_2>
    ...
```

For exec node slots:
```
<output_dir>/
  <pod_name>/
    slot/
      <slot_id>/
        <log_file_1>
        <log_file_2>
        ...
```

Example output structure:
```bash
$ find case-2
case-2
case-2/dnd-second-0
case-2/dnd-second-0/data-node.debug.log.zstd.001
case-2/end-6
case-2/end-6/slot
case-2/end-6/slot/17
case-2/end-6/slot/17/job-proxy.debug.log.zstd.2026-02-07T14:38:53Z
case-2/end-6/slot/17/job-proxy.debug.log.zstd.2026-02-07T14:44:20Z
```

When using `--grep`, compressed files (`.zstd`, `.gz`) are automatically decompressed and the extension is removed from the output filename.

---

## How Writer Filtering Works

When you specify `-w WRITER_NAME`, the tool:
1. Reads the component's config file (`/config/ytserver-<component>.yson`)
2. Extracts the `logging.writers` section
3. Filters out non-file writers (keeps only writers with `type: "file"`)
4. Finds the `file_name` for the specified writer
5. Creates a regex pattern to match log files from that writer

Example: if writer `debug` has `file_name: "/yt/master-logs/master.debug.log.zstd"`, the tool will match files like `master.debug.log.zstd*`.

**Important:** Different components have different available writers. The tool will show available writers in the debug output if a requested writer is not found.

---

## Troubleshooting

- **Writer not found:** 
  - Check the debug output to see available writers for the component
  - Different components have different writers
  - Use `--writer-force` with a regex pattern as a workaround

- **No files found:** 
  - Verify timestamps (check timezone!)
  - Ensure log directory detection is correct
  - Use `-p` to test with a single pod
  - Check that the component/group exists in the cluster CR

- **Permission denied:** 
  - Confirm your kubeconfig context is correct
  - Verify RBAC permissions allow `get` on YTsaurus CRs and `exec` on pods

- **Grep returns no results:**
  - Empty files are automatically removed when using `--grep`
  - Verify your regex pattern is correct
  - Try without `--grep` first to see if files exist

---

## Advanced Features

### Conflict Resolution

When downloading files that already exist locally, the tool will prompt:
- `r` - replace this file
- `R` - replace all files (no more prompts)
- `s` - skip this file
- `S` - skip all conflicts (no more prompts)

Use `-y` to skip the initial download confirmation, but conflict resolution will still prompt unless you choose `R` or `S`.

### Inode-based File Identification

The tool uses inode numbers to ensure it downloads the exact file it listed, even if the filename changes between listing and downloading. This prevents race conditions in log rotation scenarios.

**Implementation:**
- During file listing, the inode of each file is recorded
- During download, `find <dir> -maxdepth 1 -inum <inode> -exec ...` is used
- Inside `-exec`, a Python script opens the file and verifies its inode matches the expected value before reading
- This guarantees that even if the file was renamed (rotated) between listing and downloading, the correct content is downloaded

### Automatic Decompression for Grep

When using `--grep` with compressed files (`.zstd` or `.gz`), the tool automatically:
1. Detects the compression format from the file extension
2. Pipes through the appropriate decompressor (`zstd -dcf` or `gzip -dcf`)
3. Applies grep to the decompressed stream
4. Saves results without the compression extension

---

## Implementation Details

### Two-Phase Operation

**Phase 1: Discovery**
1. Based on namespace and cluster name, determines which pods exist and their log directories
2. Collects list of log files
3. Filters by time and `--writer` (reads `config-component.yson` to map writer names to filenames)
4. Shows statistics and waits for confirmation

**Phase 2: Download**
1. Waits for confirmation or uses `-y` flag
2. Creates directory structure
3. Streams file content through `kubectl exec` and saves locally
4. Handles log rotation safely using inode-based identification

### Technical Details

- Uses Kubernetes Python client for API access (no `kubectl` subprocess calls)
- Streams file content to avoid memory issues with large files
- Supports both kubeconfig and in-cluster authentication
- Filters files by both modification time (`mtime`) and creation time (`ctime`)
- Provides detailed statistics per pod and per writer type
- Uses `find -inum` to locate files by inode for reliability
- Heavy library imports (`kubernetes`) are only performed when the tool is invoked, not on module import
