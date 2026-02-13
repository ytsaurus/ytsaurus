# Fetch cluster logs

## Overview

`fetch_cluster_logs` is a Python CLI that collects YTsaurus pod logs from a Kubernetes cluster over a given time window.
It can either **copy** matching log files or **grep** them in-place on the pod (via `zstdgrep`) and bundle results per pod into a `tar.gz` archive.

Typical uses:
- Pull all YTsaurus server logs modified within a time range for incident analysis.
- Run a server-side grep across compressed logs and collect only matching lines.

---

## What it does (high-level)

1. Reads the YTsaurus **CR** (`kubectl get ytsaurus … -o json`) and derives:
   - component groups, counts, and log directories.
2. Constructs expected **pod names** from the component, group, and index.
3. Lists log files in the container (`kubectl exec … stat …`) and filters them by time.
4. For each pod:
   - **Copy mode**: `kubectl cp` selected files to a local temp dir.
   - **Grep mode** (`--regex`): runs `zstdgrep` inside the container and writes matches to local files.
5. Produces one archive per pod: `ytsaurus_logs_<unix_ts>_<podname>.tar.gz`.

---

## Requirements

- Python 3.8+ with:
  - `python-dateutil`
- CLI tools on your workstation:
  - `kubectl` (configured for the target cluster/namespace)
  - `tar`
- Access:
  - RBAC allowing `kubectl exec` and `kubectl cp` in the target namespace.

---

## Installation

```bash
# optional: create venv
python3 -m venv .venv && source .venv/bin/activate

pip install python-dateutil
# place the script on your PATH or call via `python -m`
```

Run the module from the repository root (or any location that resolves the module path):

```bash
python -m yt.yt.scripts.fetch_cluster_logs --help
```

---

## Supported components

| Component key      | Short name | Group name field (if any) |
|--------------------|------------|----------------------------|
| discovery          | `ds`       | —                          |
| primaryMasters     | `ms`       | —                          |
| masterCaches       | `msc`      | —                          |
| httpProxies        | `hp`       | `role`                     |
| rpcProxies         | `rp`       | `role`                     |
| dataNodes          | `dnd`      | `name`                     |
| execNodes          | `end`      | `name`                     |
| tabletNodes        | `tnd`      | `name`                     |
| queryTrackers      | `qt`       | —                          |
| queueAgents        | `qa`       | —                          |
| tcpProxies         | `tp`       | `role`                     |
| kafkaProxies       | `kp`       | `role`                     |
| controllerAgents   | `ca`       | —                          |
| schedulers         | `sch`      | —                          |
| yqlAgents          | `yqla`     | —                          |
| cypressProxies     | `cyp`      | —                          |
| bundleControllers  | `bc`       | —                          |

**Pod naming:** `"<short>-<group>-<index>"` (the `-<group>-` part is omitted if the group name is `"default"` or empty).
Example: `rp-internal-0`, `end-compute-a-3`, `ms-0`.

---

## Log directory detection

For each component/group the script inspects the CR spec’s `locations`:
- If `--exec-node-slot-index` is **unset**: picks the location with `"locationType": "Logs"`.
- If `--exec-node-slot-index N` is set (only valid for `execNodes`): uses `"locationType": "Slots"` and appends `/<N>`.

Fallback if not found: `/var/log`.

---

## Examples

Copy all RPC proxy logs in a window:

```bash
python -m yt.yt.scripts.fetch_cluster_logs   --kube-namespace yt-dev   --ytsaurus-name minisaurus   --component-name rpcProxies   --from-timestamp 2025-10-28T14:00:00+01:00   --to-timestamp   2025-10-28T16:00:00+01:00
```

Target a single pod:

```bash
python -m yt.yt.scripts.fetch_cluster_logs   --kube-namespace yt-dev   --ytsaurus-name minisaurus   --component-name httpProxies   --podname hp-2   --from-timestamp 2025-10-28T09:00:00+01:00   --to-timestamp   2025-10-28T11:00:00+01:00
```

Result: one archive per processed pod, e.g.
```
ytsaurus_logs_1730201234_hp-0.tar.gz
ytsaurus_logs_1730201234_hp-1.tar.gz
```
---

## Troubleshooting

- **No files found:** verify timestamps (timezone!) and that pod log dir detection is correct. Use `--podname` to test one pod.
- **Permission denied:** confirm your `kubectl` context and RBAC allow `exec` and `cp`.

