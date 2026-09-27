# YT Flow Docker Example — Noop Pipeline

A minimal end-to-end YT Flow pipeline run with `docker compose` from the released Flow image.
The pipeline reads messages from a random in-memory source into a stream nothing reads — the
simplest possible computation to verify the wiring. Nothing is built from source.

The controller, two workers and the runner run on your host, each in a container of
`ghcr.io/ytsaurus/flow`; a Prometheus + Grafana stack scrapes their metrics. The YT
cluster is not included: the stack works against any cluster the host reaches.

## Released artifacts

| Artifact | Used by |
|---|---|
| `ghcr.io/ytsaurus/flow` — `flow_server`, entrypoint `/usr/bin/flow_server`, working directory `/app/pipeline` | `controller`, `worker`, `worker2`, `runner` |
| `ytsaurus-flow-yt-sync-mini` from PyPI, installed into `python:3.12-slim` | `yt-sync` |

To move to another release, change the image tag and the `yt-sync` package version in
`docker-compose.yml`; the versions are listed in the [Flow releases](../../../../docs/en/admin-guide/releases.md#flow).

Every Flow service mounts this directory at `/app/pipeline` and gets its config by a path
relative to it (`--config controller.yson`); the only variable passed from your environment is
`YT_TOKEN`.

## Walkthrough

1. Install Docker with the Compose plugin (or Podman with `podman compose`) and get a YT token
   for the cluster.
2. Put the cluster into the configs — see [Configure](#configure).
3. Check that the cluster reaches your host — see [How the components connect](#how-the-components-connect);
   otherwise apply one of [Other networks](#other-networks).
4. Optionally, generate the Grafana dashboards — see [Metrics](#metrics-prometheus--grafana).
5. Start the stack — see [Run](#run).
6. Check the pipeline — see [Check](#check).
7. Stop the stack and remove the pipeline — see [Stop](#stop).

## How the components connect

All services use host networking. The controller and the workers publish the host's address in
Cypress, and addresses resolve over IPv6 only, the default of `address_resolver`. The workers
find the controller at that address, and the runner sends its commands through the cluster's RPC
proxy, which connects to the controller's RPC port on your host. The `yt flow` commands and the UI
work the same way.

So by default the host must be reachable over IPv6 from the cluster's RPC proxies on the
controller port `9001`. If it is not, or your network is IPv4 only, patch the configs as described
in [Other networks](#other-networks).

With host networking, the controller and worker RPC ports are open on every host interface, and
the controller accepts commands without a proxy signature. Make ports `9001`-`9003` reachable only
from the cluster and from hosts you trust.

## Configure

| File | Content |
|---|---|
| `pipeline.yson` | Runner config: cluster, pipeline path, and the pipeline spec |
| `controller.yson`, `worker.yson`, `worker2.yson` | Node configs: cluster, pipeline path, and the ports of each process |
| `yt_sync.py` | Creates the pipeline node with `ytsaurus-flow-yt-sync-mini`; takes the cluster from `YT_PROXY` and the folder from `YT_FLOW_FOLDER` |

All four YSON files carry the placeholder `<cluster>` in `cluster_url`. Replace it with the
full host name of the cluster's HTTP proxy, e.g. `my-cluster.example.com`, and pass the same
value as `YT_PROXY`. A short cluster name that only your `yt` CLI configuration expands is not
resolved here:

```bash
sed -i 's|<cluster>|<your-http-proxy>|' *.yson
```

The pipeline lives at `//tmp/flow/noop/pipeline`. To use another path, change `path` in all four
files and pass its parent folder to `yt_sync.py` as `YT_FLOW_FOLDER`.

If your cluster runs in Kubernetes, RPC proxy discovery returns in-cluster addresses that the
host cannot reach. Add the `clients_cache` block from
[Reaching the cluster from outside](../../../../docs/en/_includes/flow/devops/docker-environment.md#external-access)
to all four files.

## Other networks

### IPv4

Add an `address_resolver` block that switches to IPv4 to all four YSON files:

```yson
"address_resolver" = {
    "enable_ipv4" = %true;
    "enable_ipv6" = %false;
};
```

The runner may enable both; a node config must enable exactly one of them.

### The cluster cannot reach the controller

If the cluster's RPC proxies cannot connect to your host — a NAT, a firewall, a cluster in
Kubernetes — the runner's release fails with `Cannot connect to pipeline controller leader`.
Switch to the direct mode (see [Direct runner commands](../../../../docs/en/flow/tools/cli.md#direct-controller-commands)),
where the runner sends its commands straight to the controller:

1. In `pipeline.yson`, enable it:

   ```yson
   "direct_controller_commands" = {
       "enabled" = %true;
   };
   ```

2. In `docker-compose.yml`, add `YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1` to the environment of
   the `controller` service. Without it, the controller keeps trying to confirm its leadership
   through the RPC proxy, which cannot succeed.
3. In `controller.yson`, `worker.yson` and `worker2.yson`, publish the loopback address, since the
   workers and the runner run on the same host. It must match the one enabled address family:
   `"localhost_name_override" = "::1"` with IPv6, `"localhost_name_override" = "127.0.0.1"` with
   IPv4 (next to the `enable_ipv4` and `enable_ipv6` flags from [IPv4](#ipv4)):

   ```yson
   "address_resolver" = {
       "localhost_name_override" = "::1";
   };
   ```

Only the runner has the direct mode: `yt flow` commands and the UI go through the RPC proxy and
cannot reach such a controller.

## Run

```bash
cd yt/yt/flow/examples/docker
export YT_PROXY=<your-http-proxy> YT_TOKEN=<your-token>
docker compose up -d
```

`YT_PROXY` is the same host name you put into `cluster_url`; `yt-sync` creates the pipeline there.
Set `YT_FLOW_FOLDER` as well if you changed the pipeline path. Without `-d`, the command stays
attached and streams the logs of all services; `docker compose logs -f runner` shows one of them.

The services start in this order:

| Service | Role |
|---|---|
| `yt-sync` | One-shot: creates the pipeline node and its system tables in Cypress |
| `controller` | Flow controller: schedules jobs, tracks partition state |
| `worker`, `worker2` | Flow workers: execute the `reader` computation |
| `runner` | One-shot: submits the spec and starts the pipeline, then exits |
| `prometheus` | Scrapes controller/worker `/solomon_proxy/sensors`, stores time series |
| `aggr-rules` | Generates recording rules that emulate the monitoring aggregation layer |
| `grafana` | Pre-provisioned dashboards over Prometheus |

## Check

When the `runner` container exits with code 0 (`docker compose ps -a runner`), the pipeline is
running. Check it through the monitoring ports of the nodes:

```bash
# The controller and the workers are alive.
curl http://localhost:10001/orchid/build_info
curl http://localhost:10002/orchid/build_info

# The worker is connected to the controller (look for "connected": true).
curl http://localhost:10002/orchid/worker/service

# The worker runs jobs (non-empty once the pipeline is scheduled; committed_epoch_count grows).
curl http://localhost:10002/orchid/job_tracker/jobs

# The pipeline state through the RPC proxy (prints "working"; not available in the direct mode).
yt --proxy <your-http-proxy> flow get-pipeline-state //tmp/flow/noop/pipeline
```

## Ports

| Port | Service | Purpose |
|---|---|---|
| `9001` | `controller` | RPC server |
| `10001` | `controller` | HTTP monitoring and metrics |
| `9002` | `worker` | RPC server |
| `10002` | `worker` | HTTP monitoring and metrics |
| `9003` | `worker2` | RPC server |
| `10003` | `worker2` | HTTP monitoring and metrics |
| `9090` | `prometheus` | Prometheus UI and API |
| `3000` | `grafana` | Grafana UI (anonymous admin, no login) |

## Metrics (Prometheus + Grafana)

Each Flow node serves the combined metrics of the node and its companion at
`/solomon_proxy/sensors` on its `monitoring_port`. The node configs set
`resource_tracker.cpu_to_vcpu_factor = 1.0`: outside vanilla jobs nothing supplies this factor, and
without it the vCPU sensors the dashboards' CPU panels read stay at zero.

The monitoring config lives in `yt/yt/flow/docker/monitoring/`: `prometheus.yml` (scrape config),
`aggr_rules.py` (a local stand-in for the monitoring aggregation layer that sums per-worker series
into the `host="Aggr"` series the dashboards select) and `grafana/provisioning/` (datasource and
dashboard provider). This example mounts those files and adds only its static scrape targets in
`targets/`, since `prometheus.yml` uses file-based service discovery.

The dashboards, `grafana/dashboards/ytflow-*.json`, are generated from the definitions in
`yt/admin/dashboards/yt_dashboards/flow`. Generate them once before starting Grafana:

```bash
../../docker/monitoring/grafana/dashboards/generate.sh
```

Then:

- Prometheus: <http://localhost:9090> — in **Status → Targets**, the `flow_server` targets should
  be `up`.
- Grafana: <http://localhost:3000> — open **Dashboards → YT Flow**. If you generated the
  dashboards after Grafana started, restart the `grafana` service.

## Stop

```bash
docker compose down -v
```

This stops the controller and the workers; the pipeline node stays in Cypress, and the next
`docker compose up` resubmits the spec to it. To remove it:

```bash
yt --proxy <your-http-proxy> remove -r //tmp/flow/noop
```
