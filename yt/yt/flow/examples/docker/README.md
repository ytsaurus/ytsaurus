# YT Flow Docker Example — Noop Pipeline

A minimal end-to-end YT Flow pipeline runnable via `docker compose`. The
pipeline reads messages from a random in-memory source and discards them —
the simplest possible computation to verify the infrastructure wiring.

The `ya` commands below use the `ya` tool shipped at the repository root
(see [BUILD.md](../../../../../BUILD.md)); add it to `PATH` first:

```bash
export PATH="$(pwd):$PATH"   # from the repository root
```

## Build Image

A single image `flow-noop-pipeline:local` is built from
the `flow-pipeline` Dockerfile target:

```bash
ya package yt/yt/flow/examples/docker/package-noop.json --custom-version local --docker-registry ""
```

It contains two binaries:

| Binary | Path | Purpose |
|---|---|---|
| `noop_pipeline` | `/usr/bin/noop_pipeline` | Serves all flow roles: Controller, Worker, and spec submitter |
| `yt_sync` | `/usr/bin/yt_sync` | Creates required Cypress objects in YT (one-shot) |

## YT Cluster

The compose stack runs only the Flow components. The YT cluster is **not**
included; point the example at any YT via the `YT_CLUSTER` env var.

`YT_CLUSTER` is the address of the cluster's HTTP proxy — the same value the
`yt` CLI accepts as `--proxy`: a `<host>`, `<host>:<port>`, or full
`http://<host>:<port>` URL. It is substituted as `cluster_url` into the Flow
configs, so the proxy must be reachable from the host network namespace.

For a real YT cluster, pass a real YT token when starting the stack:

```bash
YT_CLUSTER=<cluster> \
YT_TOKEN=<your-token> \
...
```

## Runtime Config

`controller.yson`, `worker.yson`, and `pipeline.yson` carry literal
placeholders for `cluster_url` and the pipeline path. Each flow container
performs the substitution at startup into a writable copy before launching the
flow binary. The yt-sync container reads `TEST_CLUSTER=$YT_CLUSTER` directly.

For real YT clusters, the Controller and Worker publish a host IPv6 address so
YT proxies can call back into them.

Some Flow requests executed through a real YT cluster call back into the
Controller. A Docker-private address is not routable from YT infrastructure, 
so the compose stack uses host networking and advertises a host address that YT can reach.

Controller listens on `9001`, the two workers on `9002` and `9003`; monitoring
is available on `10001`-`10003`. The Controller RPC port must be reachable from
the YT cluster for callback RPCs to work.

The containers auto-detect `YT_FLOW_PUBLIC_ADDRESS` from the host route used to
reach `YT_CLUSTER`. Set `YT_FLOW_PUBLIC_ADDRESS` explicitly to override detection.

By default, the example uses `//tmp/$USER/flow/noop` as its YT path. Override
it with `YT_FLOW_PATH` if needed:

```bash
YT_FLOW_PATH=//tmp/$(whoami)/flow/noop \
YT_CLUSTER=<cluster> \
YT_TOKEN=<your-token> \
docker compose up
```

The image has no default entrypoint. Each `docker-compose.yml` service sets
its own entrypoint and environment:

| Service | Entrypoint | Key env |
|---|---|---|
| `yt-sync` | `yt_sync --scenario ensure --stage test --commit` | `TEST_CLUSTER` |
| `controller` | `noop_pipeline --config /tmp/config.yson` | `YT_FLOW_MODE=Controller`, `CONFIG_SRC=/app/ytflow/controller.yson` |
| `worker` | `noop_pipeline --config /tmp/config.yson` | `YT_FLOW_MODE=Worker`, `CONFIG_SRC=/app/ytflow/worker.yson` |
| `runner` | `noop_pipeline --config /tmp/config.yson` | `YT_FLOW_WAIT=0`, `CONFIG_SRC=/app/ytflow/pipeline.yson` |

## Services

| Service | Role |
|---|---|
| `yt-sync` | One-shot: creates pipeline node, queues, and tables in Cypress |
| `controller` | Flow Controller — schedules jobs, tracks partition state |
| `worker`, `worker2` | Flow Workers — execute `TNoopComputation` jobs |
| `runner` | One-shot: submits the pipeline spec to the controller, then exits |
| `prometheus` | Scrapes controller/worker `/solomon_proxy/sensors`, stores time series |
| `aggr-rules` | Generates recording rules that emulate the monitoring aggregation layer |
| `grafana` | Pre-provisioned dashboard over Prometheus |

## Ports

| Port | Service | Purpose |
|---|---|---|
| `10001` | `controller` | HTTP monitoring / Solomon metrics |
| `9001` | `controller` | RPC server |
| `10002` | `worker` | HTTP monitoring / Solomon metrics |
| `9002` | `worker` | RPC server |
| `10003` | `worker2` | HTTP monitoring / Solomon metrics |
| `9003` | `worker2` | RPC server |
| `9090` | `prometheus` | Prometheus UI / API |
| `3000` | `grafana` | Grafana UI (anonymous admin, no login) |

## Metrics (Prometheus + Grafana)

The stack ships built-in Prometheus + Grafana. Each flow node already runs an HTTP 
monitoring server (the `monitoring_port` in `controller.yson` / `worker.yson`) 
serving the combined YT Solomon exporter (node + companion) at `/solomon_proxy/sensors`.

The monitoring config lives in
`yt/yt/flow/docker/monitoring/` (`prometheus.yml` scrape config,
`aggr_rules.py` — a local stand-in for the production aggregation layer that
sums per-worker series into the `host="Aggr"` series the dashboards select,
`grafana/provisioning/` datasource + dashboard provider). This example mounts
those files and adds only its static scrape `targets/` (fixed controller/worker
ports), since the `prometheus.yml` uses file-based service discovery.

The dashboards themselves, `grafana/dashboards/ytflow-*.json`, are **generated**
from the definitions in `yt/admin/dashboards/yt_dashboards/flow`.
Generate them once before starting Grafana:

```bash
../../docker/monitoring/grafana/dashboards/generate.sh
```

This renders every flow dashboard registered with a Grafana backend, points them
at the provisioned Prometheus datasource, and writes `ytflow-<name>.json` into
`yt/yt/flow/docker/monitoring/grafana/dashboards/`.

## Running

### Prerequisites

A reachable YT cluster (see "YT cluster" section above).

### Start

```bash
cd yt/yt/flow/examples/docker
YT_CLUSTER=<cluster> docker compose up
```

Wait for the `runner` container to log `Pipeline is running`, then verify:

```bash
# Controller and Worker are alive
curl http://localhost:10001/orchid/build_info
curl http://localhost:10002/orchid/build_info

# Worker → Controller connection (look for "connected": true)
curl http://localhost:10002/orchid/worker/service

# Running jobs (non-empty once the pipeline has been scheduled)
curl http://localhost:10002/orchid/job_tracker/jobs

# Pipeline state and stats via yt CLI
yt --proxy <cluster> flow get-pipeline-state --pipeline-path //tmp/$(whoami)/flow/noop/pipeline
yt --proxy <cluster> flow describe-pipeline  --pipeline-path //tmp/$(whoami)/flow/noop/pipeline
```

### View metrics

- Prometheus: <http://localhost:9090> — check **Status → Targets**; both
  `ytflow-controller` and `ytflow-worker` should be `up`.
- Grafana: <http://localhost:3000> — open **Dashboards → YT Flow**. Anonymous
  admin is enabled, so no login is required. The dashboards appear only after
  running `grafana/dashboards/generate.sh` (see above); if you generated them
  after Grafana started, restart the `grafana` service.

### Stop

```bash
docker compose down -v
```
