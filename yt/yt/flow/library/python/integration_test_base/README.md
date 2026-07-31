# Pipeline testing framework for YT Flow

Sets up the environment for running Flow integration tests and exposes test parameters (`RUNNER_LOG_LEVEL`, `EXTERNAL_YT_CONFIG`, `--ext-py`, etc.). See details in the [docs](../../../../../docs/ru/flow/contributor/testing-framework.md).

## Monitoring stack (Prometheus + Grafana)

Any test that starts a pipeline via `start_flow_process_federation` can bring up a local monitoring stack (based on docker containers for Prometheus and Grafana), for inspecting pipeline metrics while the test runs. It is a local debugging aid: off by default, never runs under autocheck.

The stack config is shared with the docker example and lives in [`yt/yt/flow/docker/monitoring`](../../../docker/monitoring). The grafana dashboards are **not** bundled — rendering them needs the `yt_dashboards` generator, which we keep out of every flow test's build graph. So you prepare them once yourself:

```bash
# Render the flow dashboards (needs `ya` on PATH; writes ytflow-*.json into that directory).
yt/yt/flow/docker/monitoring/grafana/dashboards/generate.sh
```

Then run the test, pointing it at that directory with an **absolute** path (the test runs in a sandbox, so a relative path would resolve against the wrong directory):

```bash
ya make -A \
    --test-param MONITORING_STACK=1 \
    --test-param MONITORING_DASHBOARDS_DIR=$(realpath yt/yt/flow/docker/monitoring/grafana/dashboards) \
    yt/yt/flow/tests/working_pipeline_telemetry
```

Once the pipeline is up a banner with the Grafana/Prometheus URLs and the `ssh` forward command is printed to the terminal (like the "YT Flow test started" message, so no `--test-stderr` is needed). The test then **holds** (it looks like it is hanging — that is deliberate) so you can browse the metrics; press **Ctrl-C** to finish and tear the stack down.

When enabled:

- Prometheus scrapes every controller/worker at `/solomon_proxy/sensors` (the combined node + companion endpoint) of the running federation; Grafana is pre-provisioned with the datasource and the dashboards you rendered.
- The federation teardown **holds until you press Ctrl-C**, keeping the pipeline and the stack alive so you can browse the metrics.
- The stack is removed when the test ends (a reaper container guarantees this even on `SIGKILL`).

Prometheus and Grafana take ports `9090` and `3000` when free (so a pre-opened firewall hole or SSH tunnel matches), otherwise a random free port is used and a warning is logged.

Forward Grafana to port `3000` on your laptop and open the general dashboard:

```bash
ssh -N -L 3000:localhost:<grafana-port> <test-host>
# then open http://localhost:3000/d/ytsaurus-flow-general/yt-flow-general
```

Requirements: Docker on the test host (only with `MONITORING_STACK=1`, which is never on under autocheck). `ya` is needed only to run `generate.sh`, not by the test itself.
