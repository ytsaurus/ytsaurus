# Profile and capture backtraces in {{product-name}} Flow

Collect evidence from a live controller or worker when logs indicate a hang, high CPU, or memory growth. First enter the affected job's [job shell](vanilla/diagnostics/logs.md#logs). Use `YT_PORT_1` when {{product-name}} allocated a monitoring port; otherwise use the configured `monitoring_port`, which defaults to `10081`. If you configured another value, set `FLOW_CONFIGURED_MONITORING_PORT` to it before running the commands below. A network project or `port_count = 0` does not guarantee a fixed worker port when the Go or Python companion launcher raises `worker.port_count`. See the [Vanilla port settings](vanilla/initial-deploy.md#advanced-config).

```bash
FLOW_MONITORING_PORT="${YT_PORT_1:-${FLOW_CONFIGURED_MONITORING_PORT:-10081}}"
```

## Backtraces {#backtraces}

```bash
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/backtrace/threads" > /tmp/flow-threads.txt
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/backtrace/fibers" > /tmp/flow-fibers.txt
```

Compare repeated captures: the same wait point helps locate a stall. Keep the files with their capture time and job ID. If the endpoint truncates stacks, use `gdb -batch -ex 'thread apply all bt' -p <pid>` when GDB and `ptrace` permission are available. Attaching briefly stops that process, so target only the affected job.

## Orchid and memory {#orchid}

```bash
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/orchid/monitoring/ref_counted" > /tmp/flow-ref-counted.json
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/orchid/job_tracker" > /tmp/flow-jobs.json
```

The first response reports live reference-counted objects; the second gives worker job details. Compare captures under similar load before treating growth as a leak. If the build supports heap profiling, request `http://localhost:${FLOW_MONITORING_PORT}/ytprof/heap`; analysis needs matching binary symbols and a profile reader.

## CPU {#cpu}

When the built-in profiler is available, request `http://localhost:${FLOW_MONITORING_PORT}/ytprof/profile?d=30s` and retain the result for inspection. If that endpoint is absent, use `perf` on the process PID when the environment permits it, then compare the profile with logs and partition state. Profiling adds load; collect a short interval and do not resize a deployment from one sample.

For exceptions with unclear origins, you can temporarily set `singletons.error_backtrace_enricher.level = "enabled_for_all"` in the dynamic spec. Disable it after the investigation: symbolizing backtraces can delay processing.
