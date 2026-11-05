# Diagnose a {{product-name}} Flow pipeline

Use this sequence when a pipeline does not start, stops, or stops producing output. You need `read` permission on its node and access to its Vanilla operation. Substitute your pipeline path and cluster.

## State and jobs {#state-and-jobs}

```bash
yt --proxy <cluster> flow get-pipeline-state //path/to/pipeline
yt --proxy <cluster> flow get-flow-view //path/to/pipeline
```

The state tells you whether the pipeline is working, draining, or paused. In the flow view, find the computation and partition whose progress stopped, along with their current jobs. If the CLI prints `working` but output is absent, compare the source state, output buffers, and errors of the affected jobs.

## Logs and available metrics {#logs-and-metrics}

Read the [controller log](vanilla/diagnostics/logs.md) for state changes and job scheduling; use the worker log for an individual computation error. Fetch `stderr` of a finished job with the [operation and job ID command](vanilla/diagnostics/logs.md#finished-jobs). Save the first substantive error and its timestamp before retrying.

If your installation provides a UI or metrics, compare job status, input and output buffer sizes, lag, and epoch phase time. Specific dashboards depend on the deployment. The commands above and logs remain the diagnostic path when a dashboard is unavailable. Rising lag with working jobs leads to [slow-processing troubleshooting](troubleshooting.md#slow-processing).

## Next step {#next-step}

Match the observed status or error to the [troubleshooting guide](troubleshooting.md). For a hang, capture [backtraces and Orchid data](profiling.md) before restarting: that evidence can disappear with the job. Do not delete state as a diagnostic step.
