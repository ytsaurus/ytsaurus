# Troubleshoot {{product-name}} Flow

Start with the [pipeline state, flow view, and logs](diagnostics.md). Record the cluster, pipeline path, operation and job IDs, and the first error time. Change a spec or restart an operation only after locating the cause: a restart can discard evidence from a live job.

## Failed jobs {#failed-jobs}

**Symptom:** the controller repeatedly schedules a job that exits with an error. Find the first error in the [controller log](vanilla/diagnostics/logs.md) and fetch the finished job's `stderr` by ID. For a user code or spec error, reproduce it on a test pipeline and fix it. For OOM, compare the memory limit with actual use and collect a [memory profile](profiling.md#orchid). Do not raise limits before finding the cause.

## Retryable errors {#retryable-errors}

**Symptom:** jobs remain alive, but a {{product-name}} or external sink request keeps failing. Find the first repeated message and check target availability, permissions, and quota. If the fault is temporary, watch whether lag falls after service recovery. Otherwise, correct the address or credentials in the config and deploy according to the [release guide](vanilla/releases.md).

## Hung processing or draining {#hung-processing}

**Symptom:** `stop-pipeline` leaves the pipeline in `draining` for a long time, or output stops changing in `working`. Use `get-flow-view` to find the computation with a full input or output buffer, then inspect its worker log. Capture [thread and fiber backtraces](profiling.md#backtraces) before restarting. For a hang in user C++ code, inspect waits and any `catch (...)`: fiber control exceptions need not derive from `std::exception`. Fix the code and verify it in a test environment first. If failed jobs prevent draining during an update, follow the [bad-spec recovery procedure](vanilla/releases.md#recovery-from-bad-spec).

## Slow processing {#slow-processing}

**Symptom:** lag rises while jobs are working. Compare epoch phase time and buffer sizes if metrics are available; otherwise use `get-flow-view` and logs for the affected partition. Start with a computation near the output whose queue or processing time is growing.

Messages between pipeline computations are kept in memory, so their number is bounded. A computation farther downstream can therefore slow an earlier one. Investigate downstream computations first.

If user `Process` time dominates, collect a [CPU profile](profiling.md#cpu) and inspect external calls. Adding partitions does not split one hot key: reduce work for that key first. For an overflowing output buffer, inspect the next computation before increasing buffer size. When `Input.ReadWindow` is the wait point, inspect lagging downstream partitions and the read window; enlarge it only when infrequent commits are intentional.

## Slow {{product-name}} access {#slow-yt}

**Symptom:** table read or write waits dominate logs and epoch time. Inspect load, tablet health, and errors for the affected tables with the cluster's available tools; compare with the [dynamic table FAQ](../../user-guide/dynamic-tables/faq.md). For uneven load, inspect sharding and key distribution. Plan resharding or a processing-mode change separately: each can alter load or processing guarantees.

## Throttler errors {#throttler-errors}

**Symptom:** a job reports `Throttle()` exceptions or spends time in `Input.Throttle`. Compare demand with the [Distributed Throttler](../concepts/distributed_throttler.md) quota and check controller reachability. If demand greatly exceeds quota, reduce load or request an appropriate limit; if the controller is unavailable, restore connectivity first. Do not disable throttling to hide the error.

## Zombie processes and version mismatch {#zombie-processes}

**Symptom:** new jobs stop being assigned after a release or the pipeline enters `paused` and shows `binary mismatch` in **Messages**. A spec update may fail with `FlowCoreTargetMismatch`. Compare runner, controller, and worker revisions and look for processes from the previous operation. Wait for the switchover or redeploy a matching set of binaries. [Resetting version checks](vanilla/releases.md#flow-core-target-workarounds) is only a controlled recovery step after verifying all processes: it removes protection against zombie processes.
