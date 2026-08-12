# Logs in {{product-name}} Flow

## Logs {#logs}

Flow processes live in the jobs of a vanilla operation, so their logs are written to the job sandbox: the detailed process history goes to the `logs/flow.log` file, while the job’s `stderr` keeps only a short tail. By default, the runner configures logging as follows:

* the `logs/flow.log` file is compressed with zstd — both the active segment and the rotated ones. The active segment is rotated once it reaches `256 MiB` on disk, that is, already in compressed form (`max_segment_size`); all segments together take up no more than `4 GiB` (`max_total_size_to_keep`), and the oldest ones above the limit are deleted. The history is therefore long, but not infinite: for a long-running pipeline, its beginning is evicted. The level is `info` and above; noisy infrastructure categories (`Bus`, `Concurrency`, `RpcClient`, and others) are excluded so that the budget is spent on the pipeline’s history;
* a writer to `stderr` for messages of level `error` and above — this way crash traces end up in the job’s `stderr` (available from the operation page in the {{product-name}} UI).

You can look at `logs/flow.log` of a live job through the [job shell](../../../user-guide/problems/jobshell-and-slowjobs.md); `<job_id>` is taken from the vanilla operation page (the `controller` and `worker` tasks):

```bash
yt --proxy <cluster> run-job-shell <job_id>
```

The file is compressed, so `tail` and `grep` don’t work on it directly — you need to read it through a zstd decompressor. It’s an ordinary stream of zstd frames that any standard decompressor understands:

```bash
# inside the job: the tail of the active segment
zstd -dc logs/flow.log | tail -n 100
```

This shows only the active segment, and within it no more than `256 MiB` of history. The rest lies in the rotated segments next to it, in the same `logs/` directory: on each rotation, the active `flow.log` becomes `flow.log.1`, the previous `flow.log.1` becomes `flow.log.2`, and so on, while a new empty file is opened under the name `flow.log`. So `flow.log` is always the most recent segment, and the higher the number, the older the segment. Numbers are zero-padded to a common width (`flow.log.01`, `flow.log.02`, … once there are more than nine segments), so the order of the names matches the numeric order — and the reverse order of the names gives the segments from the oldest to the active one:

```bash
# inside the job: the whole retained history, from older records to newer ones
ls -r -1 logs/flow.log* | xargs zstd -dc
```

The `zstd` utility may not be present in the job: the set of utilities is determined by the task image. If it isn’t there, add a layer containing it through the task’s `layers` or `system_layer_path` — see [Additional parameters](../../../flow/release/launch-vanilla.md#advanced-config).

The logging level and composition are overridden through the `node_config` patch in the `vanilla` block, the `logging` section — see [TLogManagerConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NLogging_TLogManagerConfig).
