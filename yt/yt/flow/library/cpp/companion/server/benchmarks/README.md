# ProcessBatch benchmark

Measures the public companion RPC with one serialized client and server in the same process. The cases are:

- An empty batch.
- 100 messages with 128-byte payloads, without state.
- 128 messages, 16 states of each request kind, and 512-byte state payloads. Internal and external states are modified; joined states are read. This exercises all five profiled request/response state sections.

One server and job ID are reused across calibration and repetitions. Job setup and warm-up requests are outside the timed loop. The fixture checks successful replies and the expected internal/external response-state counts before measuring.

## Run

From this directory:

```bash
ya make --build=relwithdebinfo -j16
taskset -c 0,1,2,3 ./benchmarks \
    --benchmark_min_time=3s \
    --benchmark_repetitions=5 \
    --benchmark_display_aggregates_only=true \
    --benchmark_out=result.json \
    --benchmark_out_format=json
```

Choose CPUs allowed by the host's affinity mask. Build identical benchmark sources at the branch merge base and the candidate revision in separate worktrees. Run the binaries sequentially in baseline/candidate/candidate/baseline order, without concurrent builds; retain all four JSON reports. This produces ten samples per case and revision.

`MeasureProcessCPUTime` accounts for all client, server and collector threads; `UseRealTime` controls the measurement duration. See the [Google Benchmark CPU timer documentation](https://google.github.io/benchmark/user_guide.html#cpu-timers).

Report median batches/s as the reciprocal of median wall time and median process CPU time per batch. The JSON counters also report request/response bytes and message throughput.

## Scope

The candidate enables companion monitoring with its default collection settings; the merge-base implementation ignores the monitoring port. There is no HTTP scrape load. The comparison includes instrumentation and collection overhead, but also loopback transport, protobuf serialization and user-state processing; it does not isolate an individual sensor update or predict whole-pipeline throughput.

`relwithdebinfo` is optimized with assertions enabled. Google Benchmark consequently reports its library as a debug build. Use the same build mode for both revisions, retain this warning, and report sample variability rather than interpreting small differences as a proven speedup or zero overhead.
