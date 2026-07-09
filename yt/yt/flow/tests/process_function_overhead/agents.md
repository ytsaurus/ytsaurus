# Agent guide: process_function_overhead bench

Measures the worker CPU overhead of a **process function** versus the equivalent hand-written
**computation**, on the same hot path. Same random source → swift-map reducer pipeline as
`pure_swift_high_throughput`; the only difference between the two tests is computation vs
process-function dispatch.

- `test_computation` — reducer is `NExample::TReducer` (`TSwiftMapComputation::DoProcessMessage`).
- `test_function` — reducer is `NExample::TReducerFunction` (`IProcessFunction::DoProcessMessage`) hosted by
  `TProcessFunctionSwiftMapComputation`. Both do byte-for-byte the same work.

## Run both and compare

Run both tests in **one invocation on the same host** (only same-host deltas are meaningful):

```bash
cd yt/yt/flow/tests/process_function_overhead
ya make --build=profile -A . --test-param EVENT_COUNT=2e6
```

- `--build=profile` is faster than `--build=relwithdebinfo` and keeps frame pointers.
- `EVENT_COUNT=2e6` is the load level to measure at (default 2e5 is too short to be representative).

## Read the metric

```bash
grep CpuUsPerMessage test-results/py3test/testing_out_stuff/run.log
```

`CpuUsPerMessage` is worker CPU (utime+stime, summed over workers, measured only between
`working` and `completed`) divided by `EVENT_COUNT`. Each test logs its variant. The process-
function overhead is `CpuUsPerMessage(function) - CpuUsPerMessage(computation)`; it should be
close to zero if the adapter is free.

## Noise budget

Same as `pure_swift_high_throughput`: on a fixed host, n=5 typically gives stdev ≈ 0.5–1.0s on a
~70s run; a real ≥2% delta is detectable at n=5, below that plan n=10+ or treat it as noise. The
two variants run back-to-back in one invocation, so host variance cancels in the delta.

## Notes

- The comparison uses `IProcessFunction::DoProcessMessage` (per-message), the apples-to-apples
  analog of the computation's `DoProcessMessage`. A whole-batch `DoProcess` override would be a
  different (faster) processing model and is intentionally not the primary comparison — TODO if a
  separate "batch dispatch" number is wanted.
- Single worker, all partitions on it (`PARTITION_COUNT=10`), so `CpuUsPerMessage` is the clean
  per-worker CPU.
