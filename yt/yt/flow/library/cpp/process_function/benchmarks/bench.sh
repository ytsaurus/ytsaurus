#!/usr/bin/env bash

# CPU flamegraph for the process-function benchmark. Thin wrapper over the shared tool; see it for
# the available knobs (BENCH_FILTER, MIN_TIME, PERF_FREQ, NO_FLAMEGRAPH, NO_UPLOAD, OUTDIR).
#
#   $(arc root)/yt/yt/flow/library/cpp/process_function/benchmarks/bench.sh

source "$(arc root)/yt/yt/flow/tools/gbenchmark_flamegraph/gbenchmark_flamegraph.sh"
flow_gbenchmark_flamegraph
