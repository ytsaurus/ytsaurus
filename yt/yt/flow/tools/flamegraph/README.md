# flamegraph

Builds a flamegraph SVG from a ytprof pprof profile (`.pb.gz`). Its distinguishing feature is
that the topmost frames can be trace-context labels, so a CPU profile is **split by computation**
(`ytflow.computation_id` / `ytflow.computation_class_name`, attached to samples by the Flow
worker). Without grouping it renders a plain flamegraph for any pprof profile (e.g. heap).

Self-contained: parses the pprof protobuf directly, only the Python standard library is required.
Full C++ names (with template parameters) are recovered by demangling `Function.system_name`
through `c++filt` — the pretty name pprof stores is truncated.

## Capture a profile from a running service

Flow processes expose ytprof on their **monitoring HTTP port (80)**. Find a worker address in
the flow view and pull a profile:

```bash
# List worker monitoring addresses (see the "worker_statuses" map — keys are "[ipv6]:port").
yt --proxy <cluster> flow get-flow-view //path/to/pipeline

# 30-second CPU profile from one worker:
W='[2a02:6b8:...:1]'
curl -sg "http://${W}:80/ytprof/cpu/profile?d=30s" -o cpu.pb.gz

# Heap snapshot from the same worker:
curl -sg "http://${W}:80/ytprof/heap" -o heap.pb.gz
```

The binary must be a `--build=profile` build for ytprof to have frame pointers; deployed Flow
runtimes already are.

## Render

```bash
ya make yt/yt/flow/tools/flamegraph
TOOL=yt/yt/flow/tools/flamegraph/flamegraph

# CPU flamegraph split by computation (default grouping):
$TOOL cpu.pb.gz -o cpu.svg

# Plain heap flamegraph (no split), weighed by allocated bytes:
$TOOL heap.pb.gz --group '' --value space -o heap.svg
```

Open the SVG in a browser: click a frame to zoom, `Search` to highlight by substring.

Useful flags: `--group key1,key2` (top frames; empty for none), `--value cpu|space|…` (pprof
value type), `--width`, `--title`, `--no-demangle`. An `.folded` output path emits collapsed
stacks (`stack;… count`) instead of an SVG, for use with other renderers.
