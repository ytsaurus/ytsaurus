# Quick start in {{product-name}} Flow (Python)

You implement Python computations in Flow through the [companion](../../../flow/concepts/glossary.md#companion) mechanism. Python code runs in a separate gRPC process that interacts with the C++ [worker](../../../flow/concepts/glossary.md#worker).

[Python SDK source code for Flow]({{source-root}}/yt/yt/flow/library/python/companion)

[Examples]({{source-root}}/yt/yt/flow/examples/python)

## Application architecture {#architecture}

Any Flow [pipeline](../../../flow/concepts/glossary.md#pipeline) consists of three components:

- `Runner` — starts the pipeline and sets a new version of the [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec).
- `Controller` — manages the pipeline’s operation.
- `Worker` — performs the actual data processing.

## Pipeline API {#pipeline-api}

The Python SDK provides a unified approach to configure a companion: the `Pipeline` class. It lets you register [computations](../../../flow/concepts/glossary.md#stream-and-computation) and start the companion’s gRPC server:

```python
from yt.yt.flow.library.python.companion import Pipeline

pipeline = Pipeline()
pipeline.add("mapper", WordCountMapper())
pipeline.run()
```

The `pipeline.add(computation_id, function)` method registers a processing function for the computation with the specified ID. The ID must match the `computation_id` in the pipeline’s [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec).

## Computation and SourceComputation {#computation-and-source}

To create a [computation](../../../flow/concepts/glossary.md#stream-and-computation) in Python, choose the appropriate registration mode that matches the [C++ Computation type](../../../flow/concepts/companion.md#computation-types):

- `pipeline.add(id, fn)` — for `TTransformCompanionComputation` and `TSwiftMapCompanionComputation`.
- `pipeline.add(id, fn, source=True)` — for `TSwiftOrderedSourceCompanionComputation`.

```python
# SourceComputation to read data from a source
pipeline.add("reader", EventMapper(), source=True)

# Computation to process data
pipeline.add("mapper", WordCountMapper())
```

`pipeline.add()` has two required parameters:

- **computation_id** — this is used to map requests between the [worker](../../../flow/concepts/glossary.md#worker) and the companion.
- **fn** — a function with the logic for processing [messages](../../../flow/concepts/glossary.md#message). It can be an instance of `RowFunction`, `BatchFunction`, a regular function, or a class with the `on_message`/`on_messages` method.

You filter messages in source computations by using the [distribute](../../../flow/python/distribute.md) flag when emitting a message from a Process Function.

## Process Function {#process-function}

There are two types of ProcessFunction:

- `RowFunction` — receives [messages](../../../flow/concepts/glossary.md#message) and [timers](../../../flow/concepts/glossary.md#timer) one at a time and provides the `on_message` and `on_timer` methods.
- `BatchFunction` — receives the entire batch of messages and timers and provides the `on_messages` and `on_timers` methods.

For more details, see the [Computation (Python)](../../../flow/python/computation.md) section.

## Message filtering {#message-filtering}

To filter a message in SourceComputation, emit it with `output.add_message(message, distribute=False)`. The message won’t be published further along the graph, but it will still be accounted for when evaluating the watermark.

For more details, see the [distribute flag (Python)](../../../flow/python/distribute.md) section.

## Node companion {#node-companion}

The entry point to the Python companion is the `__main__.py` file. In it, you must configure the computations via `Pipeline` and call `pipeline.run()`. The `main` function from [WordCount](../../../flow/python/examples/wordcount.md) (in the file itself, it’s called from the standard `if __name__ == "__main__":`):

{% code '/yt/yt/flow/examples/python/word_count/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

If your custom functions need additional resources (a Map, cache, etc.), `main` is a good place to create them.

`pipeline.run()` has two modes, which are selected automatically based on the `YT_FLOW_COMPANION_CONFIG` environment variable:

- The variable isn’t set — this is a host-based launch. `run()` enriches the pipeline’s spec (see [Launching the pipeline](#launch)) and passes control to `flow_server`.
- The variable is set — `flow_server` has already launched the same binary in a Job as a companion. `run()` starts the companion’s gRPC server.

The same binary thus both launches the pipeline and acts as a companion inside the Job — you don’t need to deploy the companion separately.

You can also use the `@pipeline.computation` decorator for quick registration:

```python
pipeline = Pipeline()

@pipeline.computation("mapper")
def mapper(message, output, ctx):
    word = message.payload["word"]
    state = ctx.state("word-state", message)
    data = state.get_or_default({"word": word, "count": 0})
    data["count"] += 1
    state.set(data)
```

## Companion CPU parallelism {#companion-process-count}

The Python companion can scale across multiple CPU cores: at startup, it forks into `N` interpreter processes that listen on the **same** gRPC port with the `SO_REUSEPORT` option. The Linux kernel distributes incoming RPC calls among them, and each process handles its own batches under its own GIL — this gives an almost linear performance gain across N cores. By default (`companion_process_count = 0`), the fan-out is enabled only if a finite cgroup CPU quota exists; without an explicit limit (`unlimited` — a typical situation in a dev/CI container), the companion remains single-process.

`N` comes from `companion_process_count` in the companion’s config:

- `0` (default) — automatic selection based on the cgroup CPU quota: `ceil(quota)`, capped at 16. Without a finite quota — `1` (no forks).
- `>0` — an explicit value (also capped at 16). `1` returns to single-process behavior (no forks, no `SO_REUSEPORT`).

{% note warning %}

Memory consumption may grow proportionally to the number of interpreters: each process holds its own copy of the loaded pipeline and any objects used in it. If you increase `companion_process_count`, increase the companion’s `memory_limit` accordingly.

{% endnote %}

## Building with ya make {#build}

You build a project with a Python companion using `ya make`. In the `ya.make` file, you must specify dependencies on the Python SDK:

```
PEERDIR(
    yt/yt/flow/library/python/companion
)
```

{% if audience == "internal" %}

The full `__main__.py` for the working pipeline [`python_vanilla_shuffle`]({{source-root}}/yt/yt/flow/yandex/dev/pipelines/python_vanilla_shuffle) looks like this:

{% code '/yt/yt/flow/yandex/dev/pipelines/python_vanilla_shuffle/__main__.py' lang='python' %}

{% endif %}

## Launching the pipeline {#launch}

You launch the built binary with the command:

```bash
./my_pipeline --config pipeline.yson --flow-bin <path/to/flow_server>
```

Here’s what happens:

- The Python binary (it links `library/python/companion`) reads `pipeline.yson`, enriches the spec — it writes *itself* into it as a Python companion, which `flow_server` will deliver to the Job — and writes the expanded config.
- Then it passes control to the specified `flow_server` via `execv` (`flow_server --config <expanded config>`).

`flow_server` is passed explicitly via `--flow-bin` and isn’t embedded in the Python binary: this keeps the pipeline lightweight, and the person launching the pipeline chooses the `flow_server` version. You can build both binaries with a single command:

```bash
cd yt/yt/flow
ya make yandex/dev/pipelines/python_vanilla_shuffle bin/flow_server
```

`flow_server` handles the entire launch: it validates the spec, creates a vanilla Operation if needed, **sets the pipeline’s spec** (`set-pipeline-specs`), and starts the pipeline. The Python side only *builds* and enriches the spec and never sets it directly.

### The `vanilla` block {#vanilla}

If the `pipeline.yson` file includes a `vanilla` block with `enable = %true`, `flow_server` launches the pipeline as a single YT vanilla Operation (Controller + Workers) and delivers the Python binary to the Job as a companion. This is a one-click launch — you don’t need a separately started `flow_server`.

```yson
{
    "cluster_url" = "{{flow-example-cluster}}";
    "path" = "//home/flow-dev/python-vanilla-shuffle/pipeline";
    "spec" = { ... };
    "vanilla" = {
        "enable" = %true;
        "pool" = "yt-dev";
        "controller" = {
            "count" = 1;
            "cpu_limit" = 4;
            "memory_limit" = 12884901888;
        };
        "worker" = {
            "count" = 5;
            "cpu_limit" = 4;
            "memory_limit" = 12884901888;
        };
    };
}
```

Required parameters: `pool` and `worker.count`. For the other fields (`cpu_limit`, `memory_limit`, number of Controllers, etc.), reasonable default values exist — see the full list of fields and their descriptions in [TVanillaConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) and [TVanillaTaskConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

### Updating the spec of a running pipeline {#release}

`flow_server` is the only component that sets the pipeline’s spec; the Python side only builds the spec. Therefore, the process for rolling out changes to an already running pipeline is:

1. Rebuild the Python binary (`ya make ...`).
2. Run `./my_pipeline --config pipeline.yson --flow-bin <flow_server>` again.

`flow_server` will set the spec again and start the pipeline. For a vanilla launch, the make-before-break strategy is used: the new Operation is prepared (the binary is loaded into the YT cache) while the old Operation continues to run, after which a switch occurs — the old Operation ends, and the prepared new one starts. The way the old Operation ends is controlled by the `YT_FLOW_GRACEFUL_UPDATE` environment variable: `1` (default) — the pipeline stops (`stop`), `0` — it’s paused (`pause`).

## See also

- [Computation (Python)](../../../flow/python/computation.md)
- [Working with states (Python)](../../../flow/python/state.md)
- [Examples](../../../flow/python/examples/wordcount.md)
- [Companion](../../../flow/concepts/companion.md)