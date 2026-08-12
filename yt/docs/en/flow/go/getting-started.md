# Getting started with {{product-name}} Flow (Go)

Go computation support in Flow is built on the [companion](../../flow/concepts/glossary.md#companion) mechanism. Go code runs in a separate gRPC process that interacts with the C++ [worker](../../flow/concepts/glossary.md#worker).

[Go SDK source code for Flow]({{source-root}}/yt/go/flow)

[Examples]({{source-root}}/yt/yt/flow/examples/go)

The SDK is imported as `a.yandex-team.ru/yt/go/flow`.

## Application architecture {#architecture}

Every Flow [pipeline](../../flow/concepts/glossary.md#pipeline) consists of three parts:

- `Runner` — starts the pipeline and installs a new version of the [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec).
- `Controller` — manages the pipeline.
- `Worker` — performs the data processing itself.

The Go pipeline binary plays two roles: started from a host, it acts as the runner; started by the worker inside a job, it acts as the companion. The environment determines which role is needed, see [Node companion](#node-companion).

## Pipeline API {#pipeline-api}

The Go SDK provides a single way to configure the companion — the `flow.Pipeline` type. It lets you register [computations](../../flow/concepts/glossary.md#stream-and-computation) and start the companion gRPC server:

```go
pipeline := flow.NewPipeline()
pipeline.Add(flow.NewRowComputation("mapper", &wordCountMapper{}))

if err := pipeline.Run(); err != nil {
    fmt.Fprintf(os.Stderr, "word_count: %v\n", err)
    os.Exit(1)
}
```

Constructors such as `flow.NewRowComputation(computationID, fn)` bind a processing function to the computation with the given identifier, and `pipeline.Add(computations ...*flow.Computation)` registers them in the pipeline. The identifier must match `computation_id` in the pipeline [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec).

Assemble the `Pipeline` from a single goroutine and only then pass it to `Run`. The set of computations is fixed when the server starts: the worker requests it once, so a computation added after the start is no longer visible to the worker.

Typed YSON streams are registered with `pipeline.AddStreams(flow.NewYSONStream[T](id))`. The `T` structure embeds `flow.YSONMessage`, and the schema columns are derived from its `yson` tags. When started from a host, `Run` adds these schemas to `spec.streams`; you don’t need to duplicate them manually in `pipeline.yson`. For details, see [Typed YSON messages](computation.md#typed-yson-messages).

## Computation and SourceComputation {#computation-and-source}

To create a [computation](../../flow/concepts/glossary.md#stream-and-computation) in Go, choose the constructor that matches the [C++ Computation type](../../flow/concepts/companion.md#computation-types):

- `flow.NewRowComputation(id, fn)` and `flow.NewBatchComputation(id, fn)` — for `TTransformCompanionComputation` and `TSwiftMapCompanionComputation`.
- `flow.NewRowSourceComputation(id, fn)` and `flow.NewBatchSourceComputation(id, fn)` — for `TSwiftOrderedSourceCompanionComputation` and `TTransformOrderedSourceCompanionComputation`.

```go
// SourceComputation for reading data from a source
pipeline.Add(flow.NewRowSourceComputation("reader", &eventMapper{}))

// Computation for processing data
pipeline.Add(flow.NewRowComputation("reducer", &eventReducer{}))
```

The constructors take two required parameters:

- **id** — used to map requests between the [worker](../../flow/concepts/glossary.md#worker) and the companion.
- **fn** — the value that holds the [message](../../flow/concepts/glossary.md#message) processing logic. It implements the `flow.RowFunction` or `flow.BatchFunction` interface; a plain function can be passed through the `flow.RowFunc` and `flow.BatchFunc` adapters.

The type of a computation is whatever created it: a source differs from a transform only in how it is declared to the worker. A computation without a processing function is rejected on the spot — the constructor panics, because there is no longer any way to report such an error over the protocol.

Messages in source computations are filtered through the [distribute](distribute.md) flag when a message is emitted from the Process Function.

## Process Function {#process-function}

There are two kinds of ProcessFunction:

- `flow.RowFunction` — receives [messages](../../flow/concepts/glossary.md#message) one at a time, through the `OnMessage` method. [Timers](../../flow/concepts/glossary.md#timer) and visits are handled by implementing the `flow.RowTimerFunction` (`OnTimer`) and `flow.RowVisitFunction` (`OnVisit`) interfaces on the same type.
- `flow.BatchFunction` — receives the whole batch of messages at once, through the `OnMessages` method; timers and visits are handled by `flow.BatchTimerFunction` (`OnTimers`) and `flow.BatchVisitFunction` (`OnVisits`).

A computation declares only the handlers it needs. The worker delivers inputs according to the computation’s spec, and the Go SDK skips timers and visits whose handler isn’t implemented.

Every handler receives four arguments:

```go
func (*wordCountMapper) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error
```

- `ctx` — the context of the request the input arrived in: it has a deadline and is canceled when the worker abandons the batch.
- `rt` — `flow.Runtime`, which gives access to [states](state.md), computation parameters, [watermarks](../../flow/concepts/watermarks.md), and stream schemas.
- `msg` — the input message together with the key it is grouped by.
- `out` — `flow.OutputCollector` for emitting messages and setting timers.

An error returned by a handler stops the processing of the rest of the batch: the worker retries the whole request, so a partial response would cause already processed inputs to be counted twice.

For details, see [Computation (Go)](computation.md).

## Message filtering {#message-filtering}

To filter a message in a SourceComputation, emit it with `out.AddUndistributedMessage(msg)` — it isn’t published further along the graph but is still accounted for in watermark evaluation.

For details, see [The distribute flag (Go)](distribute.md).

## Node companion {#node-companion}

The entry point of a Go companion is the `main` function. In it, configure the computations through `flow.Pipeline` and call `pipeline.Run()`. The `main` function from [WordCount](examples/wordcount.md):

{% code '/yt/yt/flow/examples/go/word_count/main.go' lang='go' %}

If your functions need additional resources (a dictionary, a cache, an HTTP client, and so on), `main` is the place to create them: put them into the fields of the value that is bound to the computation.

`pipeline.Run()` has two modes, selected automatically by the pair of environment variables `YT_FLOW_MODE` and `YT_FLOW_COMPANION_CONFIG`:

- Neither is set — nobody told the process what to serve, so this is a start from a host. `Run()` enriches the pipeline spec (see [Starting a pipeline](#launch)) and hands control over to `flow_server`, so `Run()` never returns.
- At least one is set — `flow_server` has already started this same binary in a job as a companion. `Run()` brings up the companion gRPC server and serves the registered computations until the worker stops it.

The decision is made by the pair, not by a single config: a process for which the worker set `YT_FLOW_MODE` but passed no config is an underconfigured companion, and it must refuse to serve rather than take the runner branch and fail on a command line it was never given.

The same binary therefore both starts the pipeline and works as a companion inside a job — the companion doesn’t need a separate deployment.

If you need to manage the server lifecycle yourself (in tests, for example), use `pipeline.Server(opts...)` instead of `Run()`: it builds a `flow.Server` from the config in the environment. By default, the server writes request errors to `stderr`; the `flow.WithLogger` option replaces that logger.

## Companion parallelism {#companion-process-count}

A Go companion serves requests concurrently: the worker processes computation partitions in parallel, and every request is served by its own goroutine. That is why a Go companion doesn’t need the pre-fork that Python has (there it exists to work around the GIL): the `companion_process_count` parameter of the companion config is accepted and validated, but sets nothing.

If a handler starts child goroutines itself, use [`flow.Go`](computation.md#goroutines) so that their CPU and memory are accounted to the same job.

The Go SDK doesn’t yet expose its own HTTP monitoring endpoint. For diagnostics, use the worker and controller metrics and the companion logs.

{% note warning %}

A single `Computation` value — and therefore a single function bound to it — serves all requests for that identifier. A function that keeps state between calls synchronizes that state itself.

{% endnote %}

## Building with ya make {#build}

A project with a Go companion is built with `ya make`. The pipeline binary is described by the `GO_PROGRAM` module; SDK dependencies are derived from the imports, so no separate `PEERDIR` is needed for them:

```
GO_PROGRAM()

SRCS(
    main.go
    word_count_mapper.go
)

GO_TEST_SRCS(
    word_count_mapper_test.go
)

END()
```

You can build the pipeline binary and `flow_server` with a single command:

```bash
cd yt/yt/flow
ya make examples/go/word_count bin/flow_server
```

## Starting a pipeline {#launch}

Run the built binary with:

```bash
./word_count --config pipeline.yson --flow-bin <path/to/flow_server>
```

Here is what happens:

- The Go binary reads `pipeline.yson`, enriches the spec — writing *itself* into it as the Go companion that `flow_server` will deliver to the job — and writes the extended config to a temporary file.
- It then hands control over to the specified `flow_server` through `execve` (`flow_server --config <extended config>`). Replacing the process image instead of starting a child leaves the exit code and the startup signals to the caller.

Unknown command-line flags are skipped rather than rejected: the pipeline binary is your own program, and it is free to declare its own flags.

`flow_server` is passed explicitly through `--flow-bin` and isn’t embedded into the Go binary: this keeps the pipeline lightweight, and whoever starts the pipeline chooses the `flow_server` version.

The entire startup is performed by `flow_server`: it validates the spec, creates a vanilla operation if needed, **installs the pipeline spec** (`set-pipeline-specs`), and starts the pipeline. The Go side only *builds* and enriches the spec and never installs it directly.

### The `vanilla` block {#vanilla}

If `pipeline.yson` contains a `vanilla` block with `enable = %true`, `flow_server` starts the pipeline as a single YT vanilla operation (controller plus workers) and delivers the Go binary to the job as a companion. This is a one-button start — a separately deployed `flow_server` isn’t needed.

```yson
{
    "cluster_url" = "{{flow-example-cluster}}";
    "path" = "//home/flow-dev/go-word-count/pipeline";
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

The required parameters are `pool` and `worker.count`. The remaining fields (`cpu_limit`, `memory_limit`, the number of controllers, and so on) have reasonable defaults — for the full list of fields and their descriptions, see [TVanillaConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) and [TVanillaTaskConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

Spec enrichment is performed specifically for a vanilla start and consists of two edits:

- The pipeline binary is added to `vanilla.worker.local_files` under the name `go_companion` — `flow_server` delivers it to the job sandbox under that name.
- Every resource with `resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager"` gets `parameters.entrypoint.executable = "./go_companion"`, so the worker starts the companion from the sandbox itself.

{% note info %}

A pipeline started without a vanilla operation works with the companion at the host path already written in its spec — in that case enrichment changes nothing.

{% endnote %}

### Updating the spec of a running pipeline {#release}

`flow_server` is the only component that installs the pipeline spec; the Go side only builds it. So the process of rolling out changes to an already running pipeline is:

1. Rebuild the Go binary (`ya make ...`).
2. Run `./word_count --config pipeline.yson --flow-bin <flow_server>` again.

`flow_server` installs the spec anew and starts the pipeline. A vanilla start uses the make-before-break strategy: the new operation is prepared (the binary is uploaded to the YT cache) while the old operation keeps running, and then the switch happens — the old operation finishes and the prepared new one starts. The way the old operation is finished is controlled by the `YT_FLOW_GRACEFUL_UPDATE` environment variable: `1` (the default) stops the pipeline (`stop`), `0` pauses it (`pause`).

## See also

- [Computation (Go)](computation.md)
- [Working with states (Go)](state.md)
- [Testing (Go)](testing.md)
- [Examples: Word Count (Go)](examples/wordcount.md)
- [Companion](../../flow/concepts/companion.md)
