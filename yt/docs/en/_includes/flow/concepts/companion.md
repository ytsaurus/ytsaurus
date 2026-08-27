# Companion in {{product-name}} Flow

In Flow, you can run user code in a separate process. This process is called a companion process.

## Companion use cases
### Currently used

- Supporting computations in languages other than [C++](../../../flow/cpp/getting-started.md), such as [Python](../../../flow/python/getting-started.md), [Java and Kotlin](../../../flow/java/getting-started.md), and [Go](../../../flow/go/getting-started.md).

### Planned

- Isolating user C++ code from the Flow core to improve error handling, enable compilation with different flags (for example, for CUDA), and so on.
- Hot updating user code without stopping the [pipeline](../../../flow/concepts/glossary.md#pipeline).

## Workflow {#schema}

When you use a companion, the [Computation](../../../flow/concepts/glossary.md#stream-and-computation) consists of two parts: a specialized Computation on the [Worker](../../../flow/concepts/glossary.md#worker) side and a lightweight Computation on the companion side.

{% note info %}

You develop all business logic in the chosen programming language on the companion side, while the pipeline structure is still configured via the [spec](../../../flow/concepts/spec.md). In this workflow, the worker becomes an infrastructure binary that doesn’t depend on the pipeline logic. So, when you use Python, Go, Java, or Kotlin, you don’t need to write any C++ user code.

{% endnote %}

The Computation on the Worker side collects a batch of messages, enriches it with all the information needed for processing (states, parameters, watermark values, and so on), and sends it to the companion via gRPC locally, within a single host.

The batch is formed without regard to [keys](../../../flow/concepts/glossary.md#key) — one request may contain messages with different keys; this is how the worker collects batches for all computations. There is no per-key grouping in the companion protocol: if the business logic needs per-key processing, it is done in the companion code — see the [Python](../../../flow/python/computation.md#batch-function) example.

The companion returns its output in groups; each group carries [lineage](../../../flow/concepts/lineage.md) — the list of ids of the input messages of the batch its output was derived from (lineage is unrelated to keys). For when lineage must be set explicitly and what exactly to pass, see [When to set lineage explicitly](../../../flow/concepts/lineage.md#explicit-lineage).

In the future, you’ll also be able to use Unix sockets.

![](../../../flow/images/companion_v1.svg)

You manage the companion process through the [resource](../../../flow/concepts/glossary.md#resource) `CompanionManager`.

### Configuration

Here’s an example of declaring the resource in a static spec for Java:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
    "parameters" = {
        "timeout" = "10s";
        "jdk_bin_path" = "/app/ytflow/jdk/bin/java";
        "main_class" = "tech.ytsaurus.flow.examples.wordcount.NodeCompanionMain";
        "classpath" = "/app/ytflow/lib/*";
    };
    "dependencies" = {};
};
```

For a detailed description of all `TCompanionManagerParameters` parameters, see the section [CompanionManager resource configuration](../../../flow/java/computation.md#companion-manager).

The Python companion configuration is described in the section [CompanionManager resource configuration (Python)](../../../flow/python/computation.md#companion-manager).

Here’s an example of declaring a Computation in a static spec:

```yson
"computations" = {
    "mapper" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "group_by_schema" = [
            {"name" = "hash"; "expression" = "farm_hash(word)"; "type" = "uint64"; required = %true;};
            {"name" = "word"; "type" = "string";};
        ];
        "input_stream_ids" = ["words"];
        "output_stream_ids" = [];
        "required_resource_ids" = {
            "CompanionManager" = {
                "controller" = false;
                "worker" = true;
            };
        };
        "parameters" = {
            "internal_states" = ["word-state"];
        };
    };
};
```

The key point in this example is using the `CompanionManager` resource to launch the companion process and the specialized Computation class `NYT::NFlow::NCompanion::TTransformCompanionComputation`.

### C++ companion {#cpp-companion}

You can move user C++ code out of the worker into a separate process as well. The SDK is located in `yt/yt/flow/library/cpp/companion/server`: you declare the served Computations in `TPipeline`, specifying the process function type (this typed declaration replaces `YT_FLOW_DEFINE_PROCESS_FUNCTION`), and build a separate binary with the `RunCompanionMain` entry point:

```cpp
int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddSource<TMyReadFunction, TMyReadParameters>("reader");
    pipeline.AddTransform<TMyMapFunction>("mapper");
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
```

The function is selected by the name from the `processing_function` field of the Computation spec, the same way as in the in-process `TProcessFunctionComputation` adapters. The worker launches the binary through the generic `TCompanionManager` resource:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
    "parameters" = {
        "entrypoint" = {
            "executable" = "/path/to/my_companion";
        };
    };
};
```

Limitations of the first version of the C++ companion:

- sync process functions aren’t supported (the companion protocol has no Sync phase);
- static [resources](../../../flow/concepts/glossary.md#resource), distributed throttlers, and the epoch timestamp (`GetCurrentTimestamp`) aren’t available;
- `GetStreamSpecs()->ComputeKey()` can’t compute a key when `group_by_schema` has computed columns: a companion doesn’t evaluate expressions. The key arrives with the message — use `message->Key`;
- external states are supported only as `TSimpleExternalState`;
- output timers can only reference the key of one of the parent entities of the batch;
- the companion runs as a single multithreaded process (`companion_process_count` is 0 or 1).

For an example, see `yt/yt/flow/examples/cpp/companion_word_count`.

### Types of Computations for working with companions {#computation-types}

- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation`: An implementation of [TSwiftMapComputation](../../../flow/concepts/computation.md#tswiftmapcomputation) that delegates data processing to the companion process.
- `NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation`: An implementation of [TSwiftOrderedSourceComputation](../../../flow/concepts/computation.md#tswiftorderedsourcecomputation) that delegates data processing to the companion process.
- `NYT::NFlow::NCompanion::TTransformCompanionComputation`: An implementation of [TTransformComputation](../../../flow/concepts/computation.md#ttransformcomputation) that delegates data processing to the companion process.
- `NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation`: An implementation of [TTransformOrderedSourceComputation](../../../flow/concepts/computation.md#ttransformorderedsourcecomputation) that delegates data processing to the companion process.

Two modes are available for a Source computation. `TSwiftOrderedSourceCompanionComputation` doesn’t materialize the output and requires deterministic processing without user state. `TTransformOrderedSourceCompanionComputation` materializes the output and commits it together with the internal state and the source offset in the epoch transaction. Choose it for non-deterministic processing or for working with internal state; the key of such a state is the source partition key. The spec limitations are the same as for [TTransformOrderedSourceComputation](../../../flow/concepts/computation.md#ttransformorderedsourcecomputation).

For more details on implementing pipelines using companions, see [Java and Kotlin](../../../flow/java/getting-started.md), [Python](../../../flow/python/getting-started.md), and [Go](../../../flow/go/getting-started.md).

## See also

- [Computation](../../../flow/concepts/computation.md)
- [Quick start (Java)](../../../flow/java/getting-started.md)
- [Quick start (Python)](../../../flow/python/getting-started.md)
- [Quick start (Go)](../../../flow/go/getting-started.md)