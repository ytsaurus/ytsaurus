# Companion in {{product-name}} Flow

In Flow, you can run user code in a separate process. This process is called a companion process.

## Companion use cases
### Currently used

- Supporting computations in languages other than [C++](../../../flow/cpp/getting-started.md), such as [Python](../../../flow/python/getting-started.md) and [Java and Kotlin](../../../flow/java/getting-started.md).

### Planned

- Isolating user C++ code from the Flow core to improve error handling, enable compilation with different flags (for example, for CUDA), and so on.
- Hot updating user code without stopping the [pipeline](../../../flow/concepts/glossary.md#pipeline).

## Workflow {#schema}

When you use a companion, the [Computation](../../../flow/concepts/glossary.md#stream-and-computation) consists of two parts: a specialized Computation on the [Worker](../../../flow/concepts/glossary.md#worker) side and a lightweight Computation on the companion side.

{% note info %}

You develop all business logic in the chosen programming language on the companion side, while the pipeline structure is still configured via the [spec](../../../flow/concepts/spec.md). In this workflow, the worker becomes an infrastructure binary that doesn’t depend on the pipeline logic. So, when you use Python, Java, or Kotlin, you don’t need to write any C++ user code.

{% endnote %}

The Computation on the Worker side collects a batch of messages, enriches it with all the information needed for processing (states, parameters, watermark values, and so on), and sends it to the companion via gRPC locally, within a single host.

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
        "run_process" = %true;
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

### Types of Computations for working with companions

- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation`: An implementation of [TSwiftMapComputation](../../../flow/concepts/computation.md#tswiftmapcomputation) that delegates data processing to the companion process.
- `NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation`: An implementation of [TSwiftOrderedSourceComputation](../../../flow/concepts/computation.md#tswiftorderedsourcecomputation) that delegates data processing to the companion process.
- `NYT::NFlow::NCompanion::TTransformCompanionComputation`: An implementation of [TTransformComputation](../../../flow/concepts/computation.md#ttransformcomputation) that delegates data processing to the companion process.

For more details on implementing pipelines using companions, see [Java and Kotlin](../../../flow/java/getting-started.md) and [Python](../../../flow/python/getting-started.md).

## See also

- [Computation](../../../flow/concepts/computation.md)
- [Quick start (Java)](../../../flow/java/getting-started.md)
- [Quick start (Python)](../../../flow/python/getting-started.md)