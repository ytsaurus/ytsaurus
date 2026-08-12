# Computation in {{product-name}} Flow

Use a Computation as the main building block of a [pipeline](../../../flow/concepts/glossary.md#pipeline). You get messages from input [streams](../../../flow/concepts/glossary.md#stream-and-computation), process them, and send the results to output streams.

## Computation types {#computation-types}

Flow implements four basic Computation types. You’ll find each type described in the sections below.

Classes with `Swift` in their name follow the [Swift](../../../flow/concepts/swift.md) principle. This is an approach to data processing without full materialization, while preserving [exactly-once](../../../flow/concepts/glossary.md#exactly-once) guarantees and requiring deterministic transformations.

### TTransformComputation {#ttransformcomputation}

Use this for arbitrary transformations of input data. The processing result is stored in {{product-name}}, so you don’t need deterministic transformations. It supports [timers](../../../flow/concepts/glossary.md#timer), [states](../../../flow/concepts/glossary.md#state), and [Sink](../../../flow/concepts/glossary.md#sink). For a passthrough variant without business logic, use [TPassthroughComputation](#passthrough).

### TSwiftMapComputation {#tswiftmapcomputation}

This implements a deterministic Map without materializing results in {{product-name}}. It doesn’t support timers, [Source](../../../flow/concepts/glossary.md#source), or Sink. Your transformation function must be strictly deterministic — the system recomputes the result if needed. For a passthrough variant, use [TSwiftPassthroughComputation](#passthrough).

### TSwiftOrderedSourceComputation {#tswiftorderedsourcecomputation}

This is the main class for reading data from external sources. It requires that the data stream from each instance is ordered. It supports `WatermarkStrategy` to estimate [watermarks](../../../flow/concepts/glossary.md#timestamps-and-watermarks). For a passthrough variant, use [TSwiftPassthroughOrderedSourceComputation](#passthrough).

### TTransformOrderedSourceComputation {#ttransformorderedsourcecomputation}

This class processes `Source` data with arbitrary custom logic (parsing, filtering, expanding one message into several): you override `DoProcessMessage`/`DoProcess` the same way as in `TTransformComputation`, instead of chaining `TSwiftPassthroughOrderedSourceComputation` → `TTransformComputation`.

The processing result is materialized in {{product-name}}, as in `TTransformComputation`, so there are no determinism requirements: after a restart, Flow delivers the already materialized messages with the `MessageId` values previously assigned to them instead of recomputing them. The source offset, the materialized output messages, and the [states](../../../flow/concepts/stateful.md) are committed in a single {{product-name}} transaction — processing of each source message is applied exactly once, including state updates.

You declare your own state yourself, exactly as in `TTransformComputation`: a `TMutableStateKeyClient<T>` field, initialization via `initContext->InitClient(...)` in `DoInit(IJobInitContextPtr)`, and a `GetState(message->Key)` call during processing (for an example, see the [Computation (C++)](../../../flow/cpp/computation.md#ttransformorderedsourcecomputation) section).

Supported: `source_streams` (exactly one ordered `Source`), several output streams, `watermark_strategy` (`watermark_generator` estimates the source watermarks, `watermark_alignment` aligns reading, `event_timestamp_assigner` assigns `event_timestamp`), `skip_if_expression`, and messages with `distribute = false`. A non-empty `group_by_schema`, `input` streams, [timers](../../../flow/concepts/glossary.md#timer), and [key-visitor streams](../../../flow/concepts/key_visitor.md) cause a spec validation error.

## Passthrough Computation {#passthrough}

A [passthrough Computation](../../../flow/concepts/glossary.md#passthrough) doesn’t include custom business logic. Incoming messages are converted to the output [stream](../../../flow/concepts/glossary.md#stream) schema and passed on unchanged. Use it to simply align schemas between streams, for example, when reading a queue and moving data to another stream without any processing.

Flow implements three C++ classes:

| Class | Base class | Purpose |
|-------|--------------|------------|
| `TPassthroughComputation` | `TTransformComputation` | Converts `input` messages to the `output` stream schema |
| `TSwiftPassthroughComputation` | `TSwiftMapComputation` | Does the same, without materialization ([Swift](../../../flow/concepts/swift.md)) |
| `TSwiftPassthroughOrderedSourceComputation` | `TSwiftOrderedSourceComputation` | Converts `source` messages to the `output` stream |

Passthrough is natively implemented in Flow using C++ and doesn’t require a Java or Python companion. To enable it, specify the corresponding C++ class in the `computation_class_name` field in the Computation’s static spec:

```yson
"passthrough" = {
    "computation_class_name" = "NYT::NFlow::TPassthroughComputation";
    "group_by_schema" = [...];
    "input_stream_ids" = [...];
    "output_stream_ids" = [...];
};
```

For more details, see [Computation (C++)](../../../flow/cpp/computation.md#tpassthroughcomputation).

## Common properties {#common-properties}

- All execution within a single [partition](../../../flow/concepts/glossary.md#partition) is strictly single-threaded. You achieve multithreading by increasing the number of partitions.
- All Computations handle filling the message and timer metadata fields.
- The `OutputCollector` object collects output messages and timers.
- The `SetParents` method lets you manage the [lineage](../../../flow/concepts/glossary.md#lineage) of messages to correctly calculate metadata fields.

## Implementation in different languages

Each language offers its own set of interfaces for implementing a Computation:

- **C++**: inherit from base classes (`TTransformComputation`, `TSwiftMapComputation`, etc.) and override the `DoProcessMessage`/`DoProcessTimer` methods. [Learn more →](../../../flow/cpp/computation.md)
- **Java**: implement the `RowFunction` or `BatchFunction` interfaces with the `onMessage`/`onTimer` methods. [Learn more →](../../../flow/java/computation.md)
- **Python**: inherit from `RowFunction` or `BatchFunction` and use the `on_message`/`on_timer` methods. [Learn more →](../../../flow/python/computation.md)
- **YQL**: Computations are generated automatically based on a declarative description. [Learn more →](../../../flow/yql/getting-started.md)

## See also

- [Stateful processing](../../../flow/concepts/stateful.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Timers](../../../flow/concepts/timers.md)
- [Specs](../../../flow/concepts/spec.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (YQL)](../../../flow/yql/features.md)