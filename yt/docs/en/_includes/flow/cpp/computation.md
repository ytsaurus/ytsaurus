# Computation in {{product-name}} Flow (C++)

{% note info %}

Use this page to learn about the specifics of implementing Computation in C++. For a language‑agnostic description of the concept, see the [Computation](../../../flow/concepts/computation.md) section.

{% endnote %}

This section describes the base `Computation` classes and important implementation details.

Flow currently implements three base `Computation` classes:

- `TTransformComputation`
- `TSwiftOrderedSourceComputation`
- `TSwiftMapComputation`

Classes that include `Swift` in their name implement the [Swift](../../../flow/concepts/swift.md) principle. See the [Swift](../../../flow/concepts/swift.md) section for more details.

## General

- When you inherit from a base class, you can extend `Computation` parameters using macros:
  - `YT_FLOW_EXTEND_PARAMETERS`
  - `YT_FLOW_EXTEND_DYNAMIC_PARAMETERS`

  You can pass a `yson struct` to them to parse `parameters` from `ComputationSpec`. This structure must inherit from the corresponding structure of the parent class.

- All classes provide the following methods:
  - `GetContext` — to get `TComputationContext`.
  - `GetSpec` and `GetDynamicSpec` — to get the full spec of the corresponding `Computation`.
  - `GetParameters` and `GetDynamicParameters` — to get structured `parameters`.

  Although `GetParameters` and `GetDynamicParameters` return the parsing result, parsing happens only when the spec is reconfigured. If the spec hasn’t changed, calling the method again returns the already prepared object. That means the method doesn’t add extra load to the system.

- Keep the constructor as simple as possible and avoid creating complex objects.
- When logging inside `Computation`, use `YT_LOG_*`. The class already has a `NLogging::TLogger` object that’s prepared and filled with the information you need for debugging.
- Similarly, to collect various metrics, use `NProfiling::TProfiler` and the pre‑prepared `GetContext()->Profiler` object.
- Use the `DoInit` method for complex initialization, such as initializing objects for working with states.
- You can use the `DoSync` method to manually save data to a `YT` transaction. However, it’s better to use [Sink](../../../flow/concepts/glossary.md#sink) or [ExternalState](../../../flow/cpp/state.md#external-state). Directly working with `DoSync` in `SwiftComputation` can lead to undesirable behavior. In `Transform`, the method is safe but less convenient.
- Each `Computation` has a family of `DoProcess` methods. They accept either `IInputContextPtr input` with `GetMessages` and `GetTimers` methods, or a specific message or [timer](../../../flow/concepts/glossary.md#timer). The `IOutputCollectorPtr output` object is for collecting output messages and timers — see [OutputCollector](#output-collector) for more details.
- All Computations handle filling all metadata fields for `message` and `timer`, including `StreamId` or `timer.Key` if there’s no ambiguity. To create messages, you can use the `MakeMessageBuilder` method.
- All code execution within Computations is strictly single‑threaded and runs within `GetContext()->SerializedInvoker`. You achieve multithreading by increasing the number of [partitions](../../../flow/concepts/glossary.md#partition). If you still need to run some code multithreaded, use `GetContext()->PoolInvoker`, but make sure to wait for the execution results within the corresponding method.
- You can convert input messages to `NYTree::TYsonStruct`. To do this, you need to:
  - Create a class that inherits from `TYsonMessage` (this is a special child of `NYTree::TYsonStruct`).
  - Register it in the global registry using `YT_FLOW_DEFINE_YSON_MESSAGE`.
  - In the `main` function, create a `TSimpleSpecBuilder` object and register the corresponding `stream_id` in it.
  - If you use `TSimpleRunnerProgram`, you can pass this `TSimpleSpecBuilder` directly to the `TSimpleRunnerProgram` constructor.
  - If you implement `main` yourself, you’ll need to pass specs to `TSimpleSpecBuilder` to enrich them with stream information.
  - You don’t need to manually fill `spec/streams` when using `TYsonMessage` — all information will be derived from the registered `TYsonMessage + stream_id` using `TSimpleSpecBuilder`.
  - In `Computation`, you’ll have access to `ConvertToYsonMessage(message)->As<Type>()` and `ConvertToMessage(ysonMessage)` methods to convert between `TMessage => TYsonMessage` and back.

### OutputCollector {#output-collector}

The `IOutputCollectorPtr output` object is passed to `DoProcess*` methods and is for sending processing results:

| Method | Description |
| --- | --- |
| `output->AddMessage(message)` | Add an output message (a `TMessage` object obtained via `MakeMessageBuilder().Finish()`) |
| `output->AddTimer(timer)` | Add a [timer](../../../flow/concepts/glossary.md#timer) |
| `output->SetParents(parentIds)` | Set the parent ID for tracking [lineage](../../../flow/concepts/lineage.md). Returns a new `IOutputCollectorPtr` with the attached lineage context |

Use `SetParents` when the output message is logically derived from a specific subset of inputs, not the entire batch. Alternatively, use non‑batch `DoProcessMessage` and `DoProcessTimer` — they set lineage automatically.

### TMessage {#tmessage}

The `TMessage` structure is used in `DoProcessMessage` and `AddMessage` methods:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TMessageSerializer.md) %}

## TTransformComputation

This class is for arbitrary `Transform` transformations of input data. It can’t work with `Source`. The result of the work is always saved in YT, so there are no requirements for any determinism in the transformations.

Properties of `TTransformComputation`:

- It can write to YT “idle”, that is, without real changes, by overwriting existing content. You should expect such a stream to create a negligible load.

Example of working with `TTransformComputation`:

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TMyParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicMyParameters);

    using TTransformComputation::TTransformComputation;

    void DoInit() override
    {

    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        TMyParametersPtr parameters = GetParameters();
        TDynamicMyParametersPtr dynamicParameters = GetDynamicParameters();
        ...
        output->AddTimer(TSystemTimestamp(message.EventTimestamp.Underlying() + TDuration::Minutes(5).Seconds()));
        ...
    }

    void DoProcessTimer(const TTimer& timer, IOutputCollectorPtr output) override
    {
        ...
        auto builder = MakeMessageBuilder();
        builder.Payload().SetValue(...);
        output->AddMessage(builder.Finish());
        ...
    }

    void DoSync(NApi::ITransactionPtr transaction) override
    {
        ...
        transaction->ModifyRows(...);
        ...
    }
};
```

### TTimer {#ttimer}

The `TTimer` structure is used in `DoProcessTimer` and `AddTimer` methods:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSerializer.md) %}

### ProcessingMode {#processing-mode}

`TransformComputation` has a `parameters/processing_mode` parameter that lets you reduce processing guarantees in exchange for lowering the load on {{product-name}}.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_EProcessingMode.md) %}

### TPassthroughComputation

This is a child of `TTransformComputation`. It implements a [passthrough-computation](../../../flow/concepts/computation.md#passthrough) and is mainly created to demonstrate capabilities.

Features:

- It has no overridable methods.
- It can’t have more than one output.
- All incoming `input` streams are turned into `output` by converting the message using `ConvertMessageToNewSchema`.

## TSwiftMapComputation

This class implements a deterministic simple `Map` without materializing results in YT.

Features:

- It doesn’t support `sources` and `sinks`.
- It supports `timer_streams` and `key_visitor_streams` only for working with state: emitting output messages from timer or visit processing is prohibited, so output streams can’t depend on timer and visit streams in `streams_dependency`.
- It must return the same result (including order) for each input row. If the result changes on repeated runs, various negative effects can occur. It’s possible that separate parts of the system will process different versions of the output, up to duplicates, if the field values for subsequent `group-by` change.
- As a consequence, each resulting message must have exactly one parent.

### TSwiftPassthroughComputation

This is a child of `TSwiftMapComputation`. It’s similar to `TPassthroughComputation`: it simply turns `input` into `output` by converting messages to a new schema.

## TSwiftOrderedSourceComputation

This is the main class for reading data from external sources. It requires that the data stream from each instance be ordered.

Features:

- There must be exactly one `source`.
- The `Source` must be a child of `IOrderedSource`.
- It can use `watermark_strategy/event_timestamp_assigner` to assign `event_timestamp` to output messages, provided a column is specified. Otherwise, the `event_timestamp` of the output message will be taken from the `event_timestamp` of the message from `source` — that is, the creation time of the original message.
- It uses `watermark_strategy/watermark_generator` to estimate [watermarks](../../../flow/concepts/glossary.md#timestamps-and-watermarks) of input sources.
- It uses `watermark_strategy/watermark_alignment` to align stream reading relative to other streams.
- It lets you filter part of the events: a message added to `output` with `distribute=false` isn’t published but is still considered when estimating the watermark. This lets you estimate the watermark using the full stream even when a significant part of the stream is filtered out.
- `system_timestamp` is assigned at the moment the message is registered in `output`.
- It reliably saves part of the data in YT to guarantee recovery of all metadata. It doesn’t save the messages themselves in YT.
- It can write to YT “idle”, that is, without real changes. You should expect such a stream to create a minimal load.

### TSwiftPassthroughOrderedSourceComputation

This is a child of `TSwiftOrderedSourceComputation`. It’s similar to `TPassthroughComputation`: it converts `source` to `output` by converting messages to a new schema.

## FAQ

### How to configure Source and Sink? {#source-sink-configuration}

You configure `Source` and `Sink` in the `Computation` spec via the `sources` and `sinks` sections, respectively. Each `Source`/`Sink` is defined in a separate subsection with a type specified (for example, `TQueueSource`{% if audience == "internal" %}, `TLogbrokerSource`, `TLogbrokerSink`{% endif %}) and connection parameters.

For more details about available connectors, see the [Connectors](../../../flow/connectors/about.md) section.

### How do batching and partitioning work? {#batching-partitions}

Each partition is processed strictly single‑threaded. You achieve multithreading by increasing the number of partitions (`partition_count` in the spec). Batch methods `DoProcess(IInputContextPtr input, IOutputCollectorPtr output)` receive all messages and timers for the current [epoch](../../../flow/concepts/glossary.md#epoch), which lets you optimize processing.

If you need to run code multithreaded within a single partition, use `GetContext()->PoolInvoker`, but be sure to wait for completion within the current method.

### How to estimate the load on internal tables? {#internal-tables-load}

The load on {{product-name}} internal tables depends on the `Computation` type and the number of partitions. Below is an approximate estimate:

#|
|| **Computation type** | **Records per epoch per partition** | **Comment** ||
|| `TTransformComputation` | ~2–4 | State writes + commit ||
|| `TSwiftMapComputation` | 0 | Doesn’t write to YT ||
|| `TSwiftOrderedSourceComputation` | ~1–2 | Metadata for recovery ||
|#

Total load = records per partition × number of partitions × epoch frequency. For a [pipeline](../../../flow/concepts/glossary.md#pipeline) with 1000 partitions and an epoch of 1 second, `TTransformComputation` will create ~2000–4000 records/s.

## Pipeline state {#pipeline-state}

Possible pipeline states (type `EPipelineState`):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_EPipelineState.md) %}

## See also

- [Computation (concept)](../../../flow/concepts/computation.md)
- [Working with states (C++)](../../../flow/cpp/state.md)
- [Quick start (C++)](../../../flow/cpp/getting-started.md)