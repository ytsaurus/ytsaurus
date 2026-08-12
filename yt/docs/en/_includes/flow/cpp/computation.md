# Computation in {{product-name}} Flow (C++)

{% note info %}

Use this page to learn about the specifics of implementing Computation in C++. For a language-agnostic description of the concept, see the [Computation](../../../flow/concepts/computation.md) section.

{% endnote %}

This section describes the base `Computation` classes and important implementation details.

Flow currently implements four base `Computation` classes:

- `TTransformComputation`
- `TTransformOrderedSourceComputation`
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
- Similarly, to collect various metrics, use `NProfiling::TProfiler` and the pre-prepared `GetContext()->Profiler` object.
- Use the `DoInit` method for complex initialization, such as initializing objects for working with states.
- You can use the `DoSync` method to manually save data to a `YT` transaction. However, it’s better to use [Sink](../../../flow/concepts/glossary.md#sink) or [ExternalState](../../../flow/cpp/state.md#external-state). Directly working with `DoSync` in `SwiftComputation` can lead to undesirable behavior. In `Transform`, the method is safe but less convenient.
- Each `Computation` has a family of `DoProcess` methods. They accept either `IInputContextPtr input` with `GetMessages` and `GetTimers` methods, or a specific message or [timer](../../../flow/concepts/glossary.md#timer). The `IOutputCollectorPtr output` object is for collecting output messages and timers — see [OutputCollector](#output-collector) for more details.
- All Computations handle filling all metadata fields for `message` and `timer`, including `StreamId` or `timer.Key` if there’s no ambiguity. To create messages, you can use the `MakeMessageBuilder` method.
- All code execution within Computations is strictly single-threaded and runs within `GetContext()->SerializedInvoker`. You achieve multithreading by increasing the number of [partitions](../../../flow/concepts/glossary.md#partition). If you still need to run some code multithreaded, use `GetContext()->PoolInvoker`, but make sure to wait for the execution results within the corresponding method.
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

Use `SetParents` when the output message is logically derived from a specific subset of inputs, not the entire batch. Alternatively, use non-batch `DoProcessMessage` and `DoProcessTimer` — they set lineage automatically.

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

This is a child of `TTransformComputation`. It implements a [passthrough computation](../../../flow/concepts/computation.md#passthrough) and is mainly created to demonstrate capabilities.

Features:

- It has no overridable methods.
- It can’t have more than one output.
- All incoming `input` streams are turned into `output` by converting the message using `ConvertMessageToNewSchema`.

### TTransformOrderedSourceComputation {#ttransformorderedsourcecomputation}

`TTransformOrderedSourceComputation` (`yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h`) processes `source` messages with arbitrary custom logic: parsing, filtering, or expanding a single input message into several output ones. You override the same processing methods as in `TTransformComputation`, but the input is messages of one ordered `Source` rather than of `input` streams. The class replaces the `TSwiftPassthroughOrderedSourceComputation` → `TTransformComputation` chain when the only job of the intermediate computation is to process source data.

The transformation result is materialized in {{product-name}} the same way as in `TTransformComputation`:

- Output messages receive unique `MessageId` values and are durably saved in {{product-name}} before they are distributed downstream. After a restart, materialized messages that haven’t been delivered yet are distributed with the same `MessageId` values instead of being recomputed, so the transformation has no determinism requirements.
- A message can be added to `output` with an explicit `distribute` flag, for example `output->AddMessage(std::move(message), /*distribute*/ false)`. Such a message isn’t published downstream, but it participates in watermark estimation on equal terms with published ones: the watermark generator registers the read over the full set of output messages before the publication filter is applied, so the watermark can be estimated correctly over the full stream even when a significant part of it is filtered out. The source offset advances in the epoch transaction in either case.
- The `source` offset, the materialized output, and the states are committed in a single epoch transaction, so processing of each source message is applied exactly once.

Overridable methods:

* `DoInit(IJobInitContextPtr initContext)`: initialization, including creating state clients;
* `DoProcess(IInputContextPtr input, IOutputCollectorPtr output)`: the batch version of processing;
* `DoProcessMessage(const TMessage& message, IOutputCollectorPtr output)`: the most common choice for processing without state; the `DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output)` variant gives access to the metafields of the original message, including `message->Key` for addressing the state;
* `DoSync(IRetryableTransactionPtr transaction)`: manual writing into the epoch transaction.

You declare your own state exactly as in `TTransformComputation` (see [Working with states](../../../flow/cpp/state.md#internal-state)): a `TMutableStateKeyClient<T>` field, `initContext->InitClient(...)` initialization in `DoInit`, and a `GetState(message->Key)` accessor during processing. Before calling `DoProcess`, the framework loads the state for the keys of the current epoch’s messages itself. A computation instance is always bound to a single `source` key, so all messages of an epoch carry the same key and address the same state row:

```cpp
class TMyComputation
    : public TTransformOrderedSourceComputation
{
public:
    using TTransformOrderedSourceComputation::TTransformOrderedSourceComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(StateClient_, "my_state");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        auto state = StateClient_.GetState(message->Key);
        state->Counter += 1;
        ...
    }

private:
    TMutableStateKeyClient<TMyState> StateClient_;
};
```

The framework synchronizes state clients created through `IJobInitContext` in the epoch transaction atomically with the `source` offset, so an ordinary mutation, such as incrementing a counter, is exactly-once correct: no additional deduplication by `MessageId` is needed.

The computation spec is validated at startup; the following fields cause a validation error:

* `input` streams;
* [timers](../../../flow/concepts/glossary.md#timer);
* [key-visitor streams](../../../flow/concepts/key_visitor.md);
* a non-empty `group_by_schema`;
* `external_state_managers`;
* `external_state_joiners` that have no `join_on/key_schema_override` set (the source message key isn’t described by `group_by_schema`, so the key schema must be set explicitly).

`watermark_strategy` is supported: `watermark_generator` estimates the source watermarks, `watermark_alignment` aligns reading of the source relative to other streams (`read_delays` delay reading, not publication), and `event_timestamp_assigner` assigns `event_timestamp` to output messages. The atomicity of the commit of the `source` offset, the materialized output messages, and the state doesn’t depend on alignment. `skip_if_expression` is supported as well.

`skip_if_expression` is applied before processing, but after the input batch has been counted in the metrics and in the number of late messages: a filtered-out message reaches neither the state nor the output. It doesn’t affect watermark estimation either: the generator registers a read only over output messages, so a fully filtered-out batch doesn’t move `EventWatermark`, and over a long series of such batches the partition watermark stands still. The source takes the `EventWatermark` markers of input records into account when reading regardless of the filter, but with `use_source_watermark = false` (the default value) the source watermark only bounds the estimate from above and never moves it forward; it becomes the only source of the partition watermark with `use_source_watermark = true`. This exactly matches the behavior of `TSwiftOrderedSourceComputation`.

Instead of inheriting from the class, you can write the logic as a [process function](../../../flow/cpp/process-functions.md) and specify the `NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation` adapter in the spec: it runs the function on top of `TTransformOrderedSourceComputation`, with the same output materialization, the same states, and the same spec validation.

For an example, see `NYT::NFlow::NExample::TLogParserProcessFunction` from [`examples/cpp/log_parser`]({{source-root}}/yt/yt/flow/examples/cpp/log_parser): it splits a log line into records, emits the `TLogRecordMessage` YSON structure (`level`, `text`, `worst_level_so_far`), and maintains the `TWorstSeverityState` state, a running maximum severity per source partition. For more details, together with the full source code, see the [Log parser](../../../flow/cpp/examples/log_parser.md) section.

#### TProtoTransformOrderedSourceComputation {#tprototransformorderedsourcecomputation}

The `NYT::NFlow::TProtoTransformOrderedSourceComputation<TProto>` helper (`yt/yt/flow/library/cpp/parsers/proto.h`) removes manual `Protobuf` parsing from your code: it’s the counterpart of `TProtoSwiftSourceComputation<TProto>` for `TSwiftOrderedSourceComputation`.

`DoProcessMessage` is implemented for you: it reads the string column `parameters/data_column` of the raw `source` message (`"data"` by default) and parses it into `TProto`. You override one of the hooks:

* `DoProcessProto(TProto&& proto, IOutputCollectorPtr output)`: on successful parsing, without access to the original message;
* `DoProcessProto(const TInputMessageConstPtr& inputMessage, TProto&& proto, IOutputCollectorPtr output)`: the same situation, but with access to the original `source` message;
* `DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output)`: the value of the `data_column` column is missing (`null`) or `Protobuf` parsing threw an exception; by default the hook rethrows `error`, and you can override this behavior, for example to silently drop invalid messages. An empty but present string isn’t the same as a missing value: it parses successfully into a message with default values if `TProto` has no required fields, and in that case it goes to `DoProcessProto`, not to `DoProcessUnparsed`.

Responsibility for errors is split: a parsing error itself is routed to `DoProcessUnparsed`, whereas an exception from `DoProcessProto` propagates outward and interrupts the epoch, so nothing is committed. By the time such an exception occurs, the state might already have been partially modified, and that mutation can’t be silently swallowed as an “unparsed” message.

You declare your own state the same way as in `TTransformOrderedSourceComputation`, through `TMutableStateKeyClient<T>` in `DoInit`; the key for `GetState` is `inputMessage->Key`, so the `DoProcessProto(const TInputMessageConstPtr&, TProto&&, IOutputCollectorPtr)` hook suits a stateful computation.

For an example, see `NYT::NFlow::NExample::TProtoLogParserComputation` from [`examples/cpp/proto_parser`]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser): it parses `TLogRecordProto`, emits `TLogRecordMessage` (`level`, `text`, `seen_at_level`), and maintains the `TLevelCountsState` state, a counter of records of each level per source partition. The counter isn’t idempotent under reprocessing and is correct exactly because the state is committed in the same transaction as the `source` offset. For more details, see the [Proto parser](../../../flow/cpp/examples/proto_parser.md) section.

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

Each partition is processed strictly single-threaded. You achieve multithreading by increasing the number of partitions (`partition_count` in the spec). Batch methods `DoProcess(IInputContextPtr input, IOutputCollectorPtr output)` receive all messages and timers for the current [epoch](../../../flow/concepts/glossary.md#epoch), which lets you optimize processing.

If you need to run code multithreaded within a single partition, use `GetContext()->PoolInvoker`, but be sure to wait for completion within the current method.

### How to estimate the load on internal tables? {#internal-tables-load}

The load on {{product-name}} internal tables depends on the `Computation` type and the number of partitions. Below is an approximate estimate:

#|
|| **Computation type** | **Records per epoch per partition** | **Comment** ||
|| `TTransformComputation` | ~2–4 | State writes + commit ||
|| `TTransformOrderedSourceComputation` | ~2–4 | Output materialization + offsets + states ||
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