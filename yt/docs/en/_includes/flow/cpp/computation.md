# Computation in {{product-name}} Flow (C++)

{% note info %}

This page describes Computation execution modes in C++. For a language-agnostic description of the concept, see [Computation](../../../flow/concepts/computation.md).

{% endnote %}

{% note warning %}

Implement new C++ user logic only as a [process function](../../../flow/cpp/process-functions.md). Don’t create subclasses of the `Computation` base classes. This page documents execution modes, built-in computations, and the low-level API needed for framework and legacy maintenance.

{% endnote %}

This section describes the base `Computation` classes used by built-in process-function adapters and important execution-mode details.

Flow currently implements four base `Computation` classes:

- `TTransformComputation`
- `TTransformOrderedSourceComputation`
- `TSwiftOrderedSourceComputation`
- `TSwiftMapComputation`

Classes that include `Swift` in their name implement the [Swift](../../../flow/concepts/swift.md) principle. See the [Swift](../../../flow/concepts/swift.md) section for more details.

## General

The following direct-API details are for framework and existing legacy maintenance; they aren’t an authoring guide for new user computations.

- Declare process-function parameters as a regular `TYsonStruct`, name its type in `YT_FLOW_DEFINE_PROCESS_FUNCTION`, and pass values through `processing_function_parameters`.

- Perform complex process-function initialization in `Init(const IRuntimeInitContextPtr&)`.
- Select one processing granularity: `IProcessFunction` for individual messages, timers, and visits; `IBatchProcessFunction` for the whole epoch; or `IKeyedBatchProcessFunction` for one key’s batch.
- To write manually in the transform-mode transaction, also implement `ISyncProcessFunction`; otherwise use a [Sink](../../../flow/concepts/glossary.md#sink) or [ExternalState](../../../flow/cpp/state.md#external-state).
- Add output messages and timers through `IOutputCollector`; create and convert them through `IRuntimeContext`.
- Code in one partition runs strictly single-threaded. Parallelize processing by increasing the number of [partitions](../../../flow/concepts/glossary.md#partition).
- You can convert input messages to `NYTree::TYsonStruct`. To do this, you need to:
  - Create a class that inherits from `TYsonMessage` (this is a special child of `NYTree::TYsonStruct`).
  - Register it in the global registry using `YT_FLOW_DEFINE_YSON_MESSAGE`.
  - In the `main` function, create a `TSimpleSpecBuilder` object and register the corresponding `stream_id` in it.
  - If you use `TSimpleRunnerProgram`, you can pass this `TSimpleSpecBuilder` directly to the `TSimpleRunnerProgram` constructor.
  - If you implement `main` yourself, you’ll need to pass specs to `TSimpleSpecBuilder` to enrich them with stream information.
  - You don’t need to manually fill `spec/streams` when using `TYsonMessage` — all information will be derived from the registered `TYsonMessage + stream_id` using `TSimpleSpecBuilder`.
  - In a process function, use `context->ConvertToYsonMessage<T>(message)` and `context->ConvertToMessage(ysonMessage)` to convert between `TMessage => TYsonMessage` and back.

### OutputCollector {#output-collector}

The `IOutputCollectorPtr output` object is passed to process-function methods and sends processing results:

| Method | Description |
| --- | --- |
| `output->AddMessage(message)` | Add an output message (a `TMessage` object obtained via `context->MakeOutputMessageBuilder().Finish()`) |
| `output->AddTimer(timer)` | Add a [timer](../../../flow/concepts/glossary.md#timer) |
| `output->SetParents(parentIds)` | Set the parent ID for tracking [lineage](../../../flow/concepts/lineage.md). Returns a new `IOutputCollectorPtr` with the attached lineage context |

Use `SetParents` when the output message is logically derived from a specific subset of inputs, not the entire batch. Non-batch `ProcessMessage` and `ProcessTimer` set lineage automatically.

### TMessage {#tmessage}

The `TMessage` structure is used in `ProcessMessage` and `AddMessage` methods:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TMessageSerializer.md) %}

## TTransformComputation

Transform mode handles arbitrary transformations of input data. It doesn’t work with `Source`. The result is always saved in YT, so the transformation needn’t be deterministic.

Properties of `TTransformComputation`:

- It can write to YT “idle”, that is, without real changes, by overwriting existing content. You should expect such a stream to create a negligible load.

For new user logic, select this mode with `TProcessFunctionComputation`. Implement `IProcessFunction`, `IBatchProcessFunction`, or `IKeyedBatchProcessFunction`; add `ISyncProcessFunction` when you need a sync phase. For a complete example, see [Process functions](../../../flow/cpp/process-functions.md).

### TTimer {#ttimer}

The `TTimer` structure is used in `ProcessTimer` and `AddTimer` methods:

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

This mode processes `source` messages with arbitrary custom logic: parsing, filtering, or expanding one input message into several outputs. Run new user process functions under `TProcessFunctionTransformOrderedSourceComputation`. It replaces the `TSwiftPassthroughOrderedSourceComputation` → `TProcessFunctionComputation` chain when the intermediate computation only processes source data.

The transformation result is materialized in {{product-name}} the same way as in `TTransformComputation`:

- Output messages receive unique `MessageId` values and are durably saved in {{product-name}} before they are distributed downstream. After a restart, materialized messages that haven’t been delivered yet are distributed with the same `MessageId` values instead of being recomputed, so the transformation has no determinism requirements.
- A message can be added to `output` with an explicit `distribute` flag, for example `output->AddMessage(std::move(message), /*distribute*/ false)`. Such a message isn’t published downstream, but it participates in watermark estimation on equal terms with published ones: the watermark generator registers the read over the full set of output messages before the publication filter is applied, so the watermark can be estimated correctly over the full stream even when a significant part of it is filtered out. The source offset advances in the epoch transaction in either case.
- The `source` offset, the materialized output, and the states are committed in a single epoch transaction, so processing of each source message is applied exactly once.

For new user logic, implement a process function: use `Init(const IRuntimeInitContextPtr&)` for initialization, `ProcessMessage` or `Process` for processing, and, when needed, `ISyncProcessFunction::Sync` for manual writes in the epoch transaction.

A process function keeps its state in a `TMutableStateKeyClient<T>` field (see [Working with states](../../../flow/cpp/state.md#internal-state)), initializes it through `initContext->InitClient(...)`, and reads it through `GetState(message->Key)`. Before processing, the adapter loads state for the current epoch’s message keys. A computation instance is always bound to a single `source` key, so all messages in an epoch carry the same key and address the same state row.

The framework synchronizes state clients created through `IRuntimeInitContext` in the epoch transaction atomically with the `source` offset, so an ordinary mutation, such as incrementing a counter, is exactly-once correct: no additional deduplication by `MessageId` is needed.

The computation spec is validated at startup; the following fields cause a validation error:

* `input` streams;
* [timers](../../../flow/concepts/glossary.md#timer);
* [key-visitor streams](../../../flow/concepts/key_visitor.md);
* a non-empty `group_by_schema`;
* `external_state_managers`;
* `external_state_joiners` that have no `join_on/key_schema_override` set (the source message key isn’t described by `group_by_schema`, so the key schema must be set explicitly).

`watermark_strategy` is supported: `watermark_generator` estimates the source watermarks, `watermark_alignment` aligns reading of the source relative to other streams (`read_delays` delay reading, not publication), and `event_timestamp_assigner` assigns `event_timestamp` to output messages. The atomicity of the commit of the `source` offset, the materialized output messages, and the state doesn’t depend on alignment. `skip_if_expression` is supported as well.

`skip_if_expression` is applied before processing, but after the input batch has been counted in the metrics and in the number of late messages: a filtered-out message reaches neither the state nor the output. It doesn’t affect watermark estimation either: the generator registers a read only over output messages, so a fully filtered-out batch doesn’t move `EventWatermark`, and over a long series of such batches the partition watermark stands still. The source takes the `EventWatermark` markers of input records into account when reading regardless of the filter, but with `use_source_watermark = false` (the default value) the source watermark only bounds the estimate from above and never moves it forward; it becomes the only source of the partition watermark with `use_source_watermark = true`. This exactly matches the behavior of `TSwiftOrderedSourceComputation`.

Write user logic as a [process function](../../../flow/cpp/process-functions.md) and specify the `NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation` adapter in the spec. It runs the function in this mode with the same output materialization, states, and spec validation.

For an example, see `NYT::NFlow::NExample::TLogParserProcessFunction` from [`examples/cpp/log_parser`]({{source-root}}/yt/yt/flow/examples/cpp/log_parser): it splits a log line into records, emits the `TLogRecordMessage` YSON structure (`level`, `text`, `worst_level_so_far`), and maintains the `TWorstSeverityState` state, a running maximum severity per source partition. For more details, together with the full source code, see the [Log parser](../../../flow/cpp/examples/log_parser.md) section.

#### TProtoTransformOrderedSourceComputation {#tprototransformorderedsourcecomputation}

For new user logic, use `TProtoParsingProcessFunctionBase<TProto>` from `yt/yt/flow/library/cpp/parsers/proto.h`. The base reads the string column selected by `processing_function_parameters/data_column` (`"data"` by default), parses it into `TProto`, and calls `ProcessProto(message, proto, output, context)`. It routes a read or parsing error to `ProcessUnparsed(message, error, output, context)`, which rethrows by default.

Keep state in `TMutableStateKeyClient<T>` and initialize it in `Init`; the key is available as `message->Key`. For materialized ordered-source mode, run the function under `TProcessFunctionTransformOrderedSourceComputation`.

`TProtoTransformOrderedSourceComputation<TProto>` is the low-level counterpart for maintaining existing legacy code. Don’t use it as the base of a new user class.

For a process-function example with the same parsing pattern, see `NYT::NFlow::NExample::TProtoLogParserFunction` from [`examples/cpp/proto_parser`]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser). It inherits from `TProtoParsingProcessFunctionBase<TLogRecordProto>` and runs under `TProcessFunctionTransformOrderedSourceComputation`: it parses `TLogRecordProto`, emits `TLogRecordMessage` (`level`, `text`, `seen_at_level`), and maintains the `TLevelCountsState` state, a counter of records of each level per source partition. The counter isn’t idempotent under reprocessing and is correct exactly because the state is committed in the same transaction as the `source` offset. For more details, see the [Proto parser](../../../flow/cpp/examples/proto_parser.md) section.

## TSwiftMapComputation

Swift-map mode implements a deterministic simple `Map` without materializing results in YT. Run user process functions under `TProcessFunctionSwiftMapComputation`.

Features:

- It doesn’t support `sources` and `sinks`.
- It supports `timer_streams` and `key_visitor_streams` only for working with state: emitting output messages from timer or visit processing is prohibited, so output streams can’t depend on timer and visit streams in `streams_dependency`.
- It must return the same result (including order) for each input row. If the result changes on repeated runs, various negative effects can occur. It’s possible that separate parts of the system will process different versions of the output, up to duplicates, if the field values for subsequent `group-by` change.
- As a consequence, each resulting message must have exactly one parent.

### TSwiftPassthroughComputation

This is a child of `TSwiftMapComputation`. It’s similar to `TPassthroughComputation`: it simply turns `input` into `output` by converting messages to a new schema.

## TSwiftOrderedSourceComputation

Swift ordered-source mode reads data from external sources and requires the data stream from each instance to be ordered. Run user process functions under `TProcessFunctionSourceComputation`.

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

Each partition is processed strictly single-threaded. You achieve parallelism by increasing the number of partitions (`partition_count` in the spec). `IBatchProcessFunction::Process` receives all messages and timers for the current [epoch](../../../flow/concepts/glossary.md#epoch), which lets you optimize processing.

The process-function API intentionally doesn’t expose `PoolInvoker`; parallelize processing through partitions.

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

- [Process functions (C++)](../../../flow/cpp/process-functions.md)
- [Computation (concept)](../../../flow/concepts/computation.md)
- [Working with states (C++)](../../../flow/cpp/state.md)
- [Quick start (C++)](../../../flow/cpp/getting-started.md)
