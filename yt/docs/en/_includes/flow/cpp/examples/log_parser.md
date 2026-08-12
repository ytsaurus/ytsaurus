# Log parser in {{product-name}} Flow (C++)

This example shows [`TTransformOrderedSourceComputation`](../../../../flow/concepts/computation.md#ttransformorderedsourcecomputation) (for details, see [Computation (C++)](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)): the [pipeline]({{source-root}}/yt/yt/flow/examples/cpp/log_parser) reads log lines from a queue and parses them right when reading the source, without an intermediate passthrough computation and a `TTransformComputation`. The parsing logic itself is written as a [process function](../../../../flow/cpp/process-functions.md) and runs under the built-in adapter. The example also shows reading from a `Source` and maintaining your own durable state that survives restarts (see [State](#state)).

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/log_parser)

## Pipeline components

### TLogParserProcessFunction

The user logic is written as a [process function](../../../../flow/cpp/process-functions.md): a subclass of `IProcessFunction` that doesn’t depend on the `Computation` object, so you can cover it with unit tests without a cluster ([unittest]({{source-root}}/yt/yt/flow/examples/cpp/log_parser/unittest/log_parser_process_function_ut.cpp)). It’s executed by the built-in `TProcessFunctionTransformOrderedSourceComputation` adapter, which also sets the mode to ordered source (see the [list of adapters](../../../../flow/cpp/process-functions.md#how-it-works)).

In `ProcessMessage(const TInputMessageConstPtr& message, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context)`, the function reads the `line` column of the raw `source` message via `GetColumnValue<std::string>(message, "line")` and parses it with `ParseLogLine` into records of the form `"level:text"` separated by `;`, discarding records without the `:` separator, with empty text, or with a level other than `info`, `warning`, and `error`. For each valid record, it updates the state through the `StateClient_.GetState(message->Key)` accessor (see [State](#state)), builds a `TLogRecordMessage`, and emits it into the `records` output stream by calling `output->AddMessage(context->ConvertToMessage(outputRecord))`.

The transformation result, the `records` stream, is materialized in {{product-name}} as in `TTransformComputation`, so the transformation has no determinism requirements: after a restart, Flow distributes the already materialized messages with the `MessageId` values previously assigned to them instead of recomputing them.

You can still inherit from the class: that’s how the [Proto parser](../../../../flow/cpp/examples/proto_parser.md) example is written, on top of the `TProtoTransformOrderedSourceComputation<TProto>` helper over `TTransformOrderedSourceComputation`. The adapter’s spec validator is the same as the base class’s: a non-empty `group_by_schema`, timers, key-visitor streams, and `external_state_managers` are rejected in either variant (for the full [list of limitations](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)).

### The parser computation spec

Two fields of the `parser` computation spec link the function to the adapter (see [Registration](../../../../flow/cpp/process-functions.md#registration)):

```yson
"computation_class_name" = "NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation";
"processing_function" = "NYT::NFlow::NExample::TLogParserProcessFunction";
```

The remaining fields of the `parser` record describe the connections: `source_streams.queue` is a `TQueueSource` with the paths to the queue and the consumer, and `sinks.queue` is a direct external `TSyncQueueSink` for the `records` stream, so no separate sink computation is needed. For the full file, see [pipeline.yson]({{source-root}}/yt/yt/flow/examples/cpp/log_parser/pipeline.yson).

## Message types

`TLogRecordMessage` is a subclass of `TYsonMessage` (a YSON structure registered via `YT_FLOW_DEFINE_YSON_MESSAGE`) with the following fields:

- `level`: the record level (`info`, `warning`, or `error`);
- `text`: the record text;
- `worst_level_so_far`: the highest severity level (`info < warning < error`) encountered in this source partition at the time of the record (see [State](#state)).

## State {#state}

`TLogParserProcessFunction` is stateful. It keeps the `TWorstSeverityState` state in the `TMutableStateKeyClient<TWorstSeverityState> StateClient_` field, exactly as `TTransformComputation` does (see [Working with states (C++)](../../../../flow/cpp/state.md#internal-state)). The `TProcessFunctionTransformOrderedSourceComputation` adapter does the rest: it calls `Init(const IRuntimeInitContextPtr& initContext)`, where the client connects to the state by calling `initContext->InitClient(StateClient_, WorstSeverityStateName)` (the state name is `worst_severity`), and `ProcessMessage`, where the state is read through the `GetState(message->Key)` accessor and the output records are converted into messages via `context->ConvertToMessage(...)`.

The computation instance is bound to a single source partition, so all messages carry the same key and address the same state row: `state->WorstSeverity = std::max(state->WorstSeverity, SeverityRank(record.Level))`.

The framework synchronizes this state in the same epoch transaction as the advance of the source offset (see [Computation](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)); the function itself does nothing for this. Therefore, user state is exactly-once correct and doesn’t have to be idempotent under reprocessing: an ordinary, “naive” counter of processed records would be just as correct here. The example keeps a running maximum severity simply because it’s a natural aggregate for such a pipeline, not because it’s in any way safer than a counter.

## The main function

The `main` function does the following:
1. `NYT::NFlow::Initialize(argc, argv)`: initializes the Flow library.
2. `TSimpleSpecBuilder`: the builder for registering streams. `RegisterStream<TLogRecordMessage>("records")` registers the `records` stream with the `TLogRecordMessage` message type.
3. `TSimpleRunnerProgram(std::move(builder)).Run(argc, argv)`: starts the pipeline.

You don’t need to register the function in `main`: the `YT_FLOW_DEFINE_PROCESS_FUNCTION(TLogParserProcessFunction)` macro is placed at file level in `lib/log_parser_process_function.cpp`, and the file itself is linked into the library as `GLOBAL`, so the registry entry appears when the binary is initialized.

## Source code

### TLogParserProcessFunction

{% code '/yt/yt/flow/examples/cpp/log_parser/lib/log_parser_process_function.h' lang='cpp' %}

{% code '/yt/yt/flow/examples/cpp/log_parser/lib/log_parser_process_function.cpp' lang='cpp' %}

### ParseLogLine

{% code '/yt/yt/flow/examples/cpp/log_parser/lib/log_line_parser.cpp' lang='cpp' %}

## See also

- [Getting started (C++)](../../../../flow/cpp/getting-started.md)
- [Process function (C++)](../../../../flow/cpp/process-functions.md)
- [Computation (C++)](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)
- [Computation (concept)](../../../../flow/concepts/computation.md#ttransformorderedsourcecomputation)
- [Working with states (C++)](../../../../flow/cpp/state.md)
