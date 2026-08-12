# Proto parser in {{product-name}} Flow (C++)

This example shows [`TProtoTransformOrderedSourceComputation<TProto>`](../../../../flow/cpp/computation.md#tprototransformorderedsourcecomputation), a helper over [`TTransformOrderedSourceComputation`](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation) that takes over parsing of `Protobuf` messages from a `Source`: the [pipeline]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser) reads serialized `Protobuf` log records from a queue, parses them without calling `ParseFromStringOrThrow` manually, and maintains its own state, a counter of parsed records of each level.

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser)

## Pipeline components

### TProtoLogParserComputation

`TProtoLogParserComputation` is a subclass of `TProtoTransformOrderedSourceComputation<TLogRecordProto>`, where `TLogRecordProto` is a `Protobuf` message with the `level` and `text` fields. The base class reads the `data_column` column (`"data"` by default) of the raw `source` message itself, parses it into `TLogRecordProto`, and calls one of the user hooks:

- `DoProcessProto(const TInputMessageConstPtr& inputMessage, TLogRecordProto&& proto, IOutputCollectorPtr output)`: on successful parsing, it gets the state accessor by the `inputMessage->Key` key, increments the counter for the record level, builds a `TLogRecordMessage` from the `level` and `text` fields and the current counter value, and emits it into the `records` output stream;
- `DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output)`: the value of the `data_column` column is missing (`null`) or `Protobuf` parsing failed; the implementation is empty, so such messages are silently dropped. An empty but present string doesn’t get here: `TLogRecordProto` has no required fields, so it parses successfully into a message with default values and is processed in `DoProcessProto` as a regular record.

The `records` output stream is configured in the `parser` computation spec with a direct external `TSyncQueueSink`, so no separate sink computation is needed.

### The TLevelCountsState state

The computation declares the `TLevelCountsState` state exactly as `TTransformComputation` does (see [Working with states (C++)](../../../../flow/cpp/state.md#internal-state)): a `TMutableStateKeyClient<TLevelCountsState> StateClient_` field, `initContext->InitClient(StateClient_, "level_counts")` in `DoInit(IJobInitContextPtr)`, and the `StateClient_.GetState(inputMessage->Key)` accessor in `DoProcessProto`. The state holds `record_counts`, the number of parsed records of each level per source partition; the current value goes into the output message as `seen_at_level`. Such a state isn’t idempotent under reprocessing: if the source is re-read after a restart, a naive increment would produce doubled values. It’s correct because the framework synchronizes the state in the same epoch transaction as the advance of the `source` offset (see [`TTransformOrderedSourceComputation`](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)).

## Message types

`TLogRecordMessage` is a subclass of `TYsonMessage` (a YSON structure registered via `YT_FLOW_DEFINE_YSON_MESSAGE`) with the following fields:

- `level`: the record level, copied from the `level` field of the input `TLogRecordProto`;
- `text`: the record text, copied from the `text` field of the input `TLogRecordProto`;
- `seen_at_level`: how many records of this level have already been parsed in the source partition, including the current one.

## The main function

`YT_FLOW_DEFINE_COMPUTATION(TProtoLogParserComputation)` registers the computation; the macro is a namespace-scope declaration right after the class definition in `main.cpp`, not a call inside `main`. The `main` function does the following:
1. `NYT::NFlow::Initialize(argc, argv)`: initializes the Flow library.
2. `TSimpleSpecBuilder`: the builder for registering streams. `RegisterStream<TLogRecordMessage>("records")` registers the `records` stream with the `TLogRecordMessage` message type.
3. `TSimpleRunnerProgram`: starts the pipeline.

## Source code

### TProtoLogParserComputation

{% code '/yt/yt/flow/examples/cpp/proto_parser/main.cpp' lang='cpp' %}

## See also

- [Getting started (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md#tprototransformorderedsourcecomputation)
- [Computation (concept)](../../../../flow/concepts/computation.md#ttransformorderedsourcecomputation)
- [Log parser](../../../../flow/cpp/examples/log_parser.md)
