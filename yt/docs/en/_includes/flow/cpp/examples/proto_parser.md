# Proto parser in {{product-name}} Flow (C++)

This example shows `TProtoParsingProcessFunctionBase<TProto>`, a base class for [process functions](../../../../flow/cpp/process-functions.md) that handles `Protobuf` message parsing. The [pipeline]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser) reads serialized log records from a queue, parses them without calling `ParseFromStringOrThrow` manually, and maintains a state counter for parsed records of each level.

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser)

## Pipeline components

### TProtoLogParserFunction

`TProtoLogParserFunction` inherits from `TProtoParsingProcessFunctionBase<TLogRecordProto>`, where `TLogRecordProto` is a `Protobuf` message with the `level` and `text` fields. The base class reads the input column named by the `data_column` parameter (the `"data"` column by default), parses it into `TLogRecordProto`, and invokes one of the user methods:

- `ProcessProto(...)`: after successful parsing, gets the state for the message key, increments the level counter, builds a `TLogRecordMessage`, and emits it to the `records` stream;
- `ProcessUnparsed(...)`: if the input column named by the `data_column` parameter is missing (`null`) or `Protobuf` parsing fails. The implementation is empty, so such messages are silently dropped. An empty but present string parses successfully into a `TLogRecordProto` with default values because the message has no required fields.

The process function is registered via `YT_FLOW_DEFINE_PROCESS_FUNCTION`. In the `parser` spec, `TProcessFunctionTransformOrderedSourceComputation` hosts it and `processing_function` contains `NYT::NFlow::NExample::TProtoLogParserFunction`. If necessary, the input column name can be set in `processing_function_parameters/data_column`.

The `records` output stream is configured with a direct external `TSyncQueueSink`, so no separate sink computation is required.

### The TLevelCountsState state

`TProtoLogParserFunction` stores a `TMutableStateKeyClient<TLevelCountsState> StateClient_` and initializes it in `DoInit(const IRuntimeInitContextPtr&)` via `initContext->InitClient(StateClient_, "level_counts")`. `ProcessProto` accesses the state through `StateClient_.GetState(message->Key)`. The state contains `record_counts`, the number of parsed records of each level per source partition; the current value is written to the output message as `seen_at_level`.

`TProcessFunctionTransformOrderedSourceComputation` materializes output and persists state in the same epoch transaction that advances the source offset. This keeps the counter consistent with queue consumption across restarts.

## Message types

`TLogRecordMessage` is a `TYsonMessage` subclass registered via `YT_FLOW_DEFINE_YSON_MESSAGE` with the following fields:

- `level`: the record level from `TLogRecordProto`;
- `text`: the record text from `TLogRecordProto`;
- `seen_at_level`: the number of parsed records of this level in the source partition, including the current record.

## The main function

The example library contains process-function and type registration. `main` initializes Flow, registers the `records` stream through `TSimpleSpecBuilder`, and starts `TSimpleRunnerProgram`.

## Source code

### TProtoLogParserFunction

{% code '/yt/yt/flow/examples/cpp/proto_parser/lib/proto_parser_function.h' lang='cpp' %}

{% code '/yt/yt/flow/examples/cpp/proto_parser/lib/proto_parser_function.cpp' lang='cpp' %}

## See also

- [Getting started (C++)](../../../../flow/cpp/getting-started.md)
- [Process functions (C++)](../../../../flow/cpp/process-functions.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)
- [Log parser](../../../../flow/cpp/examples/log_parser.md)
