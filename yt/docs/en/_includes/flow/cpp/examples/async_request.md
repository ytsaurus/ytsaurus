# Async Request in {{product-name}} Flow (C++)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) demonstrates a pattern for asynchronous external requests using `TSwiftMapComputation`. You send events to the input, convert them into requests, process them with a deterministic processor, and accumulate the results in the [state](../../../../flow/concepts/glossary.md#state).

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/async_request)

## Pipeline components

### TStateKeeper

`TStateKeeper` inherits from `TTransformComputation` and uses `TSimpleExternalStateManager` to work with the external state. It handles two types of input messages:

- **The `event` stream**: when you receive an event, it creates a `TRequestMessage` with a unique `RequestId` and sends it to the `request` stream.
- **The `response` stream**: when you receive a response, it updates the state — it sums `total_length` from all received responses.

You distinguish the streams using `ysonMessage->Meta->StreamId`.

### TRequestProcessor

`TRequestProcessor` inherits from `TSwiftMapComputation` — this is a deterministic computation that doesn’t store input and output messages in {{product-name}}. It receives a `TRequestMessage`, performs processing (in this example, it calculates the request length), and generates a `TResponseMessage`.

You use `TSwiftMapComputation` because the request processing is a pure function: the same input data always generates the same result.

## Message types

- **TEventMessage** — a descendant of `TYsonMessage`. It contains the `Key` and `Data` fields.
- **TRequestMessage** — a descendant of `TYsonMessage`. It contains the `RequestId`, `Key`, and `Request` fields.
- **TResponseMessage** — a descendant of `TYsonMessage`. It contains the `RequestId`, `Key`, and `Length` fields.

You register all message types using the `YT_FLOW_DEFINE_YSON_MESSAGE` macro.

## Key pattern: request-response cycle

The main idea of this example is to build a request-response cycle within the pipeline using multiple streams:

1. **events** → `TStateKeeper` → **request** (request generation)
2. **request** → `TRequestProcessor` → **response** (request processing)
3. **response** → `TStateKeeper` → state (result accumulation)

`TStateKeeper` is both an event consumer and a response consumer. It uses `input_stream_ids = ["event", "response"]` and determines the input message type by `StreamId`.

## State management

`TStateKeeper` uses `TSimpleExternalStateManager` to store the sum of lengths of all processed requests. You bind the state client (`TMutableStateKeyClient<TSimpleExternalState>`) in `DoInit()` via `InitExternalStateClient(StateClient_, "/state")`. You declare the state parameters (the `path` to the table, etc.) in the `external_state_managers` section of the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) for `Computation`.

## The main function

In `main`, you register three streams:

- `RegisterStream<TEventMessage>("event")` — input events
- `RegisterStream<TRequestMessage>("request")` — requests to the processor
- `RegisterStream<TResponseMessage>("response")` — responses from the processor

## Source code

### TRequestProcessor

{% code '/yt/yt/flow/examples/cpp/async_request/lib/async_request_functions.cpp' lang='cpp' lines='[BEGIN request_processor]-[END request_processor]' keep-indents %}

### TStateKeeper

{% code '/yt/yt/flow/examples/cpp/async_request/lib/async_request_functions.cpp' lang='cpp' lines='[BEGIN state_keeper]-[END state_keeper]' keep-indents %}

