# Async Request in {{product-name}} Flow (Go)

An example of a [pipeline](../../../../flow/concepts/glossary.md#pipeline) that makes an asynchronous call to an external service: one [computation](../../../../flow/concepts/glossary.md#stream-and-computation) turns events into requests and accumulates the responses in an external [state](../../../../flow/concepts/glossary.md#state), and another one serves the requests without any state. This is a Go implementation of the same scenario as the [C++ example](../../../../flow/cpp/examples/async_request.md).

[Source code]({{source-root}}/yt/yt/flow/examples/go/async_request)

## Structure {#structure}

The companion serves two computations; `injector` stays a native source declared in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec):

1. **`state`** (`stateKeeper`) — a stateful computation grouped by `key`, which:
   - takes events from the `event` stream and produces a request in the `request` stream with a random `request_id`;
   - takes responses from the `response` stream and adds up their total length (`total_length`) in the external state `/state`.

2. **`processor`** (`requestProcessor`) — a stateless computation grouped by `request_id`: it takes requests from the `request` stream and immediately answers with the length of the request string in the `response` stream.

The `event → request → response → state` cycle closes between the two computations. An event is answered with a request rather than with the result right away, so the serving side never holds up processing: the response arrives later, as a separate [message](../../../../flow/concepts/glossary.md#message), and only then does the state of the key move on.

## `main.go` {#main-go}

The entry point: creating the pipeline and registering both computations.

{% code '/yt/yt/flow/examples/go/async_request/main.go' lang='go' %}

## `state_keeper.go` {#state-keeper-go}

Routing of the input streams (`event` / `response`) and work with the external state.

{% code '/yt/yt/flow/examples/go/async_request/state_keeper.go' lang='go' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `request_processor.go` {#request-processor-go}

The stateless request handler: it computes the length of the request string and returns a response.

{% code '/yt/yt/flow/examples/go/async_request/request_processor.go' lang='go' lines='[BEGIN request_processor]-[END request_processor]' %}

## Key patterns {#key-patterns}

- **Routing by `msg.StreamID`**: a `switch` on the input stream identifier lets one computation handle several inputs with different logic. An unknown stream is an error rather than a silent skip.
- **A random `request_id`**: `rand.Uint64()` links the request to the response. The request carries the key of the original event, so the response, partitioned by `request_id`, comes back to the state the call belongs to.
- **External state** through `flow.OpenExternalState(rt, "/state", msg)`: the row is turned into `totalLengthState` by `ConvertTo`, changed as a structure, and saved through `ConvertFrom`.
- **A stateless computation**: `requestProcessor` uses no state and is grouped by `request_id` rather than by the event key, so the requests of one key spread over all [partitions](../../../../flow/concepts/glossary.md#partition) and scale independently.
- **Stream dependency**: `streams_dependency` in the spec declares that `request` is produced from `event` — the worker takes this into account when advancing [watermarks](../../../../flow/concepts/watermarks.md).
