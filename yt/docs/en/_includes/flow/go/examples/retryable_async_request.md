# Retryable Async Request in {{product-name}} Flow (Go)

An extension of the [Async Request](../../../../flow/go/examples/async_request.md) example: the request handler retries every failed attempt through [timers](../../../../flow/concepts/glossary.md#timer). The whole request is stored in the internal [state](../../../../flow/concepts/glossary.md#state) and retried with a fixed delay until the external service answers it.

[Source code]({{source-root}}/yt/yt/flow/examples/go/retryable_async_request)

## Structure {#structure}

The pipeline consists of three [computations](../../../../flow/concepts/glossary.md#stream-and-computation):

- `injector` — a native source (`TSwiftPassthroughOrderedSourceComputation`) declared directly in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): it reads the queue and publishes the events to the `event` [stream](../../../../flow/concepts/glossary.md#stream-and-computation). It has no Go code.
- `state` (`stateKeeper`) — a transform computation grouped by `key`: for every event it opens a request in the `request` stream, and it adds the response that arrives in the `response` stream to the total response length of the key in the external state `/state`.
- `processor` (`requestProcessor`) — a transform computation grouped by `request_id`: it makes the attempts to call the external service and publishes the response to the `response` stream.

The two computations are grouped by different keys for a reason: `processor` is partitioned by request identifier, so the requests of one key are retried independently of each other. The `delay` timer stream is declared in the spec with `allow_timer_self_dependency = %true` — the computation sets timers for itself.

## `main.go` {#main-go}

The entry point: creating the pipeline and registering both computations of the companion.

{% code '/yt/yt/flow/examples/go/retryable_async_request/main.go' lang='go' %}

## `state_keeper.go` {#state-keeper-go}

Stream routing and accumulation of the results in the external state: the branch on `msg.StreamID` sends the event and the response to two different handlers.

{% code '/yt/yt/flow/examples/go/retryable_async_request/state_keeper.go' lang='go' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `request_processor.go` {#request-processor-go}

The state value is an ordinary Go structure with YSON tags. It holds everything a retry needs, including the counter of failed attempts, so the retry no longer needs the original message:

{% code '/yt/yt/flow/examples/go/retryable_async_request/request_processor.go' lang='go' lines='[BEGIN request_state]-[END request_state]' %}

The retry logic: `OnMessage` opens the request and makes the first attempt, `OnTimer` reads the request from the state and repeats it. The `attempt` method shared by both handlers either sets a timer for the next attempt or publishes the response and clears the state:

{% code '/yt/yt/flow/examples/go/retryable_async_request/request_processor.go' lang='go' lines='[BEGIN request_processor]-[END request_processor]' %}

## Key patterns {#key-patterns}

- Retries through timers: `out.AddTimer(flow.TimerRequest{TriggerTimestamp: ...})` postpones the next attempt by `retryDelay`, and `OnTimer` performs it. This is the standard way to implement retries in Flow without external queues. An empty `StreamID` in `flow.TimerRequest` means the only timer stream of the computation.
- The attempt counter lives in the state together with the request data, so it survives a [worker](../../../../flow/concepts/glossary.md#worker) restart: after a restart the retries continue from the same place.
- The input is converted into `requestMessage` once through `msg.ConvertTo(&input)`, while the retry logic works with a separate `requestState` structure that holds the counter of failed attempts.
- The same state is opened both by a message and by a timer: `flow.OpenYSONState[requestState](rt, requestStateName, msg)` and `flow.OpenYSONState[requestState](rt, requestStateName, timer)` — both inputs carry the grouping key that the state is addressed by.
- Clearing the state on success: after `state.Clear()`, a timer that fires later finds `state.Empty()` and returns without doing anything — a stale firing is harmless.
- Separation of responsibilities: `stateKeeper` knows nothing about retries, the whole logic is encapsulated in `requestProcessor`, so you can change the retry strategy without touching the accounting of the results.
- Deterministic failure simulation: `succeeds(request)` stands in for a real client of the external service — in production code there would be an HTTP call here.
