# Retryable Async Request in {{product-name}} Flow (C++)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) is similar to [Async Request](../../../../flow/cpp/examples/async_request.md), but it adds retry logic using [timers](../../../../flow/concepts/glossary.md#timer) and the internal `YsonState`.

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/retryable_async_request)

## Difference from Async Request

The key difference is that `TRequestProcessor` is hosted by `TProcessFunctionComputation` rather than `TProcessFunctionSwiftMapComputation` because it needs to:
- Store the internal [state](../../../../flow/concepts/glossary.md#state) to track the number of failed attempts.
- Use timers for retries at a specified interval.

## Pipeline Components

### TRequestProcessor

`TRequestProcessor` implements `IProcessFunction` and uses `TMutableStateKeyClient<TDelayedRequestState>` to store the internal state (Internal YsonState). You initialize the state in `Init(const IRuntimeInitContextPtr& initContext)` via `initContext->InitClient(RequestStateClient_, "request_state")`.

When you process an incoming message (`ProcessMessage`):
1. You save the request in the state with `FailedAttempts = 0`.
2. You call `TryRequest` to perform the attempt.

The `TryRequest` method contains the main retry logic:
- If the request fails (determined by `IsRequestSuccessful`), you increment the `FailedAttempts` counter and set a timer via `output->AddTimer(GetNextAttempt(context))`.
- If the request succeeds, you create a `TResponseMessage`, reset the state via `state.Clear()`, and send the response.

When the timer fires (`ProcessTimer`), you call `TryRequest` again with the current state.

### TStateKeeper

It’s fully similar to `TStateKeeper` from the [Async Request](../../../../flow/cpp/examples/async_request.md) example: it accepts incoming events and responses and stores the accumulated result in the external state.

## Retry Pattern

The retry logic is based on the following elements:

- **TDelayedRequestState** — a descendant of `NYTree::TYsonStruct`, it stores `FailedAttempts` and the `Request` itself.
- **TMutableStateKeyClient** — a client for working with the internal YsonState. Unlike `TSimpleExternalStateManager`, the state is stored in Flow’s internal tables, not in an external user table.
- **Timers** — on a failed attempt, you set a timer with a `Delay` via `output->AddTimer(GetNextAttempt(context))`.
- **Constants** — `MaxRetries = 3` defines the simulated success period: a request succeeds immediately or after no more than two retries. `Delay = 5` defines the delay between attempts.

You calculate the time for the next attempt via `context->GetCurrentTimestamp()`, which ensures correct operation with Flow’s system time.

## Pipeline Structure

1. **events** → `TStateKeeper` → **request** (request generation).
2. **request** → `TRequestProcessor` → **response** (processing with retries).
3. **response** → `TStateKeeper` → state (result accumulation).

In the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) for `TRequestProcessor`, you must register the `timer_streams` section to support retries. The process function class is specified in `processing_function`.

## Source Code

### TRequestProcessor

{% code '/yt/yt/flow/examples/cpp/retryable_async_request/lib/retryable_async_request_functions.cpp' lang='cpp' lines='[BEGIN request_processor]-[END request_processor]' keep-indents %}

### TStateKeeper

{% code '/yt/yt/flow/examples/cpp/retryable_async_request/lib/retryable_async_request_functions.cpp' lang='cpp' lines='[BEGIN state_keeper]-[END state_keeper]' keep-indents %}
