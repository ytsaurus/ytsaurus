# Retryable Async Request in {{product-name}} Flow (Python)

This extends the [AsyncRequest](../../../../flow/python/examples/async_request.md) example: the request handler supports automatic retries using [timers](../../../../flow/concepts/glossary.md#timer). If a request fails, it’s saved in the internal [state](../../../../flow/concepts/glossary.md#state) and retried after a fixed interval, up to `MAX_RETRIES` attempts.

[Source code]({{source-root}}/yt/yt/flow/examples/python/retryable_async_request)

## Structure

The pipeline includes two computations:

1. **`state`** (`StateKeeperFunction`) — a stateful computation: it routes events into requests and accumulates the total response length in the external state (similar to `async_request`).

2. **`processor`** (`RequestProcessorFunction`) — a stateful computation with retry logic:
   - On the first attempt, it saves the request in the internal state and tries to process it.
   - If processing fails, it increments the failure counter, saves the state, and sets a timer (`DELAY_SECONDS = 5`).
   - It retries the attempt after the timer fires (up to `MAX_RETRIES = 3`).
   - If successful, it sends the response and clears the state.

## `request_processor_function.py`

This is the key example file: it implements retry logic via `on_timer` and the helper method `_try_or_retry`.

{% code '/yt/yt/flow/examples/python/retryable_async_request/request_processor_function.py' lang='python' lines='[BEGIN request_processor]-[END request_processor]' %}

## `state_keeper_function.py`

It handles stream routing and accumulates results in the external state (the same as in `async_request`).

{% code '/yt/yt/flow/examples/python/retryable_async_request/state_keeper_function.py' lang='python' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `__main__.py`

This is the entry point: its structure matches `async_request`, but it imports different classes.

{% code '/yt/yt/flow/examples/python/retryable_async_request/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## Key patterns

- **Retry via timers**: `output.add_timer(int(time.time()) + DELAY_SECONDS)` delays the next retry; `on_timer` reads the state and runs `_try_or_retry` again. This is the standard pattern for implementing retries in Flow without external queues.
- **Retry counter in state**: the `failed_attempts` field is stored with the request data in `ctx.state("request-state", message)`, which ensures correctness after a restart.
- **Deterministic failure simulation**: `_is_request_succeed(request_id, failed_attempts)` simulates an unstable external service; in real code, you replace this with an HTTP client call.
- **Clearing state on success**: `state.clear()` after successful processing prevents the timer from triggering again on outdated data.
- **Separation of concerns**: `StateKeeperFunction` doesn’t know about retries — all retry logic is encapsulated in `RequestProcessorFunction`, which makes it easier to swap retry strategies.

