# Retryable Async Request in {{product-name}} Flow (Java)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) extends [AsyncRequest](../../../../flow/java/examples/async_request.md): the request handler now supports retry attempts with a delay. If a failure occurs, `RequestProcessorFunction` saves the request state in the internal [state](../../../../flow/concepts/glossary.md#state) and sets a timer for 5 seconds; success is determined by the predicate `(requestId + failedAttempts) % 3 == 0`.

[Source code (Java)]({{source-root}}/yt/yt/flow/examples/java/retryable_async_request)

[Source code (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/retryable_async_request)

## Components

### RequestProcessorFunction

This implements the retry logic using an internal YSON state and timers. When you receive a request, you save it to the state and call `tryRequest`. When the timer fires, you load the state and retry the attempt:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

{% endlist %}

The helper method `tryRequest` checks the success predicate. If the attempt fails, it increments the attempt counter, saves the state, and schedules the next attempt.

### RequestState

This is a YSON-serializable model of the request state. It stores `requestId`, `key`, the request text `request`, and the counter `failedAttempts`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/model/RequestState.java' lang='java' lines='[BEGIN request_state]-[END request_state]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/model/RequestState.kt' lang='kotlin' lines='[BEGIN request_state]-[END request_state]' keep-indents %}

{% endlist %}

### StateKeeperFunction

This is identical to the `StateKeeperFunction` from [AsyncRequest](../../../../flow/java/examples/async_request.md): it processes the `event` and `response` streams and accumulates `total_length` in the external state. The retry logic is fully encapsulated in `RequestProcessorFunction`.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### Registering computations

You register the `state` and `processor` computations with the `@FlowComputation` annotation on the classes of their process functions:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

### NodeCompanionMain

This is the entry point of the companion based on Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Key patterns

- **Retries via timers**: if a failure occurs, `output.addTimer(now + 5, 0L)` schedules a repeated call to `onTimer`; the state between attempts is stored in `YsonStateAccessor`.
- **Success predicate**: `(requestId + failedAttempts) % 3 == 0` simulates an unstable external service; in real tasks, you replace this with a check of the HTTP status or another indicator.
- **Clearing the state after success**: `accessor.clear()` is called only on a successful response, preventing reprocessing.
- **Separation of concerns**: the session state logic (`StateKeeperFunction`) is separated from the retry logic (`RequestProcessorFunction`), which makes it easier to test each part independently.

## Differences from AsyncRequest

| Aspect | AsyncRequest | RetryableAsyncRequest |
|--------|--------------|-----------------------|
| `RequestProcessorFunction` | Stateless, responds immediately | Has internal state and timers |
| Failure handling | Not provided | Retry with a 5-second delay |
| Request state | Absent | `RequestState` in YSON |

