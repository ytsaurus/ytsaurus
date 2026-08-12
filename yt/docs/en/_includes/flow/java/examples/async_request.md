# Async Request in {{product-name}} Flow (Java)

Use this pipeline example to implement an event-driven request–response cycle. Incoming events trigger requests to the handler. Responses go back to the same computation and accumulate in the external state. This example shows a cyclic pipeline topology and shared use of external state.

[Source code (Java)]({{source-root}}/yt/yt/flow/examples/java/async_request)

[Source code (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/async_request)

## Components

### StateKeeperFunction

You handle two input streams — `event` and `response`. When you get an `event`, you generate a request with a unique `request_id` and emit it to the `request` stream. When you get a `response`, you accumulate the total length of responses in the `total_length` field of the external state:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### RequestProcessorFunction

This is a stateless computation: you get a request from the `request` stream, calculate the string length, and immediately send the response to the `response` stream:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### Registering computations

You register the `state` and `processor` computations with the `@FlowComputation` annotation on their process function classes:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

### NodeCompanionMain

This is the entry point for the Spring Boot–based companion:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Key patterns

- **Cyclic topology**: the `response` stream goes back to the `state` computation, closing the cycle `event → request → response → state`. Flow explicitly supports such graphs.
- **Routing by `streamId`**: one function handles multiple input streams. You determine the message type using `message.getStreamId()`.
- **ExternalStateAccessor with PayloadBuilder**: you update the `total_length` field selectively: `current.toBuilder()` → make changes → `stateAccessor.set(updated.finish())`.
- **Configuration via Spring Boot**: you register computations with the `@FlowComputation` annotation; `flow-spring-boot-starter` manages the gRPC server lifecycle.

