# Shuffle in {{product-name}} Flow (Java)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) reads the [stream](../../../../flow/concepts/glossary.md#stream-and-computation) of events, groups them by key, and counts the number of unique events using an external [state](../../../../flow/concepts/glossary.md#state) (ExternalStateAccessor). This example shows how to configure a [companion](../../../../flow/concepts/glossary.md#companion) with Spring Boot.

[Source code (Java)]({{source-root}}/yt/yt/flow/examples/java/shuffle)

[Source code (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/shuffle)

## Companion components

### EventMapper

This is a process function for the source-[computation](../../../../flow/concepts/glossary.md#stream-and-computation) `reader`. It parses and transforms the input data. The version below is simplified. In the [actual code]({{source-root}}/yt/yt/flow/examples/java/shuffle), you also parse the `data` field as JSON using Jackson’s `ObjectMapper`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/EventMapper.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/EventMapper.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### EventReducer

This is a process function that uses [ExternalStateAccessor](../../../../flow/java/state.md#external-state) to count the number of events:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/EventReducer.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/EventReducer.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

How it works:

- You get an `ExternalStateAccessor` for the state `"shuffle-state"`, which is bound to the key of the current message.
- You extract the current state value. If there is no state, `getOrDefault()` returns an empty `Payload`.
- You create a `PayloadBuilder` from the current state and increment the counter.
- You save the updated state.

### Registering computations

You register the source computation `reader` with the `@FlowSourceComputation` annotation, and the transformation `reducer` with the `@FlowComputation` annotation:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/EventMapper.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/EventReducer.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/EventMapper.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/EventReducer.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

### NodeCompanionMain

This is the entry point of the companion based on Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Key patterns

- **Configuration via Spring Boot**: you register computations with the `@FlowSourceComputation` / `@FlowComputation` annotations; `flow-spring-boot-starter` manages the gRPC server lifecycle.
- **ExternalStateAccessor**: you work with the external state using `Payload` and `PayloadBuilder`.
- **SourceComputation with ProcessFunction**: the `reader` uses `EventMapper` to transform input data on the companion side.

