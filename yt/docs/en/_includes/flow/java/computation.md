# Computation in {{product-name}} Flow (Java)

{% note info %}

Use this page to learn the details for working with computations in Java and Kotlin. For general concepts, see the [Computation](../../../flow/concepts/computation.md) section.

{% endnote %}

## Computation types {#computation-types}

In Flow, you have two types of `Computation`: [`Swift`](../../../flow/concepts/glossary.md#swift) and `Transform`. Your choice affects how you ensure [exactly-once guarantees](../../../flow/concepts/guarantees.md) and what transformations you can implement.

| Type | Guarantee approach | Use case |
|------|--------------------|----------|
| `Swift` | The transformation code is deterministic and can be rerun if needed | Stateless transformations |
| `Transform` | The result is always stored in {{product-name}}, so no determinism requirements apply | [Stateful](../../../flow/concepts/stateful.md) transformations |

For more on processing guarantees, see the [Processing guarantees](../../../flow/concepts/guarantees.md) section.

For Java and Kotlin pipelines, you choose between `Swift` or `Transform` by setting `computation_class_name` in the static spec:
- `NYT::NFlow::NCompanion::TTransformCompanionComputation` — for `Transform`.
- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation` — for `Swift`.

## Creating a Computation {#computation}

In Java and Kotlin code, you create a `Computation` using `Computation.Builder` and register it in `PipelineContext`.

{% list tabs group=lang %}

- Java

  ```java
  var join = Computation.builder()
          .setComputationId("join")
          .setProcessFunction(new JoinProcessFunction())
          .build();
  ```

- Kotlin

  ```kotlin
  val join = Computation.builder()
          .setComputationId("join")
          .setProcessFunction(JoinProcessFunction())
          .build()
  ```

{% endlist %}

In the static spec, you create a `Computation` with the same `id` (in this example, `join`):
```yson
"join" = {
    "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    "group_by_schema" = [
        ...
    ];
    "input_stream_ids" = [...];
    "output_stream_ids" = [...];
    "parameters" = {
        ...
    };
    "timers" = {};
};
```

For more on specs, see the [Spec, DynamicSpec, and Config](../../../flow/concepts/spec.md) section.

{% note warning %}

You must provide `processFunction` (`null` is not allowed): you don’t register computations without business logic in Java. If you need [passthrough](../../../flow/concepts/glossary.md#passthrough), don’t register the computation in Java at all. Instead, specify the C++ passthrough class in `computation_class_name` in the static spec (see [Passthrough Computation](../../../flow/concepts/computation.md#passthrough)).

{% endnote %}

## SourceComputation {#sourcecomputation}

`SourceComputation` is the top node in the pipeline graph that reads data from external sources. For more details, see [Source Computation](../../../flow/concepts/computation.md#tswiftorderedsourcecomputation).

In Java, `SourceComputation` extends `Computation`. Like `Computation`, it requires the `processFunction` parameter.

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `computationId` | Yes | Unique identifier |
| `processFunction` | Yes | Function for processing messages |

### Creating a SourceComputation

{% list tabs group=lang %}

- Java

  ```java
  var reader = SourceComputation.builder()
          .setComputationId("hit_reader")
          .setProcessFunction(new HitParsingFunction())
          .build();
  ```

- Kotlin

  ```kotlin
  val reader = SourceComputation.builder()
          .setComputationId("hit_reader")
          .setProcessFunction(HitParsingFunction())
          .build()
  ```

{% endlist %}

For a passthrough Source, don’t use Java. Specify `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` in `computation_class_name` in the spec and leave the computation unregistered in the Java companion. For more details, see [Passthrough Computation](../../../flow/concepts/computation.md#passthrough).

### Interaction with Worker {#companion-info}

When you initialize `Worker`, it requests information about registered `Computation` and `SourceComputation` objects from the Java [companion](../../../flow/concepts/glossary.md#companion). `TSwiftOrderedSourceCompanionComputation` sends each input message to the Java companion, which applies `ProcessFunction` to it and returns the result. The Worker makes one request to the companion for each message.

## Process Function

You implement the business logic for data processing with a Process Function. Choose one of these two interfaces: [RowFunction]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/RowFunction.java) or [BatchFunction]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/BatchFunction.java).

{% note info %}

Choosing between `RowFunction` and `BatchFunction` depends only on your business logic. `RowFunction` doesn’t add extra processing overhead compared to `BatchFunction` because Flow internally transfers data in batches.

{% endnote %}

### RowFunction

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/RowFunction.java)

`RowFunction` receives [messages](../../../flow/concepts/glossary.md#message) and [timers](../../../flow/concepts/glossary.md#timer) one at a time. The interface provides two methods:

- `onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx)` — called for each input message.
- `onTimer(Timer timer, OutputCollector output, RuntimeContext ctx)` — called when a timer fires.

#### Example of a stateless function

{% list tabs group=lang %}

- Java

  ```java
  public class X2Mapper implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          var messageBuilder = ctx.createMessageBuilder("x2_numbers"); //1
          Long number = message.get("number", Long.class);             //2
          messageBuilder.set("number_x2", number * 2);                 //3
          output.addMessage(messageBuilder.finish());                  //4
      }
  }
  ```

- Kotlin

  ```kotlin
  class X2Mapper : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val messageBuilder = ctx.createMessageBuilder("x2_numbers") //1
          val number: Long? = message.get("number", Long::class.java)  //2
          messageBuilder.set("number_x2", number!! * 2)                //3
          output.addMessage(messageBuilder.finish())                   //4
      }
  }
  ```

{% endlist %}

Let’s walk through the code line by line:

1. `ctx.createMessageBuilder("x2_numbers")` — you create a `MessageBuilder` for the output [stream](../../../flow/concepts/glossary.md#stream-and-computation) with id = `x2_numbers`. The stream with this identifier must be present in the `output_stream_ids` list in the computation’s static [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec).
2. `message.get("number", Long.class)` — you get the value of the `number` field from the incoming message. You must pass the value’s class to the `Message#get` method to unambiguously convert the serialized form to a Java object.
3. `messageBuilder.set("number_x2", number * 2)` — you write the value to the `number_x2` field. This field must be present in the schema of the `x2_numbers` stream in the static spec.
4. `output.addMessage(messageBuilder.finish())` — the `finish` method returns the completed message, which you add to the `OutputCollector`.

### BatchFunction

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/BatchFunction.java)

`BatchFunction` receives the entire list of messages and timers that come from the [worker](../../../flow/concepts/glossary.md#worker). The interface provides two methods:

- `onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx)` — called for a batch of messages.
- `onTimers(List<Timer> timers, OutputCollector output, RuntimeContext ctx)` — called for a batch of timers.

#### Example of a batch function

{% list tabs group=lang %}

- Java

  ```java
  public class X2BatchMapper implements BatchFunction {
      @Override
      public void onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx) {
          var messageBuilder = ctx.createMessageBuilder("x2_numbers"); //1
          for (var message : messages) {                               //2
              Long number = message.get("number", Long.class);         //3
              messageBuilder.set("number_x2", number * 2);             //4
              output.addMessage(messageBuilder.finish());              //5
          }
      }
  }
  ```

- Kotlin

  ```kotlin
  class X2BatchMapper : BatchFunction {
      override fun onMessages(messages: List<ExtendedMessage>, output: OutputCollector, ctx: RuntimeContext) {
          val messageBuilder = ctx.createMessageBuilder("x2_numbers") //1
          for (message in messages) {                                  //2
              val number: Long? = message.get("number", Long::class.java) //3
              messageBuilder.set("number_x2", number!! * 2)           //4
              output.addMessage(messageBuilder.finish())               //5
          }
      }
  }
  ```

{% endlist %}

The key differences from `RowFunction` are:

- You create `MessageBuilder` once for the entire batch (line 1).
- The `finish()` method returns the completed message and resets `MessageBuilder` to its initial state, so you can reuse it for the next message (line 5).

## Registering in PipelineContext {#pipeline-context}

You must register all `Computation` objects and typed streams (created via `FlowStreams.typed`) in `PipelineContext` before you run `GrpcServerExecution`.
You don’t need to register untyped streams (created via `FlowStreams.raw`). Flow creates them automatically based on the `streams` block in the static spec.

Learn more about [Typed Streams](../../../flow/java/typed-streams.md).

{% list tabs group=lang %}

- Java

  ```java
  var context = new PipelineContext();

  // Register Computation objects.
  Computation join = Computation.builder()
          .setComputationId("join")
          .setProcessFunction(new JoinProcessFunction())
          .build();
  context.registerComputation(join);

  SourceComputation reader = SourceComputation.builder()
          .setComputationId("hit_reader")
          .setProcessFunction(new HitParsingFunction())
          .build();
  context.registerComputation(reader);

  // Register typed streams.
  context.registerStream(FlowStreams.typed("hit", Hit.class));
  context.registerStream(FlowStreams.typed("action", Action.class));
  context.registerStream(FlowStreams.typed("joined_action", JoinedAction.class));
  ```

- Kotlin

  ```kotlin
  val context = PipelineContext()

  // Register Computation objects.
  val join: Computation = Computation.builder()
          .setComputationId("join")
          .setProcessFunction(JoinProcessFunction())
          .build()
  context.registerComputation(join)

  val reader: SourceComputation = SourceComputation.builder()
          .setComputationId("hit_reader")
          .setProcessFunction(HitParsingFunction())
          .build()
  context.registerComputation(reader)

  // Register typed streams.
  context.registerStream(FlowStreams.typed("hit", Hit::class.java))
  context.registerStream(FlowStreams.typed("action", Action::class.java))
  context.registerStream(FlowStreams.typed("joined_action", JoinedAction::class.java))
  ```

{% endlist %}

{% note warning %}

Each Computation and stream must have a unique ID that matches the IDs in the static spec. If you try to register a Computation or stream with an ID that already exists, you’ll get an error and won’t be able to start the companion.

{% endnote %}

## RuntimeContext

[RuntimeContext source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/RuntimeContext.java)

[StatefulContext source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/StatefulContext.java)

`RuntimeContext` gives you access to the computation’s execution context. Key methods:

| Method | Description |
| --- | --- |
| `ctx.createMessageBuilder(streamId)` | Create a `MessageBuilder` for the specified output stream |
| `ctx.getComputationParameters()` | Get the computation’s parameters from the spec |
| `ctx.getEpochInputEventWatermark()` | Get the current [watermark](../../../flow/concepts/glossary.md#timestamps-and-watermarks) for the [epoch](../../../flow/concepts/glossary.md#epoch) |
| `ctx.getProtoStateAccessor(name, message, Class)` | Get the state as a protobuf object linked to the message’s [key](../../../flow/concepts/glossary.md#key) |
| `ctx.getYsonStateAccessor(name, message, Class)` | Get the YSON state linked to the message’s [key](../../../flow/concepts/glossary.md#key) |
| `ctx.getStateAccessor(name, message, Class, ser, deser)` | Get the state with custom serialization/deserialization |
| `ctx.getRawStateAccessor(name, message)` | Get the state as a byte array without interpretation |
| `ctx.getNoOpStateAccessor(name, message)` | Get the state that only stores the presence fact (no value) |
| `ctx.getExternalStateAccessor(name, message)` | Get the external state linked to the message’s [key](../../../flow/concepts/glossary.md#key) |

Learn more about working with states in the [Working with States (Java)](../../../flow/java/state.md) section.

## OutputCollector {#output-collector}

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/computation/OutputCollector.java)

Use `OutputCollector` to send processing results:

| Method | Description |
| --- | --- |
| `output.addMessage(message)` | Add an output message |
| `output.addTimer(triggerTimestamp)` | Add a [timer](../../../flow/concepts/glossary.md#timer) with the specified trigger time (eventTimestamp = 0) |
| `output.addTimer(triggerTimestamp, eventTimestamp)` | Add a timer with the specified trigger time and event time |
| `output.addTimer(timerStreamId, triggerTimestamp, eventTimestamp)` | Add a timer for a specific timer stream |
| `output.setParentIds(parentIds)` | Set the parent ID to track the [lineage](../../../flow/concepts/lineage.md) of messages. Returns a new `OutputCollector` |

## Spring Boot

When you use Spring Boot, you register the computation with the `@FlowComputation` annotation (or `@FlowSourceComputation` for a source) directly on the `ProcessFunction` class. The annotation is meta-annotated with `@Component`, so the class automatically becomes a Spring bean:

{% list tabs group=lang %}

- Java

  ```java
  @FlowComputation(id = "mapper")
  public class WordCountMapper implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          // process the message
      }
  }
  ```

- Kotlin

  ```kotlin
  @FlowComputation(id = "mapper")
  class WordCountMapper : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          // process the message
      }
  }
  ```

{% endlist %}

Declare streams as Spring beans of type `FlowStream<?>` (or via `ComputationProvider.getStreams()`):

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class StreamConfiguration {

      @Bean
      public FlowStream<Word> wordsStream() {
          return FlowStreams.typed("words", Word.class);
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  class StreamConfiguration {

      @Bean
      fun wordsStream(): FlowStream<Word> = FlowStreams.typed("words", Word::class.java)
  }
  ```

{% endlist %}

`FlowStreams.typed(...)` creates a typed stream that automatically serializes and deserializes messages into Java objects of the specified type. Learn more in the [Typed Streams](../../../flow/java/typed-streams.md) section.

Learn more about registration via annotations and `ComputationProvider` in the [Spring Boot Integration](../../../flow/java/spring.md) section.

## CompanionManager Resource Configuration {#companion-manager}

To run a companion in Java or Kotlin, you must declare the `CompanionManager` resource in the static spec:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
    "parameters" = {
        "timeout" = "10s";
        "jdk_bin_path" = "/app/ytflow/jdk/bin/java";
        "main_class" = "tech.ytsaurus.flow.examples.waitclickjoin.NodeCompanionMain";
        "classpath" = "/app/ytflow/lib/*";
    };
    "dependencies" = {};
};
```

The `resource_class_name` parameter specifies the resource class that will run the companion.
For a Java or Kotlin companion, `resource_class_name` must always be `NYT::NFlow::NCompanion::TJavaCompanionManager` (it supports both languages via the JVM).

Learn more about the spec in the [Spec, DynamicSpec and Config](../../../flow/concepts/spec.md) section.

## See also

- [Computation (concept)](../../../flow/concepts/computation.md)
- [Working with States (Java)](../../../flow/java/state.md)
- [Quick Start (Java)](../../../flow/java/getting-started.md)
- [Companion](../../../flow/concepts/companion.md)
