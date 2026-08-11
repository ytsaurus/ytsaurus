# Quick start with {{product-name}} Flow (Java)

You implement Java and Kotlin computations in Flow through the companion mechanism. Java or Kotlin code runs in a separate gRPC process that interacts with the C++ worker.

[Java SDK source code for Flow]({{source-root}}/yt/java/flow)

[Examples]({{source-root}}/yt/yt/flow/examples/java)

## Application architecture {#architecture}

Any Flow pipeline consists of three components:
- `Runner` — starts the pipeline and sets a new spec version.
- `Controller` — manages the pipeline’s operation.
- `Worker` — performs the actual data processing.

You use Java and Kotlin in the `Runner` and `Worker`.

![](../../../flow/images/java_flow_cluster.svg)

## Two configuration approaches

The Java SDK for Flow (with Kotlin support) provides two approaches to configure a companion:

1. **Manual** (SimpleRunnerProgram + PipelineContext + GrpcServerExecution) — suitable for simple cases where you don’t need dependency injection.
2. **Spring Boot** (auto-config with `@FlowComputation` annotations) — the recommended approach for production services with complex configuration and dependencies.

## Computation and SourceComputation

To create a computation in Java, choose the appropriate builder that matches the Computation type in C++:

- `Computation.builder()` — for `TTransformCompanionComputation` and `TSwiftMapCompanionComputation`.
- `SourceComputation.builder()` — for `TSwiftOrderedSourceCompanionComputation`.

{% list tabs group=lang %}

- Java

  ```java
  // SourceComputation for reading data from a source
  var reader = SourceComputation.builder()
         .setComputationId("reader")
         .build();

  // Computation for data processing
  var mapper = Computation.builder()
         .setComputationId("mapper")
         .setProcessFunction(new WordCountMapper())
         .build();
  ```

- Kotlin

  ```kotlin
  // SourceComputation for reading data from a source
  val reader = SourceComputation.builder()
         .setComputationId("reader")
         .build()

  // Computation for data processing
  val mapper = Computation.builder()
         .setComputationId("mapper")
         .setProcessFunction(WordCountMapper())
         .build()
  ```

{% endlist %}

`Computation.builder()` requires two mandatory parameters:
- **Computation id** — this maps requests between the worker and the companion.
- **Process function** — the function that contains the message-processing logic.

## Process Function

There are two types of ProcessFunction:

- `RowFunction` — receives messages and timers one at a time; it provides the `onMessage` and `onTimer` methods.
- `BatchFunction` — receives the entire batch of messages and timers; it provides the `onMessages` and `onTimers` methods.

For more details, see the [Computation (Java)](../../../flow/java/computation.md) section.

## Runner

This is the class with the `main` method for starting the pipeline. `SimpleRunnerProgram` is the Java equivalent of [NYT::NFlow::TSimpleRunnerProgram](../../../flow/release/basic-rules.md#launch-flow) and accepts the same configuration files and environment variables.

{% list tabs group=lang %}

- Java

  ```java
  import tech.ytsaurus.flow.pipeline.SimpleRunnerProgram;

  public class RunnerMain {
      public static void main(String[] args) throws Exception {
          SimpleRunnerProgram.runPipeline(args);
      }
  }
  ```

- Kotlin

  ```kotlin
  import tech.ytsaurus.flow.pipeline.SimpleRunnerProgram

  object RunnerMain {
      @JvmStatic
      fun main(args: Array<String>) {
          SimpleRunnerProgram.runPipeline(args)
      }
  }
  ```

{% endlist %}

## Node companion

### Manual approach

In the companion’s `main` method, you configure the computations, add them to `PipelineContext`, and start the gRPC server via `GrpcServerExecution`:

{% list tabs group=lang %}

- Java

  ```java
  import tech.ytsaurus.flow.computation.Computation;
  import tech.ytsaurus.flow.context.PipelineContext;
  import tech.ytsaurus.flow.execution.GrpcServerExecution;

  public class NodeCompanionMain {
      public static void main(String[] args) throws Exception {
          var mapper = Computation.builder()
              .setComputationId("mapper")
              .setProcessFunction(new WordCountMapper())
              .build();

          var context = new PipelineContext();
          context.registerComputation(mapper);

          GrpcServerExecution execution = new GrpcServerExecution(context);
          execution.start();
      }
  }
  ```

- Kotlin

  ```kotlin
  import tech.ytsaurus.flow.computation.Computation
  import tech.ytsaurus.flow.context.PipelineContext
  import tech.ytsaurus.flow.execution.GrpcServerExecution

  object NodeCompanionMain {
      @JvmStatic
      fun main(args: Array<String>) {
          val mapper = Computation.builder()
              .setComputationId("mapper")
              .setProcessFunction(WordCountMapper())
              .build()

          val context = PipelineContext()
          context.registerComputation(mapper)

          val execution = GrpcServerExecution(context)
          execution.start()
      }
  }
  ```

{% endlist %}

If your custom functions need additional resources (a map, cache, etc.), the companion’s `main` method is a good place to create them. These resources must be thread-safe.

### Spring Boot approach

When using Spring Boot, you register the `mapper` computation with the `@FlowComputation` annotation directly on the process-function class (the `reader` source is passthrough: it’s declared in the pipeline spec and isn’t registered in the Java companion):

{% list tabs group=lang %}

- Java

  ```java
  @FlowComputation(id = "mapper")
  public class WordCountMapper implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          // message processing
      }
  }
  ```

- Kotlin

  ```kotlin
  @FlowComputation(id = "mapper")
  class WordCountMapper : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          // message processing
      }
  }
  ```

{% endlist %}

You declare typed streams via `ComputationProvider` (the `getStreams()` method) or as separate `FlowStream<?>` beans:

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class WordCountContext implements ComputationProvider {

      @Override
      public List<FlowStream<?>> getStreams() {
          return List.of(FlowStreams.typed("words", Word.class));
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  open class WordCountContext : ComputationProvider {

      override fun getStreams(): List<FlowStream<*>> {
          return listOf(FlowStreams.typed("words", Word::class.java))
      }
  }
  ```

{% endlist %}

Spring Boot application entry point:

{% list tabs group=lang %}

- Java

  ```java
  @SpringBootApplication
  public class WordCountApplication {
      public static void main(String[] args) {
          new SpringApplicationBuilder(WordCountApplication.class)
                  .run(args);
      }
  }
  ```

- Kotlin

  ```kotlin
  @SpringBootApplication
  open class WordCountApplication {
      companion object {
          @JvmStatic
          fun main(args: Array<String>) {
              SpringApplicationBuilder(WordCountApplication::class.java).run(*args)
          }
      }
  }
  ```

{% endlist %}

The `getStreams()` method lets you register typed streams via `FlowStreams.typed(...)`, so the SDK automatically serializes and deserializes messages into Java objects.

You need two entry points to run the setup:
1. **Runner** — starts the C++ pipeline.
2. **Node companion** — starts the companion (Java or Kotlin) with the processing logic.

Flow doesn’t restrict whether you build two separate JAR files or one with two classes that have `main` methods. All [examples]({{source-root}}/yt/yt/flow/examples/java) use the single-JAR approach.

## See also

- [Computation (Java)](../../../flow/java/computation.md)
- [Working with states (Java)](../../../flow/java/state.md)
- [Examples](../../../flow/java/examples/wordcount.md)
- [Companion](../../../flow/concepts/companion.md)