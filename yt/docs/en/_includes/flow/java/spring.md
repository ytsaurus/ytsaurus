# Spring Boot integration in {{product-name}} Flow (Java)

The Java SDK Flow (supports Kotlin) provides a [Spring Boot Starter](https://docs.spring.io/spring-boot/reference/using/build-systems.html#using.build-systems.starters) to simplify configuring and launching the pipeline. The Starter automatically creates the necessary beans and manages the gRPC server’s lifecycle. The same starter works from Kotlin code without changes thanks to `WITH_KOTLINC_ALLOPEN(preset=spring)`.

One `@SpringBootApplication` class serves both roles: `YT_FLOW_MODE` selects the runner or companion, as described in [Entry point](../../../flow/java/getting-started.md#entry-point). You do not need a separate runner class.

[Source code for flow-spring-boot-starter]({{source-root}}/yt/java/flow/flow-spring-boot-starter)

## Connecting

To connect the Spring Boot Starter, you need to add the `flow-spring-boot-starter` dependency to your project.

## Quick start

### 1. Create a Spring Boot application

Use one class with a `main` method as the pipeline entry point:

{% list tabs group=lang %}

- Java

  ```java
  @SpringBootApplication
  public class PipelineMain {
      public static void main(String[] args) throws Exception {
          new SpringApplicationBuilder(PipelineMain.class)
                  .run(args);
      }
  }
  ```

- Kotlin

  ```kotlin
  @SpringBootApplication
  open class PipelineMain {
      companion object {
          @JvmStatic
          fun main(args: Array<String>) {
              SpringApplicationBuilder(PipelineMain::class.java).run(*args)
          }
      }
  }
  ```

{% endlist %}

Launch the pipeline through the same class:

```bash
./run.sh com.example.pipeline.PipelineMain --config pipeline.yson --flow-bin flow_server
```

Use the fully qualified class name: `run.sh` passes its first argument directly to `java`.

### 2. Register computations {#registration}

Annotate the `ProcessFunction` class with `@FlowComputation` (for transformations) or `@FlowSourceComputation` (for sources), and specify the computation’s ID in the annotation. The annotations are meta-annotated with `@Component`, so the class automatically becomes a Spring bean — you don’t need a separate `@Component`.

{% list tabs group=lang %}

- Java

  ```java
  @FlowComputation(id = "join")
  public class JoinProcessFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          // ...
      }
  }
  ```

- Kotlin

  ```kotlin
  @FlowComputation(id = "join")
  class JoinProcessFunction : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          // ...
      }
  }
  ```

{% endlist %}

Declare streams as Spring beans `FlowStream<?>` — they’re automatically registered in `PipelineContext`. Duplicate `streamId` values across any sources are rejected when building `PipelineContext`.

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class StreamConfiguration {

      @Bean
      public FlowStream<Hit> hitStream() {
          return FlowStreams.typed("hit", Hit.class);
      }

      @Bean
      public FlowStream<Action> actionStream() {
          return FlowStreams.typed("action", Action.class);
      }

      @Bean
      public FlowStream<JoinedAction> joinedActionStream() {
          return FlowStreams.typed("joined_action", JoinedAction.class);
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  class StreamConfiguration {

      @Bean
      fun hitStream(): FlowStream<Hit> = FlowStreams.typed("hit", Hit::class.java)

      @Bean
      fun actionStream(): FlowStream<Action> = FlowStreams.typed("action", Action::class.java)

      @Bean
      fun joinedActionStream(): FlowStream<JoinedAction> =
          FlowStreams.typed("joined_action", JoinedAction::class.java)
  }
  ```

{% endlist %}

As an alternative to separate beans, you can declare streams in one place by implementing the `ComputationProvider` interface (the `getStreams()` method) — see [ComputationProvider interface](#computation-provider). The same interface declares the proto (profile) states of the pipeline (`getStates()`), so the runner can describe them to the worker; `StateDescriptor<?>` beans are collected as well.

That’s all you need to start. The Spring Boot Starter automatically:

1. Creates `PipelineContext` and registers `Computation` objects (from annotated classes), streams and states in it.
2. Creates and configures `GrpcServerExecution`.
3. Starts the gRPC server when the application launches.
4. Stops the server correctly when the application shuts down.

## Annotations `@FlowComputation` and `@FlowSourceComputation` {#annotations}

[Source code for `@FlowComputation`]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowComputation.java)

[Source code for `@FlowSourceComputation`]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowSourceComputation.java)

The annotations mark the `ProcessFunction` class as a pipeline computation and set its ID:

| Annotation | Computation type | Purpose |
|-----------|-------------------|------------|
| `@FlowComputation(id)` | `Transform` | Transformation — processes incoming messages. |
| `@FlowSourceComputation(id)` | `Source` | Source — reads and parses incoming data. |

Key points:

- Both annotations are meta-annotated with `@Component`, so the annotated class automatically becomes a Spring bean during component scanning. You don’t need to specify a separate `@Component`.
- The annotated class must implement `RowFunction` or `BatchFunction` (subclasses of `ProcessFunction`). Otherwise, the application won’t start and will show a clear error.
- The ID must be unique among all computations and must match the computation’s ID in the pipeline specification.
- Spring DI (`@Autowired`, constructor injection) is available in annotated classes — just like in any Spring bean.

{% if audience == "internal" %}

Example from [Logbroker Wait Click Join](../../../yandex-specific/flow/java/examples/lb_wait_click_join.md):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/yandex/extensions/logbroker/examples/java/lb_wait_click_join/lb_wait_click_join/src/main/java/tech/ytsaurus/flow/examples/lbjoin/JoinFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/yandex/extensions/logbroker/examples/kotlin/lb_wait_click_join/lb_wait_click_join/src/main/kotlin/tech/ytsaurus/flow/examples/lbjoin/JoinFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

{% endif %}

## ComputationProvider interface {#computation-provider}

[Source code]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/ComputationProvider.java)

`ComputationProvider` lets you declare pipeline streams and states imperatively in one place — as an alternative to separate `FlowStream<?>` and `StateDescriptor<?>` beans. Implement it and register it as a Spring `@Configuration`. Computations aren’t registered through this interface — use `@FlowComputation` / `@FlowSourceComputation` annotations for them.

{% list tabs group=lang %}

- Java

  ```java
  public interface ComputationProvider {
      /**
       * Returns the list of streams to register in the pipeline.
       */
      List<FlowStream<?>> getStreams();

      /**
       * Returns the states the pipeline declares; empty by default.
       */
      default List<StateDescriptor<?>> getStates() { return List.of(); }
  }
  ```

- Kotlin

  ```kotlin
  interface ComputationProvider {
      /**
       * Returns the list of streams to register in the pipeline.
       */
      fun getStreams(): List<FlowStream<*>>

      /**
       * Returns the states the pipeline declares; empty by default.
       */
      fun getStates(): List<StateDescriptor<*>> = emptyList()
  }
  ```

{% endlist %}

`getStates()` declares the proto (profile) states of the pipeline: the runner fills their descriptor sets into the spec from the declared message classes, so the spec names no descriptors of its own. States of one name share a type — the owner of a state and the computations joining it declare it under one name — and states of different types need different names.

### Using Spring DI in ProcessFunction

One of the main benefits of Spring integration is that you can use Dependency Injection in `ProcessFunction`. The annotated class is a regular Spring bean, so you can inject dependencies via the constructor or `@Autowired`:

{% list tabs group=lang %}

- Java

  ```java
  @FlowComputation(id = "my_computation")
  public class MyProcessFunction implements RowFunction {

      private final MyExternalService externalService;
      private final MyCache cache;

      @Autowired
      public MyProcessFunction(MyExternalService externalService, MyCache cache) {
          this.externalService = externalService;
          this.cache = cache;
      }

      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          // externalService and cache are available here
      }
  }
  ```

- Kotlin

  ```kotlin
  @FlowComputation(id = "my_computation")
  class MyProcessFunction(
      private val externalService: MyExternalService,
      private val cache: MyCache,
  ) : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          // externalService and cache are available here
      }
  }
  ```

{% endlist %}

## Configuration {#configuration}

### Application properties

You set the settings in `application.yml` or `application.properties`:

```yaml
flow:
  server:
    port: 8080  # Optional. By default, the value is taken from the port field in YT_FLOW_COMPANION_CONFIG
```

### FlowProperties

[Source code]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowProperties.java)

| Property | Type | Default | Description |
|----------|-----|:---:|----------|
| `flow.server.port` | `Integer` | `null` | The gRPC server port. If not specified, the value is taken from the `port` field in `YT_FLOW_COMPANION_CONFIG` |

{% note info %}

In a production environment, the port is passed via the `YT_FLOW_COMPANION_CONFIG` environment variable (the `port` field), which Flow sets when starting the companion process. Explicitly specifying the port in the configuration is useful for local development and testing.

{% endnote %}

## Auto-configuration {#auto-configuration}

[Source code for FlowAutoConfiguration]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowAutoConfiguration.java)

The Spring Boot Starter automatically creates these beans:

| Bean | Creation condition | Description |
|-----|-------------------|----------|
| `PipelineContext` | An annotated bean (`@FlowComputation`/`@FlowSourceComputation`) or a `ComputationProvider` exists | Pipeline context with registered `Computation` objects and streams |
| `FlowRunnerBootstrap` | Runner mode | Starts the pipeline and exits with the `flow_server` return code |
| `CompanionExecutionConfig` | Companion mode with an annotated bean or `ComputationProvider` | gRPC server configuration (port) |
| `GrpcServerExecution` | Companion mode with a `PipelineContext` | Manages the gRPC server |
| `FlowCompanionLifecycle` | Companion mode with a `GrpcServerExecution` | Manages the server lifecycle |

`PipelineContext` is built the same way in both modes, so unit tests that inject it through `@SpringBootTest` work without selecting a mode. `FlowRunnerBootstrap` itself does not depend on that bean: it collects only streams when an actual launch begins. It does not instantiate computation beans or their caches and clients just to submit a spec.

Neither the gRPC nor the monitoring server starts in runner mode. Their configuration arrives from the worker through `YT_FLOW_COMPANION_CONFIG` and does not exist outside the companion. To avoid eagerly starting unrelated application beans, the starter defaults to `spring.main.web-application-type=none`, `spring.main.keep-alive=false`, and `spring.main.lazy-initialization=true` in runner mode. You can override these settings; test contexts keep normal Spring behavior.

`OnFlowComponentsCondition` enables the starter when the context has a `ComputationProvider` bean or a bean annotated with `@FlowComputation` or `@FlowSourceComputation`. `YT_FLOW_MODE` chooses the role. For tests that cannot set an environment variable in their JVM, `flow.run-mode` accepts `Worker` or `runner`, ignoring case, only when `YT_FLOW_MODE` is unset. If both are set and disagree, context startup fails. `flow.run-mode` does not control `spring.main.*`: those settings are selected earlier from `YT_FLOW_MODE`.

Streams and computations are collected only from the current context; beans in a `SpringApplicationBuilder.parent(...)` context do not enter the spec or companion.

`FlowRunnerBootstrap` runs at the lowest `ApplicationRunner` priority and exits the JVM after launching. Give any application runner that must run before it an explicit `@Order` below `Ordered.LOWEST_PRECEDENCE`; runners with the same lowest priority have no guaranteed order.

A test context does not launch a pipeline: `FlowRunnerBootstrap` detects common test frameworks, including JUnit, TestNG, Spring TestContext, and Cucumber. Set `flow.runner.enabled=false` for another framework. Outside tests, a launch without `--config` fails instead of silently doing nothing.

All beans are created with the `@ConditionalOnMissingBean` annotation, which lets you override any of them if needed.

## Lifecycle {#lifecycle}

[Source code for FlowCompanionLifecycle]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowCompanionLifecycle.java)

`FlowCompanionLifecycle` implements `SmartLifecycle` and manages the gRPC server’s start and stop:

- **Start**: the gRPC server starts automatically after all Spring beans are initialized (phase `Integer.MAX_VALUE`).
- **Stop**: the gRPC server stops gracefully before the Spring beans are destroyed.

## Overriding beans {#custom-beans}

If needed, you can override any auto-configured bean:

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class CustomFlowConfig {

      @Bean
      public PipelineContext pipelineContext(
              ObjectProvider<ComputationProvider> computationProviders,
              ObjectProvider<FlowStream<?>> flowStreams,
              ListableBeanFactory beanFactory
      ) {
          // FlowComponents collects computations from annotated beans,
          // and streams from ComputationProvider and FlowStream beans, just as
          // the default auto-configuration does.
          var context = FlowComponents.buildPipelineContext(
                  computationProviders, flowStreams, beanFactory);
          // Additional configuration...
          return context;
      }

      @Bean
      public CompanionExecutionConfig companionExecutionConfig() {
          // Custom port configuration
          return new CompanionExecutionConfig(9090);
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  class CustomFlowConfig {

      @Bean
      fun pipelineContext(
          computationProviders: ObjectProvider<ComputationProvider>,
          flowStreams: ObjectProvider<FlowStream<*>>,
          beanFactory: ListableBeanFactory
      ): PipelineContext {
          // FlowComponents collects computations from annotated beans,
          // and streams from ComputationProvider and FlowStream beans, just as
          // the default auto-configuration does.
          val context = FlowComponents.buildPipelineContext(
              computationProviders, flowStreams, beanFactory)
          // Additional configuration...
          return context
      }

      @Bean
      fun companionExecutionConfig(): CompanionExecutionConfig =
          // Custom port configuration
          CompanionExecutionConfig(9090)
  }
  ```

{% endlist %}

## Full application example {#full-example}

Project structure:

{% list tabs group=lang %}

- Java

  ```
  src/main/java/
  ├── PipelineMain.java               # @SpringBootApplication, only entry point
  ├── JoinProcessFunction.java        # @FlowComputation(id = "join") implements RowFunction
  ├── StreamConfiguration.java        # @Configuration with FlowStream<?> beans
  └── model/
      ├── Hit.java                    # @Entity POJO
      ├── Action.java                 # @Entity POJO
      └── JoinedAction.java           # @Entity POJO
  src/main/resources/
  └── log4j2.properties              # Logging configuration
  ```

- Kotlin

  ```
  src/main/kotlin/
  ├── PipelineMain.kt                 # @SpringBootApplication, only entry point
  ├── JoinProcessFunction.kt          # @FlowComputation(id = "join") : RowFunction
  ├── StreamConfiguration.kt          # @Configuration with FlowStream<?> beans
  └── model/
      ├── Hit.kt                      # @Entity POJO
      ├── Action.kt                   # @Entity POJO
      └── JoinedAction.kt             # @Entity POJO
  src/main/resources/
  └── log4j2.properties              # Logging configuration
  ```

{% endlist %}

{% if audience == "internal" %}Example with annotations — [Logbroker Wait Click Join](../../../yandex-specific/flow/java/examples/lb_wait_click_join.md): [Java]({{source-root}}/yt/yt/flow/yandex/extensions/logbroker/examples/java/lb_wait_click_join), [Kotlin]({{source-root}}/yt/yt/flow/yandex/extensions/logbroker/examples/kotlin/lb_wait_click_join).{% endif %}

Example with the `@FlowComputation` annotation and streams via `ComputationProvider` — [wait_click_join]({{source-root}}/yt/yt/flow/examples/java/wait_click_join) (Java) and [wait_click_join]({{source-root}}/yt/yt/flow/examples/kotlin/wait_click_join) (Kotlin).
