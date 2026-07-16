# Spring Boot интеграция в {{product-name}} Flow (Java)

Java SDK Flow (поддерживает Kotlin) предоставляет [Spring Boot Starter](https://docs.spring.io/spring-boot/reference/using/build-systems.html#using.build-systems.starters) для упрощения конфигурации и запуска процесса-компаньона. Starter автоматически создаёт необходимые бины и управляет жизненным циклом gRPC-сервера. Тот же стартер работает из Kotlin-кода без изменений благодаря `WITH_KOTLINC_ALLOPEN(preset=spring)`.

[Исходный код flow-spring-boot-starter]({{source-root}}/yt/java/flow/flow-spring-boot-starter)

## Подключение

Для подключения Spring Boot Starter необходимо добавить зависимость `flow-spring-boot-starter` в проект.

## Быстрый старт

### 1. Создание Spring Boot приложения

Класс с `main` методом для запуска компаньона:

{% list tabs group=lang %}

- Java

  ```java
  @SpringBootApplication
  public class NodeCompanionMain {
      public static void main(String[] args) throws Exception {
          new SpringApplicationBuilder(NodeCompanionMain.class)
                  .run(args);
      }
  }
  ```

- Kotlin

  ```kotlin
  @SpringBootApplication
  open class NodeCompanionMain {
      companion object {
          @JvmStatic
          fun main(args: Array<String>) {
              SpringApplicationBuilder(NodeCompanionMain::class.java).run(*args)
          }
      }
  }
  ```

{% endlist %}

### 2. Регистрация компьютейшенов {#registration}

Компьютейшены можно зарегистрировать двумя способами: аннотациями (рекомендуется) или через интерфейс `ComputationProvider`. Оба способа можно комбинировать — их компьютейшены и стримы объединяются.

#### Способ 1. Аннотации (рекомендуется)

Пометьте класс `ProcessFunction` аннотацией `@FlowComputation` (для трансформаций) или `@FlowSourceComputation` (для источников), указав в ней идентификатор компьютейшена. Аннотации мета-аннотированы `@Component`, поэтому класс автоматически становится Spring-бином — отдельный `@Component` или реализация `ComputationProvider` не нужны.

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

Стримы при использовании аннотаций объявляются как Spring-бины `FlowStream<?>` — они автоматически регистрируются в `PipelineContext`. Дубликаты `streamId` между любыми источниками отвергаются при сборке `PipelineContext`.

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

#### Способ 2. ComputationProvider

Создайте конфигурационный класс, реализующий интерфейс `ComputationProvider`:

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class PipelineConfiguration implements ComputationProvider {

      @Override
      public List<Computation> getComputations() {
          Computation join = Computation.builder()
                  .setComputationId("join")
                  .setProcessFunction(new JoinProcessFunction())
                  .build();
          return List.of(join);
      }

      @Override
      public List<FlowStream<?>> getStreams() {
          return List.of(
                  FlowStreams.typed("hit", Hit.class),
                  FlowStreams.typed("action", Action.class),
                  FlowStreams.typed("joined_action", JoinedAction.class)
          );
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  class PipelineConfiguration : ComputationProvider {

      override fun getComputations(): List<Computation> {
          val join = Computation.builder()
                  .setComputationId("join")
                  .setProcessFunction(JoinProcessFunction())
                  .build()
          return listOf(join)
      }

      override fun getStreams(): List<FlowStream<*>> {
          return listOf(
                  FlowStreams.typed("hit", Hit::class.java),
                  FlowStreams.typed("action", Action::class.java),
                  FlowStreams.typed("joined_action", JoinedAction::class.java)
          )
      }
  }
  ```

{% endlist %}

Это всё, что нужно для запуска. Spring Boot Starter автоматически:
1. Создаст `PipelineContext` и зарегистрирует в нём объекты `Computation` (из аннотаций и из `ComputationProvider`) и стримы.
2. Создаст и настроит `GrpcServerExecution`.
3. Запустит gRPC-сервер при старте приложения.
4. Корректно остановит сервер при завершении приложения.

## Аннотации `@FlowComputation` и `@FlowSourceComputation` {#annotations}

[Исходный код `@FlowComputation`]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowComputation.java)

[Исходный код `@FlowSourceComputation`]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowSourceComputation.java)

Аннотации помечают класс `ProcessFunction` как компьютейшен пайплайна и задают его идентификатор:

| Аннотация | Тип компьютейшена | Назначение |
|-----------|-------------------|------------|
| `@FlowComputation(id)` | `Transform` | Трансформация — обрабатывает входные сообщения. |
| `@FlowSourceComputation(id)` | `Source` | Источник — читает и парсит входные данные. |

Особенности:
- Обе аннотации мета-аннотированы `@Component`, поэтому аннотированный класс автоматически становится Spring-бином при сканировании компонентов. Отдельный `@Component` указывать не нужно.
- Аннотированный класс обязан реализовывать `RowFunction` или `BatchFunction` (наследников `ProcessFunction`). Иначе приложение не стартует с понятной ошибкой.
- Идентификатор должен быть уникальным среди всех компьютейшенов (как аннотированных, так и полученных из `ComputationProvider`) и совпадать с идентификатором компьютейшена в спецификации пайплайна.
- В аннотированных классах доступен Spring DI (`@Autowired`, внедрение через конструктор) — так же, как в любом Spring-бине.

{% if audience == "internal" %}

Пример из [Logbroker Wait Click Join](../../../yandex-specific/flow/java/examples/lb_wait_click_join.md):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/yandex/examples/java/lb_wait_click_join/lb_wait_click_join/src/main/java/tech/ytsaurus/flow/examples/lbjoin/JoinFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/yandex/examples/kotlin/lb_wait_click_join/lb_wait_click_join/src/main/kotlin/tech/ytsaurus/flow/examples/lbjoin/JoinFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

{% endif %}

## Интерфейс ComputationProvider {#computation-provider}

[Исходный код]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/ComputationProvider.java)

`ComputationProvider` — альтернативный способ интеграции с Flow Spring Boot Starter. Реализуйте его и зарегистрируйте как Spring `@Configuration`. Подходит, когда удобнее собирать компьютейшены и стримы императивно в одном месте.

{% list tabs group=lang %}

- Java

  ```java
  public interface ComputationProvider {
      /**
       * Возвращает список объектов Computation для регистрации в пайплайне.
       */
      List<Computation> getComputations();

      /**
       * Возвращает список стримов для регистрации в пайплайне.
       * По умолчанию возвращает пустой список.
       */
      default List<FlowStream<?>> getStreams() {
          return List.of();
      }
  }
  ```

- Kotlin

  ```kotlin
  interface ComputationProvider {
      /**
       * Возвращает список объектов Computation для регистрации в пайплайне.
       */
      fun getComputations(): List<Computation>

      /**
       * Возвращает список стримов для регистрации в пайплайне.
       * По умолчанию возвращает пустой список.
       */
      fun getStreams(): List<FlowStream<*>> = listOf()
  }
  ```

{% endlist %}

### Использование Spring DI в ProcessFunction

Одно из главных преимуществ Spring-интеграции — возможность использовать Dependency Injection для `ProcessFunction`:

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class PipelineConfiguration implements ComputationProvider {

      @Autowired
      private MyExternalService externalService;

      @Autowired
      private MyCache cache;

      @Override
      public List<Computation> getComputations() {
          // ProcessFunction может использовать Spring-бины
          var processFunction = new MyProcessFunction(externalService, cache);

          Computation computation = Computation.builder()
                  .setComputationId("my_computation")
                  .setProcessFunction(processFunction)
                  .build();
          return List.of(computation);
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  class PipelineConfiguration : ComputationProvider {

      @Autowired
      private lateinit var externalService: MyExternalService

      @Autowired
      private lateinit var cache: MyCache

      override fun getComputations(): List<Computation> {
          // ProcessFunction может использовать Spring-бины
          val processFunction = MyProcessFunction(externalService, cache)

          val computation = Computation.builder()
                  .setComputationId("my_computation")
                  .setProcessFunction(processFunction)
                  .build()
          return listOf(computation)
      }
  }
  ```

{% endlist %}

## Конфигурация {#configuration}

### Свойства приложения

Настройки задаются в `application.yml` или `application.properties`:

```yaml
flow:
  server:
    port: 8080  # Опционально. По умолчанию берётся из поля port в YT_FLOW_COMPANION_CONFIG
```

### FlowProperties

[Исходный код]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowProperties.java)

| Свойство | Тип | По умолчанию | Описание |
|----------|-----|:---:|----------|
| `flow.server.port` | `Integer` | `null` | Порт gRPC-сервера. Если не задан, берётся из поля `port` в `YT_FLOW_COMPANION_CONFIG` |

{% note info %}

В production-окружении порт передаётся через переменную окружения `YT_FLOW_COMPANION_CONFIG` (поле `port`), которую устанавливает Flow при запуске процесса-компаньона. Явное указание порта в конфигурации полезно для локальной разработки и тестирования.

{% endnote %}

## Автоконфигурация {#auto-configuration}

[Исходный код FlowAutoConfiguration]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowAutoConfiguration.java)

Spring Boot Starter автоматически создаёт следующие бины:

| Бин | Условие создания | Описание |
|-----|-------------------|----------|
| `PipelineContext` | Есть аннотированный бин (`@FlowComputation`/`@FlowSourceComputation`) или `ComputationProvider` | Контекст пайплайна с зарегистрированными объектами `Computation` и стримами |
| `CompanionExecutionConfig` | Есть аннотированный бин или `ComputationProvider` | Конфигурация gRPC-сервера (порт) |
| `GrpcServerExecution` | Есть `PipelineContext` | Управление gRPC-сервером |
| `FlowCompanionLifecycle` | Есть `GrpcServerExecution` | Управление жизненным циклом сервера |

Условие активации автоконфигурации описано в `OnFlowComponentsCondition`: starter включается, если в контексте есть хотя бы один бин `ComputationProvider` либо бин, помеченный `@FlowComputation` или `@FlowSourceComputation`.

Все бины создаются с аннотацией `@ConditionalOnMissingBean`, что позволяет переопределить любой из них при необходимости.

## Жизненный цикл {#lifecycle}

[Исходный код FlowCompanionLifecycle]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/FlowCompanionLifecycle.java)

`FlowCompanionLifecycle` реализует `SmartLifecycle` и управляет запуском и остановкой gRPC-сервера:

- **Запуск**: gRPC-сервер запускается автоматически после инициализации всех Spring-бинов (фаза `Integer.MAX_VALUE`).
- **Остановка**: gRPC-сервер останавливается корректно (graceful shutdown) перед уничтожением Spring-бинов.

## Переопределение бинов {#custom-beans}

При необходимости можно переопределить любой автоконфигурируемый бин:

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
          // FlowComponents собирает компьютейшены из аннотированных бинов и ComputationProvider,
          // а стримы — из ComputationProvider и FlowStream-бинов, как это делает
          // автоконфигурация по умолчанию.
          var context = FlowComponents.buildPipelineContext(
                  computationProviders, flowStreams, beanFactory);
          // Дополнительная настройка...
          return context;
      }

      @Bean
      public CompanionExecutionConfig companionExecutionConfig() {
          // Кастомная конфигурация порта
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
          // FlowComponents собирает компьютейшены из аннотированных бинов и ComputationProvider,
          // а стримы — из ComputationProvider и FlowStream-бинов, как это делает
          // автоконфигурация по умолчанию.
          val context = FlowComponents.buildPipelineContext(
              computationProviders, flowStreams, beanFactory)
          // Дополнительная настройка...
          return context
      }

      @Bean
      fun companionExecutionConfig(): CompanionExecutionConfig =
          // Кастомная конфигурация порта
          CompanionExecutionConfig(9090)
  }
  ```

{% endlist %}

## Пример полного приложения {#full-example}

Структура проекта:

{% list tabs group=lang %}

- Java

  ```
  src/main/java/
  ├── NodeCompanionMain.java          # @SpringBootApplication
  ├── JoinProcessFunction.java        # @FlowComputation(id = "join") implements RowFunction
  ├── StreamConfiguration.java        # @Configuration с бинами FlowStream<?>
  ├── RunnerMain.java                 # SimpleRunnerProgram.runPipeline(args)
  └── model/
      ├── Hit.java                    # @Entity POJO
      ├── Action.java                 # @Entity POJO
      └── JoinedAction.java           # @Entity POJO
  src/main/resources/
  └── log4j2.properties              # Конфигурация логирования
  ```

- Kotlin

  ```
  src/main/kotlin/
  ├── NodeCompanionMain.kt            # @SpringBootApplication
  ├── JoinProcessFunction.kt          # @FlowComputation(id = "join") : RowFunction
  ├── StreamConfiguration.kt          # @Configuration с бинами FlowStream<?>
  ├── RunnerMain.kt                   # SimpleRunnerProgram.runPipeline(args)
  └── model/
      ├── Hit.kt                      # @Entity POJO
      ├── Action.kt                   # @Entity POJO
      └── JoinedAction.kt             # @Entity POJO
  src/main/resources/
  └── log4j2.properties              # Конфигурация логирования
  ```

{% endlist %}

{% if audience == "internal" %}Пример на аннотациях — [Logbroker Wait Click Join](../../../yandex-specific/flow/java/examples/lb_wait_click_join.md): [Java]({{source-root}}/yt/yt/flow/yandex/examples/java/lb_wait_click_join), [Kotlin]({{source-root}}/yt/yt/flow/yandex/examples/kotlin/lb_wait_click_join).{% endif %}

Пример на `ComputationProvider` — [wait_click_join]({{source-root}}/yt/yt/flow/examples/java/wait_click_join) (Java) и [wait_click_join]({{source-root}}/yt/yt/flow/examples/kotlin/wait_click_join) (Kotlin).
