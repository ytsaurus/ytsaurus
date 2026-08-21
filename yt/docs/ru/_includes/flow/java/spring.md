# Spring Boot интеграция в {{product-name}} Flow (Java)

Java SDK Flow (поддерживает Kotlin) предоставляет [Spring Boot Starter](https://docs.spring.io/spring-boot/reference/using/build-systems.html#using.build-systems.starters) для упрощения конфигурации и запуска пайплайна. Starter автоматически создаёт необходимые бины и управляет жизненным циклом gRPC-сервера. Тот же стартер работает из Kotlin-кода без изменений благодаря `WITH_KOTLINC_ALLOPEN(preset=spring)`.

Один класс с `@SpringBootApplication` служит обеими точками входа: роль процесса выбирается по переменной среды `YT_FLOW_MODE`, как описано в разделе [Точка входа](../../../flow/java/getting-started.md). Отдельный класс для runner-а не нужен.

[Исходный код flow-spring-boot-starter]({{source-root}}/yt/java/flow/flow-spring-boot-starter)

## Подключение

Для подключения Spring Boot Starter необходимо добавить зависимость `flow-spring-boot-starter` в проект.

## Быстрый старт

### 1. Создание Spring Boot приложения

Класс с `main` методом — единственная точка входа пайплайна:

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

Запуск пайплайна тем же классом:

```bash
./run.sh com.example.pipeline.PipelineMain --config pipeline.yson --flow-bin flow_server
```

Класс указывается полным именем: `run.sh` передаёт первый аргумент напрямую в `java`.

### 2. Регистрация компьютейшенов {#registration}

Пометьте класс `ProcessFunction` аннотацией `@FlowComputation` (для трансформаций) или `@FlowSourceComputation` (для источников), указав в ней идентификатор компьютейшена. Аннотации мета-аннотированы `@Component`, поэтому класс автоматически становится Spring-бином — отдельный `@Component` не нужен.

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

Стримы объявляются как Spring-бины `FlowStream<?>` — они автоматически регистрируются в `PipelineContext`. Дубликаты `streamId` между любыми источниками отвергаются при сборке `PipelineContext`.

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

Как альтернативу отдельным бинам, стримы можно объявить в одном месте, реализовав интерфейс `ComputationProvider` (метод `getStreams()`) — см. [Интерфейс ComputationProvider](#computation-provider).

Это всё, что нужно для запуска. Spring Boot Starter автоматически:
1. Создаст `PipelineContext` и зарегистрирует в нём объекты `Computation` (из аннотированных классов) и стримы.
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
- Идентификатор должен быть уникальным среди всех компьютейшенов и совпадать с идентификатором компьютейшена в спецификации пайплайна.
- В аннотированных классах доступен Spring DI (`@Autowired`, внедрение через конструктор) — так же, как в любом Spring-бине.

{% if audience == "internal" %}

Пример из [Logbroker Wait Click Join](../../../flow/java/examples/lb_wait_click_join.md):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/yandex/extensions/logbroker/examples/java/lb_wait_click_join/lb_wait_click_join/src/main/java/tech/ytsaurus/flow/examples/lbjoin/JoinFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/yandex/extensions/logbroker/examples/kotlin/lb_wait_click_join/lb_wait_click_join/src/main/kotlin/tech/ytsaurus/flow/examples/lbjoin/JoinFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

{% endif %}

## Интерфейс ComputationProvider {#computation-provider}

[Исходный код]({{source-root}}/yt/java/flow/flow-spring-boot-starter/src/main/java/tech/ytsaurus/flow/spring/ComputationProvider.java)

`ComputationProvider` позволяет объявить стримы пайплайна императивно в одном месте — как альтернативу отдельным бинам `FlowStream<?>`. Реализуйте его и зарегистрируйте как Spring `@Configuration`. Компьютейшены через этот интерфейс не регистрируются — для них используйте аннотации `@FlowComputation` / `@FlowSourceComputation`.

{% list tabs group=lang %}

- Java

  ```java
  public interface ComputationProvider {
      /**
       * Возвращает список стримов для регистрации в пайплайне.
       */
      List<FlowStream<?>> getStreams();
  }
  ```

- Kotlin

  ```kotlin
  interface ComputationProvider {
      /**
       * Возвращает список стримов для регистрации в пайплайне.
       */
      fun getStreams(): List<FlowStream<*>>
  }
  ```

{% endlist %}

### Использование Spring DI в ProcessFunction

Одно из главных преимуществ Spring-интеграции — возможность использовать Dependency Injection в `ProcessFunction`. Аннотированный класс — обычный Spring-бин, поэтому в него можно внедрять зависимости через конструктор или `@Autowired`:

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
          // externalService и cache доступны здесь
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
          // externalService и cache доступны здесь
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
| `FlowRunnerBootstrap` | Режим runner-а | Запускает пайплайн и завершает процесс кодом возврата `flow_server` |
| `CompanionExecutionConfig` | Режим компаньона и есть аннотированный бин или `ComputationProvider` | Конфигурация gRPC-сервера (порт) |
| `GrpcServerExecution` | Режим компаньона и есть `PipelineContext` | Управление gRPC-сервером |
| `FlowCompanionLifecycle` | Режим компаньона и есть `GrpcServerExecution` | Управление жизненным циклом сервера |

`PipelineContext` создаётся одинаково в обоих режимах, поэтому unit-тесты, которые инжектят его через `@SpringBootTest`, работают без указания режима.

При этом сам `FlowRunnerBootstrap` от бина `PipelineContext` не зависит: для запуска он собирает **только стримы** и только в момент реального запуска. Бины компьютейшенов при этом не создаются — а вместе с ними и всё, от чего они зависят. Это важно для пайплайнов, у которых process-функции держат кэши, клиенты или пулы соединений: иначе такой пайплайн прогревал бы их при каждом запуске, а запуск падал бы всякий раз, когда эти зависимости недоступны. Отправка спеки пользовательский код не выполняет.

Ни gRPC-сервер, ни сервер мониторинга в режиме runner-а не поднимаются: соответствующие бины не создаются вовсе, поскольку их конфигурация приходит от воркера через `YT_FLOW_COMPANION_CONFIG` и вне компаньона не существует.

Чтобы в режиме runner-а не создавались остальные бины приложения, starter выставляет значения по умолчанию `spring.main.web-application-type=none`, `spring.main.keep-alive=false` и `spring.main.lazy-initialization=true`. Все три можно переопределить в конфигурации приложения. В контекст, созданный тестовым фреймворком, эти значения не попадают вовсе — тесты сохраняют обычную семантику Spring.

Условие активации автоконфигурации описано в `OnFlowComponentsCondition`: starter включается, если в контексте есть хотя бы один бин `ComputationProvider` либо бин, помеченный `@FlowComputation` или `@FlowSourceComputation`. Режим выбирается по `YT_FLOW_MODE`. Свойство `flow.run-mode` (`Worker` или `runner`, без учёта регистра) предназначено для тестов, которые не могут выставить переменную среды в своей JVM: оно действует, только когда `YT_FLOW_MODE` не задана. Если переменная задана и противоречит свойству, старт контекста падает с ошибкой — забытое в `application.yml` свойство не может переназначить роль процесса. На значения `spring.main.*` свойство не влияет: они выставляются раньше, когда окружение ещё собирается, и определяются только переменной `YT_FLOW_MODE`.

Стримы и компьютейшены собираются только из текущего контекста: бины, объявленные в родительском контексте (`SpringApplicationBuilder.parent(...)`), в спеку и в компаньон не попадают.

`FlowRunnerBootstrap` объявляет наименьший приоритет среди `ApplicationRunner`-ов и завершает JVM после запуска пайплайна. Раннер приложения, который должен успеть отработать до запуска, обязан объявить явный порядок (`@Order` со значением меньше `Ordered.LOWEST_PRECEDENCE`): раннер без аннотации получает тот же наименьший приоритет, и порядок между ними Spring не гарантирует.

Контекст, созданный тестовым фреймворком, ничего не запускает: `@SpringBootTest` тоже вызывает `ApplicationRunner`-ы, поэтому `FlowRunnerBootstrap` распознаёт тестовое окружение по стеку вызовов (JUnit, TestNG, Spring TestContext, Cucumber) и не делает ничего — тот же приём использует Spring Boot DevTools. Для фреймворка, которого распознавание не знает, есть явное свойство `flow.runner.enabled=false`. Вне теста командная строка разбирается всегда, поэтому запуск без `--config` падает с ошибкой, а не завершается успешно, ничего не отправив.

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
          // FlowComponents собирает компьютейшены из аннотированных бинов,
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
          // FlowComponents собирает компьютейшены из аннотированных бинов,
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
  ├── PipelineMain.java               # @SpringBootApplication — единственная точка входа
  ├── JoinProcessFunction.java        # @FlowComputation(id = "join") implements RowFunction
  ├── StreamConfiguration.java        # @Configuration с бинами FlowStream<?>
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
  ├── PipelineMain.kt                 # @SpringBootApplication — единственная точка входа
  ├── JoinProcessFunction.kt          # @FlowComputation(id = "join") : RowFunction
  ├── StreamConfiguration.kt          # @Configuration с бинами FlowStream<?>
  └── model/
      ├── Hit.kt                      # @Entity POJO
      ├── Action.kt                   # @Entity POJO
      └── JoinedAction.kt             # @Entity POJO
  src/main/resources/
  └── log4j2.properties              # Конфигурация логирования
  ```

{% endlist %}

{% if audience == "internal" %}Пример на аннотациях — [Logbroker Wait Click Join](../../../flow/java/examples/lb_wait_click_join.md): [Java]({{source-root}}/yt/yt/flow/yandex/extensions/logbroker/examples/java/lb_wait_click_join), [Kotlin]({{source-root}}/yt/yt/flow/yandex/extensions/logbroker/examples/kotlin/lb_wait_click_join).{% endif %}

Пример с аннотацией `@FlowComputation` и стримами через `ComputationProvider` — [wait_click_join]({{source-root}}/yt/yt/flow/examples/java/wait_click_join) (Java) и [wait_click_join]({{source-root}}/yt/yt/flow/examples/kotlin/wait_click_join) (Kotlin).
