# Computation в {{product-name}} Flow (Java)

{% note info %}

На этой странице описаны детали для Java и Kotlin при работе с компьютейшенами. Общие концепции описаны в разделе [Computation](../../../flow/concepts/computation.md).

{% endnote %}

## Типы Computation {#computation-types}

Во Flow есть два вида `Computation`: [`Swift`](../../../flow/concepts/glossary.md#swift) и `Transform`. От их выбора зависит способ обеспечения [exactly-once гарантий](../../../flow/concepts/guarantees.md) и то, какие преобразования возможно реализовать с их применением.

| Тип | Способ обеспечения гарантий | Применение |
|-----|-----------------------------|------------|
| `Swift`| Код преобразования детерминирован, при необходимости будет вызываться повторно | Stateless преобразования |
| `Transform` | Результат работы обязательно сохраняется в {{product-name}}, поэтому нет требований на какую-либо детерминированность преобразований | [Stateful](../../../flow/concepts/stateful.md) преобразования |

Подробнее про гарантии обработки — в разделе [Гарантии обработки](../../../flow/concepts/guarantees.md).

Для пайплайнов на Java и Kotlin выбор между `Swift` или `Transform` осуществляется через указание `computation_class_name` в статической спеке:
- `NYT::NFlow::NCompanion::TTransformCompanionComputation` — для `Transform`.
- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation` — для `Swift`.

## Создание Computation {#computation}

В коде на Java и Kotlin `Computation` создаётся через `Computation.Builder` и регистрируется в `PipelineContext`.

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

В статической спеке создаётся `Computation` с таким же `id` (в данном примере `join`):
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

Подробнее про спеку в разделе [Spec, DynamicSpec и Config](../../../flow/concepts/spec.md).

{% note warning %}

`processFunction` обязателен (`null` недопустим): компьютейшены без бизнес-логики в Java не регистрируют. Если нужен [passthrough](../../../flow/concepts/glossary.md#passthrough) — не регистрируйте компьютейшен в Java вовсе, а в статической спеке укажите C++-класс passthrough в `computation_class_name` (см. [Passthrough Computation](../../../flow/concepts/computation.md#passthrough)).

{% endnote %}

## SourceComputation {#sourcecomputation}

`SourceComputation` — вершина в графе пайплайна, осуществляющая чтение данных из внешних источников. Подробнее про [Source Computation](../../../flow/concepts/computation.md#tswiftorderedsourcecomputation).

В Java `SourceComputation` расширяет `Computation`. Как и у `Computation`, параметр `processFunction` обязателен.

### Параметры

| Параметр | Обязательный | Описание |
|----------|:---:|----------|
| `computationId` | Да | Уникальный идентификатор |
| `processFunction` | Да | Функция обработки сообщений |

### Создание SourceComputation

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

Для passthrough Source не используйте Java — укажите в спеке `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` в `computation_class_name` и оставьте компьютейшен незарегистрированным в Java-компаньоне. Подробнее — [Passthrough Computation](../../../flow/concepts/computation.md#passthrough).

### Взаимодействие с Worker {#companion-info}

При инициализации `Worker` запрашивает у Java-[компаньона](../../../flow/concepts/glossary.md#companion) информацию о зарегистрированных объектах `Computation` и `SourceComputation`. Каждое входное сообщение `TSwiftOrderedSourceCompanionComputation` отправляет в Java-компаньон, который применяет к нему `ProcessFunction` и возвращает результат. Worker выполняет один запрос к компаньону на каждое сообщение.

## Process Function

Бизнес-логика обработки данных реализуется через Process Function. Для реализации необходимо выбрать один из двух интерфейсов: [RowFunction]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/RowFunction.java) или [BatchFunction]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/BatchFunction.java).

{% note info %}

Использование `RowFunction` или `BatchFunction` — исключительно вопрос бизнес-логики. `RowFunction` не добавляет накладных расходов на обработку данных относительно использования `BatchFunction` благодаря тому, что Flow внутри себя осуществляет передачу данных батчами.

{% endnote %}

### RowFunction

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/RowFunction.java)

`RowFunction` получает [сообщения](../../../flow/concepts/glossary.md#message) и [таймеры](../../../flow/concepts/glossary.md#timer) по одному. Интерфейс предоставляет два метода:

- `onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx)` — вызывается для каждого входного сообщения.
- `onTimer(Timer timer, OutputCollector output, RuntimeContext ctx)` — вызывается при срабатывании таймера.

#### Пример stateless-функции

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

Разберем построчно:

1. `ctx.createMessageBuilder("x2_numbers")` — создается `MessageBuilder` для output-[стрима](../../../flow/concepts/glossary.md#stream-and-computation) с id = `x2_numbers`. Стрим с таким идентификатором должен присутствовать в списке `output_stream_ids` в статической [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) компьютейшена.

2. `message.get("number", Long.class)` — получаем значение поля `number` из входящего сообщения. В метод `Message#get` необходимо передавать класс значения для однозначного преобразования из сериализованной формы в Java-объект.

3. `messageBuilder.set("number_x2", number * 2)` — записываем значение в поле `number_x2`. Это поле должно присутствовать в схеме стрима `x2_numbers` в статической спеке.

4. `output.addMessage(messageBuilder.finish())` — метод `finish` возвращает готовое сообщение, которое добавляется в `OutputCollector`.

### BatchFunction

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/function/BatchFunction.java)

`BatchFunction` получает весь список сообщений и таймеров, пришедших от [worker](../../../flow/concepts/glossary.md#worker)-а. Интерфейс предоставляет два метода:

- `onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx)` — вызывается для батча сообщений.
- `onTimers(List<Timer> timers, OutputCollector output, RuntimeContext ctx)` — вызывается для батча таймеров.

#### Пример batch-функции

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

Ключевое отличие от `RowFunction`:

- `MessageBuilder` создается один раз за весь батч (строка 1).
- Метод `finish()` одновременно с возвратом готового сообщения сбрасывает `MessageBuilder` в исходное состояние, после чего его можно переиспользовать для следующего сообщения (строка 5).

## Регистрация в PipelineContext {#pipeline-context}

Все объекты `Computation` и типизированные стримы (созданные через `FlowStreams.typed`) должны быть зарегистрированы в `PipelineContext` перед запуском `GrpcServerExecution`.
Нетипизированные стримы (созданные через `FlowStreams.raw`) регистрировать не обязательно, Flow сам создаст их на основе блока `streams` в статической спеке.

Подробнее про [Типизированные стримы](../../../flow/java/typed-streams.md).

{% list tabs group=lang %}

- Java

  ```java
  var context = new PipelineContext();

  // Регистрация объектов Computation.
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

  // Регистрация типизированных стримов.
  context.registerStream(FlowStreams.typed("hit", Hit.class));
  context.registerStream(FlowStreams.typed("action", Action.class));
  context.registerStream(FlowStreams.typed("joined_action", JoinedAction.class));
  ```

- Kotlin

  ```kotlin
  val context = PipelineContext()

  // Регистрация объектов Computation.
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

  // Регистрация типизированных стримов.
  context.registerStream(FlowStreams.typed("hit", Hit::class.java))
  context.registerStream(FlowStreams.typed("action", Action::class.java))
  context.registerStream(FlowStreams.typed("joined_action", JoinedAction::class.java))
  ```

{% endlist %}

{% note warning %}

Каждый Computation и стрим должен иметь уникальный идентификатор, соответствующий идентификаторам в статической спеке. Попытка зарегистрировать Computation или стрим с уже существующим идентификатором приведёт к ошибке и невозможности старта компаньона.

{% endnote %}

## RuntimeContext

[Исходный код RuntimeContext]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/RuntimeContext.java)

[Исходный код StatefulContext]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/StatefulContext.java)

`RuntimeContext` предоставляет доступ к контексту выполнения компьютейшена. Основные методы:

| Метод | Описание |
| --- | --- |
| `ctx.createMessageBuilder(streamId)` | Создать `MessageBuilder` для указанного output-стрима |
| `ctx.getComputationParameters()` | Получить параметры компьютейшена из спеки |
| `ctx.getEpochInputEventWatermark()` | Получить текущий [вотермарк](../../../flow/concepts/glossary.md#timestamps-and-watermarks) [эпохи](../../../flow/concepts/glossary.md#epoch) |
| `ctx.getProtoStateAccessor(name, message, Class)` | Получить стейт в виде protobuf-объекта, привязанный к [ключу](../../../flow/concepts/glossary.md#key) сообщения |
| `ctx.getYsonStateAccessor(name, message, Class)` | Получить YSON-стейт, привязанный к [ключу](../../../flow/concepts/glossary.md#key) сообщения |
| `ctx.getStateAccessor(name, message, Class, ser, deser)` | Получить стейт с пользовательской сериализацией/десериализацией |
| `ctx.getRawStateAccessor(name, message)` | Получить стейт как массив байт без интерпретации |
| `ctx.getNoOpStateAccessor(name, message)` | Получить стейт, сохраняющий только факт наличия (без значения) |
| `ctx.getExternalStateAccessor(name, message)` | Получить внешний стейт, привязанный к [ключу](../../../flow/concepts/glossary.md#key) сообщения |

Подробнее про работу со стейтами — в разделе [Работа со стейтами (Java)](../../../flow/java/state.md).

## OutputCollector {#output-collector}

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/computation/OutputCollector.java)

`OutputCollector` используется для отправки результатов обработки:

| Метод | Описание |
| --- | --- |
| `output.addMessage(message)` | Добавить выходное сообщение |
| `output.addTimer(triggerTimestamp)` | Добавить [таймер](../../../flow/concepts/glossary.md#timer) с указанным временем срабатывания (eventTimestamp = 0) |
| `output.addTimer(triggerTimestamp, eventTimestamp)` | Добавить таймер с указанным временем срабатывания и event-временем |
| `output.addTimer(timerStreamId, triggerTimestamp, eventTimestamp)` | Добавить таймер для конкретного timer-стрима |
| `output.setParentIds(parentIds)` | Задать parent ID для отслеживания [lineage](../../../flow/concepts/lineage.md) сообщений. Возвращает новый `OutputCollector` |

## Spring Boot

При использовании Spring Boot функции обработки могут быть Spring-компонентами:

{% list tabs group=lang %}

- Java

  ```java
  @Component
  public class WordCountMapper implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          // обработка сообщения
      }
  }
  ```

- Kotlin

  ```kotlin
  @Component
  class WordCountMapper : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          // обработка сообщения
      }
  }
  ```

{% endlist %}

Регистрация компьютейшенов и стримов происходит через `ComputationProvider`:

{% list tabs group=lang %}

- Java

  ```java
  @Configuration
  public class MyContext implements ComputationProvider {

      @Autowired
      private WordCountMapper wordCountMapper;

      @Override
      public List<Computation> getComputations() {
          return List.of(
              Computation.builder()
                  .setComputationId("mapper")
                  .setProcessFunction(wordCountMapper)
                  .build()
          );
      }

      @Override
      public List<FlowStream<?>> getStreams() {
          return List.of(
              FlowStreams.typed("words", Word.class)
          );
      }
  }
  ```

- Kotlin

  ```kotlin
  @Configuration
  class MyContext : ComputationProvider {

      @Autowired
      private lateinit var wordCountMapper: WordCountMapper

      override fun getComputations(): List<Computation> {
          return listOf(
              Computation.builder()
                  .setComputationId("mapper")
                  .setProcessFunction(wordCountMapper)
                  .build()
          )
      }

      override fun getStreams(): List<FlowStream<*>> {
          return listOf(
              FlowStreams.typed("words", Word::class.java)
          )
      }
  }
  ```

{% endlist %}

`FlowStreams.typed(...)` создаёт типизированный стрим, который автоматически сериализует и десериализует сообщения в Java-объекты указанного типа. Подробнее в разделе [Typed Streams](../../../flow/java/typed-streams.md).

## Конфигурация ресурса CompanionManager {#companion-manager}

Для запуска компаньона на Java или Kotlin необходимо объявить ресурс `CompanionManager` в статической спеке:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
    "parameters" = {
        "timeout" = "10s";
        "jdk_bin_path" = "/app/ytflow/jdk/bin/java";
        "main_class" = "tech.ytsaurus.flow.examples.waitclickjoin.NodeCompanionMain";
        "classpath" = "/app/ytflow/lib/*";
        "run_process" = %true;
    };
    "dependencies" = {};
};
```

Параметр `resource_class_name` указывает на класс ресурса, который будет осуществлять запуск компаньона.
В случае компаньона на Java или Kotlin `resource_class_name` всегда должен быть `NYT::NFlow::NCompanion::TJavaCompanionManager` (поддерживает оба языка через JVM).

Подробнее про спеку в разделе [Spec, DynamicSpec и Config](../../../flow/concepts/spec.md).

## См. также

- [Computation (концепция)](../../../flow/concepts/computation.md)
- [Работа со стейтами (Java)](../../../flow/java/state.md)
- [Быстрый старт (Java)](../../../flow/java/getting-started.md)
- [Companion](../../../flow/concepts/companion.md)
