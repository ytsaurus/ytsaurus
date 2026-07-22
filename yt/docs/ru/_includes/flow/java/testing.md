# Тестирование с TestComputationHarness в {{product-name}} Flow (Java)

Модуль `flow-test-utils` предоставляет утилиты для юнит-тестирования компонентов Flow-пайплайна на Java и Kotlin без запуска реального gRPC-сервера и C++ воркеров. Центральный класс — [`TestComputationHarness`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/harness/TestComputationHarness.java), который эмулирует вызовы `doProcess` на уровне компаньона.

{% note info %}

Класс `TestComputationHarness` предназначен для **юнит-тестирования** отдельных `ProcessFunction` без запуска реального пайплайна. Для **интеграционного тестирования** полного пайплайна (с реальными C++ воркерами, очередями и стримами) используйте `FlowTestJavaBase` — см. [раздел ниже](#integration-testing).

{% endnote %}

## Общая архитектура тестирования {#architecture}

В продакшене C++ воркер отправляет gRPC-запросы компаньону, передавая сообщения, таймеры, состояние и watermark-и. `Companion` вызывает `Computation.doProcess()`, который делегирует обработку в `ProcessFunction`.

В тестах `TestComputationHarness` заменяет gRPC-слой: он принимает [`TestDoProcessRequest`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessRequest.java), конвертирует его в protobuf-формат, вызывает `CompanionRequestProcessor.processBatch()` и возвращает [`TestDoProcessResponse`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessResponse.java) с десериализованными результатами.

## Зависимости {#dependencies}

Для использования тестовых утилит необходимо добавить зависимость на `flow-test-utils`:

```
PEERDIR(
    yt/java/flow/flow-test-utils
)
```

Все примеры ниже написаны с использованием JUnit версии 5+.

## Настройка TestComputationHarness {#setup}

Для создания `TestComputationHarness` необходимо:

1. **`PipelineContext`** — контекст пайплайна с зарегистрированными объектами `Computation` и стримами.
2. **Pipeline spec** — статическая спецификация пайплайна в формате YSON (файл `pipeline.yson`).
3. **External state schemas** (опционально) — схемы внешних состояний, если computation использует `ExternalStateAccessor`.

### Builder API

{% list tabs group=lang %}

- Java

  ```java
  TestComputationHarness harness = TestComputationHarness.builder()
          .setPipelineContext(pipelineContext)       // обязательно
          .setPipelineSpec(txtSpec)                  // обязательно: String, YTreeNode или InputStream
          .addExternalStateSchema("state-name", schema) // опционально
          .setJobContext(jobContext)                 // опционально, по умолчанию timeout=10 мин
          .build();
  ```

- Kotlin

  ```kotlin
  val harness = TestComputationHarness.builder()
          .setPipelineContext(pipelineContext)       // обязательно
          .setPipelineSpec(txtSpec)                  // обязательно: String, YTreeNode или InputStream
          .addExternalStateSchema("state-name", schema) // опционально
          .setJobContext(jobContext)                 // опционально, по умолчанию timeout=10 мин
          .build()
  ```

{% endlist %}

При вызове `build()` harness автоматически:
- Извлекает информацию о стримах из pipeline spec.
- Регистрирует недостающие стримы как нетипизированные (через `FlowStreams.raw`) в `PipelineContext`.
- Создаёт внутренний `CompanionRequestProcessor`.

## Построение тестовых запросов {#test-requests}

### TestDoProcessRequest {#test-do-process-request}

[`TestDoProcessRequest`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessRequest.java) — запрос на обработку батча сообщений и/или таймеров. Создаётся через builder:

{% list tabs group=lang %}

- Java

  ```java
  var request = TestDoProcessRequest.builder("computation-id")
          .setMessages(messages)           // List<ExtendedMessage>
          .setTimers(timers)               // List<Timer>
          .setWatermarks(watermarks)       // Map<String, Long>
          .setExternalState("name", stateMap)  // Map<Payload, ExternalState>
          .setInternalState("name", stateMap)  // Map<Payload, InternalState>
          .build();
  ```

- Kotlin

  ```kotlin
  val request = TestDoProcessRequest.builder("computation-id")
          .setMessages(messages)           // List<ExtendedMessage>
          .setTimers(timers)               // List<Timer>
          .setWatermarks(watermarks)       // Map<String, Long>
          .setExternalState("name", stateMap)  // Map<Payload, ExternalState>
          .setInternalState("name", stateMap)  // Map<Payload, InternalState>
          .build()
  ```

{% endlist %}

Параметры:
- **`computationId`** — идентификатор computation, которому адресован запрос.
- **`messages`** — список входных сообщений типа `ExtendedMessage`.
- **`timers`** — список сработавших таймеров типа `Timer`.
- **`watermarks`** — карта watermark-ов по стримам (`streamId → timestamp`).
- **`externalStates`** — предзаполненное внешнее состояние.
- **`internalStates`** — предзаполненное внутреннее состояние.

### Построение сообщений {#building-messages}

#### С типизированными стримами

Если стримы зарегистрированы как типизированные (через `FlowStreams.typed(...)`), payload можно передавать как POJO-объект:

{% list tabs group=lang %}

- Java

  ```java
  ExtendedMessage msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(new Hit("hit-1", 1000L, "payload-data"))
          .setEventTimestamp(1000L)
          .build();
  ```

- Kotlin

  ```kotlin
  val msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(Hit("hit-1", 1000L, "payload-data"))
          .setEventTimestamp(1000L)
          .build()
  ```

{% endlist %}

#### С нетипизированными стримами (PayloadBuilder)

Если стримы не типизированы, payload строится через `PayloadBuilder`:

{% list tabs group=lang %}

- Java

  ```java
  var hitSchema = testHarness.getStream("hit").getSchema();
  var payload = new PayloadBuilder(hitSchema)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .set("hit_payload", "payload-data")
          .finish();

  ExtendedMessage msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(payload)
          .setEventTimestamp(1000L)
          .build();
  ```

- Kotlin

  ```kotlin
  val hitSchema = testHarness.getStream("hit").getSchema()
  val payload = PayloadBuilder(hitSchema)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .set("hit_payload", "payload-data")
          .finish()

  val msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(payload)
          .setEventTimestamp(1000L)
          .build()
  ```

{% endlist %}

{% note info %}

Метод `testHarness.getStream(streamId)` возвращает `FlowStream<?>` со схемой, извлечённой из pipeline spec. Это удобно для получения схемы стрима без ручного определения.

{% endnote %}

### Построение ключей {#building-keys}

Ключ (`Payload`) строится по схеме `group_by_schema` тестируемого `Computation` из pipeline spec.

{% note info %}

Метод `testHarness.getGroupBySchema(computationId)` возвращает `TableSchema` на основе `group_by_schema`, извлечённой из pipeline spec для тестируемого `Computation`.

{% endnote %}

{% list tabs group=lang %}

- Java

  ```java
  TableSchema keySchema = testHarness.getGroupBySchema("join");

  Payload key = new PayloadBuilder(keySchema)
          .set("hash", 0L)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .finish();
  ```

- Kotlin

  ```kotlin
  val keySchema = testHarness.getGroupBySchema("join")

  val key = PayloadBuilder(keySchema)
          .set("hash", 0L)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .finish()
  ```

{% endlist %}

{% note tip %}

Поле `hash` (farm_hash) вычисляется на стороне C++ воркера. В тестах его можно устанавливать в `0L`.

{% endnote %}

### Построение таймеров {#building-timers}

Таймер создаётся через builder:

{% list tabs group=lang %}

- Java

  ```java
  Timer timer = Timer.builder()
          .setStreamId("timer")
          .setKey(key)
          .setTriggerTimestamp(triggerTimestamp)
          .build();
  ```

- Kotlin

  ```kotlin
  val timer = Timer.builder()
          .setStreamId("timer")
          .setKey(key)
          .setTriggerTimestamp(triggerTimestamp)
          .build()
  ```

{% endlist %}

### Настройка watermark-ов {#watermarks}

Watermark-и определяют, какие сообщения считаются «опоздавшими» (late). Сообщение с `eventTimestamp < watermark` будет отброшено, если в `ProcessFunction` реализована соответствующая проверка через `ctx.getEpochInputEventWatermark()`. Пример такой проверки в [Wait Click Join на Java](../../../flow/java/examples/wait_click_join.md#late-data-check).

{% list tabs group=lang %}

- Java

  ```java
  Map<String, Long> watermarks = Map.of(
          "hit", 0L,
          "action", 0L
  );
  ```

- Kotlin

  ```kotlin
  val watermarks = mapOf(
          "hit" to 0L,
          "action" to 0L
  )
  ```

{% endlist %}

Установка watermark в `0L` означает, что все сообщения с `eventTimestamp >= 0` не будут считаться опоздавшими.

### Предзаполнение состояния {#pre-populating-state}

#### External State

{% list tabs group=lang %}

- Java

  ```java
  TableSchema stateSchema = TableSchema.builder()
          .addValue("hit_payload", ColumnValueType.STRING)
          .addValue("show_time", ColumnValueType.UINT64)
          .addValue("click_time", ColumnValueType.UINT64)
          .build();

  ExternalState preState = new ExternalState(
          new PayloadBuilder(stateSchema)
                  .set("hit_payload", "some-payload")
                  .set("show_time", showTime)
                  .set("click_time", clickTime)
                  .finish()
  );

  var request = TestDoProcessRequest.builder("join")
          .setTimers(List.of(timer))
          .setExternalState("join-state", Map.of(key, preState))
          .build();
  ```

- Kotlin

  ```kotlin
  val stateSchema = TableSchema.builder()
          .addValue("hit_payload", ColumnValueType.STRING)
          .addValue("show_time", ColumnValueType.UINT64)
          .addValue("click_time", ColumnValueType.UINT64)
          .build()

  val preState = ExternalState(
          PayloadBuilder(stateSchema)
                  .set("hit_payload", "some-payload")
                  .set("show_time", showTime)
                  .set("click_time", clickTime)
                  .finish()
  )

  val request = TestDoProcessRequest.builder("join")
          .setTimers(listOf(timer))
          .setExternalState("join-state", mapOf(key to preState))
          .build()
  ```

{% endlist %}

#### Internal State (Proto)

Для внутреннего состояния на основе protobuf:

{% list tabs group=lang %}

- Java

  ```java
  TJoinState protoState = TJoinState.newBuilder()
          .setHitPayload("some-payload")
          .setShowTime(showTime)
          .setClickTime(clickTime)
          .build();

  InternalState internalState = new InternalState(protoState.toByteArray());

  var request = TestDoProcessRequest.builder("join")
          .setTimers(List.of(timer))
          .setInternalState("join-state", Map.of(key, internalState))
          .build();
  ```

- Kotlin

  ```kotlin
  val protoState = TJoinState.newBuilder()
          .setHitPayload("some-payload")
          .setShowTime(showTime)
          .setClickTime(clickTime)
          .build()

  val internalState = InternalState(protoState.toByteArray())

  val request = TestDoProcessRequest.builder("join")
          .setTimers(listOf(timer))
          .setInternalState("join-state", mapOf(key to internalState))
          .build()
  ```

{% endlist %}

## Анализ результатов {#analyzing-response}

### TestDoProcessResponse {#test-do-process-response}

[`TestDoProcessResponse`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessResponse.java) предоставляет методы для проверки результатов обработки.

| Метод | Описание |
|-------|----------|
| `getOutputMessagesFlatten()` | Все выходные сообщения из всех transform-групп |
| `getOutputTimersFlatten()` | Все установленные таймеры из всех transform-групп |
| `getTransformResults()` | Список `TransformResult` с группировкой по parent ids |

{% note info %}

`TestDoProcessResponse` предоставляет два набора методов доступа к состоянию:

- **Полная картина** (`getExternalState(s)`, `getInternalState(s)`) — все состояния после обработки: ключи, переданные в запросе, с наложенными поверх изменениями computation. Используйте, когда нужно проверить итоговое состояние независимо от того, изменил ли его computation.
- **Только изменённые** (`getModifiedExternalState(s)`, `getModifiedInternalState(s)`) — только то, что computation действительно изменил. Именно эти состояния отправляются обратно воркеру и сохраняются. Используйте для проверки того, что стейт по ключу не менялся, или когда нужно проверить только изменения состояния.

Все методы доступа к состоянию null-safe: неизвестное имя состояния или ключ возвращают пустую карту или `null`, а не выбрасывают исключение.

{% endnote %}

Полная картина состояния (загруженные состояния с наложенными изменениями computation):

| Метод | Описание |
|-------|----------|
| `getExternalStateNames()` / `getInternalStateNames()` | Имена всех состояний (загруженных и/или изменённых) |
| `getExternalStates()` / `getInternalStates()` | Все состояния, сгруппированные по имени |
| `getExternalStates(name)` / `getInternalStates(name)` | Все записи состояния по имени (пустая карта, если нет) |
| `getExternalStateSize(name)` / `getInternalStateSize(name)` | Количество записей в состоянии (0, если нет) |
| `getExternalState(name, key)` / `getInternalState(name, key)` | Конкретная запись состояния (`null`, если нет) |

Только состояния, изменённые computation:

| Метод | Описание |
|-------|----------|
| `getModifiedExternalStateNames()` / `getModifiedInternalStateNames()` | Имена изменённых состояний |
| `getModifiedExternalStates()` / `getModifiedInternalStates()` | Изменённые состояния, сгруппированные по имени |
| `getModifiedExternalStates(name)` / `getModifiedInternalStates(name)` | Изменённые записи состояния по имени (пустая карта, если нет) |
| `getModifiedExternalStateSize(name)` / `getModifiedInternalStateSize(name)` | Количество изменённых записей (0, если нет) |
| `getModifiedExternalState(name, key)` / `getModifiedInternalState(name, key)` | Конкретная изменённая запись (`null`, если не изменена) |

### Проверка выходных сообщений

{% list tabs group=lang %}

- Java

  ```java
  var response = testHarness.doProcess(request);

  // Проверяем количество выходных сообщений
  assertEquals(1, response.getOutputMessagesFlatten().size());

  // Получаем сообщение и проверяем стрим
  var msg = response.getOutputMessagesFlatten().get(0);
  assertEquals("joined_action", msg.getStreamId());

  // Для типизированных стримов — приведение payload к POJO
  JoinedAction result = (JoinedAction) msg.getPayload();
  assertEquals("hit-1", result.getHitId());

  // Для нетипизированных стримов — чтение полей через get()
  byte[] data = msg.get("data", byte[].class);
  ```

- Kotlin

  ```kotlin
  val response = testHarness.doProcess(request)

  // Проверяем количество выходных сообщений
  assertEquals(1, response.getOutputMessagesFlatten().size)

  // Получаем сообщение и проверяем стрим
  val msg = response.getOutputMessagesFlatten()[0]
  assertEquals("joined_action", msg.getStreamId())

  // Для типизированных стримов — приведение payload к POJO
  val result = msg.getPayload() as JoinedAction
  assertEquals("hit-1", result.getHitId())

  // Для нетипизированных стримов — чтение полей через get()
  val data = msg.get("data", ByteArray::class.java)
  ```

{% endlist %}

### Проверка таймеров

{% list tabs group=lang %}

- Java

  ```java
  assertEquals(1, response.getOutputTimersFlatten().size());
  var timer = response.getOutputTimersFlatten().get(0);
  assertEquals(expectedTriggerTimestamp, timer.getTriggerTimestamp());
  ```

- Kotlin

  ```kotlin
  assertEquals(1, response.getOutputTimersFlatten().size)
  val timer = response.getOutputTimersFlatten()[0]
  assertEquals(expectedTriggerTimestamp, timer.getTriggerTimestamp())
  ```

{% endlist %}

### Проверка состояния

{% list tabs group=lang %}

- Java

  ```java
  // External state
  assertEquals(1, response.getExternalStateSize("join-state"));
  var state = response.getExternalState("join-state", key);
  assertFalse(state.isReset());
  assertEquals("payload-data", state.getValue().get("hit_payload", String.class));

  // Проверка сброса состояния
  var stateAfterTimer = response.getExternalState("join-state", key);
  assertTrue(stateAfterTimer.isReset());
  ```

- Kotlin

  ```kotlin
  // External state
  assertEquals(1, response.getExternalStateSize("join-state"))
  val state = response.getExternalState("join-state", key)!!
  assertFalse(state.isReset)
  assertEquals("payload-data", state.value.get("hit_payload", String::class.java))

  // Проверка сброса состояния
  val stateAfterTimer = response.getExternalState("join-state", key)!!
  assertTrue(stateAfterTimer.isReset)
  ```

{% endlist %}

{% list tabs group=lang %}

- Java

  ```java
  // Internal state (Proto)
  var internalState = response.getInternalState("join-state", key);
  assertFalse(internalState.isReset());
  TJoinState joinState = ProtoUtils.parseBytes(internalState.getValue(), TJoinState.class);
  assertEquals("payload-data", joinState.getHitPayload());
  ```

- Kotlin

  ```kotlin
  // Internal state (Proto)
  val internalState = response.getInternalState("join-state", key)!!
  assertFalse(internalState.isReset)
  val joinState = ProtoUtils.parseBytes(internalState.value, TJoinState::class.java)
  assertEquals("payload-data", joinState.getHitPayload())
  ```

{% endlist %}

#### Полная картина и изменённые состояния {#all-vs-modified-state}

Методы `getExternalState` / `getInternalState` возвращают **полную картину**: предзаполненное состояние доступно, даже если computation его не менял. Чтобы проверить, что именно изменил computation, используйте методы `getModified*`.

{% list tabs group=lang %}

- Java

  ```java
  // Полная картина: загруженное состояние видно, даже если не изменялось
  assertEquals(1, response.getExternalStateSize("join-state"));
  assertNotNull(response.getExternalState("join-state", key));

  // Только изменённые состояния: computation ничего не записал для этого ключа
  assertEquals(0, response.getModifiedExternalStateSize("join-state"));
  assertNull(response.getModifiedExternalState("join-state", key));

  // Неизвестные имя/ключ не бросают исключение
  assertNull(response.getExternalState("unknown", key));
  assertTrue(response.getExternalStates("unknown").isEmpty());
  ```

- Kotlin

  ```kotlin
  // Полная картина: загруженное состояние видно, даже если не изменялось
  assertEquals(1, response.getExternalStateSize("join-state"))
  assertNotNull(response.getExternalState("join-state", key))

  // Только изменённые состояния: computation ничего не записал для этого ключа
  assertEquals(0, response.getModifiedExternalStateSize("join-state"))
  assertNull(response.getModifiedExternalState("join-state", key))

  // Неизвестные имя/ключ не бросают исключение
  assertNull(response.getExternalState("unknown", key))
  assertTrue(response.getExternalStates("unknown").isEmpty())
  ```

{% endlist %}

## Пример: тест без Spring {#example-without-spring}

Полный пример юнит-теста для `JoinProcessFunction` из проекта [wait_click_join](../../../flow/java/examples/wait_click_join.md). В этом варианте `PipelineContext` создаётся вручную, без Spring-контейнера.

{% list tabs group=lang %}

- Java

  ```java
  public class JoinProcessFunctionTest {

      private static final long WAIT_SECONDS = 10L;
      private static final long BASE_HIT_TIME = 1000L;
      private static final long WATERMARK = 0L;

      private TestComputationHarness testHarness;
      private TableSchema keySchema;
      private TableSchema joinStateSchema;

      @BeforeEach
      void init() throws IOException {
          // 1. Создаём PipelineContext вручную
          var pipelineContext = new PipelineContext();

          // 2. Регистрируем computation с process function
          Computation join = Computation.builder()
                  .setComputationId("join")
                  .setProcessFunction(new JoinProcessFunction())
                  .build();
          pipelineContext.registerComputation(join);

          // 3. Регистрируем типизированные стримы
          pipelineContext.registerStream(FlowStreams.typed("hit", Hit.class));
          pipelineContext.registerStream(FlowStreams.typed("action", Action.class));
          pipelineContext.registerStream(FlowStreams.typed("joined_action", JoinedAction.class));

          // 4. Определяем схему внешнего состояния
          this.joinStateSchema = TableSchema.builder()
                  .addValue("hit_payload", ColumnValueType.STRING)
                  .addValue("show_time", ColumnValueType.UINT64)
                  .addValue("click_time", ColumnValueType.UINT64)
                  .build();

          // 5. Читаем pipeline spec и создаём harness
          var specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/wait_click_join/test/pipeline.yson");
          var txtSpec = Files.readString(Path.of(specPath));

          this.testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)
                  .setPipelineSpec(txtSpec)
                  .addExternalStateSchema("join-state", joinStateSchema)
                  .build();

          // 6. Получаем key schema для join computation (group_by_schema из pipeline.yson)
          this.keySchema = testHarness.getGroupBySchema("join");
      }

      // — Вспомогательные методы —

      private Payload buildKey(String hitId, long hitTime) {
          return new PayloadBuilder(keySchema)
                  .set("hash", 0L)
                  .set("hit_id", hitId)
                  .set("hit_time", hitTime)
                  .finish();
      }

      private ExtendedMessage buildHitMessage(String hitId, long hitTime, String hitPayload) {
          return ExtendedMessage.builder()
                  .setStreamId("hit")
                  .setKey(buildKey(hitId, hitTime))
                  .setPayload(new Hit(hitId, hitTime, hitPayload))
                  .setEventTimestamp(hitTime)
                  .build();
      }

      private Map<String, Long> defaultWatermarks() {
          return Map.of("hit", WATERMARK, "action", WATERMARK);
      }

      // — Тесты —

      @Test
      void testHitMessageStoresHitPayloadInState() {
          var messages = List.of(buildHitMessage("hit-1", BASE_HIT_TIME, "payload-data"));

          var request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(defaultWatermarks())
                  .build();

          var response = testHarness.doProcess(request);

          // Нет выходных сообщений — результат будет при срабатывании таймера
          assertTrue(response.getOutputMessagesFlatten().isEmpty());

          // Таймер должен быть установлен
          assertEquals(1, response.getOutputTimersFlatten().size());
          assertEquals(BASE_HIT_TIME + WAIT_SECONDS,
                  response.getOutputTimersFlatten().get(0).getTriggerTimestamp());

          // Состояние должно содержать hit_payload
          Payload key = buildKey("hit-1", BASE_HIT_TIME);
          var state = response.getExternalState("join-state", key);
          assertFalse(state.isReset());
          assertEquals("payload-data", state.getValue().get("hit_payload", String.class));
      }

      @Test
      void testTimerEmitsJoinedAction() {
          Payload key = buildKey("hit-10", BASE_HIT_TIME);

          // Предзаполняем состояние
          ExternalState preState = new ExternalState(
                  new PayloadBuilder(joinStateSchema)
                          .set("hit_payload", "some-payload")
                          .set("show_time", BASE_HIT_TIME + 3L)
                          .set("click_time", BASE_HIT_TIME + 7L)
                          .finish()
          );

          Timer timer = Timer.builder()
                  .setStreamId("timer")
                  .setKey(key)
                  .setTriggerTimestamp(BASE_HIT_TIME + WAIT_SECONDS)
                  .build();

          var request = TestDoProcessRequest.builder("join")
                  .setTimers(List.of(timer))
                  .setExternalState("join-state", Map.of(key, preState))
                  .build();

          var response = testHarness.doProcess(request);

          // Должно быть одно выходное сообщение
          assertEquals(1, response.getOutputMessagesFlatten().size());
          var msg = response.getOutputMessagesFlatten().get(0);
          assertEquals("joined_action", msg.getStreamId());

          JoinedAction result = (JoinedAction) msg.getPayload();
          assertEquals("hit-10", result.getHitId());
          assertTrue(result.getClick());

          // Состояние должно быть сброшено
          assertTrue(response.getExternalState("join-state", key).isReset());
      }

      @Test
      void testLateMessageIsDropped() {
          long watermark = BASE_HIT_TIME + 5L;
          long eventTimestamp = BASE_HIT_TIME + 3L; // < watermark → late

          var messages = List.of(buildActionMessage("hit-5", BASE_HIT_TIME,
                  BASE_HIT_TIME + 2L, false, eventTimestamp));

          var request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(Map.of("hit", watermark, "action", watermark))
                  .build();

          var response = testHarness.doProcess(request);

          // Опоздавшее сообщение отброшено
          assertTrue(response.getOutputMessagesFlatten().isEmpty());
          assertTrue(response.getOutputTimersFlatten().isEmpty());
          assertEquals(0, response.getExternalStateSize("join-state"));
      }
  }
  ```

- Kotlin

  ```kotlin
  class JoinProcessFunctionTest {

      companion object {
          private const val WAIT_SECONDS = 10L
          private const val BASE_HIT_TIME = 1000L
          private const val WATERMARK = 0L
      }

      private lateinit var testHarness: TestComputationHarness
      private lateinit var keySchema: TableSchema
      private lateinit var joinStateSchema: TableSchema

      @BeforeEach
      fun init() {
          // 1. Создаём PipelineContext вручную
          val pipelineContext = PipelineContext()

          // 2. Регистрируем computation с process function
          val join = Computation.builder()
                  .setComputationId("join")
                  .setProcessFunction(JoinProcessFunction())
                  .build()
          pipelineContext.registerComputation(join)

          // 3. Регистрируем типизированные стримы
          pipelineContext.registerStream(FlowStreams.typed("hit", Hit::class.java))
          pipelineContext.registerStream(FlowStreams.typed("action", Action::class.java))
          pipelineContext.registerStream(FlowStreams.typed("joined_action", JoinedAction::class.java))

          // 4. Определяем схему внешнего состояния
          joinStateSchema = TableSchema.builder()
                  .addValue("hit_payload", ColumnValueType.STRING)
                  .addValue("show_time", ColumnValueType.UINT64)
                  .addValue("click_time", ColumnValueType.UINT64)
                  .build()

          // 5. Читаем pipeline spec и создаём harness
          val specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/wait_click_join/test/pipeline.yson")
          val txtSpec = Files.readString(Path.of(specPath))

          testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)
                  .setPipelineSpec(txtSpec)
                  .addExternalStateSchema("join-state", joinStateSchema)
                  .build()

          // 6. Получаем key schema для join computation (group_by_schema из pipeline.yson)
          keySchema = testHarness.getGroupBySchema("join")
      }

      // — Вспомогательные методы —

      private fun buildKey(hitId: String, hitTime: Long): Payload =
          PayloadBuilder(keySchema)
                  .set("hash", 0L)
                  .set("hit_id", hitId)
                  .set("hit_time", hitTime)
                  .finish()

      private fun buildHitMessage(hitId: String, hitTime: Long, hitPayload: String): ExtendedMessage =
          ExtendedMessage.builder()
                  .setStreamId("hit")
                  .setKey(buildKey(hitId, hitTime))
                  .setPayload(Hit(hitId, hitTime, hitPayload))
                  .setEventTimestamp(hitTime)
                  .build()

      private fun defaultWatermarks() = mapOf("hit" to WATERMARK, "action" to WATERMARK)

      // — Тесты —

      @Test
      fun testHitMessageStoresHitPayloadInState() {
          val messages = listOf(buildHitMessage("hit-1", BASE_HIT_TIME, "payload-data"))

          val request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(defaultWatermarks())
                  .build()

          val response = testHarness.doProcess(request)

          // Нет выходных сообщений — результат будет при срабатывании таймера
          assertTrue(response.getOutputMessagesFlatten().isEmpty())

          // Таймер должен быть установлен
          assertEquals(1, response.getOutputTimersFlatten().size)
          assertEquals(BASE_HIT_TIME + WAIT_SECONDS,
                  response.getOutputTimersFlatten()[0].getTriggerTimestamp())

          // Состояние должно содержать hit_payload
          val key = buildKey("hit-1", BASE_HIT_TIME)
          val state = response.getExternalState("join-state", key)!!
          assertFalse(state.isReset)
          assertEquals("payload-data", state.value.get("hit_payload", String::class.java))
      }

      @Test
      fun testTimerEmitsJoinedAction() {
          val key = buildKey("hit-10", BASE_HIT_TIME)

          // Предзаполняем состояние
          val preState = ExternalState(
                  PayloadBuilder(joinStateSchema)
                          .set("hit_payload", "some-payload")
                          .set("show_time", BASE_HIT_TIME + 3L)
                          .set("click_time", BASE_HIT_TIME + 7L)
                          .finish()
          )

          val timer = Timer.builder()
                  .setStreamId("timer")
                  .setKey(key)
                  .setTriggerTimestamp(BASE_HIT_TIME + WAIT_SECONDS)
                  .build()

          val request = TestDoProcessRequest.builder("join")
                  .setTimers(listOf(timer))
                  .setExternalState("join-state", mapOf(key to preState))
                  .build()

          val response = testHarness.doProcess(request)

          // Должно быть одно выходное сообщение
          assertEquals(1, response.getOutputMessagesFlatten().size)
          val msg = response.getOutputMessagesFlatten()[0]
          assertEquals("joined_action", msg.getStreamId())

          val result = msg.getPayload() as JoinedAction
          assertEquals("hit-10", result.getHitId())
          assertTrue(result.getClick())

          // Состояние должно быть сброшено
          assertTrue(response.getExternalState("join-state", key)!!.isReset)
      }

      @Test
      fun testLateMessageIsDropped() {
          val watermark = BASE_HIT_TIME + 5L
          val eventTimestamp = BASE_HIT_TIME + 3L // < watermark → late

          val messages = listOf(buildActionMessage("hit-5", BASE_HIT_TIME,
                  BASE_HIT_TIME + 2L, false, eventTimestamp))

          val request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(mapOf("hit" to watermark, "action" to watermark))
                  .build()

          val response = testHarness.doProcess(request)

          // Опоздавшее сообщение отброшено
          assertTrue(response.getOutputMessagesFlatten().isEmpty())
          assertTrue(response.getOutputTimersFlatten().isEmpty())
          assertEquals(0, response.getExternalStateSize("join-state"))
      }
  }
  ```

{% endlist %}

### Многошаговый тест {#multi-step-test}

Для тестирования полного потока обработки (hit → show → click → timer) состояние передаётся между шагами вручную:

{% list tabs group=lang %}

- Java

  ```java
  @Test
  void testFullJoinFlow() {
      String hitId = "hit-20";
      long hitTime = BASE_HIT_TIME;

      // Шаг 1: обработка hit-сообщения
      var hitRequest = TestDoProcessRequest.builder("join")
              .setMessages(List.of(buildHitMessage(hitId, hitTime, "full-payload")))
              .setWatermarks(defaultWatermarks())
              .build();
      var hitResponse = testHarness.doProcess(hitRequest);

      Payload key = buildKey(hitId, hitTime);
      var stateAfterHit = hitResponse.getExternalState("join-state", key);

      // Шаг 2: обработка show-сообщения, передаём состояние из шага 1
      var showRequest = TestDoProcessRequest.builder("join")
              .setMessages(List.of(buildActionMessage(hitId, hitTime, hitTime + 3L, false, hitTime + 1L)))
              .setExternalState("join-state", Map.of(key, stateAfterHit))
              .setWatermarks(defaultWatermarks())
              .build();
      var showResponse = testHarness.doProcess(showRequest);

      var stateAfterShow = showResponse.getExternalState("join-state", key);

      // Шаг 3: обработка click-сообщения
      var clickRequest = TestDoProcessRequest.builder("join")
              .setMessages(List.of(buildActionMessage(hitId, hitTime, hitTime + 7L, true, hitTime + 2L)))
              .setExternalState("join-state", Map.of(key, stateAfterShow))
              .setWatermarks(defaultWatermarks())
              .build();
      var clickResponse = testHarness.doProcess(clickRequest);

      var stateAfterClick = clickResponse.getExternalState("join-state", key);

      // Шаг 4: срабатывание таймера
      Timer timer = Timer.builder()
              .setStreamId("timer")
              .setKey(key)
              .setTriggerTimestamp(hitTime + WAIT_SECONDS)
              .build();
      var timerRequest = TestDoProcessRequest.builder("join")
              .setTimers(List.of(timer))
              .setExternalState("join-state", Map.of(key, stateAfterClick))
              .build();
      var timerResponse = testHarness.doProcess(timerRequest);

      // Проверяем финальный результат
      assertEquals(1, timerResponse.getOutputMessagesFlatten().size());
      JoinedAction result = timerResponse.getOutputMessagesFlatten().get(0).getPayload();
      assertEquals(hitId, result.getHitId());
      assertTrue(result.getClick());

      assertTrue(timerResponse.getExternalState("join-state", key).isReset());
  }
  ```

- Kotlin

  ```kotlin
  @Test
  fun testFullJoinFlow() {
      val hitId = "hit-20"
      val hitTime = BASE_HIT_TIME

      // Шаг 1: обработка hit-сообщения
      val hitRequest = TestDoProcessRequest.builder("join")
              .setMessages(listOf(buildHitMessage(hitId, hitTime, "full-payload")))
              .setWatermarks(defaultWatermarks())
              .build()
      val hitResponse = testHarness.doProcess(hitRequest)

      val key = buildKey(hitId, hitTime)
      val stateAfterHit = hitResponse.getExternalState("join-state", key)!!

      // Шаг 2: обработка show-сообщения, передаём состояние из шага 1
      val showRequest = TestDoProcessRequest.builder("join")
              .setMessages(listOf(buildActionMessage(hitId, hitTime, hitTime + 3L, false, hitTime + 1L)))
              .setExternalState("join-state", mapOf(key to stateAfterHit))
              .setWatermarks(defaultWatermarks())
              .build()
      val showResponse = testHarness.doProcess(showRequest)

      val stateAfterShow = showResponse.getExternalState("join-state", key)!!

      // Шаг 3: обработка click-сообщения
      val clickRequest = TestDoProcessRequest.builder("join")
              .setMessages(listOf(buildActionMessage(hitId, hitTime, hitTime + 7L, true, hitTime + 2L)))
              .setExternalState("join-state", mapOf(key to stateAfterShow))
              .setWatermarks(defaultWatermarks())
              .build()
      val clickResponse = testHarness.doProcess(clickRequest)

      val stateAfterClick = clickResponse.getExternalState("join-state", key)!!

      // Шаг 4: срабатывание таймера
      val timer = Timer.builder()
              .setStreamId("timer")
              .setKey(key)
              .setTriggerTimestamp(hitTime + WAIT_SECONDS)
              .build()
      val timerRequest = TestDoProcessRequest.builder("join")
              .setTimers(listOf(timer))
              .setExternalState("join-state", mapOf(key to stateAfterClick))
              .build()
      val timerResponse = testHarness.doProcess(timerRequest)

      // Проверяем финальный результат
      assertEquals(1, timerResponse.getOutputMessagesFlatten().size)
      val result = timerResponse.getOutputMessagesFlatten()[0].getPayload() as JoinedAction
      assertEquals(hitId, result.getHitId())
      assertTrue(result.getClick())

      assertTrue(timerResponse.getExternalState("join-state", key)!!.isReset)
  }
  ```

{% endlist %}

{% note warning %}

`TestComputationHarness` не хранит состояние между вызовами `doProcess()`. Каждый вызов — это независимый батч. Для эмуляции многошаговой обработки необходимо вручную передавать состояние из ответа предыдущего шага в запрос следующего.

{% endnote %}

## Пример: тест со Spring {#example-with-spring}

При использовании Spring Boot Starter тестирование упрощается: `PipelineContext` создаётся автоматически через `FlowAutoConfiguration` на основе зарегистрированных `ComputationProvider`-бинов.

### Тестовая конфигурация {#spring-test-config}

Для тестов необходимо подменить `GrpcServerExecution` на `NoServerTestExecution`, чтобы не запускать реальный gRPC-сервер:

{% list tabs group=lang %}

- Java

  ```java
  @TestConfiguration
  public class MyTestConfiguration {

      @Bean
      public CompanionExecutionConfig companionExecutionConfig() {
          return new CompanionExecutionConfig(0, new MockEnvironmentReader().worker());
      }

      @Bean
      public GrpcServerExecution grpcServerExecution(
              PipelineContext pipelineContext,
              CompanionExecutionConfig companionExecutionConfig
      ) {
          return new NoServerTestExecution(pipelineContext, companionExecutionConfig);
      }
  }
  ```

- Kotlin

  ```kotlin
  @TestConfiguration
  class MyTestConfiguration {

      @Bean
      fun companionExecutionConfig(): CompanionExecutionConfig =
          CompanionExecutionConfig(0, MockEnvironmentReader().worker())

      @Bean
      fun grpcServerExecution(
          pipelineContext: PipelineContext,
          companionExecutionConfig: CompanionExecutionConfig
      ): GrpcServerExecution = NoServerTestExecution(pipelineContext, companionExecutionConfig)
  }
  ```

{% endlist %}

Ключевые моменты:
- [`MockEnvironmentReader`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/config/MockEnvironmentReader.java) — подменяет чтение переменных окружения. Метод `.worker()` устанавливает `YT_FLOW_MODE=Worker`.
- [`NoServerTestExecution`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/execution/NoServerTestExecution.java) — заглушка для `GrpcServerExecution`, которая не запускает реальный gRPC-сервер.
- `CompanionExecutionConfig(0, ...)` — порт `0` означает, что реальный порт не выделяется.

### Тестовый класс {#spring-test-class}

{% list tabs group=lang %}

- Java

  ```java
  @SpringBootTest(classes = MyTestConfiguration.class)
  class JoinFunctionTest {

      @Autowired
      private PipelineContext pipelineContext;

      private TestComputationHarness testHarness;
      private TableSchema keySchema;

      @BeforeEach
      void init() throws IOException {
          this.keySchema = TableSchema.builder()
                  .addValue("hash", ColumnValueType.UINT64)
                  .addValue("hit_id", ColumnValueType.STRING)
                  .addValue("hit_time", ColumnValueType.UINT64)
                  .build();

          var specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/lb_wait_click_join/test/pipeline.yson");
          var txtSpec = Files.readString(Path.of(specPath));

          this.testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)  // инжектированный через Spring
                  .setPipelineSpec(txtSpec)
                  .build();
      }

      @Test
      void testHitMessageStoresState() {
          var messages = List.of(buildHitMessage("hit-1", 1000L, "payload-data"));

          var request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(Map.of("hit", 0L, "action", 0L))
                  .build();

          var response = testHarness.doProcess(request);

          assertTrue(response.getOutputMessagesFlatten().isEmpty());
          assertEquals(1, response.getOutputTimersFlatten().size());
          assertEquals(1, response.getInternalStateSize("join-state"));
      }

      // ... вспомогательные методы и остальные тесты
  }
  ```

- Kotlin

  ```kotlin
  @SpringBootTest(classes = [MyTestConfiguration::class])
  class JoinFunctionTest {

      @Autowired
      private lateinit var pipelineContext: PipelineContext

      private lateinit var testHarness: TestComputationHarness
      private lateinit var keySchema: TableSchema

      @BeforeEach
      fun init() {
          keySchema = TableSchema.builder()
                  .addValue("hash", ColumnValueType.UINT64)
                  .addValue("hit_id", ColumnValueType.STRING)
                  .addValue("hit_time", ColumnValueType.UINT64)
                  .build()

          val specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/lb_wait_click_join/test/pipeline.yson")
          val txtSpec = Files.readString(Path.of(specPath))

          testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)  // инжектированный через Spring
                  .setPipelineSpec(txtSpec)
                  .build()
      }

      @Test
      fun testHitMessageStoresState() {
          val messages = listOf(buildHitMessage("hit-1", 1000L, "payload-data"))

          val request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(mapOf("hit" to 0L, "action" to 0L))
                  .build()

          val response = testHarness.doProcess(request)

          assertTrue(response.getOutputMessagesFlatten().isEmpty())
          assertEquals(1, response.getOutputTimersFlatten().size)
          assertEquals(1, response.getInternalStateSize("join-state"))
      }

      // ... вспомогательные методы и остальные тесты
  }
  ```

{% endlist %}

### Отличия от теста без Spring {#spring-vs-no-spring}

| Аспект | Без Spring | Со Spring |
|--------|-----------|-----------|
| Создание `PipelineContext` | Вручную: `new PipelineContext()` | Автоматически через `FlowAutoConfiguration` |
| Регистрация Computation | `pipelineContext.registerComputation(...)` | Через `ComputationProvider.getComputations()` |
| Регистрация стримов | `pipelineContext.registerStream(...)` | Через `ComputationProvider.getStreams()` |
| Инъекция зависимостей в ProcessFunction | Вручную через конструктор | Автоматически через `@Autowired` |
| Подмена gRPC-сервера | Не нужна (нет сервера) | `NoServerTestExecution` + `MockEnvironmentReader` |

## Интеграционное тестирование с FlowTestJavaBase {#integration-testing}

Для полного интеграционного тестирования пайплайна (с реальными C++ воркерами, очередями и стримами) используется базовый класс `FlowTestJavaBase`.

### Зависимости {#integration-dependencies}

```
PEERDIR(
    yt/yt/flow/library/python/integration_test_base
)
```

### Настройка {#java-test-setup}

Тест наследуется от `FlowTestJavaBase` и задаёт два обязательных атрибута:

```python
from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import FlowTestJavaBase
import yatest.common

class TestWordCount(FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(
        "yt/yt/flow/examples/java/word_count/wordcount/"
    )
    JAVA_RUNNER_MAIN_CLASS = "tech.ytsaurus.flow.examples.wordcount.RunnerMain"
```

| Атрибут | Описание |
|---------|----------|
| `JAVA_RUNNER_BINARY_DIR` | Путь к директории с бинарём Java-раннера (содержит `run.sh`) |
| `JAVA_RUNNER_MAIN_CLASS` | Полное имя main-класса Java-раннера |

[Примеры интеграционных тестов (Java)]({{source-root}}/yt/yt/flow/examples/java)

{% note warning %}

Интеграционные тесты требуют развёрнутого кластера {{product-name}} и запускаются через `ya make -ttt`. Для быстрой итерации используйте юнит-тесты с `TestComputationHarness`, описанные выше.

{% endnote %}

{% include notitle [_](../testing-integration-body.md) %}

{% include notitle [_](../testing-test-param-body.md) %}

## См. также

- [Computation (Java)](../../../flow/java/computation.md)
- [Работа со стейтами (Java)](../../../flow/java/state.md)
- [Тестирование (Python)](../../../flow/python/testing.md)
