# External State в {{product-name}} Flow (Java)

External State — механизм работы с внешним состоянием (стейтом), хранящимся во внешней динамической таблице {{product-name}}. Пользователь самостоятельно создаёт таблицу для хранения стейта{% if audience == "internal" %} (например, с помощью [YtSync]({{yt-sync-docs}}/)){% endif %} на том же кластере, где развёрнут пайплайн.

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../../flow/concepts/stateful.md).

## Обзор

External State в Java SDK Flow (Java и Kotlin) представлен классом `ExternalStateAccessor`, который предоставляет типизированный доступ к строкам внешней динамической таблицы. Стейт привязан к ключу сообщения (`group_by_schema`). Подробнее про `StateAccessor` и работу со стейтом: [State Accessor](../../../flow/java/state-accessor.md).

{% note info %}

Если нужен **только read-only** доступ к внешнему стейту (join по ключу с кэшированием по TTL, без модификации), на стороне фреймворка существует отдельный механизм — **External State Joiner**. Он доступен и в Java/Kotlin (см. [Read-only joiner](#read-only-joiner)), и в C++ ([External State Joiner](../../../flow/cpp/state.md#external-state-joiner)) и объявляется в спеке компьютейшена в top-level секции `external_state_joiners` (на одном уровне с `external_state_managers`).

{% endnote %}

## Отличие от Internal State

| Характеристика | External State | Internal State |
|---|---|---|
| Хранение | Внешняя динамическая таблица | Внутренние таблицы Flow |
| Создание таблицы | Пользователь создаёт самостоятельно | Автоматически |
| Формат данных | Типизированный `Payload` (строка таблицы) | Произвольный (YSON, Protobuf, raw bytes) |
| Доступ из других систем | Да (сортированная динамическая таблица) | Нет |
| Схема | Определяется схемой таблицы | Определяется пользователем |

Подробнее о Internal State см. [Internal State](../../../flow/java/internal-state.md).

## Получение ExternalStateAccessor

External state описывается константой `ExternalStateDescriptor`, создаваемой через `StateDescriptors.external(...)`. Дескриптор обычно объявляется один раз на класс `RowFunction`:

```java
private static final ExternalStateDescriptor JOIN_STATE =
        StateDescriptors.external("/join-state");
```

`ExternalStateAccessor` для конкретного [ключа](../../../flow/concepts/glossary.md#key) получается через `RuntimeContext` (который наследует `StatefulContext`):

{% list tabs group=lang %}

- Java

  ```java
  // Для сообщения
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("state-name", message);

  // Для таймера
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("state-name", timer);
  ```

- Kotlin

  ```kotlin
  // Для сообщения
  val stateAccessor = ctx.getExternalStateAccessor("state-name", message)

  // Для таймера
  val stateAccessor = ctx.getExternalStateAccessor("state-name", timer)
  ```

{% endlist %}

Параметры:
- дескриптор — `ExternalStateDescriptor`, имя которого совпадает с ключом в `external_state_managers` статической спеки и обязательно начинается с `/`;
- `message` / `timer` — сообщение или таймер, для [ключа](../../../flow/concepts/glossary.md#key) которого нужно получить стейт.

{% note warning %}

Имя external state валидируется: оно должно начинаться с `/`, не быть пустым, не оканчиваться на `/` и не содержать двух подряд `/`. Вызов `StateDescriptors.external("join-state")` (без ведущего `/`) бросит `IllegalArgumentException`. Это же имя должно фигурировать в [спеке](#static-spec).

{% endnote %}

## Основные операции

### Чтение стейта

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", message);

  // Получение Optional<Payload>
  Optional<Payload> maybeState = stateAccessor.get();
  if (maybeState.isPresent()) {
      Payload state = maybeState.get();
      String value = state.get("field_name", String.class);
  }

  // Получение стейта с дефолтным значением (пустой Payload со схемой стейта)
  Payload state = stateAccessor.getOrDefault();
  ```

- Kotlin

  ```kotlin
  val stateAccessor = ctx.getExternalStateAccessor("join-state", message)

  // Получение Optional<Payload>
  val maybeState = stateAccessor.get()
  if (maybeState.isPresent) {
      val state = maybeState.get()
      val value = state.get("field_name", String::class.java)
  }

  // Получение стейта с дефолтным значением (пустой Payload со схемой стейта)
  val state = stateAccessor.getOrDefault()
  ```

{% endlist %}

### Запись стейта

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", message);

  // Получение текущего стейта и модификация через PayloadBuilder
  PayloadBuilder builder = stateAccessor.getOrDefault().toBuilder();
  builder.set("hit_payload", "some_value");
  builder.set("show_time", 1234567890L);
  stateAccessor.set(builder.finish());
  ```

- Kotlin

  ```kotlin
  val stateAccessor = ctx.getExternalStateAccessor("join-state", message)

  // Получение текущего стейта и модификация через PayloadBuilder
  val builder = stateAccessor.getOrDefault().toBuilder()
  builder.set("hit_payload", "some_value")
  builder.set("show_time", 1234567890L)
  stateAccessor.set(builder.finish())
  ```

{% endlist %}

### Очистка стейта

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", timer);

  // Очистка стейта (удаление строки из таблицы)
  stateAccessor.clear();
  ```

- Kotlin

  ```kotlin
  val stateAccessor = ctx.getExternalStateAccessor("join-state", timer)

  // Очистка стейта (удаление строки из таблицы)
  stateAccessor.clear()
  ```

{% endlist %}

{% note info %}

Пустой стейт соответствует отсутствию строки в таблице. Если строки нет, `get()` вернёт `Optional.empty()`. При вызове `clear()` строка будет удалена из таблицы.

{% endnote %}

## Конфигурация в статической спеке {#static-spec}

Для использования External State необходимо объявить external state manager в секции `external_state_managers` `Computation` в статической спеке:

```yson
"computations" = {
    "join" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "group_by_schema" = [
            {"name" = "hash"; "expression" = "farm_hash(hit_id)"; "type" = "uint64"};
            {"name" = "hit_id"; "type" = "string"};
            {"name" = "hit_time"; "type" = "uint64"};
        ];
        "input_stream_ids" = ["action"; "hit"];
        "output_stream_ids" = ["joined_action"];
        "external_state_managers" = {
            "/join-state" = {
                "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                "parameters" = {
                    "path" = "//path/to/state/table";
                };
            };
        };
        "parameters" = {
            "wait_for_actions" = "10s";
        };
    };
};
```

Ключевые поля:
- `external_state_managers` — секция с описанием внешних состояний (top-level поле компьютейшена, на одном уровне с `parameters`, не вложенное в него).
- Ключ внутри `external_state_managers` (например, `"/join-state"`) — имя стейта, обязательно начинающееся с `/`. То же имя передаётся в дескриптор `StateDescriptors.external("/join-state")`, через который Computation получает аксессор: `ctx.getState(JOIN_STATE_DESCRIPTOR, message)`.
- `external_state_manager_class_name` — полное имя зарегистрированного класса state manager'а; по умолчанию `"NYT::NFlow::TSimpleExternalStateManager"`.
- `parameters/path` — путь к динамической таблице {{product-name}}, в которой хранится стейт.

## Read-only joiner {#read-only-joiner}

Если компьютейшену нужно **только читать** external state, которым владеет (пишет) другой компьютейшен, используется read-only joiner. Joiner не является владельцем таблицы стейта: он лишь джойнит её строки по ключу сообщения. Это позволяет нескольким компьютейшенам читать одну таблицу, сохраняя единственного писателя.

Дескриптор создаётся через `StateDescriptors.externalReadOnly(...)`, а аксессор — через тот же `ctx.getState(...)`:

{% list tabs group=lang %}

- Java

  ```java
  private static final JoinedExternalStateDescriptor RATING_SETTINGS =
          StateDescriptors.externalReadOnly("/rating-settings");

  ReadOnlyExternalStateAccessor accessor = ctx.getState(RATING_SETTINGS, message);
  Payload settings = accessor.getOrDefault();
  ```

- Kotlin

  ```kotlin
  private val ratingSettings = StateDescriptors.externalReadOnly("/rating-settings")

  val accessor = ctx.getState(ratingSettings, message)
  val settings = accessor.getOrDefault()
  ```

{% endlist %}

`ReadOnlyExternalStateAccessor` поддерживает `get()` и `getOrDefault()`; вызовы `set(...)` и `clear()` бросают `UnsupportedOperationException`, так как joiner не владеет таблицей.

В статической спеке joiner объявляется в top-level секции `external_state_joiners` (на одном уровне с `external_state_managers`); имя должно совпадать с аргументом `externalReadOnly(...)`:

```yson
"external_state_joiners" = {
    "/rating-settings" = {
        "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
        "parameters" = {
            "path" = "//path/to/state/table";
        };
    };
};
```

{% note warning %}

`getOrDefault()` на joiner'е может построить пустой `Payload` только если для запрошенных ключей пришла схема джойнимой таблицы. Если ни одной строки по ключам не найдено и схема недоступна, `getOrDefault()` бросит `IllegalStateException` — в таком случае используйте `get()` и обрабатывайте `Optional.empty()`.

{% endnote %}

## Создание таблицы для стейта

Таблица для External State должна быть создана заранее. Схема ключевых колонок таблицы должна совпадать с `group_by_schema` `Computation`. Пример схемы:

| name | type | sort_order | expression |
|------|------|:---:|------------|
| `hash` | `uint64` | `ascending` | `farm_hash(hit_id)` |
| `hit_id` | `string` | `ascending` | |
| `hit_time` | `uint64` | `ascending` | |
| `hit_payload` | `string` | | |
| `show_time` | `uint64` | | |
| `click_time` | `uint64` | | |

{% if audience == "internal" %}Для создания таблицы рекомендуется использовать [YtSync]({{yt-sync-docs}}/).{% endif %}

## Полный пример

Пример из [wait_click_join]({{source-root}}/yt/yt/flow/examples/java/wait_click_join):

{% list tabs group=lang %}

- Java

  ```java
  public class JoinProcessFunction implements RowFunction {

      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          var streamId = message.getStreamId();

          // Получение ExternalStateAccessor для текущего ключа
          ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", message);

          // Чтение текущего стейта (или создание пустого)
          PayloadBuilder joinState = stateAccessor.getOrDefault().toBuilder();

          if ("hit".equals(streamId)) {
              Hit hit = message.getPayload();
              joinState.set("hit_payload", hit.getHitPayload());
          } else if ("action".equals(streamId)) {
              Action action = message.getPayload();
              if (Boolean.TRUE.equals(action.isClick())) {
                  joinState.set("click_time", action.getActionTime());
              } else {
                  joinState.set("show_time", action.getActionTime());
              }
          }

          // Сохранение обновлённого стейта
          stateAccessor.set(joinState.finish());

          // Установка таймера для закрытия ключа
          long maxTime = hitTime + waitTime;
          output.addTimer(maxTime, hitTime);
      }

      @Override
      public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
          ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", timer);
          Payload joinState = stateAccessor.get().orElseThrow();

          // Генерация выходного сообщения на основе стейта
          if (joinState.get("show_time", Long.class) != null
                  && joinState.get("show_time", Long.class) != 0) {
              JoinedAction result = new JoinedAction();
              // ... заполнение полей из стейта ...
              output.addMessage(new Message("joined_action", result));
          }

          // Очистка стейта после обработки
          stateAccessor.clear();
      }
  }
  ```

- Kotlin

  ```kotlin
  class JoinProcessFunction : RowFunction {

      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val streamId = message.getStreamId()

          // Получение ExternalStateAccessor для текущего ключа
          val stateAccessor = ctx.getExternalStateAccessor("join-state", message)

          // Чтение текущего стейта (или создание пустого)
          val joinState = stateAccessor.getOrDefault().toBuilder()

          if ("hit" == streamId) {
              val hit: Hit = message.getPayload()
              joinState.set("hit_payload", hit.getHitPayload())
          } else if ("action" == streamId) {
              val action: Action = message.getPayload()
              if (Boolean.TRUE == action.isClick()) {
                  joinState.set("click_time", action.getActionTime())
              } else {
                  joinState.set("show_time", action.getActionTime())
              }
          }

          // Сохранение обновлённого стейта
          stateAccessor.set(joinState.finish())

          // Установка таймера для закрытия ключа
          val maxTime = hitTime + waitTime
          output.addTimer(maxTime, hitTime)
      }

      override fun onTimer(timer: Timer, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor = ctx.getExternalStateAccessor("join-state", timer)
          val joinState = stateAccessor.get().orElseThrow()

          // Генерация выходного сообщения на основе стейта
          if (joinState.get("show_time", Long::class.java) != null
                  && joinState.get("show_time", Long::class.java) != 0L) {
              val result = JoinedAction()
              // ... заполнение полей из стейта ...
              output.addMessage(Message("joined_action", result))
          }

          // Очистка стейта после обработки
          stateAccessor.clear()
      }
  }
  ```

{% endlist %}
