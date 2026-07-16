# Internal State в {{product-name}} Flow (Java)

Internal State — механизм работы с внутренним состоянием (стейтом), хранящимся во внутренних таблицах Flow. В отличие от [External State](../../../flow/java/external-state.md), пользователю не нужно самостоятельно создавать таблицы — Flow управляет ими автоматически.

Подробнее про `StateAccessor` и работу со стейтом: [State Accessor](../../../flow/java/state-accessor.md).

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../../flow/concepts/stateful.md).

## Обзор

Java SDK Flow (Java и Kotlin) предоставляет несколько видов аксессоров состояния для работы с Internal State, различающихся форматом сериализации:

| Accessor | Формат | Описание |
|----------|--------|----------|
| [YsonStateAccessor](../../../flow/java/internal-state.md#yson-state-accessor) | YSON | Сериализация через `@YTreeObject` аннотации |
| [ProtoStateAccessor](../../../flow/java/internal-state.md#proto-state-accessor) | Protobuf | Сериализация через Protobuf |
| [DefaultStateAccessor](../../../flow/java/internal-state.md#default-state-accessor) | Произвольный | Пользовательские serializer/deserializer |
| [RawStateAccessor](../../../flow/java/internal-state.md#raw-state-accessor) | `byte[]` | Без сериализации (сырые байты) |
| [NoOpStateAccessor](../../../flow/java/internal-state.md#noop-state-accessor) | - | Хранит только факт наличия стейта |

Все аксессоры реализуют общий интерфейс `StateAccessor<T>`.

## Интерфейс StateAccessor {#state-accessor}

{% list tabs group=lang %}

- Java

  ```java
  public interface StateAccessor<T> {
      /** Получить значение стейта. */
      Optional<T> get();

      /** Получить значение стейта или дефолтное значение. */
      default T getOrDefault(T defaultValue);

      /** Установить значение стейта. */
      void set(T value);

      /** Очистить/удалить стейт для ключа. */
      void clear();

      /** Получить класс стейта. */
      Class<T> getStateClass();
  }
  ```

- Kotlin

  ```kotlin
  interface StateAccessor<T> {
      /** Получить значение стейта. */
      fun get(): Optional<T>

      /** Получить значение стейта или дефолтное значение. */
      fun getOrDefault(defaultValue: T): T

      /** Установить значение стейта. */
      fun set(value: T)

      /** Очистить/удалить стейт для ключа. */
      fun clear()

      /** Получить класс стейта. */
      fun getStateClass(): Class<T>
  }
  ```

{% endlist %}

## YsonStateAccessor {#yson-state-accessor}

`YsonStateAccessor` использует YSON-сериализацию. Класс стейта должен быть аннотирован `@YTreeObject`.

### Получение аксессора

{% list tabs group=lang %}

- Java

  ```java
  // Для сообщения
  YsonStateAccessor<MyState> stateAccessor =
          ctx.getYsonStateAccessor("state-name", message, MyState.class);

  // Для таймера
  YsonStateAccessor<MyState> stateAccessor =
          ctx.getYsonStateAccessor("state-name", timer, MyState.class);
  ```

- Kotlin

  ```kotlin
  // Для сообщения
  val stateAccessor: YsonStateAccessor<MyState> =
          ctx.getYsonStateAccessor("state-name", message, MyState::class.java)

  // Для таймера
  val stateAccessor: YsonStateAccessor<MyState> =
          ctx.getYsonStateAccessor("state-name", timer, MyState::class.java)
  ```

{% endlist %}

### Пример класса стейта

{% list tabs group=lang %}

- Java

  ```java
  import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
  import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeField;

  @YTreeObject
  public class CounterState {
      @YTreeField(key = "count")
      private long count;

      @YTreeField(key = "last_update")
      private long lastUpdate;

      // Конструктор по умолчанию обязателен
      public CounterState() {}

      // Геттеры и сеттеры...
      public long getCount() { return count; }
      public void setCount(long count) { this.count = count; }
      public long getLastUpdate() { return lastUpdate; }
      public void setLastUpdate(long lastUpdate) { this.lastUpdate = lastUpdate; }
  }
  ```

- Kotlin

  ```kotlin
  import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject
  import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeField

  @YTreeObject
  class CounterState {
      @YTreeField(key = "count")
      var count: Long = 0

      @YTreeField(key = "last_update")
      var lastUpdate: Long = 0

      // Конструктор по умолчанию обязателен
      constructor()
  }
  ```

{% endlist %}

### Пример использования

{% list tabs group=lang %}

- Java

  ```java
  public class CounterFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          YsonStateAccessor<CounterState> stateAccessor =
                  ctx.getYsonStateAccessor("counter", message, CounterState.class);

          // Получение текущего стейта или создание нового
          CounterState state = stateAccessor.getOrDefault(new CounterState());

          // Модификация стейта
          state.setCount(state.getCount() + 1);
          state.setLastUpdate(message.getEventTimestamp());

          // Сохранение стейта
          stateAccessor.set(state);
      }
  }
  ```

- Kotlin

  ```kotlin
  class CounterFunction : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor: YsonStateAccessor<CounterState> =
                  ctx.getYsonStateAccessor("counter", message, CounterState::class.java)

          // Получение текущего стейта или создание нового
          val state: CounterState = stateAccessor.getOrDefault(CounterState())

          // Модификация стейта
          state.count = state.count + 1
          state.lastUpdate = message.getEventTimestamp()

          // Сохранение стейта
          stateAccessor.set(state)
      }
  }
  ```

{% endlist %}

## ProtoStateAccessor {#proto-state-accessor}

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/ProtoStateAccessor.java)

`ProtoStateAccessor` использует Protobuf-сериализацию. Класс стейта должен наследовать `com.google.protobuf.MessageLite`.

### Получение аксессора

{% list tabs group=lang %}

- Java

  ```java
  // Для сообщения
  ProtoStateAccessor<MyProtoState> stateAccessor =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState.class);

  // Для таймера
  ProtoStateAccessor<MyProtoState> stateAccessor =
          ctx.getProtoStateAccessor("state-name", timer, MyProtoState.class);
  ```

- Kotlin

  ```kotlin
  // Для сообщения
  val stateAccessor: ProtoStateAccessor<MyProtoState> =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState::class.java)

  // Для таймера
  val stateAccessor: ProtoStateAccessor<MyProtoState> =
          ctx.getProtoStateAccessor("state-name", timer, MyProtoState::class.java)
  ```

{% endlist %}

### Метод getOrDefault

`ProtoStateAccessor` предоставляет метод `getOrDefault()` без параметров, который возвращает дефолтный Protobuf-объект:

{% list tabs group=lang %}

- Java

  ```java
  ProtoStateAccessor<MyProtoState> stateAccessor =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState.class);

  // Получение стейта или дефолтного Protobuf-объекта
  MyProtoState state = stateAccessor.getOrDefault();
  ```

- Kotlin

  ```kotlin
  val stateAccessor: ProtoStateAccessor<MyProtoState> =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState::class.java)

  // Получение стейта или дефолтного Protobuf-объекта
  val state: MyProtoState = stateAccessor.getOrDefault()
  ```

{% endlist %}

### Пример использования

{% list tabs group=lang %}

- Java

  ```java
  public class ProtoCounterFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          ProtoStateAccessor<CounterProto> stateAccessor =
                  ctx.getProtoStateAccessor("counter", message, CounterProto.class);

          CounterProto state = stateAccessor.getOrDefault();

          // Модификация через Protobuf builder
          CounterProto updatedState = state.toBuilder()
                  .setCount(state.getCount() + 1)
                  .setLastUpdate(message.getEventTimestamp())
                  .build();

          stateAccessor.set(updatedState);
      }
  }
  ```

- Kotlin

  ```kotlin
  class ProtoCounterFunction : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor: ProtoStateAccessor<CounterProto> =
                  ctx.getProtoStateAccessor("counter", message, CounterProto::class.java)

          val state: CounterProto = stateAccessor.getOrDefault()

          // Модификация через Protobuf builder
          val updatedState: CounterProto = state.toBuilder()
                  .setCount(state.getCount() + 1)
                  .setLastUpdate(message.getEventTimestamp())
                  .build()

          stateAccessor.set(updatedState)
      }
  }
  ```

{% endlist %}

## DefaultStateAccessor {#default-state-accessor}

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/DefaultStateAccessor.java)

`DefaultStateAccessor` позволяет использовать произвольные функции сериализации и десериализации.

### Получение аксессора

{% list tabs group=lang %}

- Java

  ```java
  // Для сообщения
  DefaultStateAccessor<MyState> stateAccessor = ctx.getStateAccessor(
          "state-name",
          message,
          MyState.class,
          state -> serialize(state),      // Function<MyState, byte[]>
          bytes -> deserialize(bytes)     // Function<byte[], MyState>
  );

  // Для таймера
  DefaultStateAccessor<MyState> stateAccessor = ctx.getStateAccessor(
          "state-name",
          timer,
          MyState.class,
          state -> serialize(state),
          bytes -> deserialize(bytes)
  );
  ```

- Kotlin

  ```kotlin
  // Для сообщения
  val stateAccessor: DefaultStateAccessor<MyState> = ctx.getStateAccessor(
          "state-name",
          message,
          MyState::class.java,
          { state -> serialize(state) },      // (MyState) -> ByteArray
          { bytes -> deserialize(bytes) }     // (ByteArray) -> MyState
  )

  // Для таймера
  val stateAccessor: DefaultStateAccessor<MyState> = ctx.getStateAccessor(
          "state-name",
          timer,
          MyState::class.java,
          { state -> serialize(state) },
          { bytes -> deserialize(bytes) }
  )
  ```

{% endlist %}

### Пример с Jackson

{% list tabs group=lang %}

- Java

  ```java
  public class JsonCounterFunction implements RowFunction {
      private static final ObjectMapper mapper = new ObjectMapper();

      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          DefaultStateAccessor<CounterState> stateAccessor = ctx.getStateAccessor(
                  "counter",
                  message,
                  CounterState.class,
                  state -> {
                      try { return mapper.writeValueAsBytes(state); }
                      catch (Exception e) { throw new RuntimeException(e); }
                  },
                  bytes -> {
                      try { return mapper.readValue(bytes, CounterState.class); }
                      catch (Exception e) { throw new RuntimeException(e); }
                  }
          );

          CounterState state = stateAccessor.getOrDefault(new CounterState());
          state.setCount(state.getCount() + 1);
          stateAccessor.set(state);
      }
  }
  ```

- Kotlin

  ```kotlin
  class JsonCounterFunction : RowFunction {
      companion object {
          private val mapper = ObjectMapper()
      }

      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor: DefaultStateAccessor<CounterState> = ctx.getStateAccessor(
                  "counter",
                  message,
                  CounterState::class.java,
                  { state ->
                      try { mapper.writeValueAsBytes(state) }
                      catch (e: Exception) { throw RuntimeException(e) }
                  },
                  { bytes ->
                      try { mapper.readValue(bytes, CounterState::class.java) }
                      catch (e: Exception) { throw RuntimeException(e) }
                  }
          )

          val state: CounterState = stateAccessor.getOrDefault(CounterState())
          state.count = state.count + 1
          stateAccessor.set(state)
      }
  }
  ```

{% endlist %}

## RawStateAccessor {#raw-state-accessor}

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/RawStateAccessor.java)

`RawStateAccessor` работает с сырыми байтами без сериализации/десериализации.

### Получение аксессора

{% list tabs group=lang %}

- Java

  ```java
  RawStateAccessor stateAccessor = ctx.getRawStateAccessor("state-name", message);
  ```

- Kotlin

  ```kotlin
  val stateAccessor: RawStateAccessor = ctx.getRawStateAccessor("state-name", message)
  ```

{% endlist %}

### Пример использования

{% list tabs group=lang %}

- Java

  ```java
  RawStateAccessor stateAccessor = ctx.getRawStateAccessor("raw-state", message);

  Optional<byte[]> maybeBytes = stateAccessor.get();
  if (maybeBytes.isPresent()) {
      byte[] data = maybeBytes.get();
      // Обработка сырых данных...
  }

  // Запись сырых данных
  stateAccessor.set(new byte[]{0x01, 0x02, 0x03});

  // Очистка
  stateAccessor.clear();
  ```

- Kotlin

  ```kotlin
  val stateAccessor: RawStateAccessor = ctx.getRawStateAccessor("raw-state", message)

  val maybeBytes: Optional<ByteArray> = stateAccessor.get()
  if (maybeBytes.isPresent) {
      val data: ByteArray = maybeBytes.get()
      // Обработка сырых данных...
  }

  // Запись сырых данных
  stateAccessor.set(byteArrayOf(0x01, 0x02, 0x03))

  // Очистка
  stateAccessor.clear()
  ```

{% endlist %}

## NoOpStateAccessor {#noop-state-accessor}

[Исходный код]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/NoOpStateAccessor.java)

`NoOpStateAccessor` хранит только факт наличия стейта для [ключа](../../../flow/concepts/glossary.md#key), без какого-либо payload. Полезен для отслеживания уже обработанных ключей (дедупликация).

### Получение аксессора

{% list tabs group=lang %}

- Java

  ```java
  NoOpStateAccessor stateAccessor = ctx.getNoOpStateAccessor("seen-keys", message);
  ```

- Kotlin

  ```kotlin
  val stateAccessor: NoOpStateAccessor = ctx.getNoOpStateAccessor("seen-keys", message)
  ```

{% endlist %}

### Пример использования

{% list tabs group=lang %}

- Java

  ```java
  public class DeduplicationFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          NoOpStateAccessor stateAccessor = ctx.getNoOpStateAccessor("seen-keys", message);

          // Проверяем, был ли ключ уже обработан
          if (stateAccessor.get().isPresent()) {
              // Ключ уже обработан, пропускаем
              return;
          }

          // Отмечаем ключ как обработанный
          stateAccessor.touch();

          // Обработка сообщения...
          output.addMessage(new Message("output", message.getPayload()));
      }
  }
  ```

- Kotlin

  ```kotlin
  class DeduplicationFunction : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor: NoOpStateAccessor = ctx.getNoOpStateAccessor("seen-keys", message)

          // Проверяем, был ли ключ уже обработан
          if (stateAccessor.get().isPresent) {
              // Ключ уже обработан, пропускаем
              return
          }

          // Отмечаем ключ как обработанный
          stateAccessor.touch()

          // Обработка сообщения...
          output.addMessage(Message("output", message.getPayload()))
      }
  }
  ```

{% endlist %}

## Конфигурация в статической спеке {#static-spec}

Internal State не требует создания внешних таблиц. Стейты автоматически хранятся во внутренних таблицах Flow (`states` и `partition_states`).

Имена внутренних стейтов должны быть объявлены в секции `internal_states` параметров [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) в статической спеке:

```yson
"computations" = {
    "counter" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "group_by_schema" = [
            {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"};
            {"name" = "key"; "type" = "string"};
        ];
        "input_stream_ids" = ["input"];
        "output_stream_ids" = ["output"];
        "parameters" = {
            "internal_states" = ["counter"];
        };
    };
};
```

Имя стейта в коде (первый аргумент `ctx.getYsonStateAccessor(...)`, `ctx.getProtoStateAccessor(...)` и т.д.) должно совпадать с именем, объявленным в `internal_states`.
