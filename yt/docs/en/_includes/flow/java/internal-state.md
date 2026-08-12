# Internal State in {{product-name}} Flow (Java)

Internal State is a mechanism for working with internal state stored in Flow’s internal tables. Unlike [External State](../../../flow/java/external-state.md), you don’t need to create the tables yourself — Flow manages them automatically.

Learn more about `StateAccessor` and working with state: [State Accessor](../../../flow/java/state-accessor.md).

General information about stateful processing is in the [Stateful processing](../../../flow/concepts/stateful.md) section.

## Overview

The Java SDK Flow (Java and Kotlin) provides several types of state accessors for working with Internal State; they differ by serialization format:

| Accessor | Format | Description |
|----------|--------|-------------|
| [YsonStateAccessor](../../../flow/java/internal-state.md#yson-state-accessor) | YSON | Serialization via `@YTreeObject` annotations |
| [ProtoStateAccessor](../../../flow/java/internal-state.md#proto-state-accessor) | Protobuf | Serialization via Protobuf |
| [DefaultStateAccessor](../../../flow/java/internal-state.md#default-state-accessor) | Custom | User-defined serializer/deserializer |
| [RawStateAccessor](../../../flow/java/internal-state.md#raw-state-accessor) | `byte[]` | No serialization (raw bytes) |
| [NoOpStateAccessor](../../../flow/java/internal-state.md#noop-state-accessor) | — | Stores only the fact that the state exists |

All accessors implement the common `StateAccessor<T>` interface.

## StateAccessor interface {#state-accessor}

{% list tabs group=lang %}

- Java

  ```java
  public interface StateAccessor<T> {
      /** Get the state value. */
      Optional<T> get();

      /** Get the state value or a default value. */
      default T getOrDefault(T defaultValue);

      /** Set the state value. */
      void set(T value);

      /** Clear/delete the state for the key. */
      void clear();

      /** Get the state class. */
      Class<T> getStateClass();
  }
  ```

- Kotlin

  ```kotlin
  interface StateAccessor<T> {
      /** Get the state value. */
      fun get(): Optional<T>

      /** Get the state value or a default value. */
      fun getOrDefault(defaultValue: T): T

      /** Set the state value. */
      fun set(value: T)

      /** Clear/delete the state for the key. */
      fun clear()

      /** Get the state class. */
      fun getStateClass(): Class<T>
  }
  ```

{% endlist %}

## YsonStateAccessor {#yson-state-accessor}

`YsonStateAccessor` uses YSON serialization. The state class must be annotated with `@YTreeObject`.

### Getting the accessor

{% list tabs group=lang %}

- Java

  ```java
  // For a message
  YsonStateAccessor<MyState> stateAccessor =
          ctx.getYsonStateAccessor("state-name", message, MyState.class);

  // For a timer
  YsonStateAccessor<MyState> stateAccessor =
          ctx.getYsonStateAccessor("state-name", timer, MyState.class);
  ```

- Kotlin

  ```kotlin
  // For a message
  val stateAccessor: YsonStateAccessor<MyState> =
          ctx.getYsonStateAccessor("state-name", message, MyState::class.java)

  // For a timer
  val stateAccessor: YsonStateAccessor<MyState> =
          ctx.getYsonStateAccessor("state-name", timer, MyState::class.java)
  ```

{% endlist %}

### Example state class

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

      // A default constructor is required
      public CounterState() {}

      // Getters and setters...
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

      // A default constructor is required
      constructor()
  }
  ```

{% endlist %}

### Example usage

{% list tabs group=lang %}

- Java

  ```java
  public class CounterFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          YsonStateAccessor<CounterState> stateAccessor =
                  ctx.getYsonStateAccessor("counter", message, CounterState.class);

          // Get the current state or create a new one
          CounterState state = stateAccessor.getOrDefault(new CounterState());

          // Modify the state
          state.setCount(state.getCount() + 1);
          state.setLastUpdate(message.getEventTimestamp());

          // Save the state
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

          // Get the current state or create a new one
          val state: CounterState = stateAccessor.getOrDefault(CounterState())

          // Modify the state
          state.count = state.count + 1
          state.lastUpdate = message.getEventTimestamp()

          // Save the state
          stateAccessor.set(state)
      }
  }
  ```

{% endlist %}

## ProtoStateAccessor {#proto-state-accessor}

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/ProtoStateAccessor.java)

`ProtoStateAccessor` uses Protobuf serialization. Your state class must inherit from `com.google.protobuf.MessageLite`.

### Get the accessor

{% list tabs group=lang %}

- Java

  ```java
  // For a message
  ProtoStateAccessor<MyProtoState> stateAccessor =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState.class);

  // For a timer
  ProtoStateAccessor<MyProtoState> stateAccessor =
          ctx.getProtoStateAccessor("state-name", timer, MyProtoState.class);
  ```

- Kotlin

  ```kotlin
  // For a message
  val stateAccessor: ProtoStateAccessor<MyProtoState> =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState::class.java)

  // For a timer
  val stateAccessor: ProtoStateAccessor<MyProtoState> =
          ctx.getProtoStateAccessor("state-name", timer, MyProtoState::class.java)
  ```

{% endlist %}

### The getOrDefault method

`ProtoStateAccessor` provides a parameterless `getOrDefault()` method that returns a default Protobuf object:

{% list tabs group=lang %}

- Java

  ```java
  ProtoStateAccessor<MyProtoState> stateAccessor =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState.class);

  // Get the state or a default Protobuf object
  MyProtoState state = stateAccessor.getOrDefault();
  ```

- Kotlin

  ```kotlin
  val stateAccessor: ProtoStateAccessor<MyProtoState> =
          ctx.getProtoStateAccessor("state-name", message, MyProtoState::class.java)

  // Get the state or a default Protobuf object
  val state: MyProtoState = stateAccessor.getOrDefault()
  ```

{% endlist %}

### Usage example

{% list tabs group=lang %}

- Java

  ```java
  public class ProtoCounterFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          ProtoStateAccessor<CounterProto> stateAccessor =
                  ctx.getProtoStateAccessor("counter", message, CounterProto.class);

          CounterProto state = stateAccessor.getOrDefault();

          // Modify using the Protobuf builder
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

          // Modify using the Protobuf builder
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

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/DefaultStateAccessor.java)

`DefaultStateAccessor` lets you use custom serialization and deserialization functions.

### Get the accessor

{% list tabs group=lang %}

- Java

  ```java
  // For a message
  DefaultStateAccessor<MyState> stateAccessor = ctx.getStateAccessor(
          "state-name",
          message,
          MyState.class,
          state -> serialize(state),      // Function<MyState, byte[]>
          bytes -> deserialize(bytes)     // Function<byte[], MyState>
  );

  // For a timer
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
  // For a message
  val stateAccessor: DefaultStateAccessor<MyState> = ctx.getStateAccessor(
          "state-name",
          message,
          MyState::class.java,
          { state -> serialize(state) },      // (MyState) -> ByteArray
          { bytes -> deserialize(bytes) }     // (ByteArray) -> MyState
  )

  // For a timer
  val stateAccessor: DefaultStateAccessor<MyState> = ctx.getStateAccessor(
          "state-name",
          timer,
          MyState::class.java,
          { state -> serialize(state) },
          { bytes -> deserialize(bytes) }
  )
  ```

{% endlist %}

### Example with Jackson

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

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/RawStateAccessor.java)

Use `RawStateAccessor` to work with raw bytes without serialization or deserialization.

### Get the accessor

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

### Example usage

{% list tabs group=lang %}

- Java

  ```java
  RawStateAccessor stateAccessor = ctx.getRawStateAccessor("raw-state", message);

  Optional<byte[]> maybeBytes = stateAccessor.get();
  if (maybeBytes.isPresent()) {
      byte[] data = maybeBytes.get();
      // Process raw data...
  }

  // Write raw data
  stateAccessor.set(new byte[]{0x01, 0x02, 0x03});

  // Clear the data
  stateAccessor.clear();
  ```

- Kotlin

  ```kotlin
  val stateAccessor: RawStateAccessor = ctx.getRawStateAccessor("raw-state", message)

  val maybeBytes: Optional<ByteArray> = stateAccessor.get()
  if (maybeBytes.isPresent) {
      val data: ByteArray = maybeBytes.get()
      // Process raw data...
  }

  // Write raw data
  stateAccessor.set(byteArrayOf(0x01, 0x02, 0x03))

  // Clear the data
  stateAccessor.clear()
  ```

{% endlist %}

## NoOpStateAccessor {#noop-state-accessor}

[Source code]({{source-root}}/yt/java/flow/flow-core/src/main/java/tech/ytsaurus/flow/context/NoOpStateAccessor.java)

`NoOpStateAccessor` stores only the fact that a state exists for a [key](../../../flow/concepts/glossary.md#key), with no payload. Use it to track already processed keys (for deduplication).

### Get the accessor

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

### Example usage

{% list tabs group=lang %}

- Java

  ```java
  public class DeduplicationFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          NoOpStateAccessor stateAccessor = ctx.getNoOpStateAccessor("seen-keys", message);

          // Check if the key was already processed
          if (stateAccessor.get().isPresent()) {
              // The key is already processed, skip it
              return;
          }

          // Mark the key as processed
          stateAccessor.touch();

          // Process the message...
          output.addMessage(new Message("output", message.getPayload()));
      }
  }
  ```

- Kotlin

  ```kotlin
  class DeduplicationFunction : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor: NoOpStateAccessor = ctx.getNoOpStateAccessor("seen-keys", message)

          // Check if the key was already processed
          if (stateAccessor.get().isPresent) {
              // The key is already processed, skip it
              return
          }

          // Mark the key as processed
          stateAccessor.touch()

          // Process the message...
          output.addMessage(Message("output", message.getPayload()))
      }
  }
  ```

{% endlist %}

## Configuration in the static spec {#static-spec}

Internal State doesn’t require you to create external tables. States are automatically stored in Flow’s internal tables (`states` and `partition_states`).

You must declare the names of internal states in the `internal_states` section of the [computation](../../../flow/concepts/glossary.md#stream-and-computation) parameters in the static spec:

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

The state name in your code (the first argument of `ctx.getYsonStateAccessor(...)`, `ctx.getProtoStateAccessor(...)`, and so on) must match the name declared in `internal_states`.
