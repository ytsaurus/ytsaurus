# Working with states in {{product-name}} Flow (Java)

{% note info %}

This page describes details for Java and Kotlin when working with states. General concepts are described in the [Stateful computations](../../../flow/concepts/stateful.md) section.

{% endnote %}

The Java SDK Flow (Java and Kotlin) provides several types of state accessors for working with state. The most commonly used are:

- **YsonStateAccessor** — for YSON states stored in Flow’s internal tables.
- **ExternalStateAccessor** — for external states stored in separate dynamic tables in {{product-name}}.

You also have access to **ProtoStateAccessor**, **DefaultStateAccessor**, **RawStateAccessor**, and **NoOpStateAccessor**. For more details on all types, see the [Internal State](../../../flow/java/internal-state.md) section.

## YsonStateAccessor {#yson-state}

`YsonStateAccessor<T>` provides typed access to a YSON state tied to a message key. You get the accessor via `RuntimeContext`:

{% list tabs group=lang %}

- Java

  ```java
  StateAccessor<T> stateAccessor = ctx.getYsonStateAccessor("state-name", message, StateClass.class);
  ```

- Kotlin

  ```kotlin
  val stateAccessor: StateAccessor<T> = ctx.getYsonStateAccessor("state-name", message, StateClass::class.java)
  ```

{% endlist %}

Parameters:
- `"state-name"` — the state name; it must match the name registered in the [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) of the [computation](../../../flow/concepts/glossary.md#stream-and-computation) (`parameters/internal_states`).
- `message` — the current message from which you extract the [grouping key](../../../flow/concepts/glossary.md#key).
- `StateClass.class` — the Java class for serializing or deserializing the state.

### StateAccessor methods

| Method | Description |
| --- | --- |
| `get()` | Get the current state value (`Optional<T>`) |
| `set(T value)` | Set a new state value |
| `getOrDefault(T defaultValue)` | Get the value or return the default value |
| `clear()` | Delete the state for the current key |

### Example: WordCountMapper

{% list tabs group=lang %}

- Java

  ```java
  @Component
  public class WordCountMapper implements RowFunction {

      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          Word input = message.getPayload();

          StateAccessor<WordCountState> stateAccessor =
                  ctx.getYsonStateAccessor("word-state", message, WordCountState.class);

          var state = stateAccessor.getOrDefault(new WordCountState(input.getWord(), 0));
          state.setCount(state.getCount() + 1);
          stateAccessor.set(state);
      }
  }
  ```

- Kotlin

  ```kotlin
  @Component
  class WordCountMapper : RowFunction {

      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val input: Word = message.getPayload()

          val stateAccessor = ctx.getYsonStateAccessor("word-state", message, WordCountState::class.java)

          val state = stateAccessor.getOrDefault(WordCountState(input.word, 0))
          state.count = state.count + 1
          stateAccessor.set(state)
      }
  }
  ```

{% endlist %}

In this example:
- You extract a `Word` object with the `word` field from the message.
- You get an accessor for the `"word-state"` state, which is tied to the current message’s key.
- If the state for this key doesn’t exist, you create a new `WordCountState` object with an initial counter value of 0.
- You increase the counter value and update the state.

Annotate the state class with `@YTreeObject` to serialize it to YSON:

{% list tabs group=lang %}

- Java

  ```java
  @YTreeObject
  public class WordCountState {
      private String word;
      private long count;

      public WordCountState() {}

      public WordCountState(String word, long count) {
          this.word = word;
          this.count = count;
      }

      // getters and setters
      public String getWord() { return word; }
      public void setWord(String word) { this.word = word; }
      public long getCount() { return count; }
      public void setCount(long count) { this.count = count; }
  }
  ```

- Kotlin

  ```kotlin
  @YTreeObject
  class WordCountState {
      var word: String = ""
      var count: Long = 0
      constructor()
      constructor(word: String, count: Long) {
          this.word = word
          this.count = count
      }
  }
  ```

{% endlist %}

## ExternalStateAccessor {#external-state}

`ExternalStateAccessor` provides access to an external state stored in a separate dynamic table in {{product-name}}. You describe the external state with an `ExternalStateDescriptor` constant, which you create via `StateDescriptors.external(...)`:

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor externalStateAccessor = ctx.getExternalStateAccessor("state-name", message);
  ```

- Kotlin

  ```kotlin
  val externalStateAccessor = ctx.getExternalStateAccessor("state-name", message)
  ```

{% endlist %}

### ExternalStateAccessor methods

| Method | Description |
| --- | --- |
| `get()` | Get the current state value (`Optional<Payload>`) |
| `getOrDefault()` | Get the value or an empty `Payload` |
| `set(Payload value)` | Set a new state value |
| `clear()` | Delete the state for the current key |

`Payload` is an untyped container with field access by name. Use `PayloadBuilder` to modify it.

### Example: EventReducer

{% list tabs group=lang %}

- Java

  ```java
  public class EventReducer implements RowFunction {

      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          ExternalStateAccessor externalStateAccessor =
                  ctx.getExternalStateAccessor("shuffle-state", message);

          Payload state = externalStateAccessor.getOrDefault();

          PayloadBuilder stateBuilder = state.toBuilder();
          if (state.get("count", Long.class) == null) {
              stateBuilder.set("count", 1L);
          } else {
              stateBuilder.set("count", state.get("count", Long.class) + 1);
          }

          externalStateAccessor.set(stateBuilder.finish());
      }
  }
  ```

- Kotlin

  ```kotlin
  class EventReducer : RowFunction {

      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val externalStateAccessor = ctx.getExternalStateAccessor("shuffle-state", message)

          val state = externalStateAccessor.getOrDefault()

          val stateBuilder = state.toBuilder()
          if (state.get("count", Long::class.java) == null) {
              stateBuilder.set("count", 1L)
          } else {
              stateBuilder.set("count", state.get("count", Long::class.java) + 1)
          }

          externalStateAccessor.set(stateBuilder.finish())
      }
  }
  ```

{% endlist %}

In this example:
- You declare the `SHUFFLE_STATE` descriptor for the external state `"/shuffle-state"`.
- You get the accessor for the message key via `ctx.getState(SHUFFLE_STATE, message)`.
- You extract the current state value as `Payload`.
- You use `PayloadBuilder` to create an updated version of the state with an increased counter.
- You save the updated state back.

## State in timers {#state-in-timers}

When you process timers, the state is available via the `timer` object, which contains the [grouping key](../../../flow/concepts/glossary.md#key):

{% list tabs group=lang %}

- Java

  ```java
  @Override
  public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
      ExternalStateAccessor stateAccessor =
              ctx.getExternalStateAccessor("join-state", timer);

      Payload joinState = stateAccessor.get().orElseThrow();

      // process the state and generate output messages
      var messageBuilder = ctx.createMessageBuilder("output_stream");
      messageBuilder.set("hit_id", joinState.get("hit_id", String.class));
      // ... fill in the remaining fields ...
      output.addMessage(messageBuilder.finish());

      // clear the state after processing
      stateAccessor.clear();
  }
  ```

- Kotlin

  ```kotlin
  override fun onTimer(timer: Timer, output: OutputCollector, ctx: RuntimeContext) {
      val stateAccessor = ctx.getExternalStateAccessor("join-state", timer)

      val joinState = stateAccessor.get().orElseThrow()

      // process the state and generate output messages
      val messageBuilder = ctx.createMessageBuilder("output_stream")
      messageBuilder.set("hit_id", joinState.get("hit_id", String::class.java))
      // ... fill in the remaining fields ...
      output.addMessage(messageBuilder.finish())

      // clear the state after processing
      stateAccessor.clear()
  }
  ```

{% endlist %}

The `clear()` method removes the state for the given key. Make sure to call it after closing the window or finalizing the processing to avoid accumulating outdated data.

## Binding to group_by_schema {#group-by}

The key that you use to access the state is defined by the `group_by_schema` field in the computation spec. The state accessor automatically extracts the key from the provided message or timer.

For more details on `group_by_schema` and its impact on state handling, see the [Stateful computations](../../../flow/concepts/stateful.md) section.

## State configuration in the spec {#spec-config}

### YSON state (internal_states)

You register YSON states in the computation spec via `parameters/internal_states`:

```yson
"computations" = {
    "mapper" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "parameters" = {
            "internal_states" = ["word-state"];
        };
    };
};
```

### External state

You register the external state in the `external_state_managers` section of `Computation`. The name must start with `/` and match the value passed to `StateDescriptors.external(...)`:

```yson
"computations" = {
    "reducer" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "external_state_managers" = {
            "/shuffle-state" = {
                "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                "parameters" = {
                    "path" = "//path/to/state/table";
                };
            };
        };
        "parameters" = {};
    };
};
```

The `external_state_manager_class_name` field specifies the registered external state manager class. For a typical scenario, this is `"NYT::NFlow::TSimpleExternalStateManager"`. For more details on available managers, see the [C++ documentation](../../../flow/cpp/state.md#external-state).

You need to create the state table in advance{% if audience == "internal" %}, for example, using [YtSync]({{yt-sync-docs}}/){% endif %}.

## See also

- [Stateful computations](../../../flow/concepts/stateful.md)
- [Internal State (Java)](../../../flow/java/internal-state.md)
- [External State (Java)](../../../flow/java/external-state.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Getting started (Java)](../../../flow/java/getting-started.md)
