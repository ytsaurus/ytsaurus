# External State in {{product-name}} Flow (Java)

External State is a mechanism for working with external state stored in an external dynamic table in {{product-name}}. You create the table to store the state yourself{% if audience == "internal" %} (for example, using [YtSync]({{yt-sync-docs}})){% endif %} on the same cluster where the pipeline is deployed.

For general information about stateful processing, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

## Overview

In the Java SDK Flow (Java and Kotlin), External State is represented by the `ExternalStateAccessor` class, which provides typed access to the rows of an external dynamic table. The state is bound to the message key (`group_by_schema`). For more details about `StateAccessor` and working with state, see [State Accessor](../../../flow/java/state-accessor.md).

{% note info %}

If you need **read-only** access to the external state (a key-based join with TTL caching, without modification), the framework provides a separate mechanism: **External State Joiner**. It’s available in both Java/Kotlin (see [Read-only joiner](#read-only-joiner)) and C++ ([External State Joiner](../../../flow/cpp/state.md#external-state-joiner)) and is declared in the computation spec in the top-level `external_state_joiners` section (at the same level as `external_state_managers`).

{% endnote %}

## Difference from Internal State

| Characteristic | External State | Internal State |
|---|---|---|
| Storage | External dynamic table | Flow’s internal tables |
| Table creation | You create it yourself | Automatic |
| Data format | Typed `Payload` (table row) | Arbitrary (YSON, Protobuf, raw bytes) |
| Access from other systems | Yes (sorted dynamic table) | No |
| Schema | Defined by the table schema | Defined by you |

For more details about Internal State, see [Internal State](../../../flow/java/internal-state.md).

## Getting ExternalStateAccessor

External state is described by the `ExternalStateDescriptor` constant, which you create via `StateDescriptors.external(...)`. You usually declare the descriptor once per `RowFunction` class:

```java
private static final ExternalStateDescriptor JOIN_STATE =
        StateDescriptors.external("/join-state");
```

You get the `ExternalStateAccessor` for a specific [key](../../../flow/concepts/glossary.md#key) via `RuntimeContext` (which inherits `StatefulContext`):

{% list tabs group=lang %}

- Java

  ```java
  // For a message
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("state-name", message);

  // For a timer
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("state-name", timer);
  ```

- Kotlin

  ```kotlin
  // For a message
  val stateAccessor = ctx.getExternalStateAccessor("state-name", message)

  // For a timer
  val stateAccessor = ctx.getExternalStateAccessor("state-name", timer)
  ```

{% endlist %}

Parameters:
- Descriptor — `ExternalStateDescriptor`, whose name matches the key in the `external_state_managers` static spec and must start with `/`.
- `message` / `timer` — the message or timer for whose [key](../../../flow/concepts/glossary.md#key) you need to get the state.

{% note warning %}

The external state name is validated: it must start with `/`, must not be empty, must not end with `/`, and must not contain two consecutive `/` characters. Calling `StateDescriptors.external("join-state")` (without the leading `/`) will throw an `IllegalArgumentException`. The same name must appear in the [spec](#static-spec).

{% endnote %}

## Main operations

### Reading the state

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", message);

  // Get Optional<Payload>
  Optional<Payload> maybeState = stateAccessor.get();
  if (maybeState.isPresent()) {
      Payload state = maybeState.get();
      String value = state.get("field_name", String.class);
  }

  // Get the state with a default value (an empty Payload with the state schema)
  Payload state = stateAccessor.getOrDefault();
  ```

- Kotlin

  ```kotlin
  val stateAccessor = ctx.getExternalStateAccessor("join-state", message)

  // Get Optional<Payload>
  val maybeState = stateAccessor.get()
  if (maybeState.isPresent) {
      val state = maybeState.get()
      val value = state.get("field_name", String::class.java)
  }

  // Get the state with a default value (an empty Payload with the state schema)
  val state = stateAccessor.getOrDefault()
  ```

{% endlist %}

### Writing the state

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", message);

  // Get the current state and modify it using PayloadBuilder
  PayloadBuilder builder = stateAccessor.getOrDefault().toBuilder();
  builder.set("hit_payload", "some_value");
  builder.set("show_time", 1234567890L);
  stateAccessor.set(builder.finish());
  ```

- Kotlin

  ```kotlin
  val stateAccessor = ctx.getExternalStateAccessor("join-state", message)

  // Get the current state and modify it using PayloadBuilder
  val builder = stateAccessor.getOrDefault().toBuilder()
  builder.set("hit_payload", "some_value")
  builder.set("show_time", 1234567890L)
  stateAccessor.set(builder.finish())
  ```

{% endlist %}

### Clearing the state

{% list tabs group=lang %}

- Java

  ```java
  ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", timer);

  // Clear the state (delete the row from the table)
  stateAccessor.clear();
  ```

- Kotlin

  ```kotlin
  val stateAccessor = ctx.getExternalStateAccessor("join-state", timer)

  // Clear the state (delete the row from the table)
  stateAccessor.clear()
  ```

{% endlist %}

{% note info %}

An empty state corresponds to the absence of a row in the table. If there’s no row, `get()` returns `Optional.empty()`. Calling `clear()` deletes the row from the table.

{% endnote %}

## Configuration in the static spec {#static-spec}

To use External State, you must declare an external state manager in the `external_state_managers` section of the `Computation` in the static spec:

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

Key fields:
- `external_state_managers` — the section describing external states (a top-level field of the computation, at the same level as `parameters`, not nested in it).
- The key inside `external_state_managers` (for example, `"/join-state"`) is the state name, which must start with `/`. You pass the same name to the descriptor `StateDescriptors.external("/join-state")`, through which the Computation gets the accessor: `ctx.getState(JOIN_STATE_DESCRIPTOR, message)`.
- `external_state_manager_class_name` — the fully qualified name of the registered state manager class; the default is `"NYT::NFlow::TSimpleExternalStateManager"`.
- `parameters/path` — the path to the {{product-name}} dynamic table where the state is stored.

## Read-only joiner {#read-only-joiner}

Use a read-only joiner when a computation needs only to read an external state that another computation owns (writes). The joiner isn’t the owner of the state table: it just joins its rows by the message key. This lets multiple computations read one table while keeping a single writer.

You create the descriptor with `StateDescriptors.externalReadOnly(...)`, and the accessor with the same `ctx.getState(...)`:

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

`ReadOnlyExternalStateAccessor` supports `get()` and `getOrDefault()`. Calls to `set(...)` and `clear()` throw `UnsupportedOperationException` because the joiner doesn’t own the table.

In the static spec, declare the joiner in the top-level `external_state_joiners` section (at the same level as `external_state_managers`). The name must match the argument to `externalReadOnly(...)`:

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

`getOrDefault()` on a joiner can build an empty `Payload` only if the schema for the requested keys arrives from the joined table. If no rows are found for the keys and the schema isn’t available, `getOrDefault()` throws `IllegalStateException`. In that case, use `get()` and handle `Optional.empty()`.

{% endnote %}

## Creating a table for the state

You must create the table for External State in advance. The schema of the table’s key columns must match the `group_by_schema` of the `Computation`. Here’s an example schema:

| name | type | sort_order | expression |
|------|------|:---:|------------|
| `hash` | `uint64` | `ascending` | `farm_hash(hit_id)` |
| `hit_id` | `string` | `ascending` | |
| `hit_time` | `uint64` | `ascending` | |
| `hit_payload` | `string` | | |
| `show_time` | `uint64` | | |
| `click_time` | `uint64` | | |

{% if audience == "internal" %}To create the table, we recommend using [YtSync]({{yt-sync-docs}}/).{% endif %}

## Complete example

Example from [wait_click_join]({{source-root}}/yt/yt/flow/examples/java/wait_click_join):

{% list tabs group=lang %}

- Java

  ```java
  public class JoinProcessFunction implements RowFunction {

      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          var streamId = message.getStreamId();

          // Get the ExternalStateAccessor for the current key
          ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", message);

          // Read the current state (or create an empty one)
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

          // Save the updated state
          stateAccessor.set(joinState.finish());

          // Set the timer that closes the key
          long maxTime = hitTime + waitTime;
          output.addTimer(maxTime, hitTime);
      }

      @Override
      public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
          ExternalStateAccessor stateAccessor = ctx.getExternalStateAccessor("join-state", timer);
          Payload joinState = stateAccessor.get().orElseThrow();

          // Build the output message from the state
          if (joinState.get("show_time", Long.class) != null
                  && joinState.get("show_time", Long.class) != 0) {
              JoinedAction result = new JoinedAction();
              // ... fill in the fields from the state ...
              output.addMessage(new Message("joined_action", result));
          }

          // Clear the state after processing
          stateAccessor.clear();
      }
  }
  ```

- Kotlin

  ```kotlin
  class JoinProcessFunction : RowFunction {

      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val streamId = message.getStreamId()

          // Get the ExternalStateAccessor for the current key
          val stateAccessor = ctx.getExternalStateAccessor("join-state", message)

          // Read the current state (or create an empty one)
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

          // Save the updated state
          stateAccessor.set(joinState.finish())

          // Set the timer that closes the key
          val maxTime = hitTime + waitTime
          output.addTimer(maxTime, hitTime)
      }

      override fun onTimer(timer: Timer, output: OutputCollector, ctx: RuntimeContext) {
          val stateAccessor = ctx.getExternalStateAccessor("join-state", timer)
          val joinState = stateAccessor.get().orElseThrow()

          // Build the output message from the state
          if (joinState.get("show_time", Long::class.java) != null
                  && joinState.get("show_time", Long::class.java) != 0L) {
              val result = JoinedAction()
              // ... fill in the fields from the state ...
              output.addMessage(Message("joined_action", result))
          }

          // Clear the state after processing
          stateAccessor.clear()
      }
  }
  ```

{% endlist %}
