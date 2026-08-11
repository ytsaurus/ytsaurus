# External State in {{product-name}} Flow (Python)

Use External State to work with external state stored in an external dynamic table in {{product-name}}. You create the table to store the state yourself, on the same cluster where the pipeline is deployed.

For general information about stateful processing, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

## Overview

In the Python SDK, External State is represented by the `ExternalStateAccessor` class. This class is a subclass of `Payload` and gives you dict-like access to the columns of the external dynamic table. The state is bound to the message key (`group_by_schema`). For more details about `StateAccessor` and working with state, see [StateAccessor](../../../flow/python/state-accessor.md).

{% note info %}

If you need only read-only access to the external state (a key-based join with TTL caching, without modification), the framework provides a separate mechanism: **External State Joiner**. Currently, the joiner is available only in C++ ([External State Joiner](../../../flow/cpp/state.md#external-state-joiner)) and is declared in the computation spec, in the top-level `external_state_joiners` section (at the same level as `external_state_managers`).

{% endnote %}

## Difference from Internal State

| Characteristic | External State | Internal State |
|---|---|---|
| Storage | External dynamic table | Flow internal tables |
| Table creation | You create it yourself | Automatic |
| Data format | Typed `Payload` (table row) | Arbitrary (YSON, Protobuf, raw bytes) |
| Access from other systems | Yes (sorted dynamic table) | No |
| Schema | Defined by the table schema | Defined by you |

For more details about Internal State, see [Internal State](../../../flow/python/internal-state.md).

## Getting ExternalStateAccessor {#getting-accessor}

You get `ExternalStateAccessor` through `RuntimeContext` (`ctx`):

```python
# For a message
state = ctx.external_state("/state-name", message)

# For a timer
state = ctx.external_state("/state-name", timer)
```

Parameters:
- `"/state-name"` — a string with the state name from the `external_state_managers` section of the [static spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). The name must start with `/` and match the key in the spec.
- `message` / `timer` — the message or timer for which you need to get the state for the [key](../../../flow/concepts/glossary.md#key).

{% note warning %}

The external state name is validated: it must start with `/`, must not be empty, must not end with `/`, and must not contain two consecutive `/` characters. Calling `ctx.external_state("state-name", message)` (without the leading `/`) will raise a `ValueError`.

{% endnote %}

## ExternalStateAccessor as Payload {#accessor-as-payload}

[Source code]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`ExternalStateAccessor` inherits from the `Payload` class, which lets you read column values directly:

```python
state = ctx.external_state("/shuffle-state", message)

# Dict-like access
value = state["count"]          # Raises KeyError if the column doesn't exist
value = state.get("count")      # Returns None if the column doesn't exist
exists = "count" in state       # Checks existence
```

The `Payload` class also provides:
- `keys()` — a list of column names with non-empty values.
- `to_dict()` — converts to a standard Python dict.

## Main operations {#operations}

### Reading state

```python
state = ctx.external_state("/join-state", message)

# Read a column value
hit_payload = state.get("hit_payload", str)
show_time = state.get("show_time")

# Dict-like access
try:
    value = state["hit_payload"]
except KeyError:
    value = None
```

### Writing state with PayloadBuilder

To modify the state, use the `to_builder()` / `set()` / `finish()` pattern:

```python
state = ctx.external_state("/join-state", message)

# Get a builder with current values
builder = state.to_builder()

# Update the required fields
builder.set("hit_payload", "some_value")
builder.set("show_time", 1234567890)

# Save the updated state
state.set(builder.finish())
```

The `to_builder()` method returns a `PayloadBuilder` pre-filled with the current state values. The `builder.set(column, value)` method returns the builder itself (it supports chaining calls). The `builder.finish()` method creates a new `Payload` and resets the builder.

### Clearing state

```python
state = ctx.external_state("/join-state", timer)

# Delete the row from the table
state.clear()
```

{% note info %}

An empty state corresponds to the absence of a row in the table. If the row doesn't exist, `state.get("column")` returns `None`. Calling `clear()` removes the row from the table.

{% endnote %}

## Configuration in the static spec {#static-spec}

To use External State, you must declare an external state manager in the `external_state_managers` section of the [computation](../../../flow/concepts/glossary.md#stream-and-computation) in the static spec:

```yson
"computations" = {
    "reducer" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "group_by_schema" = [
            {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"};
            {"name" = "key"; "type" = "string"};
        ];
        "input_stream_ids" = ["input"];
        "output_stream_ids" = ["output"];
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

Key fields:
- `external_state_managers` — a top-level section inside `Computation` that describes external state managers (previously `parameters/external_states`).
- The key inside `external_state_managers` (for example, `"/shuffle-state"`) — the state name you use in Python code when calling `ctx.external_state("/shuffle-state", message)`. The name must start with `/`.
- `external_state_manager_class_name` — the name of the registered external state manager class. For a typical scenario, use `"NYT::NFlow::TSimpleExternalStateManager"`;{% if audience == "internal" %} for BigRT profiles, use `"NYT::NFlow::NBigRTExtensions::TProfileManager<TUserProfile>"`.{% endif %} For more details, see the [C++ documentation](../../../flow/cpp/state.md#external-state).
- `parameters.path` — the path to the {{product-name}} dynamic table where the state is stored.

## Creating a table for the state

You must create the table for External State in advance. The schema of the table's key columns must match the computation's `group_by_schema`.

{% if audience == "internal" %}To create the table, we recommend using [YtSync]({{yt-sync-docs}}/).{% endif %}

## Complete example — EventReducer from Shuffle {#example}

Example from [shuffle]({{source-root}}/yt/yt/flow/examples/python/shuffle):

```python
from yt.yt.flow.library.python.companion.computation import RowFunction


class EventReducer(RowFunction):
    """Counts the number of events for each key using external state."""

    def on_message(self, message, output, ctx):
        state = ctx.external_state("/shuffle-state", message)
        builder = state.to_builder()
        builder.set("count", (state.get("count") or 0) + 1)
        state.set(builder.finish())
```

[Full source code]({{source-root}}/yt/yt/flow/examples/python/shuffle/event_reducer.py)

Work pattern:
1. Get the current state with `ctx.external_state(...)`.
2. Create a builder with `state.to_builder()`.
3. Update the required fields with `builder.set(...)`.
4. Save with `state.set(builder.finish())`.

## See also

- [StateAccessor (Python)](../../../flow/python/state-accessor.md)
- [Internal State (Python)](../../../flow/python/internal-state.md)
- [Working with states (Python)](../../../flow/python/state.md) — a brief overview
- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [External State (Java)](../../../flow/java/external-state.md)
- [Shuffle example](../../../flow/python/examples/shuffle.md)