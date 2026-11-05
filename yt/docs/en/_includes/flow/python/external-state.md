# External State in {{product-name}} Flow (Python)

Use External State to work with external state stored in an external dynamic table in {{product-name}}. You create the table to store the state yourself, on the same cluster where the pipeline is deployed.

Use `ctx.external_state(...)` and `ctx.joined_external_state(...)` for row-format state. For a manager or joiner that exposes protobuf state, use the [protobuf accessors](#proto-external-state).

For general information about stateful processing, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

## Overview {#overview}

In the Python SDK, row-format External State is represented by the `ExternalStateAccessor` class. This class is a subclass of `Payload` and gives you dict-like access to the columns of the external dynamic table. The state is bound to the message key (`group_by_schema`). For more details about `StateAccessor` and working with state, see [StateAccessor](../../../flow/python/state-accessor.md).

If you need read-only access by key, use an **External State Joiner**. Declare it in the computation's top-level `external_state_joiners` section, alongside `external_state_managers`, and read it through `ctx.joined_external_state(...)` as described in [Read-only joins](#external-state-joiner).

## Difference from Internal State {#difference-from-internal-state}

#|
|| **Characteristic** | **Row-format External State** | **Internal State** ||
|| Storage | External dynamic table | Flow internal tables ||
|| Table creation | You create it yourself | Automatic ||
|| Data format | Typed `Payload` (table row) | Arbitrary (YSON, Protobuf, raw bytes) ||
|| Access from other systems | Yes (sorted dynamic table) | No ||
|| Schema | Defined by the table schema | Defined by you ||
|#

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

### Reading state {#reading-state}

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

### Writing state with PayloadBuilder {#writing-state}

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

### Clearing state {#clearing-state}

```python
state = ctx.external_state("/join-state", timer)

# Delete the row from the table
state.clear()
```

{% note info %}

An empty state corresponds to the absence of a row in the table. If the row doesn't exist, `state.get("column")` returns `None`. Calling `clear()` removes the row from the table.

{% endnote %}

## Protobuf external state {#proto-external-state}

`ctx.proto_external_state(name, message_or_timer, proto_class)` returns a `ProtoExternalStateAccessor`. `ctx.joined_proto_external_state(name, message_or_timer, proto_class)` returns the same accessor in read-only mode. Pass a protobuf message class as `proto_class`, and a message or timer with a key. The name follows the [external-state naming rules](#getting-accessor) and must identify an entry in `external_state_managers` or, for joined state, `external_state_joiners`.

The configured manager or joiner must expose protobuf state: its companion descriptor must declare format `proto` and a `proto_type` equal to `proto_class.DESCRIPTOR.full_name`. Choosing an accessor does not change the manager's format. `TSimpleExternalStateManager` and `TSimpleExternalStateJoiner` expose row-format state and cannot serve these protobuf accessors. The concrete profile-state implementation is an internal extension; the row configuration below does not enable protobuf external state.

The accessor provides these operations:

#|
|| **Method** | **Result** ||
|| `get()` | An independent protobuf message snapshot, or `None` for missing or reset state. ||
|| `get_or_default()` | A snapshot of the value, or a fresh empty instance of `proto_class`. Takes no default argument and does not create state. ||
|| `set(value)` | Serialize and save a replacement snapshot. Fields absent from `value` are cleared. Later edits to `value` do not change the saved snapshot. ||
|| `clear()` | Reset the state. A subsequent `get()` returns `None`. ||
|#

Changes to a returned message take effect only after `set()`. This differs from [internal state with live values](../../../flow/python/internal-state.md#in-place). Storing an empty protobuf message preserves a present value; `clear()` resets it. The last explicit `set()` or `clear()` wins for that key. Joined accessors allow reads only: `set()` and `clear()` raise `ReadOnlyExternalStateError`, and modifying a returned snapshot has no effect on state.

An unknown state name, missing key, wrong format, or mismatched protobuf type raises `ValueError`. Passing a message instance instead of a class, or passing the wrong message type to `set()`, raises `TypeError`. Malformed or uninitialized stored messages and serialization failures raise `ValueError`. A failed serialization leaves the previous state unchanged.

See the [accessor implementation]({{source-root}}/yt/yt/flow/library/python/companion/context.py) and [protobuf external-state tests]({{source-root}}/yt/yt/flow/library/python/companion/test/test_proto_external_state.py).

{% if audience == "internal" %}

### Profile-state setup {#profile-state-setup}

Configure `NYT::NFlow::NProfileState::TProfileStateManager` in `external_state_managers`, or `NYT::NFlow::NProfileState::TProfileStateJoiner` in `external_state_joiners`. The worker binary must link the profile-state manager library. Create and mount a table with a compatible annotated-protobuf layout before launch; the layout must carry a valid protobuf binding. A joined profile with a key-schema override cannot serve a companion computation.

Whole-message replacements must satisfy that layout: key fields present in the message must match the state key, and unbound top-level fields are rejected. Coordinate schema changes across readers and writers because a replacement clears absent fields.

Declare the name-to-message-class mapping through `proto_external_states` or `joined_proto_external_states` when calling `Pipeline.add(...)` or `Pipeline.computation(...)`. When `Pipeline.run()` launches the pipeline from the host, the runner supplies `parameters.descriptor_layout.message_name` and a self-contained `descriptor_set` if a descriptor source is missing. An explicit `layout` or `descriptor_set` takes precedence. A different explicit `message_name` is left for the worker's descriptor pool to resolve. These declarations do not create a manager, joiner, or table; the matching entry must already exist in the spec.

Use the real [Python launcher]({{source-root}}/yt/yt/flow/yandex/extensions/profile_state/examples/python/wait_click_join_profile/__main__.py) with its [pipeline spec]({{source-root}}/yt/yt/flow/yandex/extensions/profile_state/examples/python/wait_click_join_profile/pipeline.yson). For annotations, supported field types, table schema, codec configuration, and migration restrictions, see the [profile-state setup guide]({{source-root}}/yt/yt/flow/yandex/extensions/profile_state/README.md). Descriptor-source selection is covered by the [runner tests]({{source-root}}/yt/yt/flow/library/python/runner/tests/test_profile_state.py).

{% endif %}

## Row-format configuration in the static spec {#static-spec}

To use row-format External State, declare an external state manager in the `external_state_managers` section of the [computation](../../../flow/concepts/glossary.md#stream-and-computation) in the static spec:

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
- `external_state_manager_class_name` — the name of the registered external state manager class. For row-format state, use `"NYT::NFlow::TSimpleExternalStateManager"`.{% if audience == "internal" %} For protobuf profiles, see [Profile-state setup](#profile-state-setup).{% endif %} For more details, see the [C++ documentation](../../../flow/cpp/state.md#external-state).
- `parameters.path` — the path to the {{product-name}} dynamic table where the state is stored.

## Read-only joins {#external-state-joiner}

An external state joiner loads the row whose key matches the current message or timer. Declare `NYT::NFlow::TSimpleExternalStateJoiner` under `external_state_joiners`; the state name must start with `/` and must match the name passed to `ctx.joined_external_state`. See the runnable [External State Join pipeline spec]({{source-root}}/yt/yt/flow/examples/python/external_state_join/pipeline.yson) and the [Static Table Join pipeline spec]({{source-root}}/yt/yt/flow/examples/python/static_table_join/pipeline.yson).

The accessor has the same read methods as `ExternalStateAccessor`, but `set()` and `clear()` raise `ReadOnlyExternalStateError`. A missing joined row produces an empty payload with the joined table schema, so `get("column")` returns `None`; the examples skip output for that message. An unknown joiner name raises `ValueError`.

{% code '/yt/yt/flow/examples/python/external_state_join/__main__.py' lang='python' lines='[BEGIN lookup_join]-[END lookup_join]' %}

The joiner keeps a per-job TTL cache. Its cache is disabled by default (`ttl = 0`), so keys are loaded again in the next epoch. Configure `dynamic_spec.computations.<computation>.external_state_joiners.<name>.parameters.cache` to reuse rows until their TTL expires. Joined rows are loaded as unmodified state and are never returned to the worker as updates.

See the [joiner implementation]({{source-root}}/yt/yt/flow/library/cpp/computation/simple_external_state_manager.cpp), [TTL-cache implementation]({{source-root}}/yt/yt/flow/library/cpp/common/state_cache.cpp), [Python joined-state conversion]({{source-root}}/yt/yt/flow/library/python/companion/proto_mapper.py), [state holder]({{source-root}}/yt/yt/flow/library/python/companion/state.py), and [read-only accessor]({{source-root}}/yt/yt/flow/library/python/companion/context.py).

## Creating a table for the state {#creating-table}

You must create the table for External State in advance. The schema of the table's key columns must match the computation's `group_by_schema`.

{% if audience == "internal" %}To create the table, we recommend using [YtSync]({{yt-sync-docs}}/).{% endif %}

## Complete example — EventReducer from Shuffle {#example}

Example from [shuffle]({{source-root}}/yt/yt/flow/examples/python/shuffle):

{% code '/yt/yt/flow/examples/python/shuffle/event_reducer.py' lang='python' lines='[BEGIN event_reducer]-[END event_reducer]' %}

[Full source code]({{source-root}}/yt/yt/flow/examples/python/shuffle/event_reducer.py)

Work pattern:
1. Get the current state with `ctx.external_state(...)`.
2. Create a builder with `state.to_builder()`.
3. Update the required fields with `builder.set(...)`.
4. Save with `state.set(builder.finish())`.

## See also {#see-also}

- [StateAccessor (Python)](../../../flow/python/state-accessor.md)
- [Internal State (Python)](../../../flow/python/internal-state.md)
- [Working with states (Python)](../../../flow/python/state.md) — a brief overview
- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [External State (Java)](../../../flow/java/external-state.md)
- [Shuffle example](../../../flow/python/examples/shuffle.md)
- [Static Table Join example](../../../flow/python/examples/static_table_join.md)
- [External State Join example](../../../flow/python/examples/external_state_join.md)
