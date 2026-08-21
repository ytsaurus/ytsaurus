# Internal State in {{product-name}} Flow (Python)

Internal State is a mechanism for working with internal [state](../../../flow/concepts/glossary.md#state) stored in Flow’s internal tables. Unlike [External State](../../../flow/python/external-state.md), you don’t need to create tables manually—Flow manages them automatically.

Learn more about `StateAccessor` and general principles for working with state: [StateAccessor](../../../flow/python/state-accessor.md).

General information about stateful processing is in the [Stateful processing](../../../flow/concepts/stateful.md) section.

## Overview

The Python SDK provides three types of accessors for working with Internal State, which differ by serialization format. To work with external state, use a separate accessor: [ExternalStateAccessor](../../../flow/python/external-state.md).

| Accessor | Format | Description |
|----------|--------|----------|
| [YsonStateAccessor](#yson-state-accessor) | YSON (dict) | Serializes a Python dict using YSON |
| [RawStateAccessor](#raw-state-accessor) | `bytes` | No serialization (raw bytes) |
| [ProtoStateAccessor](#proto-state-accessor) | Protobuf | Serializes using Protobuf |

All accessors provide the same set of methods: `get()`, `set(value)`, `clear()`, `get_or_default(default)`.

## YsonStateAccessor {#yson-state-accessor}

[Source code]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`YsonStateAccessor` uses YSON serialization. The state is stored as a Python dict, which is automatically serialized to YSON and deserialized back.

### Get the accessor

```python
# For a message
state = ctx.state("state-name", message)

# For a timer
state = ctx.state("state-name", timer)
```

### Methods

| Method | Return type | Description |
|-------|-----------------|----------|
| `get()` | `dict` or `None` | Deserialize and return the current value |
| `set(value)` | — | Serialize and save the value (dict or bytes) |
| `clear()` | — | Delete the state for the current key |
| `get_or_default(default)` | `dict` | Return the current value or `default` |

### Example from WordCount

{% code '/yt/yt/flow/examples/python/word_count/word_count_mapper.py' lang='python' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

[Full source code]({{source-root}}/yt/yt/flow/examples/python/word_count/word_count_mapper.py)

Here, the state is tied to the [message](../../../flow/concepts/glossary.md#message) key (defined via `group_by_schema` in the [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec)). Each unique word has its own independent counter.

## RawStateAccessor {#raw-state-accessor}

[Source code]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`RawStateAccessor` works with raw bytes without serialization or deserialization.

### Get the accessor

```python
# For a message
state = ctx.raw_state("state-name", message)

# For a timer
state = ctx.raw_state("state-name", timer)
```

### Methods

| Method | Return type | Description |
|-------|-----------------|----------|
| `get()` | `bytes` or `None` | Get the raw bytes |
| `set(value: bytes)` | — | Save the raw bytes |
| `clear()` | — | Delete the state for the current key |
| `get_or_default(default: bytes)` | `bytes` | Return the current value or `default` |

### Usage example

```python
state = ctx.raw_state("raw-state", message)

data = state.get()
if data is not None:
    # Process raw data...
    pass

# Write raw data
state.set(b"\x01\x02\x03")

# Clear
state.clear()
```

## ProtoStateAccessor {#proto-state-accessor}

[Source code]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`ProtoStateAccessor` uses Protobuf serialization. The state is deserialized into an instance of the specified Protobuf class.

### Get the accessor

```python
# For a message
state = ctx.proto_state("state-name", message, TJoinState)

# For a timer
state = ctx.proto_state("state-name", timer, TJoinState)
```

The third argument is the Protobuf message class used for deserialization.

### Methods

| Method | Return type | Description |
|-------|-----------------|----------|
| `get()` | Proto object or `None` | Deserialize and return the value |
| `set(value)` | — | Serialize and save the Proto object |
| `clear()` | — | Delete the state for the current key |
| `get_or_default(default=None)` | Proto object | Return the value, `default`, or an empty instance of the Proto class |

{% note info %}

The `get_or_default()` method with no arguments returns an empty instance of the Proto class (equivalent to `ProtoClass()`). This is convenient for initializing the state on the first access.

{% endnote %}

{% if audience == "internal" %}

### Example from lb_wait_click_join

{% note info %}

The `TJoinState` proto definition is in the Java example directory (`yt/yt/flow/yandex/extensions/logbroker/examples/java/lb_wait_click_join/proto`) because the proto files are shared between Java and Python: `from yt.yt.flow.yandex.extensions.logbroker.examples.java.lb_wait_click_join.proto.message_pb2 import TJoinState`.

{% endnote %}

`JoinFunction.on_message`:

{% code '/yt/yt/flow/yandex/extensions/logbroker/examples/python/lb_wait_click_join/join_function.py' lang='python' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

`JoinFunction.on_timer`:

{% code '/yt/yt/flow/yandex/extensions/logbroker/examples/python/lb_wait_click_join/join_function.py' lang='python' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

{% endif %}

## Configuration in the static spec {#static-spec}

Internal State doesn’t require you to create external tables. States are automatically stored in Flow’s internal tables.

You must declare the names of internal states in the `internal_states` section of the [computation](../../../flow/concepts/glossary.md#stream-and-computation) parameters in the static spec:

{% code '/yt/yt/flow/examples/python/word_count/test/pipeline.yson' lang='yson' %}

The state name in your code (the first argument of `ctx.state(...)`, `ctx.raw_state(...)`, or `ctx.proto_state(...)`) must match the name declared in `internal_states`.

{% note warning %}

If the state name isn’t declared in `internal_states`, calling it via `ctx.state(...)` will raise a `ValueError` exception.

{% endnote %}

## See also

- [StateAccessor (Python)](../../../flow/python/state-accessor.md)
- [External State (Python)](../../../flow/python/external-state.md)
- [Working with states (Python)](../../../flow/python/state.md) — brief overview
- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [Internal State (Java)](../../../flow/java/internal-state.md)
- [WordCount example](../../../flow/python/examples/wordcount.md)
{% if audience == "internal" %}- [lb_wait_click_join example](../../../yandex-specific/flow/python/examples/lb_wait_click_join.md){% endif %}