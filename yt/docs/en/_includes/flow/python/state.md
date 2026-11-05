# Working with states in {{product-name}} Flow (Python)

{% note info %}

This page describes the Python API for working with states. For general state concepts, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

{% endnote %}

## YSON State {#yson-state}

The simplest way to work with a state is using the YSON format. You store the state as a Python Map, which is automatically serialized to YSON:

```python
state = ctx.state("state-name", message)
```

This returns a `YsonStateAccessor` with the following methods:
- `get()` — get the current value (`dict` or `None`).
- `set(dict)` — save the value.
- `clear()` — delete the state.
- `get_or_default(dict)` — get the current value or return the default value.

Example from [WordCount](../../../flow/python/examples/wordcount.md):

{% code '/yt/yt/flow/examples/python/word_count/word_count_mapper.py' lang='python' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

Here, the state is tied to the message key, which is defined via `group_by_schema` in the [spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). Each unique key has its own independent state.

## Raw State {#raw-state}

To store a state as raw bytes:

```python
state = ctx.raw_state("state-name", message)
```

This returns a `RawStateAccessor` with the following methods:
- `get()` — get the value (`bytes` or `None`).
- `set(bytes)` — save the value.
- `clear()` — delete the state.
- `get_or_default(bytes)` — get the value or return the default value.

## Proto State {#proto-state}

To store a state as a Protobuf message:

```python
state_accessor = ctx.proto_state("state-name", message, TJoinState)
```

This returns a `ProtoStateAccessor` with the following methods:
- `get()` — deserialize and return the Protobuf object (or `None`).
- `set(proto_message)` — serialize and save the value.
- `clear()` — delete the state.
- `get_or_default(default=None)` — get the value or return the default. If you don’t specify a default, it returns an empty instance of the Proto class.

{% if audience == "internal" %}

Example from [Logbroker WaitClickJoin](../../../yandex-specific/flow/python/examples/lb_wait_click_join.md) — `JoinFunction.on_message`. The `TJoinState` class is imported from the proto module that’s shared between Java and Python: `yt.yt.flow.yandex.extensions.logbroker.examples.java.lb_wait_click_join.proto.message_pb2`:

{% code '/yt/yt/flow/yandex/extensions/logbroker/examples/python/lb_wait_click_join/join_function.py' lang='python' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endif %}

## External State {#external-state}

Row-format external state works like a Payload and gives you dict-like access to fields:

```python
state = ctx.external_state("/state-name", message)
```

The state name must start with `/` and match the key in `external_state_managers` in the static spec. If you call `ctx.external_state("state-name", message)` without the leading `/`, it raises a `ValueError`.

This returns an `ExternalStateAccessor`, which is also a `Payload`:
- `state.get("field")` — read a field.
- `state["field"]` — dict-like read of a field.
- `state.to_builder()` — get a `PayloadBuilder` with the current values.
- `state.set(payload)` — save a new value (it accepts a Payload from `builder.finish()`).
- `state.clear()` — delete the state.

Example from [Shuffle](../../../flow/python/examples/shuffle.md) (EventReducer):

{% code '/yt/yt/flow/examples/python/shuffle/event_reducer.py' lang='python' lines='[BEGIN event_reducer]-[END event_reducer]' %}

The pattern for working with an external state is:
1. Get the current state using `ctx.external_state(...)`.
2. Create a builder with `state.to_builder()`.
3. Update the required fields with `builder.set(...)`.
4. Save the changes with `state.set(builder.finish())`.

For protobuf external state, use `ctx.proto_external_state(name, message_or_timer, proto_class)`. Its `get()` returns an independent message snapshot or `None`; `get_or_default()` returns a snapshot or a fresh empty message without creating state. Save edits explicitly with `set(value)`; `clear()` resets the state. This requires a protobuf-capable manager and a matching message class. Choosing this accessor does not change a row-format manager. See [protobuf external state](../../../flow/python/external-state.md#proto-external-state) for prerequisites, availability, and errors.

For read-only joins, use `ctx.joined_external_state(...)` for rows or `ctx.joined_proto_external_state(...)` for protobuf. Both reject `set()` and `clear()` with `ReadOnlyExternalStateError`; declare the joiner in `external_state_joiners`.

## State in timers {#state-in-timers}

The API for working with a state in a timer handler is the same — you pass the `timer` object instead of `message`:

```python
def on_timer(self, timer, output, ctx):
    state = ctx.external_state("/join-state", timer)
    # Read the state
    show_time = state.get("show_time")
    hit_payload = state.get("hit_payload")
    # Clear the state after processing
    state.clear()
```

Example from [WaitClickJoin](../../../flow/python/examples/wait_click_join.md) (JoinProcessFunction):

{% code '/yt/yt/flow/examples/python/wait_click_join/join_process_function.py' lang='python' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

## Binding a state to a key {#group-by-schema}

In `TTransformCompanionComputation`, state binds to the [message key](../../../flow/concepts/glossary.md#key) defined by `group_by_schema` in the computation spec. Messages with the same key share one state.

`TTransformOrderedSourceCompanionComputation` does not support `group_by_schema`. Its internal state uses the source partition key, so messages from one partition share that state. See [Computation (Python)](../../../flow/python/computation.md#sourcecomputation) for choosing a SourceComputation class, and [Stateful processing](../../../flow/concepts/stateful.md) for key configuration.

## Configuring states in the spec {#spec-configuration}

Internal states (YSON, Raw, Proto) must be declared in the [computation](../../../flow/concepts/glossary.md#stream-and-computation) parameters in the `internal_states` section. The state name in your code (the first argument of `ctx.state(...)`) must match the name declared in the spec.

External states are configured via `external_state_managers` in the computation spec, or `external_state_joiners` for read-only joins. The key (for example, `"/shuffle-state"`) sets the state name and must start with `/`. For row-format state, use `"NYT::NFlow::TSimpleExternalStateManager"` as `external_state_manager_class_name`; its schema describes the available columns. Protobuf external state requires a compatible implementation and protobuf descriptor. See [row-format configuration](../../../flow/python/external-state.md#static-spec), [protobuf prerequisites](../../../flow/python/external-state.md#proto-external-state), and the [C++ documentation](../../../flow/cpp/state.md#external-state).

## See also {#see-also}

- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Quick start (Python)](../../../flow/python/getting-started.md)
