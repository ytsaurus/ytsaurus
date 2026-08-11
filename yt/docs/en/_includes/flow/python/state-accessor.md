# StateAccessor in {{product-name}} Flow (Python)

Use StateAccessor to read, modify, and delete state values. For general information about stateful processing, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

## How it works {#how-it-works}

In Flow, the [state](../../../flow/concepts/glossary.md#state) is stored in [sorted dynamic tables](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md). If you’re using [external state](../../../flow/python/external-state.md), you create this table yourself. If you’re using [internal state](../../../flow/python/internal-state.md), Flow automatically creates and manages the tables.

The key columns in the state table match the `group_by_schema` of the [computation](../../../flow/concepts/glossary.md#stream-and-computation) that uses this state. This means the state is tied to the message key — all messages with the same key share one state.

## Reading and writing data {#reading-and-writing-data}

The [worker](../../../flow/concepts/glossary.md#worker) directly handles table operations (reading, writing, and deleting data). When the worker receives a new batch of messages, it loads the state values for all keys in the batch and sends them to the [companion](../../../flow/concepts/companion.md) along with the messages and timers. For more details, see the [interaction schema](../../../flow/concepts/companion.md#schema).

You write new values to the state table transactionally within an [epoch](../../../flow/concepts/glossary.md#epoch).

## Four accessor types {#accessor-types}

The Python SDK provides four accessor types for working with state:

| Accessor | Format | Retrieval | Description |
|----------|--------|-----------|-------------|
| [YsonStateAccessor](../../../flow/python/internal-state.md#yson-state-accessor) | YSON (dict) | `ctx.state(name, msg)` | Serializes a Python dict to YSON |
| [RawStateAccessor](../../../flow/python/internal-state.md#raw-state-accessor) | `bytes` | `ctx.raw_state(name, msg)` | Raw bytes without serialization |
| [ProtoStateAccessor](../../../flow/python/internal-state.md#proto-state-accessor) | Protobuf | `ctx.proto_state(name, msg, ProtoClass)` | Serializes using Protobuf |
| [ExternalStateAccessor](../../../flow/python/external-state.md) | Payload (table row) | `ctx.external_state("/name", msg)` | Typed access to an external table |

The first three accessors work with [internal state](../../../flow/python/internal-state.md) (tables are automatically managed by Flow). `ExternalStateAccessor` works with [external state](../../../flow/python/external-state.md) (you create the table yourself).

## Common API {#common-api}

All internal accessors (`YsonStateAccessor`, `RawStateAccessor`, `ProtoStateAccessor`) provide the same set of methods:

| Method | Description |
|--------|-------------|
| `get()` | Get the state value (or `None` if the state doesn’t exist) |
| `set(value)` | Set the state value |
| `clear()` | Delete the state for the current key |
| `get_or_default(default)` | Get the value or return `default` |

## Getting an accessor {#getting-accessor}

You get an accessor through `RuntimeContext` (`ctx`) inside `on_message` or `on_timer`:

```python
class MyFunction(RowFunction):
    def on_message(self, message, output, ctx):
        # YSON (dict)
        yson_state = ctx.state("state-name", message)

        # Raw bytes
        raw_state = ctx.raw_state("state-name", message)

        # Protobuf
        proto_state = ctx.proto_state("state-name", message, MyProtoClass)

        # External (the name must start with "/")
        ext_state = ctx.external_state("/state-name", message)

    def on_timer(self, timer, output, ctx):
        # Similarly, but pass timer instead of message
        state = ctx.state("state-name", timer)
```

Parameters:
- `name` — a string with the state name declared in the [static spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). For internal states, this is an arbitrary string from `internal_states`; for external states, it’s a key from `external_state_managers` that must start with `/`.
- `message` / `timer` — the message or timer for which you need to get the state for the [key](../../../flow/concepts/glossary.md#key).
- `ProtoClass` (only for `proto_state`) — the Protobuf message class for deserialization.

## When to use which type {#choosing-type}

| Situation | Recommended accessor |
|-----------|----------------------|
| A simple dictionary with several fields | `ctx.state()` (YSON) |
| Arbitrary binary data | `ctx.raw_state()` (Raw) |
| Structured data with a fixed schema | `ctx.proto_state()` (Protobuf) |
| Data that needs to be accessed from other systems | `ctx.external_state("/name", msg)` (External) |
| Data that requires a custom table | `ctx.external_state("/name", msg)` (External) |

## See also

- [Internal State (Python)](../../../flow/python/internal-state.md)
- [External State (Python)](../../../flow/python/external-state.md)
- [Working with states (Python)](../../../flow/python/state.md) — a brief overview of all types
- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [StateAccessor (Java)](../../../flow/java/state-accessor.md)