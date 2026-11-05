# StateAccessor in {{product-name}} Flow (Python)

Use StateAccessor to read, modify, and delete state values. For general information about stateful processing, see the [Stateful processing](../../../flow/concepts/stateful.md) section.

## How it works {#how-it-works}

In Flow, the [state](../../../flow/concepts/glossary.md#state) is stored in [sorted dynamic tables](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md). If you’re using [external state](../../../flow/python/external-state.md), you create this table yourself. If you’re using [internal state](../../../flow/python/internal-state.md), Flow automatically creates and manages the tables.

For `TTransformCompanionComputation`, the key columns in the state table match the `group_by_schema` of the [computation](../../../flow/concepts/glossary.md#stream-and-computation). For the internal state of `TTransformOrderedSourceCompanionComputation`, the source partition key is used instead; this SourceComputation does not support `group_by_schema`. In both cases, messages with the same key share one state.

## Reading and writing data {#reading-and-writing-data}

The [worker](../../../flow/concepts/glossary.md#worker) directly handles table operations (reading, writing, and deleting data). When the worker receives a new batch of messages, it loads the state values for all keys in the batch and sends them to the [companion](../../../flow/concepts/companion.md) along with the messages and timers. For more details, see the [interaction schema](../../../flow/concepts/companion.md#schema).

You write new values to the state table transactionally within an [epoch](../../../flow/concepts/glossary.md#epoch).

## Accessor types {#accessor-types}

Choose an accessor for the state location and format:

#|
|| **Accessor** | **Format** | **Retrieval** | **Description** ||
|| [YsonStateAccessor](../../../flow/python/internal-state.md#yson-state-accessor) | YSON (dict) | `ctx.state(name, msg)` | Serializes a Python dict to YSON ||
|| [RawStateAccessor](../../../flow/python/internal-state.md#raw-state-accessor) | `bytes` | `ctx.raw_state(name, msg)` | Raw bytes without serialization ||
|| [ProtoStateAccessor](../../../flow/python/internal-state.md#proto-state-accessor) | Protobuf | `ctx.proto_state(name, msg, ProtoClass)` | Serializes using Protobuf ||
|| [ExternalStateAccessor](../../../flow/python/external-state.md) | Payload (table row) | `ctx.external_state("/name", msg)` | Typed access to an external table ||
|| [ReadOnlyExternalStateAccessor](../../../flow/python/external-state.md#external-state-joiner) | Payload (table row) | `ctx.joined_external_state("/name", msg)` | Read-only access through a row-format joiner ||
|| [ProtoExternalStateAccessor](../../../flow/python/external-state.md#proto-external-state) | Protobuf | `ctx.proto_external_state("/name", msg, ProtoClass)` | Explicit snapshot writes through a protobuf-capable manager ||
|| [ProtoExternalStateAccessor (read-only)](../../../flow/python/external-state.md#proto-external-state) | Protobuf | `ctx.joined_proto_external_state("/name", msg, ProtoClass)` | Read-only snapshots through a protobuf-capable joiner ||
|#

The first three accessors work with [internal state](../../../flow/python/internal-state.md) (tables are automatically managed by Flow). External accessors use a configured manager or joiner and a table you create yourself. Their format is determined by that implementation; choosing a Python accessor does not change it. See the [protobuf prerequisites and availability](../../../flow/python/external-state.md#proto-external-state).

## Common internal-state API {#common-api}

All internal accessors (`YsonStateAccessor`, `RawStateAccessor`, `ProtoStateAccessor`) provide the same set of methods:

#|
|| **Method** | **Description** ||
|| `get()` | Get the state value (or `None` if the state doesn’t exist) ||
|| `set(value)` | Set the state value ||
|| `clear()` | Delete the state for the current key ||
|| `get_or_default(default)` | Get the value or return `default` ||
|#

Internal accessors also provide `read_only()`. Their values are [live and shared within a batch](../../../flow/python/internal-state.md#in-place). External accessors have a separate contract: row access reads fields through `Payload`, and protobuf access returns independent message snapshots that require explicit `set()` calls. Joined external accessors reject writes.

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
- `name` — a string with the state name declared in the [static spec](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). For internal states, this is a string from `internal_states`; for external states, it is a key from `external_state_managers`, or `external_state_joiners` for joined access, and must start with `/`.
- `message` / `timer` — the message or timer for which you need to get the state for the [key](../../../flow/concepts/glossary.md#key).
- `ProtoClass` (for `proto_state`, `proto_external_state`, and `joined_proto_external_state`) — the Protobuf message class for deserialization. For external state, its descriptor name must match the manager or joiner's `proto_type`.

## When to use which type {#choosing-type}

#|
|| **Situation** | **Recommended accessor** ||
|| A simple dictionary with several fields | `ctx.state()` (YSON) ||
|| Arbitrary binary data | `ctx.raw_state()` (Raw) ||
|| Structured data with a fixed schema | `ctx.proto_state()` (Protobuf) ||
|| Data that needs to be accessed from other systems | `ctx.external_state("/name", msg)` (External) ||
|| Data that requires a custom table | `ctx.external_state("/name", msg)` (External) ||
|| A row-format lookup without writes | `ctx.joined_external_state("/name", msg)` ||
|| An external manager that exposes protobuf state | `ctx.proto_external_state("/name", msg, ProtoClass)` ||
|| A protobuf lookup through a compatible joiner | `ctx.joined_proto_external_state("/name", msg, ProtoClass)` ||
|#

## See also {#see-also}

- [Internal State (Python)](../../../flow/python/internal-state.md)
- [External State (Python)](../../../flow/python/external-state.md)
- [Working with states (Python)](../../../flow/python/state.md) — a brief overview of all types
- [Stateful processing (concept)](../../../flow/concepts/stateful.md)
- [StateAccessor (Java)](../../../flow/java/state-accessor.md)
