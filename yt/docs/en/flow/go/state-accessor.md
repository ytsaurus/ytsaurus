# StateAccessor in {{product-name}} Flow (Go)

StateAccessor is the interface for reading, modifying, and deleting [state](../../flow/concepts/glossary.md#state) values.
General information about stateful processing is available in the [Stateful processing](../../flow/concepts/stateful.md) section.

## How it works {#how-it-works}

[State](../../flow/concepts/glossary.md#state) in Flow is stored in [sorted dynamic tables](../../user-guide/dynamic-tables/sorted-dynamic-tables.md).
For [external state](external-state.md), you create this table yourself; for [internal state](internal-state.md), the tables are created and managed by Flow automatically.

For `TTransformCompanionComputation`, the key columns of the state table match the `group_by_schema` of the [computation](../../flow/concepts/glossary.md#stream-and-computation). For the internal state of `TTransformOrderedSourceCompanionComputation`, the key is the source partition key: `group_by_schema` isn’t supported in such a SourceComputation. In every case, messages with the same key share one state.

In Go, an accessor is a value that binds two things together: the holder of the state with a given name and the key of a specific input. The holders live in `flow.Runtime` and are available directly (`rt.InternalState(name)`, `rt.ExternalState(name)`, `rt.JoinedExternalState(name)`), but an ordinary computation doesn’t need them: the accessor is exactly the convenient view of the state of one key.

## Reading and writing data {#reading-and-writing-data}

Working with the table directly (reading, writing, and deleting data) is done by the [worker](../../flow/concepts/glossary.md#worker). When the next batch of messages arrives, the worker loads the state values for all the keys in the batch and sends them to the [companion](../../flow/concepts/companion.md) together with the messages and timers. For more details, see the [interaction schema](../../flow/concepts/companion.md#schema).

New values are written to the state table transactionally within an [epoch](../../flow/concepts/glossary.md#epoch).

What travels back to the worker isn’t the whole state but a delta: only the changed records. For Raw, Proto, and External states, the write is performed via `Set` or `Clear`; for a YSON state, by changing the value from `Value()` or by calling `Clear`. A plain read sends nothing.

`Clear` doesn’t erase the record from the accessor, it marks the record as deleted, and the deletion reaches the worker in exactly that form. For the computation there is no difference: a state the request didn’t bring and a state cleared in this request are both read as missing — the computation sees the state as it will be after the response.

{% note warning %}

`flow.Runtime` and all the accessors opened from it belong to the goroutine serving the request and aren’t designed for concurrent use. If a handler parallelizes its work, read and write the state in the same goroutine the handler was called in. The rules for starting child goroutines are described in the [Goroutines in a handler](computation.md#goroutines) section.

{% endnote %}

## Accessor types {#accessor-types}

The Go SDK provides five types of accessors:

| Accessor | Format | Opening | Description |
|----------|--------|----------|----------|
| [RawStateAccessor](internal-state.md#raw-state-accessor) | `[]byte` | `flow.OpenRawState(rt, name, input)` | Raw bytes without serialization |
| [YSONState](internal-state.md#yson-state) | YSON | `flow.OpenYSONState[T](rt, name, input)` | Serialization of a Go value into YSON |
| [ProtoStateAccessor](internal-state.md#proto-state-accessor) | Protobuf | `flow.OpenProtoState[T](rt, name, input)` | Serialization via Protobuf |
| [ExternalStateAccessor](external-state.md) | Go structure (table row) | `flow.OpenExternalState(rt, "/name", input)` | Reading and writing a row of an external table |
| [JoinedExternalStateAccessor](external-state.md) | Go structure | `flow.OpenJoinedExternalState(rt, "/name", input)` | Read-only access to another computation’s state table |

The first three work with [internal state](internal-state.md), whose tables are managed by Flow. `ProtoStateAccessor` serializes explicit writes on top of `RawStateAccessor`. `YSONState` holds a mutable value and saves it automatically after a successful batch.

`ExternalStateAccessor` and `JoinedExternalStateAccessor` work with [external state](external-state.md) — a dynamic table that you create yourself. They differ in rights: the first is available to the computation that owns the state, the second to the computation that only reads it.

## Internal state API {#common-api}

The Raw and Proto accessors use explicit read and write operations:

| Method | `RawStateAccessor` | `ProtoStateAccessor[T, PT]` |
|-------|--------------------|-----------------------------|
| `Get()` | `([]byte, bool)` | `(PT, bool, error)` |
| `Or(fallback)` | `[]byte` | `(PT, error)` |
| `Set(value)` | `error` | `error` |
| `Clear()` | `error` | `error` |

A YSON state is changed in place:

| Method | Result type | Description |
|-------|----------------|----------|
| `Empty()` | `bool` | The value is missing |
| `Value()` | `*T` | The mutable value; creates a zero value if the state is missing |
| `Clear()` | — | Delete the value |

YSON deserialization is performed in `OpenYSONState`. Changes made through `Value()` are saved automatically only after all the batch handlers have completed successfully.

`ExternalStateAccessor` converts a table row into a Go structure:

| Method | Result type | Description |
|-------|----------------|----------|
| `ConvertTo(&value)` | `(bool, error)` | Read the row into the structure; `bool` distinguishes a missing row |
| `ConvertFrom(&value)` | `error` | Save the fields of the structure into the row |
| `Clear()` | `error` | Delete the row |

`JoinedExternalStateAccessor` provides `ConvertTo(&value)`, but neither writing nor clearing. For dynamic schemas, both accessors keep the low-level `Get`, `Or`, and `Schema`, and the owner also keeps `Builder` and `Set`.

## Getting an accessor {#getting-accessor}

An accessor is opened by a free function inside `OnMessage`, `OnTimer`, or `OnVisit`:

```go
func (*myFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    // YSON
    ysonState, err := flow.OpenYSONState[myState](rt, "state-name", msg)

    // Raw bytes
    rawState, err := flow.OpenRawState(rt, "state-name", msg)

    // Protobuf
    protoState, err := flow.OpenProtoState[TMyState](rt, "state-name", msg)

    // External state: the name must start with "/"
    extState, err := flow.OpenExternalState(rt, "/state-name", msg)

    // Read-only external state
    joined, err := flow.OpenJoinedExternalState(rt, "/reference", msg)
}

func (*myFunction) OnTimer(
    ctx context.Context,
    rt flow.Runtime,
    timer flow.Timer,
    out flow.OutputCollector,
) error {
    // The same thing, but a timer is passed instead of a message
    state, err := flow.OpenYSONState[myState](rt, "state-name", timer)
}
```

Parameters:

- `rt` — `flow.Runtime`, the second argument of the handler.
- `name` — the name of the state declared in the [static spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec). For internal states, it is an arbitrary string from `internal_states`; for external ones, a key from `external_state_managers` or `external_state_joiners` that must start with `/`.
- `input` — the input to whose [key](../../flow/concepts/glossary.md#key) the accessor is bound. Any value implementing `flow.Input` will do: `flow.ExtendedMessage`, `flow.Timer`, and `flow.Visit`.

The type parameter is specified only for the YSON and Proto accessors and sets the type of the state: `flow.OpenProtoState[TMyState]` produces an accessor over `*TMyState`. This is exactly why the opening functions are free functions rather than methods of `Runtime`: methods in Go have no type parameters of their own.

Opening returns an error if the state name isn’t suitable:

| Error | Reason |
|--------|---------|
| `flow.ErrUnknownState` | The name isn’t declared in the computation spec |
| `flow.ErrInvalidStateName` | The external state name isn’t an absolute path |
| `flow.ErrStateNotRead` | The request didn’t bring an external state with this name |
| `flow.ErrNoStateSchema` | The external state arrived without the schema of its rows |

{% note info %}

`flow.ErrStateNotRead` when opening a joined state is a normal situation, not a failure: the worker joins only those keys for which it found rows, so a batch that didn’t match any row of the reference dataset arrives without such a state. Distinguish this error with `errors.Is` and treat it as an absence of data.

{% endnote %}

## When to use each type {#choosing-type}

| Situation | Recommended accessor |
|----------|------------------------|
| A structure with several fields whose schema changes together with the code | `flow.OpenYSONState[T]` (YSON) |
| Arbitrary binary data or your own serialization | `flow.OpenRawState` (Raw) |
| Structured data with a fixed schema shared with other languages | `flow.OpenProtoState[T]` (Protobuf) |
| Data that must be accessible from other systems | `flow.OpenExternalState` (External) |
| Data that requires a user table | `flow.OpenExternalState` (External) |
| A reference dataset that the computation only reads | `flow.OpenJoinedExternalState` (Joined) |

## See also

- [Internal State (Go)](internal-state.md)
- [External State (Go)](external-state.md)
- [Working with states (Go)](state.md) — a brief overview of all the types
- [Computation (Go)](computation.md)
- [Stateful processing](../../flow/concepts/stateful.md)
