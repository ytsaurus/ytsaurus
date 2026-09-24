# Internal State in {{product-name}} Flow (Go)

Internal State is the mechanism for working with internal [state](../../flow/concepts/glossary.md#state) stored in the internal tables of Flow. Unlike [External State](external-state.md), you don’t need to create the tables yourself — Flow manages them automatically.

For more on accessors and the general principles of working with state, see [State Accessor (Go)](state-accessor.md).

General information about stateful processing is available in the [Stateful processing](../../flow/concepts/stateful.md) section.

## Overview {#overview}

The Go SDK provides three kinds of accessors for working with Internal State, differing in the serialization format. Separate accessors are used for external state — see [External State (Go)](external-state.md).

| Accessor | Format | Opened by the function |
|---|---|---|
| [YSONState](#yson-state) | YSON | `flow.OpenYSONState[T]` |
| [RawStateAccessor](#raw-state-accessor) | `[]byte` | `flow.OpenRawState` |
| [ProtoState](#proto-state) | Protobuf | `flow.OpenProtoState[T]` |

`RawStateAccessor` reads and writes raw bytes explicitly through `Get`, `Set`, and `Clear`. `YSONState` and `ProtoState` provide a mutable value: the changes made to it are serialized automatically after the batch handlers complete successfully — see [Changing the value in place](#in-place).

Each of the `flow.OpenXxxState` functions takes three arguments:

- `rt` — the `flow.Runtime` of the handler.
- `name` — the state name. Internal state names don’t start with `/` and must be declared in `parameters.internal_states` of the [computation](../../flow/concepts/glossary.md#stream-and-computation) [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec), see [Configuration in the static spec](#static-spec).
- `input` — the input to whose [key](../../flow/concepts/glossary.md#key) the state is bound. Any value implementing `flow.Input` will do: `flow.ExtendedMessage`, `flow.Timer`, `flow.Visit`.

{% note info %}

The accessor shows the state as it will be after the response to the worker: a state that wasn’t in the incoming request and a state cleared in the same call through `Clear` are read the same way — as missing.

{% endnote %}

## Changing the value in place {#in-place}

The value returned by `Value()`, `Get()`, and `Or()` is live: it is decoded once per key and batch, every accessor for that key returns the same value, and the changes made to it are written to the state at the end of the batch without a `Set()` call. Nothing is written when the value did not change: a state that serializes to the bytes it arrived with does not go back. The default from `Value()` and `Or()` becomes the state value and is written as after `Set()`, so it can be changed right away:

```go
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", msg)
if err != nil {
    return err
}
state.Value().Count++
```

`ProtoState.Set()` still replaces the whole message, and `Clear()` removes the state. An empty Protobuf message serializes to zero bytes, and zero bytes are the absence of a state: an `Or(&T{})` default is not written, while a message whose fields were all unset and a `Set(&T{})` both delete the state. `RawStateAccessor` hands out no mutable value: its `Get()` returns a copy of the bytes, and they are only written through `Set()`. [External State](external-state.md) states are not tracked in place either.

A key has a single decoded value per batch, so opening the same state and key with another Go type returns an error. If a handler returned an error, the in-place changes don’t make it into the response to the worker. A state that Go serializes to bytes other than the ones it arrived with is rewritten canonically on the first read — once per key.

To detect the changes, a value that was read is re-encoded at the end of the batch. When the computation only reads a state, use `ReadOnly()`: that view returns the same value but does not track it, and `ReadOnlyYSONState.Value()` and `ReadOnlyProtoState.Or()` do not create the state. It has no write methods, so writing through it does not compile. The value is shared with the mutable state of the same key: if that state is also opened for writing somewhere, the changes made through the view still reach the worker.

```go
count := state.ReadOnly().Value().Count
```

## YSONState {#yson-state}

[Source code]({{source-root}}/yt/go/flow/context.go)

`YSONState[T]` stores the state as a YSON-serialized value of type `T`. The type can be any structure with `yson` tags, as well as a map, a slice, or a scalar — anything `yson.Marshal` understands.

### Getting the state {#getting-yson-state}

```go
// For a message
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", msg)

// For a timer
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", timer)
```

Deserialization is performed on opening. Opening the same state and key again within the request returns the same mutable value.

### Methods {#yson-methods}

| Method | Result type | Description |
|---|---|---|
| `Empty()` | `bool` | Check whether the value is missing |
| `Get()` | `(*T, bool)` | Get the mutable value. The second result distinguishes a saved state from a missing one |
| `Value()` | `*T` | Get the mutable value; a zero value is created if the state is missing |
| `Clear()` | — | Delete the value |
| `ReadOnly()` | `ReadOnlyYSONState[T]` | A read-only view |

The changes to the value are serialized automatically after all the batch handlers complete successfully. If a handler returned an error, the changes to the YSON state don’t make it into the response to the worker.

### Example from WordCount {#yson-example}

The type the pipeline stores for one word:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_state]-[END word_count_state]' %}

The message handler:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

[Full source code]({{source-root}}/yt/yt/flow/examples/go/word_count/word_count_mapper.go)

Here the state is bound to the key of the [message](../../flow/concepts/glossary.md#message). For a new key, `Empty()` returns `true`, and `Value()` creates an empty `wordCountState`. Assignments to the fields are saved without a separate `Set`.

The same state is also opened in the [timer](../../flow/concepts/glossary.md#timer) handler. This is how the [URL Downloader](examples/url_downloader.md) works: `OnMessage` accumulates a batch, and `OnTimer` reads it and clears it through `Clear`.

## RawStateAccessor {#raw-state-accessor}

[Source code]({{source-root}}/yt/go/flow/context.go)

`RawStateAccessor` works with raw bytes without serialization and deserialization. It is the accessor the other two are built on top of — take it when you define the state format yourself.

### Getting the accessor {#getting-raw-accessor}

```go
// For a message
state, err := flow.OpenRawState(rt, "raw-state", msg)

// For a timer
state, err := flow.OpenRawState(rt, "raw-state", timer)
```

### Methods {#raw-methods}

| Method | Result type | Description |
|---|---|---|
| `Get()` | `([]byte, bool)` | Get the raw bytes. The second result distinguishes a saved state from a missing one |
| `Or(fallback []byte)` | `[]byte` | Return the current value, or `fallback` if there is no state |
| `Set(data []byte)` | `error` | Save the raw bytes; empty bytes delete the state |
| `Clear()` | `error` | Delete the state for the current key |

The `Get` and `Or` methods don’t return an error: there is nothing to deserialize here.

### Usage example {#raw-example}

```go
state, err := flow.OpenRawState(rt, "raw-state", msg)
if err != nil {
    return err
}

// Reading the raw data
if data, ok := state.Get(); ok {
    // Processing the raw data...
    _ = data
}

// Writing the raw data
if err := state.Set([]byte{0x01, 0x02, 0x03}); err != nil {
    return err
}

// Clearing
return state.Clear()
```

## ProtoState {#proto-state}

[Source code]({{source-root}}/yt/go/flow/context.go)

`ProtoState` serializes the state through Protobuf. The type of the Protobuf message is given in its value form, and the state returns a pointer to it: `flow.OpenProtoState[TJoinState]` returns a state over `*TJoinState`.

### Getting the state {#getting-proto-accessor}

```go
// For a message
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)

// For a timer
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", timer)
```

Deserialization is performed on opening. Opening the same state and key again within the request returns the same mutable message.

### Methods {#proto-methods}

| Method | Result type | Description |
|---|---|---|
| `Empty()` | `bool` | Check whether the value is missing |
| `Get()` | `(*T, bool)` | Get the mutable message. The second result distinguishes a saved state from a missing one |
| `Or(fallback *T)` | `*T` | Return the current message, or write and return `fallback` if there is no state |
| `Set(value *T)` | `error` | Replace the whole state value; an empty message deletes the state |
| `Clear()` | — | Delete the state for the current key |
| `ReadOnly()` | `ReadOnlyProtoState[T, PT]` | A read-only view |

{% note info %}

Unlike in Python, where `get_or_default()` without arguments returns an empty instance of the Proto class, in Go the default value is set explicitly — pass `&T{}` if you want to start with an empty message. An empty message serializes to zero bytes, and zero bytes mean the state is absent: such a message is not written for a key that has no state, and deletes the state of a key that has one. Note that a message whose every field holds its zero value is empty in exactly this sense.

{% endnote %}

### Usage example {#proto-example}

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
if err != nil {
    return err
}

window := state.Or(&TJoinState{})
window.ShowTime = showTime

return nil
```

## Configuration in the static spec {#static-spec}

Internal State doesn’t require creating external tables. The states are stored automatically in the internal tables of Flow.

The names of internal states must be declared in the `internal_states` section of the [computation](../../flow/concepts/glossary.md#stream-and-computation) parameters in the static spec:

{% code '/yt/yt/flow/examples/go/word_count/test/pipeline.yson' lang='yson' %}

The state name in the code (the second argument of `flow.OpenYSONState`, `flow.OpenRawState`, or `flow.OpenProtoState`) must match the name declared in `internal_states`.

{% note warning %}

If the state name isn’t declared in `internal_states`, the opening function returns an error wrapping `flow.ErrUnknownState`; the error text lists the declared names. An error returned from a handler stops the processing of the whole batch — the worker retries the entire request.

{% endnote %}

## See also

- [State Accessor (Go)](state-accessor.md)
- [External State (Go)](external-state.md)
- [Working with states (Go)](state.md) — a brief overview
- [Stateful processing](../../flow/concepts/stateful.md)
- [Examples: Word Count (Go)](examples/wordcount.md)
