# Working with states in {{product-name}} Flow (Go)

{% note info %}

This page describes the Go API for working with states. The general state concepts are described in the [Stateful processing](../../flow/concepts/stateful.md) section.

{% endnote %}

A [state](../../flow/concepts/glossary.md#state) accessor is opened by the free function `flow.OpenXxxState(rt, name, input)` with three arguments:

- `rt` — `flow.Runtime`, the second argument of any handler (see [Process Function](getting-started.md#process-function)).
- `name` — the state name declared in the computation [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec).
- `input` — the input to whose [key](../../flow/concepts/glossary.md#key) the accessor is bound: `flow.ExtendedMessage`, `flow.Timer`, or `flow.Visit`. All three implement the `flow.Input` interface, so working with state looks the same in a message, timer, and visit handler.

The opening functions are free functions rather than methods of `Runtime`: the YSON and Proto accessors are parameterized by the state type, and methods in Go have no type parameters of their own.

An accessor addresses the state of exactly one key and lives within a single request. Only the records the computation wrote to travel to the worker: reading a state doesn’t send it back.

## YSON State {#yson-state}

The simplest way to work with state is the YSON format. The state is described by an ordinary Go structure with `yson` tags:

```go
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", msg)
if err != nil {
    return err
}

if state.Empty() {
    state.Value().Word = word
}
state.Value().Count++
return nil
```

`flow.YSONState[T]` provides `Empty()`, `Value() *T`, and `Clear()`. `Value()` returns a mutable value and creates a zero value for a missing state. A separate `Set` isn’t needed: after the batch is processed successfully, the SDK serializes the changes automatically; if the handler fails, they are discarded.

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_state]-[END word_count_state]' %}

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

The state is bound to the message key, which is defined through `group_by_schema` in the computation spec. An independent value is stored for every unique key.

## Raw State {#raw-state}

To store the state as raw bytes:

```go
state, err := flow.OpenRawState(rt, "raw-state", msg)
```

Returns `flow.RawStateAccessor` with the methods:

- `Get() ([]byte, bool)` — get the value and the indication of its presence.
- `Or(fallback []byte) []byte` — get the value or `fallback`.
- `Set(data []byte) error` — save the value.
- `Clear() error` — delete the state.

The YSON and Proto accessors are wrappers over the raw one: `RawStateAccessor` itself is needed when the computation performs the serialization on its own.

## Proto State {#proto-state}

To store the state as a Protobuf message:

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
```

The state type is named by its value form (`TJoinState`), and the accessor works with a pointer to it (`*TJoinState`) — it is on the pointer that the generated code implements `proto.Message`.

Returns `flow.ProtoStateAccessor[T, PT]` with the methods:

- `Get() (PT, bool, error)` — deserialize and return the message.
- `Or(fallback PT) (PT, error)` — return the saved message or `fallback`.
- `Set(value PT) error` — serialize and save.
- `Clear() error` — delete the state.

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
if err != nil {
    return err
}

window, err := state.Or(&TJoinState{})
if err != nil {
    return err
}
window.HitPayload = payload

return state.Set(window)
```

## External State {#external-state}

An external state is a row of your own dynamic table. In user code it is represented by an ordinary Go structure with `yson` tags:

```go
state, err := flow.OpenExternalState(rt, "/shuffle-state", msg)
```

The state name is an absolute path matching the key in the `external_state_managers` section of the static spec. A name without a leading `/` is rejected with the `flow.ErrInvalidStateName` error, an undeclared name with the `flow.ErrUnknownState` error.

Returns `flow.ExternalStateAccessor`. The main operations:

- `ConvertTo(&value) (bool, error)` — fill the structure with the saved row; `bool` distinguishes a missing row.
- `ConvertFrom(&value) error` — save the fields of the structure into the state row.
- `Clear() error` — delete the row.

The low-level `Get`, `Or`, `Builder`, `Set`, and `Schema` are needed only for dynamic schemas and column-wise processing.

An example from [Shuffle](examples/shuffle.md):

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer.go' lang='go' lines='[BEGIN event_reducer]-[END event_reducer]' %}

The pattern for working with external state:

1. Open the state through `flow.OpenExternalState(...)`.
2. Convert the row into a structure through `state.ConvertTo(&value)`.
3. Change the fields of the structure and save it through `state.ConvertFrom(&value)`.

For more details, see the [External State (Go)](external-state.md) section.

## Joined External State {#joined-external-state}

If an external state is needed for reading only — for example, to enrich messages with a reference dataset — the computation declares it in the `external_state_joiners` section and opens it with a separate function:

```go
reference, err := flow.OpenJoinedExternalState(rt, "/reference", msg)
```

Returns `flow.JoinedExternalStateAccessor`. The row is read into a structure through `ConvertTo(&value)`; the accessor has no writing by design, because a joined state is filled from the request and never travels back. The low-level `Get`, `Or`, and `Schema` are available for dynamic schemas.

The namespaces don’t intersect: a state the computation owns isn’t available through `flow.OpenJoinedExternalState`, and vice versa.

{% code '/yt/yt/flow/examples/go/external_state_join/lookup_join.go' lang='go' lines='[BEGIN lookup_join]-[END lookup_join]' %}

[Source code of the example]({{source-root}}/yt/yt/flow/examples/go/external_state_join)

{% note warning %}

The worker joins only those keys for which it found rows. A batch that didn’t match any key of the reference dataset arrives without the joined state at all, and opening returns `flow.ErrStateNotRead` — the request simply didn’t bring a state with that name. This isn’t a processing error: distinguish it with `errors.Is` and treat it as an absence of data.

{% endnote %}

## State in timers {#state-in-timers}

The API for working with state in a [timer](../../flow/concepts/glossary.md#timer) handler is identical — a timer is passed instead of a message:

```go
state, err := flow.OpenExternalState(rt, "/join-state", timer)
```

Here is an example from [WaitClickJoin](examples/wait_click_join.md) — the window is closed by a timer, and the state is cleared right after the result is published:

{% code '/yt/yt/flow/examples/go/wait_click_join/join_function.go' lang='go' lines='[BEGIN on_timer]-[END on_timer]' %}

A state cleared in this request is read as missing from then on: the computation sees the state as it will be after the response to the worker.

## Binding the state to the key {#group-by-schema}

In `TTransformCompanionComputation`, the state is bound to the message [key](../../flow/concepts/glossary.md#key), which is defined through `group_by_schema` in the computation spec. All the messages with the same key share one state. The key the accessor is bound to is taken from the input — that is why one handler can’t accidentally write the state of a foreign key.

In `TTransformOrderedSourceCompanionComputation`, the `group_by_schema` field isn’t supported. The key of the internal state is the source partition key, so all the messages of one partition share the state. For more on choosing the class for a SourceComputation, see the [Computation (Go)](computation.md#sourcecomputation) section.

For more on configuring keys, see [Stateful processing](../../flow/concepts/stateful.md).

The key schema is available in the handler through `rt.KeySchema()`, and the key itself through the `Key` field of the input (`msg.Key`, `timer.Key`, `visit.Key`).

## Configuring states in the spec {#spec-configuration}

A state that isn’t declared in the spec can’t be opened: the opening function returns `flow.ErrUnknownState` with the list of the declared names. States are declared in three different places of the [computation](../../flow/concepts/glossary.md#stream-and-computation) description:

- Internal states (YSON, Raw, Proto) — as a list of names in `parameters.internal_states`. The names are arbitrary, without a leading `/`.
- External states the computation owns — in the `external_state_managers` section. The section key sets the state name (an absolute path), and the `external_state_manager_class_name` field sets the registered manager class; for the typical scenario it is `"NYT::NFlow::TSimpleExternalStateManager"`.
- External states the computation only reads — in the `external_state_joiners` section, next to `external_state_managers`.

```yson
"mapper" = {
    "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    "external_state_managers" = {
        "/shuffle-state" = {
            "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
            "parameters" = {
                "path" = "<cluster=cluster_name>//path/to/state";
            };
        };
    };
    "external_state_joiners" = {
        "/reference" = {
            "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
            "parameters" = {
                "path" = "//path/to/current";
            };
        };
    };
    "parameters" = {
        "internal_states" = ["word-state"];
    };
};
```

The tables of internal states are created and maintained by Flow, while the external state table is created by you. For more details, see the [Internal State (Go)](internal-state.md) and [External State (Go)](external-state.md) sections.

## See also

- [State Accessor (Go)](state-accessor.md)
- [Internal State (Go)](internal-state.md)
- [External State (Go)](external-state.md)
- [Computation (Go)](computation.md)
- [Stateful processing](../../flow/concepts/stateful.md)
