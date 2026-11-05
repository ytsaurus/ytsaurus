# External State in {{product-name}} Flow (Go)

External State is the mechanism for working with external [state](../../flow/concepts/glossary.md#state) stored in an external {{product-name}} dynamic table. You create the table for storing the state yourself, on the same cluster the [pipeline](../../flow/concepts/glossary.md#pipeline) is deployed on.

General information about stateful processing is available in the [Stateful processing](../../flow/concepts/stateful.md) section.

## Overview {#overview}

External State in the Go SDK is represented by two accessors: `flow.ExternalStateAccessor` for the state the computation **owns**, and `flow.JoinedExternalStateAccessor` for the state the computation only **reads**. Both convert a row of an external dynamic table into a Go structure with `yson` tags. The state is bound to the [key](../../flow/concepts/glossary.md#key) of the message (`group_by_schema`).

| Accessor | Spec section | Opened by the function | Writing |
|---|---|---|---|
| [ExternalStateAccessor](#getting-accessor) | `external_state_managers` | `flow.OpenExternalState` | Yes |
| [JoinedExternalStateAccessor](#joined-external-state) | `external_state_joiners` | `flow.OpenJoinedExternalState` | No |

Ownership and reading live in different namespaces: a state declared in `external_state_managers` isn’t available through `flow.OpenJoinedExternalState`, and vice versa. For more on accessors and the general principles of working with state, see [State Accessor (Go)](state-accessor.md).

## Difference from Internal State {#vs-internal-state}

| Characteristic | External State | Internal State |
|---|---|---|
| Storage | External dynamic table | Internal tables of Flow |
| Table creation | You create it yourself | Automatic |
| Data format | A Go structure over a table row | Arbitrary (YSON, Protobuf, raw bytes) |
| Access from other systems | Yes (a sorted dynamic table) | No |
| Schema | Defined by the table schema | Defined by you |
| State name | An absolute path starting with `/` | An ordinary name without `/` |

For more on internal state, see [Internal State (Go)](internal-state.md).

## Getting the accessor {#getting-accessor}

[Source code]({{source-root}}/yt/go/flow/context.go)

`flow.ExternalStateAccessor` is opened through `flow.Runtime`:

```go
// For a message
state, err := flow.OpenExternalState(rt, "/shuffle-state", msg)

// For a timer
state, err := flow.OpenExternalState(rt, "/shuffle-state", timer)
```

The arguments are the same as for the internal state accessors:

- `rt` — the `flow.Runtime` of the handler.
- `name` — the state name from the `external_state_managers` section of the [static spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec) (`"/shuffle-state"` in the example). The name must start with `/` and match the key in the spec.
- `input` — the input to whose key the state is bound: `flow.ExtendedMessage`, `flow.Timer`, or `flow.Visit` (anything implementing `flow.Input`).

{% note warning %}

The name of an external state is validated: it must start with `/`, must not be empty, must not be the root `/` itself, must not end with `/`, and must not contain two consecutive `/`. A malformed name gives an error wrapping `flow.ErrInvalidStateName`; a well-formed name that isn’t declared in the spec gives an error wrapping `flow.ErrUnknownState`.

{% endnote %}

Unlike internal state, external state can’t be created out of nothing: the row schema is known from the state table rather than from the spec, so it arrives together with the request. If the request didn’t bring a state with this name, opening returns an error wrapping `flow.ErrStateNotRead`. For a state the computation owns, the worker supplies a row for every key of the batch (an empty one for keys nothing has been saved for yet), so in practice this error occurs with [joined state](#joined-external-state).

## Main operations {#operations}

### Reading and writing typed state {#read}

Describe the columns you use with a structure. Pointers let you distinguish a missing column from its zero value:

```go
type joinState struct {
    ShowTime  *uint64 `yson:"show_time"`
    ClickTime *uint64 `yson:"click_time"`
}
```

`ConvertTo` reads the row into the structure, and the second result reports whether the row existed:

```go
state, err := flow.OpenExternalState(rt, "/join-state", msg)
if err != nil {
    return err
}

var window joinState
_, err = state.ConvertTo(&window)
if err != nil {
    return err
}
```

After the change, the structure is saved by the reverse conversion:

```go
window.ShowTime = &showTime
return state.ConvertFrom(&window)
```

`ConvertFrom` updates the fields of the structure on top of the current row, so the columns not present in the structure are preserved. For dynamic schemas, the low-level `Get`, `Or`, `Builder`, `Set`, and `Schema` remain available.

### Clearing the state {#clear}

```go
state, err := flow.OpenExternalState(rt, "/join-state", timer)
if err != nil {
    return err
}

// Deleting the row from the table
return state.Clear()
```

{% note info %}

An empty state corresponds to the absence of a row in the table: `ConvertTo` for such a key returns `false` as its first result. Only the rows the computation changed through `Set` or `Clear` travel back to the worker — the state protocol transfers a delta, not the whole state.

{% endnote %}

## Configuration in the static spec {#static-spec}

To use External State, declare an external state manager in the `external_state_managers` section of the [computation](../../flow/concepts/glossary.md#stream-and-computation) in the static spec. Here is an example from [static_table_join]({{source-root}}/yt/yt/flow/examples/go/static_table_join), where the `reference_loader` computation owns the reference dataset:

{% code '/yt/yt/flow/examples/go/static_table_join/test/pipeline.yson' lang='yson' %}

The key fields:

- `external_state_managers` — the top-level section inside the computation describing the external state managers.
- The key inside `external_state_managers` (here `"/reference_state"`) — the state name used in the Go code when calling `flow.OpenExternalState(rt, "/reference_state", msg)`. The name must start with `/`.
- `external_state_manager_class_name` — the name of the registered external state manager class. For the typical scenario, it is `"NYT::NFlow::TSimpleExternalStateManager"`;{% if audience == "internal" %} for BigRT profiles, `"NYT::NFlow::NBigRTExtensions::TProfileManager<TUserProfile>"`.{% endif %} For more details, see the [C++ documentation](../../flow/cpp/state.md#external-state).
- `parameters.path` — the path to the {{product-name}} dynamic table the state is stored in.

## Creating the table for the state {#state-table}

The table for External State must be created in advance. The key columns of the table must match the `group_by_schema` of the computation: the worker finds the state row exactly by the message key.

{% if audience == "internal" %}

To create the table, we recommend using [YtSync]({{yt-sync-docs}}/). Here is the description of the state table from [Shuffle](examples/shuffle.md), whose `reducer` computation groups messages by `farm_hash(value), value`:

{% code '/yt/yt/flow/examples/go/shuffle/test/yt_sync.py' lang='python' lines='[BEGIN yt_sync_tables]-[END yt_sync_tables]' %}

{% else %}

You create the table with standard commands, for example `yt create table ... --attributes '{dynamic=true; schema=...}'` and `yt mount-table` — see the [Create command](../../user-guide/storage/cypress-example.md#create) section. The [Shuffle](examples/shuffle.md) example's `reducer` computation groups messages by `farm_hash(value), value`; create the state table with matching key columns `hash` (`uint64`) and `value` (`string`), plus a `count` (`int64`) value column.

{% endif %}

## Complete example — eventReducer from Shuffle {#example}

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer.go' lang='go' lines='[BEGIN event_reducer]-[END event_reducer]' %}

[Full source code]({{source-root}}/yt/yt/flow/examples/go/shuffle/event_reducer.go)

The working pattern:

1. Open the state through `flow.OpenExternalState(...)`.
2. Read it into a structure through `state.ConvertTo(&value)`.
3. Change the fields and save the structure through `state.ConvertFrom(&value)`.

## Joined External State {#joined-external-state}

If an external state is needed **for reading only** — for example, to enrich events with a reference dataset that another computation populates — joined state is used. On the framework side it is served by the [External State Joiner](../../flow/cpp/state.md#external-state-joiner), which reads the table with TTL-based caching; in the spec it is declared in the `external_state_joiners` section (at the same level as `external_state_managers`).

In Go, such a state is represented by the separate `flow.JoinedExternalStateAccessor` type, which has neither `Set` nor `Clear`: the joiner never writes back, and a response claiming otherwise would be rejected by the [worker](../../flow/concepts/glossary.md#worker).

### Getting the accessor {#getting-joined-accessor}

```go
reference, err := flow.OpenJoinedExternalState(rt, "/reference_state", msg)
```

| Method | Result type | Description |
|---|---|---|
| `ConvertTo(&value)` | `(bool, error)` | Fill the structure with the joined row; `bool` reports whether it was found |

For dynamic schemas, the low-level `Get`, `Or`, and `Schema` are available.

{% note warning %}

The worker joins only those keys for which it found rows. Therefore a batch that didn’t match on any key arrives without the joined state at all, and `flow.OpenJoinedExternalState` returns an error wrapping `flow.ErrStateNotRead`. This isn’t a failure: handle it as “there is nothing to enrich with” rather than as an error of the computation.

{% endnote %}

### Configuration in the static spec {#joined-static-spec}

The `enricher` computation from the [static_table_join]({{source-root}}/yt/yt/flow/examples/go/static_table_join) example reads the very reference dataset owned by `reference_loader` from the [section above](#static-spec):

{% code '/yt/yt/flow/examples/go/static_table_join/test/pipeline.yson' lang='yson' %}

The fields of `external_state_joiners` repeat the fields of `external_state_managers` up to the class name: `external_state_joiner_class_name` instead of `external_state_manager_class_name`. The `parameters.path` path is resolved on every access, so if you point it at a symlink, switching the symlink replaces the whole reference dataset under a running pipeline, without a restart.

### Example {#joined-example}

{% code '/yt/yt/flow/examples/go/static_table_join/enricher.go' lang='go' lines='[BEGIN enricher]-[END enricher]' %}

[Full source code]({{source-root}}/yt/yt/flow/examples/go/static_table_join/enricher.go)

The same technique, but with a reference dataset populated by an external process rather than by the pipeline, is covered in the [external_state_join]({{source-root}}/yt/yt/flow/examples/go/external_state_join) example: there `parameters.path` points at a symlink, and replacing the symlink changes the reference dataset entirely.

## See also

- [State Accessor (Go)](state-accessor.md)
- [Internal State (Go)](internal-state.md)
- [Working with states (Go)](state.md) — a brief overview
- [Stateful processing (concept)](../../flow/concepts/stateful.md)
- [Examples: Shuffle (Go)](examples/shuffle.md)
