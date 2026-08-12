# External State Join in {{product-name}} Flow (Go)

An example of a [pipeline](../../../../flow/concepts/glossary.md#pipeline) that enriches events from a queue with data from a read-only external [state](../../../../flow/concepts/glossary.md#state). The reference table is attached through `TSimpleExternalStateJoiner`, and the Go computation looks a row up by the event key.

[Source code]({{source-root}}/yt/yt/flow/examples/go/external_state_join)

## Structure {#structure}

- `event_reader` — a native queue source that publishes the `event` stream.
- `lookup_join` (`lookupJoin`) — a Go transform computation grouped by `key`. It reads `/reference` through `external_state_joiners` and publishes the enriched message to the `enriched` stream.
- A queue sink stores the messages from `enriched`.

The joiner path in the spec may point to a Cypress link. This lets you switch the reference data to a new version of the table atomically, without restarting the pipeline.

## `main.go` {#main-go}

The entry point registers the schemas of the input and output streams, adds the computation, and starts the pipeline.

{% code '/yt/yt/flow/examples/go/external_state_join/main.go' lang='go' %}

## `lookup_join.go` {#lookup-join-go}

The computation decodes the typed input message, reads the joined state row, and creates a typed output message.

{% code '/yt/yt/flow/examples/go/external_state_join/lookup_join.go' lang='go' %}

## Key patterns {#key-patterns}

- `flow.OpenJoinedExternalState(rt, referenceStateName, msg)` opens the read-only state for the key of the current message.
- `flow.ErrStateNotRead` means that there is no joined row for the key; in this case the example publishes no result.
- `ConvertTo` stays at the SDK boundary: the business logic works with `eventMessage`, `referenceState`, and `enrichedMessage` rather than with raw wire-protocol rows.
- The joined external state isn’t changed from the computation: the `ConvertFrom` method isn’t used for it.
