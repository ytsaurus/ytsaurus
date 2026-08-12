# Static Table Join in {{product-name}} Flow (Go)

An example of a [pipeline](../../../../flow/concepts/glossary.md#pipeline) that loads a static reference table into an external [state](../../../../flow/concepts/glossary.md#state) and then enriches events from a queue by the shared key.

[Source code]({{source-root}}/yt/yt/flow/examples/go/static_table_join)

## Structure {#structure}

- `reference_reader` — a native static-table source that publishes the reference rows to the `reference` stream.
- `reference_loader` (`referenceLoader`) — a Go transform computation that normalizes the value and writes it to the external state `/reference_state`.
- `event_reader` — a native queue source that publishes the `event` stream.
- `enricher` (`enricher`) — a Go transform computation that reads `/reference_state` through `external_state_joiners` and publishes the result to the `enriched` stream.

The manager and the joiner refer to the same state table: the first one fills it from the static table, the second one performs a read-only lookup for the incoming events.

## `main.go` {#main-go}

The entry point registers three typed streams and both Go computations.

{% code '/yt/yt/flow/examples/go/static_table_join/main.go' lang='go' %}

## `reference_loader.go` {#reference-loader-go}

The loader decodes a reference row, opens the external state for its key, and saves the normalized value.

{% code '/yt/yt/flow/examples/go/static_table_join/reference_loader.go' lang='go' %}

## `enricher.go` {#enricher-go}

The enricher reads the joined state row and creates an output message only for a key that was found.

{% code '/yt/yt/flow/examples/go/static_table_join/enricher.go' lang='go' %}

## Key patterns {#key-patterns}

- `flow.OpenExternalState` and `ConvertFrom` are used at the loading stage to write the reference data.
- `flow.OpenJoinedExternalState` and `ConvertTo` are used at the enrichment stage for the read-only lookup.
- Both computations work with typed structures; the payload is converted only at the input and the output of the handler.
- The stream schemas are derived from the `yson` tags and registered through `flow.NewYSONStream`.
