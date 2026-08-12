# Shuffle in {{product-name}} Flow (Go)

An example of a [pipeline](../../../../flow/concepts/glossary.md#pipeline) built from two Go [computations](../../../../flow/concepts/glossary.md#stream-and-computation): the source computation parses JSON and sends typed [messages](../../../../flow/concepts/glossary.md#message), and the transform computation counts the events in an external [state](../../../../flow/concepts/glossary.md#state). Between them stand native passthrough computations that regroup the event by four different keys.

[Source code]({{source-root}}/yt/yt/flow/examples/go/shuffle)

## Structure {#structure}

- `reader` (source, `TSwiftOrderedSourceCompanionComputation`) — `eventMapper`: parses the JSON from the `data` column and sends a typed message to the `event` [stream](../../../../flow/concepts/glossary.md#stream-and-computation).
- `shuffle_a` … `shuffle_d` — native `TSwiftPassthroughComputation` declared in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec). Each of them regroups the flow by its own key (`key_a` … `key_d`) and publishes it to a separate stream, `event_a` … `event_d`. They have no Go code.
- `reducer` (transform, `TTransformCompanionComputation`) — `eventReducer`: it is subscribed to all four streams and counts how many times a value arrives, in the external state `/shuffle-state`.

An event written to the queue once reaches the reducer along four different paths, so it is counted four times. The companion serves only the ends of the pipeline — `reader` and `reducer`.

## `main.go` {#main-go}

The entry point: registering the source and transform computations with a single `pipeline.Add` call.

{% code '/yt/yt/flow/examples/go/shuffle/main.go' lang='go' %}

## `event_mapper.go` {#event-mapper-go}

The JSON structure that the input queue carries in the `data` column. The four keys are independent of each other — that is exactly what gives the shuffle stages something to regroup by:

{% code '/yt/yt/flow/examples/go/shuffle/event_mapper.go' lang='go' lines='[BEGIN event]-[END event]' %}

The source function converts the input row into `sourceMessage`, parses the JSON into `event`, and publishes a typed message: a flow can only be regrouped by a stream column, so the parsed keys are moved into separate fields.

{% code '/yt/yt/flow/examples/go/shuffle/event_mapper.go' lang='go' lines='[BEGIN event_mapper]-[END event_mapper]' %}

## `event_reducer.go` {#event-reducer-go}

The transform function with an external state for counting the events:

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer.go' lang='go' lines='[BEGIN event_reducer]-[END event_reducer]' %}

## Key patterns {#key-patterns}

- A pipeline of several computations: a [source](../../../../flow/concepts/glossary.md#source) built on `flow.NewRowSourceComputation` plus a transform built on `flow.NewRowComputation`. Both are registered by a single `pipeline.Add`.
- Parsing JSON and creating a typed message: `msg.ConvertTo(&input)` → `json.Unmarshal` → `flow.ConvertFrom(rt, event)` → `out.AddMessage(msg)`.
- External state through `flow.OpenExternalState(rt, "/shuffle-state", msg)`: `ConvertTo` reads the counter into a structure, `ConvertFrom` saves the update. The name of an external state is an absolute path that matches the key in `external_state_managers` of the spec.
- `state.ConvertTo` returns whether the row exists: for a key the state has never been written for, the counter starts from zero.
- Regrouping the flow is the job of the native passthrough computations: you can count the same value across several cuts without writing a single line of Go code for the shuffle stages themselves.
