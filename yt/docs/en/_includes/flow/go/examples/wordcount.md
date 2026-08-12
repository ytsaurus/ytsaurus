# Word Count in {{product-name}} Flow (Go)

The simplest example of a stateful [pipeline](../../../../flow/concepts/glossary.md#pipeline) in Go: counting the occurrences of every word in an internal YSON [state](../../../../flow/concepts/glossary.md#state).

[Source code]({{source-root}}/yt/yt/flow/examples/go/word_count)

## Structure {#structure}

The pipeline consists of two [computations](../../../../flow/concepts/glossary.md#stream-and-computation):

- `reader` — a native source (`TSwiftPassthroughOrderedSourceComputation`) declared directly in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): it reads the queue and publishes the rows to the `words` [stream](../../../../flow/concepts/glossary.md#stream-and-computation). It has no Go code.
- `mapper` — a transform computation (`TTransformCompanionComputation`) served by the companion: it reads the `words` stream and updates the counter of the word in the internal state.

The messages are grouped by word (`group_by_schema` with `farm_hash(word)` and `word`), so the state of the key being processed is the counter of exactly one word. The result of the pipeline lies in the internal state table: nothing is sent further along the graph.

## `main.go` {#main-go}

The entry point: creating the pipeline, registering the only computation, and starting it.

{% code '/yt/yt/flow/examples/go/word_count/main.go' lang='go' %}

## `word_count_mapper.go` {#word-count-mapper-go}

The state value is an ordinary Go structure with YSON tags: it is stored in the internal state table exactly in this form.

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_state]-[END word_count_state]' %}

The `flow.RowFunction` that opens the internal state by the name `word-state` through `flow.OpenYSONState` and increments the counter:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

## Key patterns {#key-patterns}

- The simplest stateful pipeline with a single computation on the companion side: the source stays native, and no Go code is needed for it.
- Internal YSON state through `flow.OpenYSONState[T](rt, name, msg)`: `Value()` returns a mutable structure that the SDK saves after a successful batch.
- The state name (`word-state`) matches the name from `parameters.internal_states` of the computation in the spec.
- The state key is defined by `group_by_schema` from the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) — in this case, by the `word` field.
- The input is converted into `wordMessage` once through `msg.ConvertTo(&input)`, after which the handler works with the fields of the structure.
