# Testing in {{product-name}} Flow (Go)

{% note info %}

This page describes **unit testing** of the [computations](../../flow/concepts/glossary.md#stream-and-computation) of a Go [pipeline](../../flow/concepts/glossary.md#pipeline) through the `flowtest` harness, as well as **integration testing** of the full pipeline through `FlowTestGoBase`.

{% endnote %}

## General testing architecture {#architecture}

In production, the C++ [worker](../../flow/concepts/glossary.md#worker) sends gRPC requests to the [companion](../../flow/concepts/companion.md), passing [messages](../../flow/concepts/glossary.md#message), [timers](../../flow/concepts/glossary.md#timer), visits, [states](state.md), and [watermarks](../../flow/concepts/watermarks.md). The companion parses the request, builds a `flow.Job` and a `flow.Runtime` from it, and calls the Process Function of the registered computation.

In unit tests, the worker’s place is taken by `flowtest.Harness` from the [`flowtest`]({{source-root}}/yt/go/flow/flowtest) package. The harness stores what the worker reports to the companion — the streams, the key schema, the declared states, and the parameters — and runs the computation through the same job, the same runtime, and the same dispatching as the companion server does, down to rendering the response into the wire format. As a result, a message to an undeclared stream, a key that cannot be encoded, or a state written with empty bytes fails in the unit test rather than in the job.

The value under test is the same `*flow.Computation` that is registered in the pipeline: the harness is given the result of `flow.NewRowComputation` or a related constructor, and a source differs from a transform only in what created it — you don’t need to tell the harness about that separately.

Unit tests need no cluster, no gRPC connection, and no `flow_server`. Tests are written with the standard `testing` package; the examples use [testify](https://github.com/stretchr/testify) (`require`) for assertions.

## Dependencies {#dependencies}

The harness needs no separate `PEERDIR`: the dependencies of a Go module are derived from the imports. It is enough to list the test files in `GO_TEST_SRCS` of the pipeline module:

{% code '/yt/yt/flow/examples/go/word_count/ya.make' lang='text' %}

And to add a `gotest` directory next to it with a `GO_TEST_FOR` module, through which the tests are run:

{% code '/yt/yt/flow/examples/go/word_count/gotest/ya.make' lang='text' %}

## Testing a Process Function {#testing-process}

### Creating a harness {#harness}

The harness is created by the `flowtest.New(tb, computation, opts)` function. The first argument is `*testing.T` (`*testing.B` and `*testing.F` also work): the harness reports every usage error through it, so only what the test asserts remains in the test.

```go
h := flowtest.New(t, flow.NewRowComputation("mapper", &wordCountMapper{}), flowtest.Options{
    Streams:        map[string]flow.Schema{"words": flowtest.Schema("word:string")},
    KeySchema:      flowtest.Schema("word:string"),
    InternalStates: []string{wordStateName},
})
```

The fields of `flowtest.Options`:

| Field | Description |
|------|----------|
| `Streams` | The streams the computation exchanges messages over, by stream identifier. The computation can read and write only the streams listed here. |
| `KeySchema` | The schema of the key the inputs are grouped by. A computation without grouping leaves the field empty. |
| `InternalStates` | The names of the [internal states](internal-state.md) the computation declares — they reach it as `parameters.internal_states`. |
| `ExternalStates` | The schemas of the [external states](external-state.md) the computation owns, by state name. The names are absolute paths, as the worker requires. |
| `JoinedExternalStates` | The schemas of the external states the computation reads without owning them. |
| `Parameters` | The `parameters` map of the static [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec) — what the computation reads through `rt.Parameters()`. |
| `DynamicParameters` | The `parameters` map of the dynamic spec. |

A column schema is assembled by the `flowtest.Schema("word:string", "count:int64")` helper — the type names are the same as in {{product-name}}. Build a schema that cannot be described this way through `flow.NewSchema` from `schema.Schema`.

For a typed YSON stream, use the same schema the pipeline registers: `flow.YSONMessageSchema[event]()`. This way, a structure with an embedded `flow.YSONMessage` describes both the spec columns and the test input rows.

For a plain structure without `flow.YSONMessage`, `flowtest.SchemaOf(event{})` remains. It follows the common `schema.Infer`: in particular, a Go string becomes `utf8`. If the schema must match an existing spec exactly, use `flowtest.Schema`.

### Inputs {#inputs}

The inputs of one batch are built by harness methods and passed to `Process` in a single call:

| Method | What it builds |
|-------|------------|
| `h.Key(flowtest.Row{...})` | A key according to the `KeySchema` schema. |
| `h.Message(streamID, row)` | A message without a key — what a computation without grouping receives. |
| `h.KeyedMessage(streamID, key, row)` | A message together with the key it is grouped by. |
| `h.Timer(key, triggerTimestamp)` | A fired timer of the key. |
| `h.Visit(key)` | A visit of a key from a key visitor stream. |
| `h.SetWatermark(streamID, watermark)` | The watermark of a stream; it holds until it is set again. |

Every message is given its own identifier, just as the worker does. The timestamps stay zero: a test that needs them can simply set them on the result.

```go
msg := h.KeyedMessage("hits", key, flowtest.Row{"hit_id": "h1"})
msg.EventTimestamp = 1000
```

`h.Process(inputs ...flow.Input)` runs the computation over the batch and returns `*flowtest.Response`; if processing returned an error, the test fails. The state survives a run: what the computation wrote is applied to the state of the next run — exactly the way the worker applies the response delta before sending the next batch. A test that needs a clean slate should build a new harness.

### A complete example {#unit-test-example}

The unit tests of the mapper from [WordCount](examples/wordcount.md) — the harness, a batch of messages, and a check of the internal state:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper_test.go' lang='go' lines='[BEGIN unit_test]-[END unit_test]' %}

### Processing errors {#errors}

An error returned by a handler stops the processing of the whole batch: the worker retries the request, so there is no such thing as a partial response. Such a run is checked with the `h.ProcessError` method, which returns the error and fails if processing succeeded instead:

```go
err := h.ProcessError(h.Message("queue", flowtest.Row{"data": "}not json{"}))

require.ErrorContains(t, err, "parsing the data column")
```

A run that ended with an error produces no output and doesn’t change the state — that is why `ProcessError` doesn’t return a `Response`.

### Timers and watermarks {#timers-and-watermarks}

A timer is built from a key and a trigger time. In a pipeline with several timer streams, the one you need is selected by the `StreamID` field — an empty value means the pipeline’s only timer stream:

```go
timer := h.Timer(key, closeTime)
timer.StreamID = timerStream

r := h.Process(timer)
```

The watermark of a stream is set by `h.SetWatermark` and holds for all subsequent runs. This is how dropping of late data is checked: the computation reads `rt.MinWatermark()`, the minimum across the input streams, so a stream that hasn’t advanced keeps the window open for the rest.

```go
h.SetWatermark(hitStream, hitTime+3)
h.SetWatermark(actionStream, 0)
```

For a complete set of tests of a window with a timer and watermarks, see [Wait Click Join]({{source-root}}/yt/yt/flow/examples/go/wait_click_join/join_function_test.go).

## Testing states {#testing-states}

The state a computation starts a run with is put into the harness before the `Process` call, and the result is read from `Response`. For details on the accessors themselves, see the [State Accessor](state-accessor.md) section.

### Internal state {#internal-state}

The name of an internal state must be declared in `InternalStates`; otherwise the harness reports exactly the same error as the runtime in a job does.

| Method | What it puts |
|-------|------------|
| `h.PutInternalState(name, key, data)` | Raw bytes, which `flow.OpenRawState` reads. |
| `h.PutInternalStateYSON(name, key, value)` | A value serialized to YSON — what `flow.OpenYSONState` reads. |
| `h.PutInternalStateProto(name, key, value)` | A serialized protobuf message for `flow.OpenProtoState`. |

The state is read back with `Response` methods:

```go
var counter wordCountState
require.True(t, r.InternalStateYSON(wordStateName, key, &counter))
require.EqualValues(t, 1, counter.Count)
```

### External state {#external-state}

An external state owned by the computation is put with `h.PutExternalState(name, key, row)` and is read as a row: `r.ExternalState` returns a `flow.Payload`, and `r.ExternalStateRow` an already decoded `flowtest.Row`.

{% note info %}

An internal state and a joined external state reach a run only for the keys they store something for. An external state owned by the computation reaches it for every key of the batch, empty where nothing is stored: the worker resolves the computation’s own state for every key it passes — which is exactly what makes it possible to write state for a key seen for the first time.

{% endnote %}

The unit tests of the reducer from [Shuffle](examples/shuffle.md), which counts events in an external state:

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer_test.go' lang='go' lines='[BEGIN reducer_unit_test]-[END reducer_unit_test]' %}

### Joined external state {#joined-external-state}

A joined external state — a state the computation reads without owning it — is put with `h.PutJoinedExternalState(name, key, row)` and read through `r.JoinedExternalState` / `r.JoinedExternalStateRow`. It cannot be written to: nothing written into a read-only state leaves the response.

```go
h := flowtest.New(t, flow.NewRowComputation("lookup_join", &lookupJoin{}), flowtest.Options{
    Streams:   map[string]flow.Schema{"event": flowtest.Schema("key:uint64")},
    KeySchema: flowtest.Schema("hash:uint64", "key:uint64"),
    JoinedExternalStates: map[string]flow.Schema{
        referenceStateName: flowtest.Schema("hash:uint64", "key:uint64", "name:string"),
    },
})

h.PutJoinedExternalState(referenceStateName, key, flowtest.Row{"key": uint64(1), "name": "alice"})
```

A key no row was put for doesn’t reach the computation — just as in production, where the worker joins what it found and nothing beyond that. For a complete set of tests, see [external_state_join]({{source-root}}/yt/yt/flow/examples/go/external_state_join/lookup_join_test.go).

## Analyzing the results {#analyzing-response}

The `*flowtest.Response` returned by `Process` is what the run produced: the collected output and the states in the form they will be saved in.

| Method | What it returns |
|-------|----------------|
| `Groups()` | `[]flow.OutputGroup` — the output groups in the order they appeared. |
| `Messages()` | The output messages of all groups, in order. |
| `MessagesOn(streamID)` | The output messages of a single stream. |
| `Rows()` | The payloads of the output messages, decoded into `flowtest.Row` and aligned with `Messages()`. |
| `Distribute()` | The [distribute](distribute.md) flag of every message, aligned with `Messages()`. |
| `Timers()` | `[]flow.TimerRequest` — the timers the computation asked the worker to set. |

An output group is the lineage of the output, not the shape of the input: `RowFunction` opens one group per input, `BatchFunction` one per batch, and groups nothing was written into are dropped.

The states are read like this:

| Method | What it returns |
|-------|----------------|
| `InternalStateRaw(name, key)` | The bytes the internal state stores for the key. |
| `InternalStateYSON(name, key, dst)` | Deserializes the YSON of the internal state into `dst`. |
| `InternalStateProto(name, key, dst)` | Deserializes the protobuf message of the internal state into `dst`. |
| `InternalStateReset(name, key)` | The run cleared the state of the key. |
| `InternalStateWritten(name)` | The run wrote into the state: only what was written reaches the worker. |
| `InternalStateLen(name)` | The number of keys the state was read or written for. |
| `ExternalState(name, key)`, `ExternalStateRow(name, key)` | The row of the external state — as a `flow.Payload` and as a `flowtest.Row`. |
| `ExternalStateReset(name, key)`, `ExternalStateWritten(name)`, `ExternalStateLen(name)` | The same for the external state. |
| `JoinedExternalState(name, key)`, `JoinedExternalStateRow(name, key)` | The row of the joined external state. |

The state is reported as it will be saved: a record cleared by the run reads as absent, and `*Reset` is what tells it apart from one that never existed.

## Running unit tests {#running-unit-tests}

Unit tests are `SMALL` tests and need no cluster.

{% if audience == "internal" %}

```bash
cd yt/yt/flow
ya test examples/go/word_count
```

You can filter a single test by name:

```bash
ya test examples/go/word_count -F 'TestCounterSurvivesTheBatch'
```

{% else %}

```bash
cd yt/yt/flow/examples/go/word_count
go test ./...
```

You can filter a single test by name:

```bash
go test ./... -run 'TestCounterSurvivesTheBatch'
```

{% endif %}

## Integration testing with FlowTestGoBase {#e2e-tests}

For full integration testing of a pipeline (with real C++ workers, queues, and streams), use the `FlowTestGoBase` base class — a Python test that runs the same Go binary that goes to production.

In such a test, the pipeline is started by the runner, not by the test itself: the Go binary starts as `./word_count --config pipeline.yson --flow-bin flow_server`, enriches the [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec), and hands control over to `flow_server`, which installs it. The companion in the job is brought up by the worker — exactly as in production.

### Dependencies {#integration-dependencies}

An integration test needs a cluster recipe, `DEPENDS` on the pipeline binary and `flow_server`, and `DATA` with the spec. The full `ya.make` of the test from [WordCount](examples/wordcount.md):

{% code '/yt/yt/flow/examples/go/word_count/test/ya.make' lang='text' %}

### Setup {#go-test-setup}

The test inherits from `FlowTestGoBase` and sets the `GO_COMPANION_BINARY` attribute:

{% code '/yt/yt/flow/examples/go/word_count/test/test_wordcount.py' lang='python' lines='[BEGIN test_setup]-[END test_setup]' %}

| Attribute | Description |
|---------|----------|
| `GO_COMPANION_BINARY` | The path to the Go pipeline binary: the same binary is both the runner and the companion. |
| `VANILLA_WORKER_PORT_COUNT` | The number of ports per worker; `3` by default — rpc, monitoring, and the port the worker brings the companion up on. |

The pipeline is started by the `start_flow_process_federation` method, which is given the spec through the `--config` argument; the base class sets `--flow-bin` itself. For a local federation, it also writes the path to the built binary into the companion resources, so that the worker starts it from disk.

[An example of the WordCount E2E test]({{source-root}}/yt/yt/flow/examples/go/word_count/test/test_wordcount.py)

{% note warning %}

Integration tests require a deployed {{product-name}} cluster and are of the `MEDIUM` size, so they are run with `ya test -tt`. For fast iteration, use the unit tests described above.

{% endnote %}

{% include notitle [_](../../_includes/flow/testing-integration-body.md) %}

{% include notitle [_](../../_includes/flow/testing-test-param-body.md) %}

## See also

- [Computation (Go)](computation.md)
- [Working with states (Go)](state.md)
- [State Accessor (Go)](state-accessor.md)
- [Examples: Word Count (Go)](examples/wordcount.md)
- If you are working on Flow itself — [Pipeline testing framework](../../flow/contributor/testing-framework.md).
