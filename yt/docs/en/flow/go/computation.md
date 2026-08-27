# Computation in {{product-name}} Flow (Go)

{% note info %}

This page describes Go-specific details of working with computations. For general concepts, see the [Computation](../../flow/concepts/computation.md) section.

{% endnote %}

## Computation types {#computation-types}

In Flow, there are two kinds of `Computation`: [`Swift`](../../flow/concepts/glossary.md#swift) and `Transform`. Your choice determines how exactly-once guarantees are provided and what transformations you can implement with them.

| Type | Guarantee approach | Use case |
|-----|-----------------------------|------------|
| `Swift`| The transformation code is deterministic and will be called again if needed | Stateless transformations |
| `Transform` | The result is always stored in YT, so no determinism requirements apply to the transformations | Stateful transformations [Learn more](../../flow/concepts/stateful.md) |

When using a [companion](../../flow/concepts/glossary.md#companion), you select `Swift` or `Transform` by specifying `computation_class_name` in the static [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec):

- `NYT::NFlow::NCompanion::TTransformCompanionComputation` — for `Transform`.
- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation` — for `Swift`.
- `NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation` — for a `Swift` source.
- `NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation` — for a `Transform` source.

On the Go side, the choice of constructor doesn’t select `Swift` versus `Transform`: it selects what the computation is declared as to the [worker](../../flow/concepts/glossary.md#worker), a source or a transform. `Swift` and `Transform` computations are created by the same constructors, and it is `computation_class_name` in the spec that tells them apart.

| Constructor | Type reported to the worker | `computation_class_name` in the spec |
|-------------|-------------------------|----------------------------------|
| `flow.NewRowComputation(id, fn)` | `Transform` | `TTransformCompanionComputation` or `TSwiftMapCompanionComputation` |
| `flow.NewBatchComputation(id, fn)` | `Transform` | `TTransformCompanionComputation` or `TSwiftMapCompanionComputation` |
| `flow.NewRowSourceComputation(id, fn)` | `Source` | `TSwiftOrderedSourceCompanionComputation` or `TTransformOrderedSourceCompanionComputation` |
| `flow.NewBatchSourceComputation(id, fn)` | `Source` | `TSwiftOrderedSourceCompanionComputation` or `TTransformOrderedSourceCompanionComputation` |

For a source, `TSwiftOrderedSourceCompanionComputation` is suitable only for deterministic processing without user state. If a SourceComputation uses [internal state](state.md) or non-deterministic logic, specify `TTransformOrderedSourceCompanionComputation` in the spec: the worker materializes the output and commits it together with the state and the source offset. The internal state key in such a computation is the source partition key.

## Creating a Computation {#computation}

A computation is created by a constructor and registered in `flow.Pipeline` through `pipeline.Add`. Here is an example from [Shuffle](examples/shuffle.md), where the companion serves both ends of the pipeline — the source and the transform:

{% code '/yt/yt/flow/examples/go/shuffle/main.go' lang='go' %}

The constructors take two required parameters:

| Parameter | Required | Description |
|----------|:---:|----------|
| `id` | Yes | The computation identifier, matching the key in `computations` of the static spec |
| `fn` | Yes | The value that holds the processing logic: `flow.RowFunction` or `flow.BatchFunction` |

{% note warning %}

`fn == nil` is not allowed: the constructor panics on the spot. A computation without a processing function would fail every batch sent to it, and there is no longer any way to report such an error over the protocol.

If you need [passthrough](../../flow/concepts/glossary.md#passthrough), don’t register the computation in Go at all. Instead, specify the C++ passthrough class in `computation_class_name` in the static spec (see [Passthrough Computation](../../flow/concepts/computation.md#passthrough)).

{% endnote %}

In the static spec, you create a Computation with the same `id` (in this example, `mapper`):

```yson
"mapper" = {
    "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    "group_by_schema" = [
        ...
    ];
    "input_stream_ids" = [...];
    "output_stream_ids" = [...];
    "required_resource_ids" = {
        "CompanionManager" = {
            "worker" = true;
            "controller" = false;
        };
    };
    "parameters" = {
        ...
    };
};
```

For more on specs, see the [Spec, DynamicSpec, and Config](../../flow/concepts/spec.md) section.

## SourceComputation {#sourcecomputation}

`SourceComputation` is the node in the [pipeline](../../flow/concepts/glossary.md#pipeline) graph that reads data from external sources. On the worker side it corresponds to [TSwiftOrderedSourceComputation](../../flow/concepts/computation.md#tswiftorderedsourcecomputation) or [TTransformOrderedSourceComputation](../../flow/concepts/computation.md#ttransformorderedsourcecomputation).

In Go, a source is created by the `flow.NewRowSourceComputation` and `flow.NewBatchSourceComputation` constructors. The processing function interface of a source is the same as that of a transform: a source differs from a transform only in how it is declared to the worker.

### Creating a SourceComputation {#creating-sourcecomputation}

```go
pipeline.Add(flow.NewRowSourceComputation("reader", &eventMapper{}))
```

For a passthrough source, don’t use Go. Instead, specify `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` in `computation_class_name` in the spec, and leave the computation unregistered in the Go companion. For details, see [Passthrough Computation](../../flow/concepts/computation.md#passthrough).

### Interaction with Worker {#companion-info}

When initializing, the [worker](../../flow/concepts/glossary.md#worker) requests from the Go companion the list of registered computations together with their type (`Source` or `Transform`). The list is fixed when the gRPC server starts: a computation added to the `Pipeline` after the start is no longer visible to the worker.

After that, the worker addresses the computation by its `id` and sends batches of inputs. The worker reports the streams a source supplies messages to not once per job but on every request, so `rt.StreamSpecs()` and `rt.MessageBuilder(...)` in a source always describe the current request.

## Process Function {#process-function}

Data processing logic is implemented in a Process Function. Choose one of two interfaces: [`flow.RowFunction`]({{source-root}}/yt/go/flow/computation.go) or [`flow.BatchFunction`]({{source-root}}/yt/go/flow/computation.go).

{% note info %}

Choosing `RowFunction` or `BatchFunction` is purely a matter of business logic. `RowFunction` adds no data processing overhead compared to `BatchFunction`, because Flow passes data in batches internally.

{% endnote %}

Every handler receives four arguments:

| Argument | Description |
| --- | --- |
| `ctx context.Context` | The context of the request the input arrived in. It has a deadline and is canceled when the worker abandons the batch |
| `rt flow.Runtime` | Access to [states](state.md), computation parameters, [watermarks](../../flow/concepts/watermarks.md), and stream schemas |
| `msg` / `timer` / `visit` | The input being processed, together with the [key](../../flow/concepts/glossary.md#key) it is grouped by |
| `out flow.OutputCollector` | Collecting output messages and setting [timers](../../flow/concepts/glossary.md#timer) |

An error returned by a handler stops the processing of the rest of the batch: the worker retries the whole request, so a partial response would cause already processed inputs to be counted twice. The companion returns the `INTERNAL` gRPC error to the worker with the original text and by default writes it to `stderr`; the logger can be replaced with the `flow.WithLogger` option (see [Node companion](getting-started.md#node-companion)).

### RowFunction {#row-function}

`flow.RowFunction` receives [messages](../../flow/concepts/glossary.md#message) one at a time. The interface declares a single method, `OnMessage`.

#### Typed YSON messages {#typed-yson-messages}

The Go counterpart of C++ `TYsonMessage` is a Go structure with an embedded `flow.YSONMessage`. Payload fields are described by `yson` tags:

```go
type numberMessage struct {
    flow.YSONMessage

    Number int64 `yson:"number"`
}

type doubledMessage struct {
    flow.YSONMessage

    NumberX2 int64 `yson:"number_x2"`
}
```

These structures define both the handler API and the stream schemas. The runner adds the derived schemas to the spec before starting `flow_server`:

```go
pipeline.AddStreams(
    flow.NewYSONStream[numberMessage]("numbers"),
    flow.NewYSONStream[doubledMessage]("x2_numbers"),
)
```

In the handler, the input is decoded into a structure, and the output is created and encoded without working with `Payload` and `MessageBuilder` directly:

```go
type x2Mapper struct{}

var _ flow.RowFunction = (*x2Mapper)(nil)

func (*x2Mapper) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    var input numberMessage
    if err := msg.ConvertTo(&input); err != nil {
        return err
    }

    output := flow.NewYSONMessage[doubledMessage]("x2_numbers")
    output.NumberX2 = input.Number * 2

    encoded, err := flow.ConvertFrom(rt, output)
    if err != nil {
        return err
    }
    out.AddMessage(encoded)
    return nil
}
```

`msg.ConvertTo(&input)` transfers the stream identifier, the timestamps, and the input ID into `input.Meta`. The key of a message, timer, or visit is converted by the same method: `msg.Key.ConvertTo(&key)`. `flow.NewYSONMessage` sets the output stream; the event and system timestamps can be changed through `output.Meta` before the `flow.ConvertFrom` call if needed.

The low-level `flow.Payload`, `flow.PayloadBuilder`, and `rt.MessageBuilder` remain available for dynamic schemas and column-wise processing.

#### Stateful function example {#stateful-example}

The function from [WordCount](examples/wordcount.md) counts the occurrences of every word in the key’s [state](../../flow/concepts/glossary.md#state):

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

### Optional handlers {#optional-handlers}

[Timers](../../flow/concepts/glossary.md#timer) and visits from [key visitor streams](../../flow/concepts/key_visitor.md) are handled by separate interfaces declared on the same type:

| Interface | Method | Input |
| --- | --- | --- |
| `flow.RowTimerFunction` | `OnTimer(ctx, rt, timer, out)` | `flow.Timer` |
| `flow.RowVisitFunction` | `OnVisit(ctx, rt, visit, out)` | `flow.Visit` |

This is how handler optionality is expressed in Go: a computation declares only the methods it needs. The worker delivers timers and visits according to the computation’s spec, and the Go SDK skips those whose handler isn’t implemented. User structures implement these interfaces on a pointer, so that the value isn’t copied when the methods are called; the `var _ flow.RowFunction = (*myFunction)(nil)` check fixes the contract at compile time.

```go
type urlDownloadFunction struct{}

var (
    _ flow.RowFunction      = (*urlDownloadFunction)(nil)
    _ flow.RowTimerFunction = (*urlDownloadFunction)(nil)
)

// The required handler: the type implements flow.RowFunction.
func (*urlDownloadFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    // ...
    out.AddTimer(flow.TimerRequest{TriggerTimestamp: uint64(time.Now().Add(flushDelay).Unix())})
    return nil
}

// Declaring OnTimer on the same type adds timer handling.
func (*urlDownloadFunction) OnTimer(
    ctx context.Context,
    rt flow.Runtime,
    timer flow.Timer,
    out flow.OutputCollector,
) error {
    // ...
    return nil
}
```

For complete examples, see [URL Downloader](examples/url_downloader.md) and [Wait Click Join](examples/wait_click_join.md).

### BatchFunction {#batch-function}

`flow.BatchFunction` receives the whole batch of messages that arrived from the [worker](../../flow/concepts/glossary.md#worker) in a single call of the `OnMessages` method. Timers and visits are handled by the `flow.BatchTimerFunction` (`OnTimers`) and `flow.BatchVisitFunction` (`OnVisits`) interfaces.

A batch corresponds to one worker request and may contain messages with different [keys](../../flow/concepts/glossary.md#key); per-key grouping, if needed, is done in user code (see [Companion](../../flow/concepts/companion.md#schema)).

#### Batch function example {#batch-example}

```go
type x2BatchMapper struct{}

var _ flow.BatchFunction = (*x2BatchMapper)(nil)

func (*x2BatchMapper) OnMessages(
    ctx context.Context,
    rt flow.Runtime,
    msgs []flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    for _, msg := range msgs {
        var input numberMessage
        if err := msg.ConvertTo(&input); err != nil {
            return err
        }

        output := flow.NewYSONMessage[doubledMessage]("x2_numbers")
        output.NumberX2 = input.Number * 2
        encoded, err := flow.ConvertFrom(rt, output)
        if err != nil {
            return err
        }
        out.AddMessage(encoded)
    }
    return nil
}
```

Unlike in `RowFunction`, the output of a batch function relates to the batch as a whole: the [lineage](../../flow/concepts/lineage.md) of the output messages consists of the identifiers of all the batch inputs, not of one. A row function is called per input, and its output relates to exactly that input.

### Functions without their own type {#function-adapters}

A computation that needs neither its own fields nor timer and visit handlers doesn’t have to declare a type: a plain function is passed through the `flow.RowFunc` and `flow.BatchFunc` adapters.

```go
pipeline.Add(flow.NewRowComputation("mapper", flow.RowFunc(
    func(
        ctx context.Context,
        rt flow.Runtime,
        msg flow.ExtendedMessage,
        out flow.OutputCollector,
    ) error {
        return nil
    },
)))
```

## Message filtering {#message-filtering}

Messages in source computations are filtered with the per-message [distribute](distribute.md) flag: the message is emitted from the Process Function by calling `out.AddUndistributedMessage(msg)` and isn’t published further along the graph, but is accounted for in [watermark](../../flow/concepts/watermarks.md) evaluation.

The worker reads the flag only on the source path. A transform publishes a message regardless of the flag, so filtering in a transform simply means not calling `out.AddMessage`.

## Registering in a Pipeline {#pipeline-registration}

All computations are registered through `pipeline.Add`, which takes them as a variadic argument:

```go
pipeline := flow.NewPipeline()

pipeline.Add(
    // A Transform computation
    flow.NewRowComputation("reducer", &eventReducer{}),
    // A Source computation
    flow.NewRowSourceComputation("reader", &eventMapper{}),
)
```

Assemble the `Pipeline` from a single goroutine and only then pass it to `pipeline.Run()`.

{% note warning %}

Every Computation must have a unique identifier matching the identifiers in the static spec. Registering two computations with the same `id` results in the `flow.ErrDuplicateComputation` error while the server is being built, and the companion cannot start.

{% endnote %}

A single `Computation` value — and therefore a single function bound to it — serves all requests for that identifier. The worker processes computation partitions in parallel, and every request is served by its own goroutine, so a function that keeps state between calls synchronizes that state itself.

## Goroutines in a handler {#goroutines}

The handler already runs in its own goroutine. If you need additional parallelism inside it, start child goroutines through `flow.Go(ctx, fn)` rather than with the `go` statement: this way the companion keeps the consumed CPU and memory attributed to the current job.

`flow.Go` only starts the function. The handler waits for all child goroutines itself, collects their errors, and finishes them before it returns. Fire-and-forget work is not allowed: after the return, the request context is canceled, and the result can no longer be added to the response to the worker.

```go
results := make(chan result, len(requests))
var wg sync.WaitGroup

for index, request := range requests {
    wg.Add(1)
    flow.Go(ctx, func(ctx context.Context) {
        defer wg.Done()
        value, err := callService(ctx, request)
        results <- result{index: index, value: value, err: err}
    })
}

wg.Wait()
close(results)
```

`flow.Runtime`, the state accessors, and `OutputCollector` are not designed for concurrent use. In child goroutines, perform only independent business logic or I/O; read and change state and collect output messages in the original handler goroutine after `wg.Wait()`.

## Runtime {#runtime}

[Source code]({{source-root}}/yt/go/flow/context.go)

`flow.Runtime` (`rt`) gives access to the computation execution context:

| Method | Description |
| --- | --- |
| `rt.MessageBuilder(streamID)` | Create a `MessageBuilder` for the given output [stream](../../flow/concepts/glossary.md#stream-and-computation) |
| `rt.Parameters()` | Computation parameters from the static spec |
| `rt.DynamicParameters()` | Computation parameters from the dynamic spec |
| `rt.KeySchema()` | The schema of the [key](../../flow/concepts/glossary.md#key) the batch is grouped by |
| `rt.StreamSpecs()` | The computation streams and their schemas |
| `rt.MinWatermark()` | The minimum [watermark](../../flow/concepts/glossary.md#timestamps-and-watermarks) across all input streams |
| `rt.Watermark(streamID)` | The [watermark](../../flow/concepts/glossary.md#timestamps-and-watermarks) of a specific stream |
| `rt.InternalState(name)` | The holder of an internal [state](../../flow/concepts/glossary.md#state) |
| `rt.ExternalState(name)` | The holder of an external state owned by the computation |
| `rt.JoinedExternalState(name)` | The holder of a joined external state (read-only) |

Holders are a low-level interface: in user code, the key’s state is opened by the `flow.OpenYSONState`, `flow.OpenProtoState`, `flow.OpenRawState`, and `flow.OpenExternalState` accessors. For details, see the [Working with states (Go)](state.md) and [State Accessor (Go)](state-accessor.md) sections.

### The low-level MessageBuilder {#message-builder}

For dynamic schemas, an output message can be created through `MessageBuilder`:

```go
builder, err := rt.MessageBuilder("stream_id")
if err != nil {
    return err
}

msg, err := builder.Set("field_name", value).Finish()
if err != nil {
    return err
}

out.AddMessage(msg)
```

The `Finish()` method returns a ready `flow.Message` without modifying the builder. The `stream_id` identifier must be present in the `output_stream_ids` list of the computation’s static [spec](../../flow/concepts/glossary.md#spec-and-dynamic-spec); otherwise `rt.MessageBuilder` returns `flow.ErrUnknownStream`.

The builder is typed by the stream schema: `Set` converts the given value to the wire type of the column. A row is assembled one column at a time, so `Set` returns the builder itself rather than an error: the first rejected value is remembered, the subsequent `Set` calls do nothing, and the error is returned from `Finish()` — `flow.ErrTypeMismatch` if the value doesn’t fit into the column, and `flow.ErrColumnNotFound` if the schema has no such column. Values of `any` and composite columns are serialized to YSON, and a `[]byte` written into such a column is treated as already serialized YSON.

A whole row is written with a single `builder.SetStruct(v)` call: the columns are taken from the yson tags of the `v` structure — the same tags [states](state.md) are serialized by. A column that isn’t in the stream schema is rejected as an error. The reverse operation is `payload.ConvertTo(&v)`: it fills the structure fields from the columns of the same name, leaving as is those that aren’t in the row.

`builder.SetEventTimestamp(ts)` and `builder.SetSystemTimestamp(ts)` are also available. The worker fills both fields by default; `SetSystemTimestamp` is usually not needed in user code.

### Computation parameters {#parameters}

`flow.Parameters` are the parameters from the spec, left unserialized: only the computation itself knows what its configuration looks like.

```go
var waitForActions bool
if err := rt.Parameters().Get("wait_for_actions", &waitForActions); err != nil {
    return err
}
```

`Get(name, dst)` deserializes the parameter from YSON into `dst` and returns `flow.ErrParameterNotFound` if the parameter is absent. Use the `Has(name)` method to check whether a parameter is present, and `Names()` to get the list of the names that are set.

### Watermarks {#watermarks}

```go
// The minimum watermark across all input streams
minWatermark := rt.MinWatermark()

// The watermark of a specific stream
watermark, ok := rt.Watermark("stream_id")
```

The second value returned by `rt.Watermark` tells whether the request reported the watermark of that stream. `rt.MinWatermark()` is zero if the request reported no watermarks at all: event time hasn’t moved yet.

## OutputCollector {#output-collector}

[Source code]({{source-root}}/yt/go/flow/output.go)

`flow.OutputCollector` is used to send processing results:

| Method | Description |
| --- | --- |
| `out.AddMessage(msg)` | Add an output message (a `flow.Message` value obtained from `builder.Finish()`) |
| `out.AddUndistributedMessage(msg)` | Add a source message with `distribute = false` |
| `out.AddTimer(timer)` | Set a [timer](../../flow/concepts/glossary.md#timer) on the key being processed |
| `out.WithParentIDs(parentIDs...)` | Return a collector that writes into a separate group with the given [lineage](../../flow/concepts/lineage.md) |

An example of creating an output message and a timer:

```go
func (*myFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    output := flow.NewYSONMessage[outputMessage]("output_stream")
    output.Field = value
    encoded, err := flow.ConvertFrom(rt, output)
    if err != nil {
        return err
    }
    out.AddMessage(encoded)

    // Creating a timer
    out.AddTimer(flow.TimerRequest{TriggerTimestamp: 1000, EventTimestamp: 500})
    return nil
}
```

The `StreamID` field of `flow.TimerRequest` selects the timer stream; an empty value means the pipeline’s only timer stream.

`OutputCollector` is not designed for concurrent use: the collector belongs to the goroutine serving the request.

## ExtendedMessage {#extended-message}

An incoming [message](../../flow/concepts/glossary.md#message) (`flow.ExtendedMessage`) contains:

- `msg.ConvertTo(&value)` — converts the message payload into a structure with an embedded `flow.YSONMessage`.
- `msg.Key` — the message [key](../../flow/concepts/glossary.md#key) from `group_by_schema`; the key structure is filled through `msg.Key.ConvertTo(&key)`.
- `msg.StreamID` — the identifier of the input [stream](../../flow/concepts/glossary.md#stream-and-computation) (`string`).
- `msg.EventTimestamp` — the event timestamp of the message (`uint64`).
- `msg.SystemTimestamp` — the time the message was created (`uint64`).
- `msg.ID` — the message identifier assigned by the worker (`string`).

For dynamic schemas, you can work with `msg.Payload` directly. This low-level API provides the `Int64`, `Uint64`, `Float64`, `Bool`, `String`, `Bytes`, `Any(column, dst)`, `Has(column)`, and `Columns()` accessors.

## Timer {#timer}

A [timer](../../flow/concepts/glossary.md#timer) value (`flow.Timer`) contains:

- `timer.Key` — the timer [key](../../flow/concepts/glossary.md#key): `timer.Key.String("host")`.
- `timer.StreamID` — the identifier of the timer stream (`string`).
- `timer.TriggerTimestamp` — the trigger time (`uint64`).
- `timer.EventTimestamp` — the event timestamp (`uint64`).

A visit, `flow.Visit`, is arranged the same way but without a trigger time: it carries `Key`, `StreamID`, and the timestamps. For details, see [Key Visitor Streams](../../flow/concepts/key_visitor.md).

## Configuring the CompanionManager resource {#companion-manager}

To start a Go companion, declare the `CompanionManager` resource in the static spec:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
    "parameters" = {
        "entrypoint" = {
            "executable" = "./go_companion";
        };
    };
    "dependencies" = {};
};
```

The `resource_class_name` parameter points to the resource class that will start the companion.
For a Go companion, `resource_class_name` must always be `NYT::NFlow::NCompanion::TCompanionManager`.

The companion process is described by the `entrypoint` parameter (`executable`, `args`, `env`); the worker starts the companion itself and monitors its lifecycle. When [starting a pipeline from a host](getting-started.md#launch) through `pipeline.Run()`, you don’t need to fill in `entrypoint` manually: the Go binary writes `entrypoint = {"executable" = "./go_companion"}` itself, and `flow_server` delivers the binary to the job under that name.

The `companion_process_count` parameter is accepted and validated by a Go companion but sets nothing: pre-forking is needed by Python because of the GIL, whereas a Go companion serves requests concurrently with goroutines. For details, see [Companion parallelism](getting-started.md#companion-process-count).

For more on specs, see the [Spec, DynamicSpec, and Config](../../flow/concepts/spec.md) section.

## See also

- [Computation (concept)](../../flow/concepts/computation.md)
- [Getting started (Go)](getting-started.md)
- [Working with states (Go)](state.md)
- [The distribute flag (Go)](distribute.md)
- [Companion](../../flow/concepts/companion.md)
