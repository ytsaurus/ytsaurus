# The distribute flag in {{product-name}} Flow (Go)

The `distribute` flag is a per-message flag that you set when you add an output [message](../../flow/concepts/glossary.md#message) in a [source computation](getting-started.md#computation-and-source). It controls whether the message is published further along the processing graph.

The `distribute` flag ensures:

- Correct [watermark](../../flow/concepts/watermarks.md) evaluation: messages with `distribute=false` are still accounted for by the watermark generator (unlike simply skipping a message in the handler, which breaks watermark evaluation).
- Assignment of deterministic identifiers to messages.

{% note warning %}

To filter a message in a source, don’t skip it in `OnMessage` — add it with `distribute=false` instead. This way, the message isn’t published further, but it remains accounted for in watermark evaluation.

{% endnote %}

## When to use distribute=false {#when-to-use}

Use the `distribute=false` flag when:

- You need to filter some of the output messages at the source computation stage.
- Correct watermark evaluation is important.

The `out.AddMessage(msg)` method publishes the message further. `out.AddUndistributedMessage(msg)` keeps it accounted for but doesn’t publish it.

## Usage {#usage}

The filtering logic moves into the processing function: a regular message is added with `AddMessage`, a filtered one with `AddUndistributedMessage`.

```go
type hitMessage struct {
    flow.YSONMessage
    HitID      uint64 `yson:"hit_id"`
    HitPayload string `yson:"hit_payload"`
}

// hitParsingFunction parses the input row and drops duplicates.
type hitParsingFunction struct{}

var _ flow.RowFunction = (*hitParsingFunction)(nil)

func (*hitParsingFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    var input hitMessage
    if err := msg.ConvertTo(&input); err != nil {
        return err
    }

    hit := flow.NewYSONMessage[hitMessage]("hit")
    hit.HitID = input.HitID
    hit.HitPayload = input.HitPayload
    encoded, err := flow.ConvertFrom(rt, hit)
    if err != nil {
        return err
    }

    // Duplicates are added but not published further.
    isDuplicate := input.HitPayload == "duplicate_payload"
    if isDuplicate {
        out.AddUndistributedMessage(encoded)
    } else {
        out.AddMessage(encoded)
    }
    return nil
}
```

The flag is set separately for every message, so within a single handler call some messages can be published and others not. The order of messages is preserved: a dropped message stays in the output group and takes part in the [lineage](../../flow/concepts/lineage.md), it just doesn’t go further along the graph.

## The flag is read only on the source path {#source-path-only}

The [worker](../../flow/concepts/glossary.md#worker) reads the `distribute` flag only on the source path. That is why the Go SDK rejects `AddUndistributedMessage` in a transform with the `flow.ErrDistributeOnTransform` error.

{% note info %}

A transform filters a message by simply not collecting it: don’t call `AddMessage` for it. The watermark isn’t affected — it is advanced by the input messages of the source, not by the output of a transform.

{% endnote %}

## Registering a source computation {#registration}

A source computation is created by the `flow.NewRowSourceComputation` constructor (or `flow.NewBatchSourceComputation`) and registered in the pipeline through `pipeline.Add`. No separate filtering parameter is needed — the decision about publishing is made in the processing function:

{% code '/yt/yt/flow/examples/go/shuffle/main.go' lang='go' %}

The type of a computation is whatever created it: a source differs from a transform only in how it is declared to the worker. The `distribute` flag is therefore taken into account for exactly those computations that were created by the source constructors.

## Checking in tests {#testing}

The flag values are visible in offline tests: the `Distribute()` method of the run result returns the flags in the same order as `Messages()` and `Rows()`.

```go
r := h.Process(
    h.Message("hits", flowtest.Row{"hit_id": uint64(1), "hit_payload": "payload"}),
    h.Message("hits", flowtest.Row{"hit_id": uint64(2), "hit_payload": "duplicate_payload"}),
)

require.Equal(t, []bool{true, false}, r.Distribute())
```

For details, see [Testing (Go)](testing.md).

## See also

- [Computation (Go)](computation.md)
- [Testing (Go)](testing.md)
- [Examples: Shuffle (Go)](examples/shuffle.md)
- [Watermarks](../../flow/concepts/watermarks.md)
- [The distribute flag (Python)](../../flow/python/distribute.md)
