# Swift in {{product-name}} Flow

**Swift** is a data processing principle in {{product-name}} Flow where the result of a [computation](../../../flow/concepts/glossary.md#computation) **isn’t saved in {{product-name}}**. Instead, the transformation function must be strictly deterministic: if you need the result again (for example, when restarting a job), it’s recomputed from the same input data.

{% if audience == "internal" %}

The principle originated in [BigRT](https://docs.yandex-team.ru/big_rt/) and was moved to Flow.

{% endif %}

## Why you need it {#motivation}

In the classic approach (`TTransformComputation`), each [epoch](../../../flow/concepts/glossary.md#epoch) ends with a transactional write of results to {{product-name}}. This ensures exactly-once delivery but puts a load on the cluster: for each input message, the system performs a lookup and writes to the deduplication table, plus one write for each output message.

Swift removes this limitation: if the function is deterministic, you don’t need to store its output — you can reproduce it when needed. This lets you:

- Reduce the load on {{product-name}} to zero or a minimum (only metadata).
- Increase throughput for stateless transformations.

## How exactly-once guarantees are maintained {#exactly-once}

Even though output data isn’t written to {{product-name}}, the [exactly-once](../../../flow/concepts/glossary.md#exactly-once) guarantees are preserved thanks to determinism:

- If a job fails before delivering the result, Flow restarts it and gets **the same output** from the same input data.
- The [Message Distributor](../../../flow/concepts/glossary.md#message-distributor) keeps delivering the message until it receives an acknowledgment (`MarkPersisted`) from the receiver, which prevents data loss.

So, exactly-once is ensured not by storing the output but by the **idempotency** of the computation.

## Determinism requirement {#determinism}

The transformation function in a Swift computation must be **deterministic**: for the same input data, it must return the same output, including the order of messages.

The requirement also covers the binding of output messages to their parents (`set_parent_ids`): on recomputation, each output group must get the same parents in the same order. A typical mistake is iterating over an unordered structure (such as a hash table or a set) when grouping the batch by keys: the order of groups changes from run to run, and the recomputation produces a different output. Note that by default every output message of a Swift computation must have exactly one parent; multiple parents are allowed only with [`allow_batching_with_relaxed_guarantees`](../../../flow/concepts/guarantees.md#swift-allow-batching-with-relaxed-guarantees) — see [When to set lineage explicitly](../../../flow/concepts/lineage.md#explicit-lineage) for details.

{% note warning %}

Breaking determinism when updating a pipeline without a [drain](../../../flow/concepts/glossary.md#start-stop-pause-pipeline) can lead to duplicates or loss of intermediate messages: different parts of the system might process different versions of the output.

{% endnote %}

There can be exceptions to this rule, driven by the pipeline’s business logic specifics. But the developer of the business logic must clearly understand why they’re implementing them and what mechanisms ensure the pipeline’s overall result stays correct.

## Swift computation classes {#classes}

Flow implements two base Swift classes:

### TSwiftMapComputation {#swift-map}

A deterministic Map without materializing results in {{product-name}}.

- **Load on {{product-name}}:** ~0 writes per epoch (only system background processes).
- **Doesn’t support:** [Source](../../../flow/concepts/glossary.md#source), [Sink](../../../flow/concepts/glossary.md#sink).
- **Supports:** [timers](../../../flow/concepts/glossary.md#timer) and [key-visitor streams](../../../flow/concepts/key_visitor.md) — only for working with [state](../../../flow/concepts/glossary.md#state), for example, for background cleanup (GC). Emitting output messages from timer or visit processing is **prohibited**: the output stream can’t depend on a timer or visit stream in `streams_dependency`. Since timer streams are added by default to the dependencies of each output, a spec with timers and outputs must explicitly define `streams_dependency`.
- **Requires:** strict determinism and that each resulting message has exactly one parent — the input message.

For more details, see the [Computation (C++)](../../../flow/cpp/computation.md#tswiftmapcomputation) section.

### TSwiftPassthroughComputation {#swift-passthrough-map}

A [Passthrough computation](../../../flow/concepts/glossary.md#passthrough) that inherits from `TSwiftMapComputation`. It converts `input` messages to the `output` stream schema without custom logic. For more details, see [Computation (C++)](../../../flow/cpp/computation.md#tswiftpassthroughcomputation).

### TSwiftOrderedSourceComputation {#swift-source}

The main class for reading ordered data from external sources.

- **Load on {{product-name}}:** ~1–2 writes per epoch (metadata for recovery; the messages themselves aren’t stored).
- **Supports:** `WatermarkStrategy` for evaluating [watermarks](../../../flow/concepts/glossary.md#timestamps-and-watermarks).
- **Requires:** exactly one [Source](../../../flow/concepts/glossary.md#source) that implements `IOrderedSource`.

For more details, see the [Computation (C++)](../../../flow/cpp/computation.md#tswiftorderedsourcecomputation) section.

### TSwiftPassthroughOrderedSourceComputation {#swift-passthrough-source}

A Passthrough computation that inherits from `TSwiftOrderedSourceComputation`. It converts `source` messages to the `output` stream by mapping them to a new schema. For more details, see [Computation (C++)](../../../flow/cpp/computation.md#tswiftpassthroughorderedsourcecomputation).

## Comparison with TTransformComputation {#comparison}

| Type | Writes to {{product-name}} per epoch | Timer support | State support | Determinism requirement |
|------|--------------------------------------|---------------|---------------|-------------------------|
| `TTransformComputation` | 2 per input message and 1 per output message | Yes | Yes | No |
| `TSwiftOrderedSourceComputation` | ~1–2 (metadata) | No | No | Yes |
| `TSwiftPassthroughOrderedSourceComputation` | ~1–2 (metadata) | No | No | Yes |
| `TSwiftMapComputation` | ~0 | Yes (state only) | Yes | Yes |
| `TSwiftPassthroughComputation` | ~0 | No | No | Yes |

## See also

- [Computation](../../../flow/concepts/computation.md)
- [Processing guarantees](../../../flow/concepts/guarantees.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (Go)](../../../flow/go/computation.md)