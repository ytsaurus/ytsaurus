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

Even though output data isn’t written to {{product-name}}, [exactly-once](../../../flow/concepts/glossary.md#exactly-once) delivery to another Flow computation is preserved thanks to determinism:

- If a job fails before delivering the result, Flow restarts it and gets **the same output** from the same input data.
- The [Message Distributor](../../../flow/concepts/glossary.md#message-distributor) keeps delivering the message until it receives an acknowledgment (`MarkPersisted`) from the receiver, which prevents data loss.

So, exactly-once is ensured not by storing the output but by the **idempotency** of the computation and deduplication at the recipient.

`TSwiftMapComputation` does not support [Sink](../../../flow/concepts/glossary.md#sink). `TSwiftOrderedSourceComputation` supports direct delivery to an external sink. Deferred publications provide output-epoch finalization but do not by themselves upgrade at-least-once input to exactly-once.

## Determinism requirement {#determinism}

The transformation function in a Swift computation must be **deterministic**: for the same input data, it must return the same output, including the order of messages.

The requirement also covers [lineage](../../../flow/concepts/lineage.md) — the binding of output messages to their parents: on recomputation, each output must get the same parents in the same order. A typical mistake is iterating over an unordered structure (such as a hash table or a set) when splitting the batch into groups: the order of groups changes from run to run, and the recomputation produces a different output. By default every output message of a Swift computation has exactly one parent; multiple parents are allowed only with [`allow_batching_with_relaxed_guarantees`](../../../flow/concepts/guarantees.md#swift-allow-batching-with-relaxed-guarantees) and in [`TSwiftStableBatchSourceComputation`](#swift-stable-batch-source).

{% note warning %}

Breaking determinism when updating a pipeline without a [drain](../../../flow/concepts/glossary.md#start-stop-pause-pipeline) can lead to duplicates or loss of intermediate messages: different parts of the system might process different versions of the output.

{% endnote %}

There can be exceptions to this rule, driven by the pipeline’s business logic specifics. But the developer of the business logic must clearly understand why they’re implementing them and what mechanisms ensure the pipeline’s overall result stays correct.

By default, Flow appends the derived message’s sequence number to its message ID. If two replays of a Swift computation emit different messages at the same position, those messages get the same message ID. This can cause data loss or duplicates; if the messages have different keys, it can also violate Flow’s internal invariants. In C++, Java, Python, and Go, output options can remove emission order from message identity by selecting a payload hash or user-defined suffix (see the [C++ API description](../../../flow/cpp/process-functions.md#message-id-suffixes)). This prevents that particular ID collision only when the semantic suffix is stable; it doesn’t make a nondeterministic computation deterministic.

## Swift computation classes {#classes}

Flow implements the following Swift classes:

### TSwiftMapComputation {#swift-map}

A deterministic Map without materializing results in {{product-name}}.

- **Load on {{product-name}}:** ~0 writes per epoch (only system background processes).
- **Doesn’t support:** [Source](../../../flow/concepts/glossary.md#source) or [Sink](../../../flow/concepts/glossary.md#sink).
- **Supports:** [timers](../../../flow/concepts/glossary.md#timer) and [key-visitor streams](../../../flow/concepts/key_visitor.md) — only for working with [state](../../../flow/concepts/glossary.md#state), for example, for background cleanup (GC). Emitting output messages from timer or visit processing is **prohibited**: the output stream can’t depend on a timer or visit stream in `streams_dependency`. Since timer streams are added by default to the dependencies of each output, a spec with timers and outputs must explicitly define `streams_dependency`.
- **Requires:** strict determinism and that each resulting message has exactly one parent — the input message.

For more details, see the [Computation (C++)](../../../flow/cpp/computation.md#tswiftmapcomputation) section.

### TSwiftPassthroughComputation {#swift-passthrough-map}

A [Passthrough computation](../../../flow/concepts/glossary.md#passthrough) that inherits from `TSwiftMapComputation`. It converts `input` messages to the `output` stream schema without custom logic. For more details, see [Computation (C++)](../../../flow/cpp/computation.md#tswiftpassthroughcomputation).

### TSwiftOrderedSourceComputation {#swift-source}

The main class for reading ordered data from external sources.

- **Load on {{product-name}}:** ~1–2 writes per epoch (metadata for recovery; the messages themselves aren’t stored).
- **Supports:** [Sink](../../../flow/concepts/glossary.md#sink) and `WatermarkStrategy` for evaluating [watermarks](../../../flow/concepts/glossary.md#timestamps-and-watermarks). Direct delivery to an external sink without deduplication is at-least-once.
- **Requires:** exactly one [Source](../../../flow/concepts/glossary.md#source) that implements `IOrderedSource`.

For more details, see the [Computation (C++)](../../../flow/cpp/computation.md#tswiftorderedsourcecomputation) section.

### TSwiftStableBatchSourceComputation {#swift-stable-batch-source}

An ordered source on which a [process function](../../../flow/cpp/process-functions.md#stable-batch-aggregation) can aggregate the messages of one read, for example to collapse the messages of a hot key before they overload a consumer partition. Exactly-once is kept without materializing the output.

- **Read groups.** Every read from the source is one group: the function gets it whole in one call, and Flow publishes and completes it as a whole. The source advances past a group only when all of its output messages are delivered. The boundaries of every group that is not fully persisted are kept in the source state, so a replay after a restart reads exactly the same groups, and the function sees the same messages.
- **Group size** comes from the batching settings in the computation's dynamic spec: `batch_duration`, `max_rows_per_batch`, and `max_bytes_per_batch`. A read that is not full closes after `batch_duration`, and under backlog every read is full. For bigger groups, raise the row and byte limits. A replay reads each group whole, whatever the current batching settings, so lowering the limits does not split groups that were already read, and a replayed group takes about as much memory as at its first read.
- **Sources.** A source must return the same records when it rereads an offset range. Supported classes are `TQueueSource`{% if audience == "public" %}, `TYdbSource`, `TYdbFramingSource`, `TYdbMultiLineSource`{% endif %}{% if audience == "internal" %}, `TBigRTQueueSource`, `TLogbrokerSource`, `TLogbrokerFramingSource`, `TLogbrokerMultiLineSource`, `TLogbrokerBinaryEventLogSource`{% endif %}. A spec with any other source (for example, `TKafkaSource`, `TRandomSource` or the static table `TSource`) fails validation.
- **Message IDs.** An output message with several parents gets the ID of its max parent plus a suffix; the sequence number of the suffix counts per (max parent, stream) pair. The alignment timestamp is the max parent's, and the event timestamp is the minimum over the parents. If the function sets the parents per key through `SetParents`, the message IDs of a key keep their order from group to group. A `FromUserDefined(...)` suffix must be unique within a (max parent, stream) pair, and `FromPayloadHash()` is allowed only for a message with one parent.
- **Removing data from the head of the source** (a trim) or an external reposition is data loss, and it can cut into a group in flight. Output messages whose max parent survives keep their message IDs if the function sets the parents per key through `SetParents` or gives a key its own `FromUserDefined(...)` suffix. If every output has all messages of the group as its parents, the sequence numbers of the suffixes can shift after such a removal.
- **`skip_if_expression`.** A new expression applies to the groups first read after the change. A group that is read again is filtered with the expression in effect at its first read.
- **`malformed_message_policy`.** {% if audience == "public" %}YDB {% endif %}{% if audience == "internal" %}BigRT and Logbroker {% endif %}sources replay each group with the policy in effect at its first read.
- **The consumer** deduplicates output messages by message ID, as for any Swift computation.
- **Doesn't support:** timers, state, `watermark_alignment/read_delays`, [companions](../../../flow/concepts/companion.md), or [process function graphs](../../../flow/cpp/process-functions.md#function-graph).
- **Switching** an existing computation to or from this class is a static spec change. Do it on a [drained and stopped](../../../flow/concepts/glossary.md#start-stop-pause-pipeline) pipeline after the controller and worker binaries are updated.

### TSwiftPassthroughOrderedSourceComputation {#swift-passthrough-source}

A Passthrough computation that inherits from `TSwiftOrderedSourceComputation`. It converts `source` messages to the `output` stream by mapping them to a new schema. For more details, see [Computation (C++)](../../../flow/cpp/computation.md#tswiftpassthroughorderedsourcecomputation).

## Comparison with TTransformComputation {#comparison}

| Type | Writes to {{product-name}} per epoch | Timer support | State support | Determinism requirement |
|------|--------------------------------------|---------------|---------------|-------------------------|
| `TTransformComputation` | 2 per input message and 1 per output message | Yes | Yes | No |
| `TSwiftOrderedSourceComputation` | ~1–2 (metadata) | No | No | Yes |
| `TSwiftPassthroughOrderedSourceComputation` | ~1–2 (metadata) | No | No | Yes |
| `TSwiftStableBatchSourceComputation` | ~1–2 (metadata) | No | No | Yes |
| `TSwiftMapComputation` | ~0 | Yes (state only) | Yes | Yes |
| `TSwiftPassthroughComputation` | ~0 | No | No | Yes |

## See also

- [Computation](../../../flow/concepts/computation.md)
- [Processing guarantees](../../../flow/concepts/guarantees.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (Go)](../../../flow/go/computation.md)
