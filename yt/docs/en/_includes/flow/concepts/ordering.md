# Message processing order in {{product-name}} Flow

In a distributed streaming system, it’s not practical to ensure a strict global order for all messages: data comes from multiple sources and is processed in parallel across different [partitions](../../../flow/concepts/glossary.md#partition) and [computations](../../../flow/concepts/glossary.md#computation). Flow addresses this differently, using the `AlignmentTimestamp` mechanism.

Each message carries an [AlignmentTimestamp](#alignment-timestamp)—a monotonically increasing timestamp that defines the processing priority:

- **Within a single [stream](../../../flow/concepts/glossary.md#stream)**: messages are processed in the order `(AlignmentTimestamp, message_id)`—this gives a fully deterministic order.
- **Across different [streams](../../../flow/concepts/glossary.md#stream)**: a merging priority queue is used: messages older than [`stale_input_boost_age`](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicComputationSpec) go first, then ordering is by [stream priority](#stream-priorities) and then by [StabilizedEventTimestamp](#stabilized-event-timestamp) (calculated from `AlignmentTimestamp`) with an adjustment for [stream_delays](../../../flow/concepts/spec.md#inputordering); if values are equal, ordering uses `TaskId`.

In addition to prioritization, there’s an order guarantee for **derived messages**—see the [Order guarantees](#ordering-guarantees) section for details.

## AlignmentTimestamp {#alignment-timestamp}

Along with [SystemTimestamp and EventTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks), each message in Flow includes the `AlignmentTimestamp` field—a timestamp used to align processing progress across streams and partitions.

Rules for setting `AlignmentTimestamp`:

- **Messages from source streams**: equal to `WriteTimestamp`, which is the time when the message was written to the persistent queue (for example, in QYT{% if audience == "internal" %} or Logbroker{% endif %}).
- **Output messages from `TransformComputation`**: equal to `SystemTimestamp` at the time the message is created.
- **In all other cases**: inherited unchanged from parent messages.

## StabilizedEventTimestamp {#stabilized-event-timestamp}

`StabilizedEventTimestamp` is a computed timestamp equal to `AlignmentTimestamp` + `bias`, where `bias` is the average difference between `EventTimestamp` and `AlignmentTimestamp` within a stream. `bias` is calculated based on the messages in the stream that are currently being processed. It’s used as a non-decreasing substitute for `EventTimestamp` to prioritize the processing of messages from different streams.

## Order guarantees {#ordering-guarantees}

Message order in a stream isn’t guaranteed in general, but there is a specific guarantee for derived messages.

If, in the input stream, message A precedes message B, and they have derived messages A' and B' such that:

- their [lineage](../../../flow/concepts/lineage.md) keys match (meaning the [grouping keys](../../../flow/concepts/glossary.md#key) are the same in all intermediate computations along the path from the source to the current computation),
- and in the current computation, A' and B' share the same key,

then A' will be processed before B'.

## Partition prioritization within a single stream {#partition-prioritization}

Within a single source stream, partitions are prioritized by the `WriteTimestamp` of their messages: partitions with earlier messages are processed first. This prioritization is implemented through the processing order of output messages in subsequent computations.

## Cross-stream prioritization {#cross-stream-prioritization}

Across different streams, messages older than [`stale_input_boost_age`](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TDynamicComputationSpec) go first. Then prioritization is based on [stream priority](#stream-priorities) and, for equal priorities, on `EventTimestamp`, but indirectly: ordering actually happens by `AlignmentTimestamp` with a stream-specific adjustment and the delay from `stream_delays`.

The adjustment is calculated as the average difference between `EventTimestamp` and `AlignmentTimestamp` among the messages currently in all output buffers for that stream:

$$ordering\_priority = AlignmentTimestamp + avg(EventTimestamp − AlignmentTimestamp)$$

This approach accounts for each stream’s specifics (for example, the systematic delay between the time a message is written to the queue and the event time) and ensures a fairer cross-stream ordering.

## Stream priorities {#stream-priorities}

A computation can assign priorities to its input streams in the static spec parameter `input_ordering.stream_priorities`: integers from -15 to 15. A higher value means a more important stream; streams that are not listed have priority 0. For example, to keep a large non-urgent backfill from delaying the processing of current data:

```yson
input_ordering = {
    stream_priorities = {
        backfill = -1;
    };
};
```

Messages of different streams are compared by stream priority first, and only for equal priorities by `StabilizedEventTimestamp` with the delay from `stream_delays`. Messages of a lower-priority stream wait while higher-priority streams have ready messages, unless they are older than `stale_input_boost_age` (2 hours by default); such messages are processed first. Staleness is detected approximately, so some stale messages can wait longer than this threshold. Spare capacity is not left idle. Priorities also apply when messages are sent between jobs and in the [distributed throttler](../../../flow/concepts/distributed_throttler.md#priority).

### Side effects {#side-effects}

While a demoted stream waits to be processed:

- its watermark lags, and so do the watermarks of the streams and [timers](../../../flow/concepts/timers.md) that depend on it. This can trigger [watermark alignment](../../../flow/concepts/spec.md#watermark-alignment);
- its messages accumulate in buffers. Once the buffers are full, the computations that write this stream slow down, and with them their other output streams and the other subscribers of the demoted stream;
- the `input_messages` and `compact_input_messages` tables grow, because they are trimmed by the pipeline-wide watermark.

Messages older than `stale_input_boost_age` are processed first, which shortens these effects.

## See also

- [Computation](../../../flow/concepts/computation.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Timers](../../../flow/concepts/timers.md)