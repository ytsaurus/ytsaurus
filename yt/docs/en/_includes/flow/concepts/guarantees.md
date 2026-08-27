# Processing guarantees in {{product-name}} Flow

## Overview {#overview}

Flow provides **exactly-once semantics** for event processing by default. This means:

- Consumers get the result of processing each input message exactly once, with no losses or duplicates.
- The [state](../../../flow/concepts/stateful.md) is updated atomically along with message processing.

If needed, you can relax the semantics to [**at-least-once**](#at-least-once) or [**at-most-once**](#at-most-once) — for example, to reduce latency or lower the pipeline’s resource consumption.

## Exactly-once semantics {#exactly-once}

### How it works {#how-it-works}

Exactly-once is ensured by three mechanisms that work together.

#### 1. Lease transaction {#lease}

A `Lease` is a controller-owned master transaction that the controller creates for each [job](../../../flow/concepts/glossary.md#job). Users do not create the Lease or control its lifetime. The controller keeps the Lease active with periodic pings and checks the state of all Leases on every scheduling cycle.

Every ordinary transaction in which a job commits [epoch](../../../flow/concepts/glossary.md#epoch) data lists that job’s Lease as a prerequisite in `prerequisite_transaction_ids`. The Lease must remain active for the commit to succeed. A Lease ends in one of two ways. The controller aborts it whenever it removes the job: during partition rebalancing, after a job failure or a lost worker, or when the pipeline stops, pauses, or completes. A Lease also expires on its own if the controller stops keeping it alive with pings. Both routes activate the same barrier.

After the Lease is aborted or expires, commits guarded by that prerequisite are rejected, so a zombie job cannot write data after the controller no longer considers it the owner of its [partition](../../../flow/concepts/glossary.md#partition). Even when it has no data, a job periodically commits a nearly empty transaction with the same prerequisite to detect that its Lease has been lost.

When a Lease expires, the controller notices it on the next scheduling cycle and removes the job it belonged to. While the pipeline remains active and scheduling continues, after a Lease is aborted or expires the controller removes the old job if it is still present, schedules the necessary replacement jobs for the affected current partitions, and creates a new Lease for each replacement job. Each replacement restores state from the last committed epoch, resumes processing the affected current partitions, and may reprocess only uncommitted work. When the pipeline completes, stops, or pauses, the controller instead removes jobs, terminates their Leases, and schedules no replacements.

Lease is a commit-time ownership barrier. By itself, it does not perform [input message deduplication](#input-dedup), provide [output data atomicity](#output-atomicity) or [state atomicity](#automatic-guarantees), or cover [arbitrary external side effects](#side-effects).

#### 2. Input message deduplication {#input-dedup}

- **Internal streams** (`input`): deduplication by `message_id` using the internal `input_messages` table or its compact variant `compact_input_messages`. The compact variant is selected automatically when `experimental_enable_non_uint_key` is disabled (partitioning is done exclusively by a `uint64` hash column); you can override this behavior with the `use_compact_input_messages` parameter. Older `message_id` values are removed from this table using [SystemWatermark](../../../flow/concepts/glossary.md#timestamps-and-watermarks).
- **Sources** (`source`): deduplication by offsets — consumer offsets advance only after a successful [epoch](../../../flow/concepts/glossary.md#epoch) commit.
- **[Timers](../../../flow/concepts/glossary.md#timer)**: the result of timer processing is committed in the same transaction as the timer’s deletion.

#### 3. Output data atomicity {#output-atomicity}

The way output messages are delivered depends on the [sink](../../../flow/concepts/glossary.md#sink) type:

- **Synchronous sinks** (for example, `TSyncQueueSink`) write data to the target system in the same epoch transaction. The write and acknowledgment are atomic.
- **Asynchronous sinks** (for example, `TAsyncQueueSink`{% if audience == "internal" %}, `TLogbrokerSink`{% endif %}) synchronously store messages in {{product-name}} in the `output_messages` table, then send messages to the target system after the epoch is committed, using the `(producer_id, seqNo)` pair for deduplication on the receiver side (Queue API{% if audience == "internal" %}, Logbroker{% endif %}). If sending fails, messages from `output_messages` are sent again with the same `seqNo`. This essentially implements the [transactional outbox](https://en.wikipedia.org/wiki/Inbox_and_outbox_pattern#The_outbox_pattern) pattern with deduplication in the target system, if it supports it.

### What the developer must do {#developer-responsibilities}

Flow’s exactly-once guarantees apply to the pipeline’s internal state and built-in delivery mechanisms. However, your custom code can break these guarantees if you don’t follow these rules.

#### Computation determinism {#determinism}

Determinism requirements depend on the computation type.

- **For [Swift](../../../flow/concepts/swift.md) computations** (`TSwiftMapComputation`, `TSwiftOrderedSourceComputation`) determinism is strictly required. In Swift computations, computation results aren’t materialized between epochs and can be recalculated on restart. If different attempts to compute the same message yield different results, you might mix results from different attempts, or lose or duplicate data.
- **For regular computations** (`TTransformComputation`) non-deterministic code is technically allowed: exactly one attempt (out of all attempts caused by job restarts) is applied atomically. No result mixing occurs. However, if your code produces different output on a rerun, it can lead to unexpected behavior — especially if it has side effects (like writing to external systems) or updates state based on non-deterministic values.

{% note warning "What breaks determinism" %}

- Using the current time (`Now()`, `time.time()`) in business logic.
- Generating random numbers.
- Calling external services whose results can change between calls.

{% endnote %}

#### Side effects {#side-effects}

Any action outside Flow — an HTTP request, writing to a file, sending a notification — **isn’t covered** by exactly-once guarantees. If a job restarts, such an action might be executed again.

If side effects are unavoidable, you must ensure their idempotency yourself (for example, by using the message’s unique ID as an idempotency key).

#### External state {#external-state}

Updates to Flow’s [internal state](../../../flow/concepts/stateful.md) (YSON-state, External State via `IExternalStateManager`) are atomic and safe. However, if a computation modifies data in an external system (a database, cache), those changes aren’t part of the epoch transaction and might be duplicated.

### What Flow guarantees automatically {#automatic-guarantees}

- Internal streams between [computations](../../../flow/concepts/glossary.md#computation) — exactly-once.
- [State](../../../flow/concepts/stateful.md) updates — atomic with message processing.
- Timer processing and deletion — in the same transaction.
- Advancing [source](../../../flow/concepts/glossary.md#source) offsets — only after a successful epoch commit.

## At-least-once semantics {#at-least-once}

At-least-once guarantees that each message is processed **at least once**, but allows repeated processing. This can be useful when your business logic is naturally idempotent and the cost of deduplication is unreasonably high.

### Mode `processing_mode: at_least_once_consistent` {#processing-mode}

You set the `processing_mode` parameter at the `TTransformComputation` level in the spec. The default value is `exactly_once`.

```
"computations" = {
    "MyComputation" = {
        "processing_mode" = "at_least_once_consistent";
        ...
    };
};
```

**What changes**: input message deduplication by `message_id` is disabled. Messages might be processed again after a job restart.

**What doesn’t change**: state remains consistent (that’s why “consistent” is in the mode name). Epoch transactionality, the Lease mechanism, and the atomicity of state updates all keep working.

**When to use it**: when deduplication overhead is significant and your business logic is idempotent (for example, `insert_or_assign` instead of `increment`).

### Batching in Swift computations: `allow_batching_with_relaxed_guarantees` {#swift-allow-batching-with-relaxed-guarantees}

You set the `allow_batching_with_relaxed_guarantees` parameter in the `parameters` block of a `TSwiftMapComputation` computation. The default value is `%false`.

```
"computations" = {
    "MyBatcher" = {
        "parameters" = {
            "allow_batching_with_relaxed_guarantees" = %true;
        };
        ...
    };
};
```

**What changes**: a single output message can combine multiple input messages (batching) — for example, to collapse many small messages into one large message and reduce the load on the number of messages per partition downstream. The combined message gets a `MessageId` derived deterministically from the set of its parents' `MessageId`s, and an input message is considered processed only after all its children are delivered.

**What this means for guarantees**: on restart, the batch boundaries might differ. A replay that reproduces a batch with the same set of parents yields the same `MessageId` and is deduplicated; but if a parent ends up in a batch with a different composition, that batch's `MessageId` is different — downstream computations must be ready to see each parent’s content more than once (at-least-once). Also, the `MessageId` order within a single key isn’t preserved on the downstream computations’ side: any logic that relies on `MessageId` ordering within a key must be rewritten to account for this.

**When to use it**: when you need to aggregate the message stream (batching/collapsing), and the downstream logic is idempotent and doesn’t depend on `MessageId` order within a key. With the flag disabled (default), each output message has exactly one parent and the Swift computation keeps exactly-once guarantees.

{% if audience == "internal" %}

### At-least-once sinks {#at-least-once-sinks}

The [Logbroker](../../../yandex-specific/flow/extensions/logbroker.md) connector provides separate sink classes with at-least-once semantics:

- `TAtLeastOnceLogbrokerSink`
- `TAtLeastOnceLogbrokerFramingSink`

These sinks inherit from `TSyncSinkBase` and write to Logbroker directly during epoch processing, **without intermediate storage** of output messages in {{product-name}} tables.

**Why at-least-once**: writing to Logbroker is an external operation that isn’t rolled back if the epoch transaction fails. If the epoch transaction doesn’t commit after a successful write to Logbroker, the same messages are sent again on restart. There’s no persistent `producer_id`/`seqNo` that would let Logbroker deduplicate such repeats.

**Trade-off**: skipping intermediate storage in {{product-name}} reduces latency and cost, but allows duplicates in Logbroker on failures.

{% endif %}

## At-most-once semantics {#at-most-once}

At-most-once guarantees that each message is processed **no more than once**, but allows message loss.

You achieve this semantics by configuring `at_most_once_strategy` on asynchronous sinks:

```
"sinks" = {
    "my_sink" = {
        "at_most_once_strategy" = {
            "enabled" = %true;
            "total_queue_bytes_limit" = 104857600;  // 100 MB
            "suspend_destruction_duration" = 60000;   // ms
        };
        ...
    };
};
```

**How it works**: messages are placed in an internal queue. If the queue is full (`total_queue_bytes_limit`), new messages **are dropped without an error**. Deduplication and delivery guarantees are absent.

**When to use it**: for telemetry, metrics, best-effort notifications — scenarios where losing some messages is acceptable and throughput is more important than reliability.

## Ordering guarantees {#ordering}

Flow **doesn’t guarantee** global message processing order. However, there are guarantees for derived messages with matching [keys](../../../flow/concepts/glossary.md#key) within a single [lineage](../../../flow/concepts/lineage.md) chain.

For details, see the [Message processing order](../../../flow/concepts/ordering.md) section.

## How connectors affect guarantees {#connectors}

Each [connector](../../../flow/connectors/about.md) has its own set of guarantees, which depend on the connection type (source/sink) and the sink variant (sync/async/at-least-once).

### Queue (QYT) {#queue-guarantees}

- **Source**: exactly-once — consumer offsets advance only after epoch commit.
- **Synchronous sink** (`TSyncQueueSink`): exactly-once — writing to the queue happens in the main epoch transaction. Works only on the main processing cluster.
- **Asynchronous sink** (`TAsyncQueueSink`): exactly-once — messages are stored in `output_messages`, then delivered with `producer_id` + `seqNo`. Queue API deduplicates repeats. Works cross-cluster.

For more details, see [Queue](../../../flow/connectors/queue.md).

### Static Table {#static-table-guarantees}

- **Source**: exactly-once — deduplication by read ranges.
- **Sink** (`TArrivalOrderTableSink`): exactly-once — the output table and its progress commit in a single master transaction; a per-partition frontier deduplicates replay, so a partially covered replay writes only the uncovered tail without restarting the job; the delivery callback fires only after that external commit and the following Flow commit.

For more details, see [Static Table](../../../flow/connectors/static-table.md).

{% if audience == "internal" %}

### Logbroker (YDB Topics) {#logbroker-guarantees}

- **Source**: exactly-once — consumer offsets are fixed after epoch commit.
- **Exactly-once sinks** (`TLogbrokerSink`, `TLogbrokerFramingSink`): messages are stored in `output_messages`, then delivered with `producer_id` + `seqNo` with retries. Logbroker deduplicates repeats within a single cluster. The cost is an extra write to {{product-name}}.
- **At-least-once sinks** (`TAtLeastOnceLogbrokerSink`, `TAtLeastOnceLogbrokerFramingSink`): writing to Logbroker without intermediate storage. Lower latency, but duplicates are possible on failures.

#|
|| | **Exactly-once sink** | **At-least-once sink** ||
|| Intermediate storage in {{product-name}} | Yes (`output_messages`) | No ||
|| Deduplication in Logbroker | `producer_id` + `seqNo` | No ||
|| Latency | Higher (extra write) | Lower ||
|| Duplicates on failure | No | Possible ||
|#

For more details, see [Logbroker](../../../yandex-specific/flow/extensions/logbroker.md).

{% endif %}

{% if audience == "internal" %}

### ClickHouse {#clickhouse-guarantees}

The ClickHouse extension provides three implementations; your choice directly affects message processing guarantees:

- **Exactly-once sink** (`TClickHouseBatchingSink`, recommended by default): messages are stored in `output_messages`, deterministically grouped into batches by the same `MessageId` boundaries, and each batch is delivered with an `insert_deduplication_token` equal to the batch’s maximum `MessageId`. The deduplication token is derived from the batch content, so a byte-identical replay carries the **same** deduplication token even after `group_by` repartitioning, and exactly-once survives repartitioning. ClickHouse deduplicates the repeat by the deduplication token.
- **At-least-once sink** (`TAtLeastOnceClickHouseSink`): the epoch batch is written synchronously during processing without intermediate storage and without a deduplication token. Lower latency and load on {{product-name}}. Losses are excluded: batch writing is retried until success, and the epoch isn’t committed until writing succeeds. On failure after a successful write but before epoch commit, the same batch is written again, so duplicates are possible.
- **At-most-once sink** (`TAtMostOnceClickHouseSink`): per-message fire-and-forget via the sink’s limited in-memory queue; if it’s full (limit `total_queue_bytes_limit`), messages are silently dropped. Each message generates a separate `INSERT`, so to avoid creating many small parts in ClickHouse, you should aggregate the stream with batching upstream in the pipeline.

Exactly-once requires one of the ClickHouse table engines that deduplicate inserted blocks (see [Data Replication](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication) and [SharedMergeTree](https://clickhouse.com/docs/cloud/reference/shared-merge-tree) in the ClickHouse documentation):

- `ReplicatedMergeTree`
- `ReplicatedReplacingMergeTree`
- `ReplicatedSummingMergeTree`
- `ReplicatedAggregatingMergeTree`
- `SharedMergeTree`

A regular `MergeTree` is suitable only with an explicitly set `non_replicated_deduplication_window`. On a non-deduplicating table, the sink fails at `Init`. The deduplication window is finite and gets evicted: if a replay reaches ClickHouse later than the deduplication token’s eviction, exactly-once degrades to at-least-once. At `Init`, the sink compares the server window with the `replay_horizon` parameter, and if the window is shorter, it writes a `WARN`-level warning to the worker log, for example: `Server-default block dedup window 1d for db.tbl is shorter than the replay horizon 3d; a replay outliving the dedup token degrades exactly-once to at-least-once`.

Summary table of guarantees by sink class:

#|
|| | **Exactly-once sink** | **At-least-once sink** | **At-most-once sink** ||
|| Intermediate storage in {{product-name}} | Yes (`output_messages`) | No | No ||
|| Deduplication in ClickHouse | `insert_deduplication_token` (max `MessageId`) | No | No ||
|| Latency | Higher (extra write) | Lower | Lower ||
|| Loss on failure | No | No | Possible ||
|| Duplicates on failure | No | Possible | No ||
|#

For more details, see [ClickHouse extension](../../../yandex-specific/flow/extensions/clickhouse.md).

{% endif %}

### Service log {#servicelog-guarantees}

- **Source**: exactly-once — deduplication by read ranges. The service log is an infinite source that cycles through the table. Exactly-once guarantees apply within each cycle.
- No sink. The sorted dynamic table used as a service log source is often populated by the pipeline itself via [external state](../../../flow/concepts/stateful.md#external-state) operations. For example, a `TTransformComputation` type computation updates table records at each epoch.

For more details, see [Service log](../../../flow/connectors/servicelog.md).

{% if audience == "internal" %}

### GrUT WatchLog {#grut-guarantees}

- **Source**: exactly-once — consumer offsets are fixed after epoch commit.
- No sink.

For more details, see [GrUT WatchLog](../../../yandex-specific/flow/extensions/grut-watchlog.md).

### BigRT Queue {#bigrt-guarantees}

An extension over the [Queue](../../../flow/connectors/queue.md) connector that adds batching and compression. Guarantees are identical to the base Queue connector:

- **Source**: exactly-once.
- **Synchronous sink** (`TSyncBigRTQueueSink`): exactly-once.
- **Asynchronous sink** (`TAsyncBigRTQueueSink`): exactly-once.

For more details, see [BigRT Queue](../../../yandex-specific/flow/extensions/bigrt.md).

### Monium {#monium-guarantees}

Monium is a pull-based metrics system; unlike queues (Queue, Logbroker, BigRT), it has neither a persisted offset on the server side nor a message-level deduplication mechanism (`producer_id`/`seqNo`). This limits the extension’s guarantees compared to queue connectors.

- **Source** (`TMoniumSource`): the poller periodically calls `DataService.Read` for the half-open interval `[LastPolledTo, min(now, LastPolledTo + poll_interval))` (`from_time` is included, `to_time` is excluded — matches `containsOpenClose` from Solomon’s `Interval`) and emits each time-series point as a separate message plus a technical heartbeat at the end of each poll. After a successful response, `LastPolledTo` is set to the poll’s `to_time`, so a point at the boundary goes only to the next poll — poll windows don’t overlap, and points at window boundaries aren’t duplicated. On pipeline restart, `LastPolledTo` is restored from `GetPersistedEventWatermark()` (this is the `Meta.EventWatermark` of the last committed heartbeat, equal to the corresponding poll’s `to_time`), so downtime of any duration **doesn’t cause point loss** — the source iteratively catches up with history in `poll_interval` steps. At the internal streams level, each message gets a unique offset (exactly-once for internal streams); Flow doesn’t deduplicate by point content (`(sensor, timestamp, labels)`) — this is **at-least-once** at the metric points level.
- **Sink** (`TMoniumSink`): exactly-once at the framework level — messages are stored in `output_messages`, then delivered via `MetricsDataService.Write` with exponential backoff. Monium **doesn’t support** message-level deduplication, so on a network retry the same `(timestamp, labels)` point might be written again. This is safe for metrics whose overwrites are idempotent (`DGAUGE`, `IGAUGE` — the last write by `(timestamp, labels)` wins), but it can cause artifacts for delta metrics; monitor your sensors’ behavior and, if in doubt, use `DGAUGE`.

For more details, see [Monium](../../../yandex-specific/flow/extensions/monium.md).

{% endif %}

## Fault tolerance {#fault-tolerance}

- The [pipeline](../../../flow/concepts/glossary.md#pipeline) survives the failure of individual machines and data centers.
- While the pipeline remains active and scheduling continues, if a job fails, the controller removes it, aborts its [Lease](#lease) if it is still active, and schedules the necessary replacement jobs for the affected current partitions, with a new Lease for each replacement job. See [Lease transaction](#lease) for the fencing and recovery sequence.
- Internal streams are stored in {{product-name}} dynamic tables — data loss isn’t possible during normal storage operation.
- [Automatic partition balancing](../../../flow/about.md) redistributes load when the cluster topology changes.

## See also

- [Flow overview](../../../flow/about.md)
- [Message processing order](../../../flow/concepts/ordering.md)
- [Timers](../../../flow/concepts/timers.md)
- [Stateful processing](../../../flow/concepts/stateful.md)
- [Connectors](../../../flow/connectors/about.md)
