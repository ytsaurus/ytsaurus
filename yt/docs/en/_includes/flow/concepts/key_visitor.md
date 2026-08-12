# Key Visitor Streams in {{product-name}} Flow

## Why you need key-visitor streams {#why-key-visitors}

Stateful computations accumulate an [internal state](../../../flow/concepts/glossary.md#state) per key while processing incoming messages. You often need to periodically scan this state—even without an incoming message for a specific key. Common tasks include:

- **TTL / eviction**: iterate over all keys and delete outdated records.
- **Periodic aggregates**: emit the current value for each key once a day.
- **Forced re-evaluation**: trigger a recalculation for all keys in the background.

Standard timers (see [Timers](../../../flow/concepts/timers.md)) aren’t suitable here. You must register them for each key in advance, but the set of keys can grow without an explicit “new key appeared” event. For example, keys may arrive only via state stores, not through a message stream.

A key-visitor stream solves this problem. A background task in the worker periodically scans the entire state of a partition and emits a `TVisit` message for each key into a special internal stream. The computation subscribes to this stream via `DoProcessVisit` / `process_visit` and decides how to handle its state—just like it would for a regular incoming message.

## How it works {#how-it-works}

### Pass lifecycle {#pass-lifecycle}

1. **Background fill**. Each partition runs a background loop. It reads the state page by page using `KeyStates::List` and passes the keys to an internal visit buffer. The speed is regulated by a throttler configured so that one full pass takes the specified `Period`.
2. **Emit**. Ready `TVisit` messages are delivered by the engine via `GetNextBatch` and reach the computation in `DoProcessVisit`.
3. **Coverage**. After visits for a key range are delivered to the consumer, the range is marked as *Committed* in `TKeyVisitorStore`. The coverage is persisted to the system table `key_visitor_states`. That’s why a worker restart or partition rebalance doesn’t cause a re-scan.
4. **End of pass**. When the coverage is complete, the background loop immediately calls `StartNewPass`. The pace is set by the throttler, so the next pass still takes `Period`. Rotation is atomic: a single Sync-transaction deletes the previous pass’s rows and seeds the first interval of the new one. If a crash happens midway, it rolls back, and the coverage is preserved.
5. **Final pass**. When all input streams are Completed, the next pass is marked *Final*. After its commit, the visit stream becomes `Empty` and doesn’t start new passes. You’re guaranteed at least one full pass after the inputs finish.

### Partitioning {#partitioning}

The state is partitioned by the uint64 hash of the first column in `group_by_schema` (usually `farm_hash(key)`). Each partition uses its own `TKeyVisitor` to scan its hash range.

Inside a partition, the range is split into a statically defined number of *buckets* (`bucket_count`). Buckets are scanned in round-robin order. This smooths the load and keeps the “unfinished coverage” evenly distributed across the partition. That way, if a worker crashes, it doesn’t discard progress in one narrow area.

## Guarantees {#guarantees}

| Property | Guarantee |
|---|---|
| Each key per period | You get exactly one visit per key (no duplicates or omissions). |
| Worker restart | Coverage is preserved; rotation is atomic, and a crash keeps the old pass. |
| Partition rebalance | The new worker sees the committed coverage via `key_visitor_states`. |
| Input completion | At least one final pass is guaranteed; `Empty` is declared only after it. |
| `Period` | Best-effort. Under throttler load or slow `KeyStates` reads, the achieved period grows. See observability below. |
| Scan order | Within a bucket, keys are sorted; between buckets, the order is round-robin. Not event-time. |

## Which computations support visit streams {#supported-computations}

| Computation | Visit stream support |
|---|---|
| `TTransformComputation` | ✓ |
| `TSwiftMapComputation` | ✓ (only for state handling: emitting to output from `DoProcessVisit` is forbidden, see [Swift](../../../flow/concepts/swift.md#swift-map)) |
| `TSwiftOrderedSourceComputation` | ✗ |

## Configuration {#configuration}

To make a computation accept a visit stream, you must fill the `key_visitor_streams` field in its static spec:

```yson
"tester" = {
    "computation_class_name" = "...";
    "group_by_schema" = [...];
    "key_visitor_streams" = {
        "visit_iter" = {};
    };
    ...
};
```

### Schema requirements {#schema-requirements}

- `group_by_schema` must start with a `uint64` column. This column is used as the hash to split the partition into buckets. The check runs at spec submit time.

### Static spec parameters {#static-params}

- `names`: a list of internal-state names to count keys from. If unspecified or empty, all keys from all internal states of the computation are used.
- `external_names`: a list of [external-state managers](../../../flow/concepts/glossary.md#state) and visitor-driven external-state joiners (see [Static table joiner](#static-table-joiner)) for the computation, whose tables are used to count keys. This lets the visitor scan the external state (including that of companion computations) and evict outdated records via `clear()` in the visit handler. If only `external_names` is specified (without `names`), only the listed external state is scanned; internal states aren’t scanned. If neither `names` nor `external_names` is specified, all internal states are scanned, but external state and joiners aren’t; a joiner is scanned only if explicitly listed in `external_names`, and in no more than one visit stream of the computation (checked at spec submit time).

### Dynamic spec parameters {#dynamic-params}

In `dynamic_spec.computations.<id>.key_visitor_streams.<name>`:

- `period`: the target duration for one full pass. Default is 1 day.
- `max_scan_rows_per_iteration`: the limit for a single `KeyStates::List` call (in rows, not keys). It must be strictly greater than the maximum number of internal-state names per key; otherwise, the scan stalls (see [Diagnostics](#diagnostics)).
- `buffer_row_limit`: the maximum size of the internal buffer of ready visits between the background fill and `GetNextBatch`.
- `background_fill_period`: the pause between iterations of the background loop in idle state. Each such iteration performs one read (`KeyStates::List`) per scanned source. So this parameter sets the base frequency of the visitor’s read requests—about `1 / background_fill_period` per second per source per partition (with the default of 500 ms, that’s about 2 reads/s), independent of `period` (which only affects the width of the hash slice for each read). Under load (cap-hit, bucket/pass change), iterations are rescheduled immediately, and the frequency can be higher.

### Computation with only a visit stream {#key-visitor-only}

A key-visitor stream can be the **only** source of work for a computation: `input_stream_ids` is empty, there are no `source_streams`, and work comes only from scanning the external state (`external_names`). This is the auditor pattern: periodically re-evaluate keys in an external table without a message stream. Such a computation is partitioned by uint64 hash ranges in the same way as an input-driven one (see [Partitioning](#partitioning)); the number of partitions is set by `min_partition_count` / `max_partition_count` / `desired_partition_count` in the dynamic spec.

The requirement for `group_by_schema` (first column must be `uint64`, see [Schema requirements](#schema-requirements)) is mandatory for such a computation—the partition ranges are built from it.

{% note warning %}

**One external table, one writer.** A visitor computation modifies the external state. Don’t modify the same external table from multiple computations: concurrent writes from different computations aren’t serialized and lead to races and lost updates. At spec validation time, YT path ownership is checked: if two computations declare writes (via their external-state managers) to the same `(cluster, path)`, the spec submit fails with the `claimed for writing` error.

{% endnote %}

### Static table joiner {#static-table-joiner}

The scan can read not only the computation’s state but also an external **static** table—via the external-state joiner `TStaticTableKeyVisitorJoiner`, listed in `external_names`. The table must be strictly sorted by the computation’s `group_by_schema`: the prefix of its key columns must match that schema in names and types. The joiner reads the table sequentially, in the same key order as the state scan, and passes the table row to `DoProcessVisit` as a read-only state for the visit key. Table keys take part in the scan on an equal footing with state keys: a visit arrives even for a key that isn’t yet in the computation’s own state.

This is the basis of the reconciliation pattern: a periodic scan aligns the computation’s own state with the external table—keys present in the table are updated, and keys missing from it are deleted. Requirements for the table, behavior when the source is unavailable, and a code example are in the [TStaticTableKeyVisitorJoiner](../../../flow/cpp/state.md#static-table-key-visitor-joiner) section.

### `streams_dependency` {#deps}

By default, the visit stream is **not** part of the computation’s `streams_dependency`: visitors are usually for internal cleanup and don’t produce output, and a stuck visitor shouldn’t block the completion of output streams. The “last pass” signal is delivered to the visitor locally by the worker (`SetUpstreamCompleted`)—this doesn’t require an edge in the graph.

If the computation **emits messages to output from `DoProcessVisit`**, you must explicitly list the visit stream as the parent of that output in `streams_dependency`. Example:

```yson
"streams_dependency" = {
    "visits" = ["keys"; "visit_iter"];
};
```

Here, `visits` is the output stream, `keys` is the input, and `visit_iter` is the key-visitor stream. For a computation with only a visit stream (no input), there’s a single parent: `"visits" = ["visit_iter"]`.

## Observability {#observability}

### Visit EventTimestamp {#event-timestamp}

Each emitted `TVisit` carries an `EventTimestamp` equal to the expected scan time for that key. The formula is:

```
scheduleLag = max(0, elapsed − ScannedFraction · Period)
EventTimestamp = now − scheduleLag
```

where `elapsed` is the time since the start of the current pass (taken as the minimum `PassStartedAt` among already scanned intervals; it’s persisted in `key_visitor_states`, so it survives restarts and rebalances).

If the scan runs on schedule, `scheduleLag = 0`, and `EventTimestamp ≈ SystemTimestamp`. If you fall behind (for example, the throttler limits throughput due to a slow backend), `EventTimestamp` lies in the past, and the visit stream’s watermark lags by exactly the amount of the delay. This gives a direct signal to the downstream consumer and shows up as event-lag in standard flow metrics.

### Diagnostics {#diagnostics}

- **`max_scan_rows_per_iteration` is too small**. If a single key has more internal-state names than `max_scan_rows_per_iteration`, the scan can’t progress: the limit is hit mid-key, all its rows are discarded, and no progress is made. The computation reports the error `/key_visitor/<stream>/scan_cap_stall` via `StatusProfiler` (`Key visitor stalled: a single key has more than max_scan_rows_per_iteration = N rows`). The error clears automatically once the read progresses. The solution is to increase `max_scan_rows_per_iteration` via `Reconfigure`.
- **State backend is unavailable**. If reading the state during a scan fails, the worker doesn’t crash: an error is reported to `/key_visitor/<stream>/background_fill`, and the read is automatically retried. A transient YT/RPC error resolves on retry; a persistent issue (schema mismatch, missing table) remains visible in the status and doesn’t break the pipeline.