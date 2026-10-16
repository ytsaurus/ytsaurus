# Working with ClickHouse in {{product-name}} Flow

Writing to [ClickHouse](https://clickhouse.com/docs) is implemented as an optional Flow plugin. The extension provides only a [sink](../../../flow/concepts/glossary.md#sink) (there is no source): data is written directly to ClickHouse over the native protocol (TCP, port 9000 by default), optionally over TLS (see the [`enable_tls` parameter](#parameters)). See the [ClickHouse Flow extension source code]({{source-root}}/yt/yt/flow/extensions/clickhouse/).

The extension is built on the native C++ client [contrib/libs/clickhouse-cpp]({{source-root}}/contrib/libs/clickhouse-cpp). Each sink instance holds one `clickhouse::Client` connection per shard on a single dedicated action queue (the client is synchronous and not thread-safe). The shards of one batch are written sequentially in this queue: the base ordered sink requires batches to complete strictly in the order of their numbers.

## Sink family {#sink-family}

The sink class selects the guarantee level. `TAtMostOnceClickHouseSink` remains at-most-once in both strategy modes; the strategy selects whether delivery is ordered and persisted or uses a bounded lossy queue:

- `NYT::NFlow::TClickHouseBatchingSink` &mdash; **the recommended option**, exactly-once with deterministic batching (one `INSERT` per batch). Repartition-safe (see [Processing guarantees](../../../flow/concepts/guarantees.md#clickhouse-guarantees)).
- `NYT::NFlow::TShardedClickHouseBatchingSink` &mdash; exactly-once with the same batching for the multi-shard `shard_hosts` form.
- `NYT::NFlow::TAtLeastOnceClickHouseSink` &mdash; at-least-once: the epoch batch is written synchronously during processing, without intermediate storage and without a deduplication token. Latency and load on {{product-name}} are lower. A failed write prevents the epoch from committing, so messages are not lost, but duplicates are possible on failure. Known transient errors are retried without a limit; unclassified errors are attempted at most `max_insert_attempts` times in total (10 by default), and permanent errors fail immediately. Suitable for idempotent data marts (`ReplacingMergeTree`, `argMax` rollups).
- `NYT::NFlow::TAtMostOnceClickHouseSink` &mdash; at-most-once in both modes. With the default static setting `at_most_once_strategy.enabled = false`, pending messages stay in `output_messages` and use ordered delivery; a connection or session failure before `INSERT` starts may therefore be retried. Set the option to `true` to send each message independently through a bounded in-memory queue without waiting for delivery. Its dynamic limit is `at_most_once_strategy.total_queue_bytes_limit`, and the sink logs a warning when it drops messages on overflow. In either mode, once `INSERT` has started, a failed insert is acknowledged instead of retried, so it does not block subsequent messages.

For either exactly-once class, a permanent insert error or exhausted unclassified insert attempts produces the `Giving up insert into ClickHouse` error. Every later write handled by the same sink instance then fails, so the pipeline stops making progress. Correct the target table or configuration, then pause and start the pipeline to create a new sink instance.

A synchronous exactly-once sink (`TSyncClickHouseSink`) does not exist: ClickHouse does not participate in the {{product-name}} table transaction, so exactly-once for ClickHouse necessarily goes through an asynchronous transactional outbox.

## Target table {#target-table}

Exactly-once relies on ClickHouse block deduplication by the `insert_deduplication_token` query setting that the sink sends with each `INSERT`. Block deduplication is enabled by default only for the `Replicated*MergeTree` and `SharedMergeTree` engines.

- **Recommended**: `ReplicatedMergeTree` / `SharedMergeTree` — block deduplication is enabled by default.
- A single-host, non-replicated `MergeTree` without block deduplication is accepted with a warning, but exactly-once degrades to at-least-once. Set `non_replicated_deduplication_window` to enable block deduplication.
- A `Distributed` table as the target is **rejected**: the sink routes rows to shards itself, so the target must be the local table of each shard. Otherwise the server would forward the block itself and break the link between the deduplication token and the shard.
- Engines outside the `MergeTree` family are rejected.

Each sink instance creates its writer session lazily on the first write and keeps it until the instance stops. When the session starts, the sink connects to all configured endpoints and validates their metadata. Each endpoint must be reachable. Within a shard, all endpoints must have the same engine and the same complete ordered schema `(name, type, default_kind, default_expression)`; the schemas of different shards must also match. For `Replicated*MergeTree`, the sink verifies the logical identity of the table: `zookeeper_name` and `zookeeper_path` must match, while `replica_name` may differ between replicas. The check is performed for every delivery guarantee level.

Dynamic reconfiguration does not repeat the complete endpoint metadata validation. A `write_timeout` change recreates the clients and checks the deduplication window again. Changes to `replay_horizon` or `async_insert` update the corresponding deduplication-window check. After changing the target table, pause and start the pipeline so that new sink instances read and validate its current engine, schema, and replication identity.

More than one host within a shard is rejected for a plain `MergeTree` or `SharedMergeTree`: the sink cannot prove that the hosts expose the same logical table. A `shard_hosts` mapping with one host per shard is accepted. A single-host `SharedMergeTree` is supported. For a single-host plain `MergeTree`, a warning is issued about the need to configure `non_replicated_deduplication_window`.

### Deduplication window and replay horizon {#dedup-window}

The deduplication window is finite. ClickHouse bounds it separately by block count in `replicated_deduplication_window` and by time in `replicated_deduplication_window_seconds`; asynchronous inserts use `replicated_deduplication_window_seconds_for_async_inserts` for the time bound. These server settings can vary by ClickHouse version and configuration. A replay that arrives after its token has been evicted is inserted again, so exactly-once holds only if the replay reaches ClickHouse before eviction.

The `replay_horizon` parameter sets an upper bound on the replay lag. When the writer session starts, the sink compares it with the server-side `replicated_deduplication_window_seconds`, or, with `async_insert = true`, with `replicated_deduplication_window_seconds_for_async_inserts`. If the selected window is shorter, the sink writes a structured warning to the worker log with the attributes `Database`, `Table`, `DedupWindowSetting`, `ReplayHorizon`, and `DedupWindow`. The per-table value of the window cannot be read through the native client, so when creating the table, set the corresponding time window with a margin relative to `replay_horizon`.

## Sharding {#sharding}

A single sink can write to several independent shards. A shard is a set of replicas of one table with its own block deduplication log.

- **Single-host form**: `host` + `port`. The simplest option: one table on one host.
- **Unsharded form with multiple hosts**: `hosts` — a flat list of hosts of one table, all sharing `port`.
- **Multi-shard form**: `shard_hosts` — a mapping from a shard name to the list of its hosts. `port`, `database`, and `table` are shared by all shards.

The forms are mutually exclusive: exactly one of `host`, `hosts`, and `shard_hosts` must be set. For exactly-once, the unsharded `TClickHouseBatchingSink` accepts only `host` or `hosts`, while `TShardedClickHouseBatchingSink` requires `shard_hosts`. The at-least-once and at-most-once sinks accept any of the three forms. A single-entry `hosts` list is rejected during spec validation — use `host` in that case.

```yson
"shard_hosts" = {
    "a" = ["ch-a-1"; "ch-a-2"];
    "b" = ["ch-b-1"; "ch-b-2"];
};
```

The sink routes rows itself in order to organize deduplication correctly. It does not reproduce the sharding expression of any existing `Distributed` table. The scheme is correct for the case of "N independent local tables that are read through a `Distributed` table that simply unions them". If the same tables are also populated by another writer through `Distributed`, or if read queries rely on `optimize_skip_unused_shards`, local `JOIN`s, sharded dictionaries, or `distributed_group_by_no_merge`, such queries will be incorrect.

### Host selection within a shard {#host-selection}

The `host_selection_policy` parameter sets the order of endpoints within each shard. The `ordered_round_robin` value is used by default and preserves the order from the spec. With `random_start`, the client picks a uniformly distributed starting position once at construction and rotates the list. After that, clickhouse-cpp iterates over all endpoints in round-robin order; there is no new random permutation before each attempt.

The policy affects only the starting point of failover. It does not change row routing, deduplication tokens, or the topology fingerprint.

### Routing key {#sharding-key}

`sharding_key_columns` sets the columns whose values are used to compute the routing key. The parameter is meaningful only together with `shard_hosts`; in the unsharded forms it is rejected during spec validation. If the list is empty (the default), the message ID serves as the key: it is stable across replays and uniformly distributed, but there is **no co-location** — rows with the same business key land on different shards. Set `sharding_key_columns` explicitly if co-location is required.

The shard is selected by [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) over the key and the shard names: for each shard, a fingerprint of the "key, shard name" pair is computed, and the row goes to the shard with the maximum value. This yields an important property: when a shard is added or removed, only the keys of the added or removed shard move (approximately $1/N$ by the number of shards), while the rest stay in place.

### Deduplication token and shard names {#sharding-token}

The shard deduplication token is the batch token (the maximum message ID in the batch) with the `:<shard name>` suffix. The token is bound to the batch boundary and the shard name rather than to the contents of the sub-batch, so a replay presents ClickHouse with the same token and the same block. In the unsharded forms, the token has no suffix.

Two consequences follow:

- **A shard name is a permanent identifier.** Renaming a shard is as expensive as re-sharding. The `:<shard name>` suffix is substituted into the token by the sink at insert time, so all batches after the rename go to ClickHouse with a new token. Rows that have already been inserted are not rewritten: their tokens remain forever in the ClickHouse block deduplication log under the previous name and will never match any replay again. In addition, the name takes part in routing, so a rename moves the shard's keys.
- Adding a replica to a shard or replacing a dead host **does not affect tokens**: the token is bound to the shard name, not to the host list.

### Payload determinism requirement {#sharding-determinism}

The values of the `sharding_key_columns` columns must be **stable across replays**. If a row's key value changes while the message ID stays the same, the row moves to another shard: the new shard inserts it (producing a duplicate), while the old one receives a *different* block under the already used token and will probably drop it entirely. This is a requirement for upstream message processing; the sink cannot verify it.

### Topology change guard {#topology-guard}

Changing the set of shards or `sharding_key_columns` while undelivered batches remain in the sink state is **rejected with an error at startup**. Otherwise such batches would be replayed under the new routing, and some rows would be duplicated or lost. The guard applies to both batching implementations: `TClickHouseBatchingSink` and `TShardedClickHouseBatchingSink`.

To switch from `host` or `hosts` to `shard_hosts`, wait for the pipeline to drain completely, then simultaneously replace the class with `TShardedClickHouseBatchingSink` and the host form with `shard_hosts`. Keep the sink name and thereby its state prefix. After the switch, rolling the binary back to a version whose class registry does not contain `TShardedClickHouseBatchingSink` rejects the unchanged sharded spec before the batching sink state is read. Switching the class back while undelivered sharded state exists is not a safe rollback.

The error at partition startup looks like this:

```
Refusing to start the ClickHouse sink: the shard topology changed while 3 batch(es) are still undelivered; replaying them under the new topology would duplicate or drop rows. Restore the previous shard_hosts / sharding_key_columns, let the pipeline drain to completion, then apply the change
    persisted_topology_fingerprint = unsharded
    spec_topology_fingerprint      = 3f2a17c9b4e05d81
    oldest_undelivered_batch_bound = 1-4-17
```

Look for it in the partition errors in the flow view (`yt flow get-flow-view` or the pipeline page in the UI) and in the worker job log.

The sink stores two kinds of fingerprints in its own state: a routing fingerprint and a logical target fingerprint for each shard. At startup they are compared with the current spec and the ClickHouse metadata. While undelivered batches remain, a mismatch of any of them blocks startup.

The routing fingerprint covers the shard names and `sharding_key_columns` (including their order). Shards are canonically sorted by name, so reordering the mapping keys changes nothing. The unsharded forms (`host` and `hosts`) share a constant fingerprint; switching from them to `shard_hosts` changes the fingerprint and the token format and is blocked until a full drain.

Host lists are not part of the routing fingerprint but are taken into account by the logical target check. For `Replicated*MergeTree`, the target is defined by the `zookeeper_name`/`zookeeper_path` pair: replacing a dead replica or changing the host list is allowed if the new endpoints point to the same table in Keeper. For a plain `MergeTree` and a single-host `SharedMergeTree`, the identity of the target cannot be proven from metadata, so the fingerprint includes the database, table, port, and host list. Changing them while batches are undelivered is rejected: restore the previous endpoint, drain the pipeline, and only then change the configuration. If the previous endpoint is lost irrecoverably, the sink deliberately does not replay batches into another unproven target automatically.

The check is performed **per partition**: a partition that still has undelivered batches refuses to start, while a drained partition switches to the new topology right away. "Refuses" means a stop rather than a warning: the sink's `Init` throws an error, the partition job fails, and the controller restarts it according to the regular failure handling rules — that is, the partition fails on every start and makes no progress at all until the spec is reverted to the previous topology. Therefore, when updating without a drain, the topology may be applied partially, and after reverting to the previous spec the partitions that have already switched may fail. Change the topology only after the pipeline has drained completely (graceful update).

A full drain protects undelivered data, but it does not move rows that have already been written and does not preserve the historical co-location of keys. After changing the set of shards, separately redistribute the stored data between shards if queries need a uniform layout of old and new rows.

The at-least-once and at-most-once sinks carry no deduplication token, so the topology change guard described above does not apply to them.

## Type mapping {#type-mapping}

The list of columns, their types, and their order are **inferred automatically** from the target table: when the writer session starts, before the first send, the sink reads its schema from `system.columns`. Only the columns produced by the [stream](../../../flow/concepts/glossary.md#stream) are sent; a table column that is absent from the stream is not sent — ClickHouse substitutes its `DEFAULT`. `MATERIALIZED` and `ALIAS` columns are skipped (ClickHouse computes them). For each column being sent, the yson type of the stream is validated against the ClickHouse column type; a mismatch causes a failure when the writer session starts.

#|
|| **ClickHouse column** | **Stream yson / YT type** ||
|| `Int8` | `int8` ||
|| `Int16` | `int16` ||
|| `Int32` | `int32` ||
|| `Int64` | `int64` ||
|| `UInt8` | `uint8` ||
|| `UInt16` | `uint16` ||
|| `UInt32` | `uint32` ||
|| `UInt64` | `uint64` ||
|| `Float32` | `float` ||
|| `Float64` | `double` ||
|| `String` | `string` or `utf8` ||
|| `FixedString(N)` | `string` or `utf8` (the length is checked on write) ||
|| `LowCardinality(String)` | `string` or `utf8` ||
|| `Bool` | `boolean` ||
|| `Date` | `date` ||
|| `DateTime` | `datetime` ||
|| `Nullable(T)` | the optional (`optional`) yson type corresponding to `T` ||
|#

Not supported: `Decimal`, `DateTime64`, `Enum`, `UUID`, `IPv4`/`IPv6`, `Array`/`Tuple`/`Map`, and any composite / yson types. A column of an unsupported type is allowed in the table only if the stream does not write it (its `DEFAULT` is applied then).

## Parameters {#parameters}

Columns are not specified in the spec — they are [inferred automatically](#type-mapping) from the target table.

Exactly one of the host forms (`host`, `hosts`, or `shard_hosts`) must be present in the spec; the remaining connection parameters are common to all sink classes.

Static spec parameters of `TClickHouseBatchingSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TClickHouseBatchingSink.md) %}

Dynamic spec parameters of `TClickHouseBatchingSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TClickHouseBatchingSink.md) %}

Static spec parameters of `TShardedClickHouseBatchingSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TShardedClickHouseBatchingSink.md) %}

Dynamic spec parameters of `TShardedClickHouseBatchingSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TShardedClickHouseBatchingSink.md) %}

Static spec parameters of `TAtLeastOnceClickHouseSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TAtLeastOnceClickHouseSink.md) %}

Dynamic spec parameters of `TAtLeastOnceClickHouseSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAtLeastOnceClickHouseSink.md) %}

Static spec parameters of `TAtMostOnceClickHouseSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TAtMostOnceClickHouseSink.md) %}

Dynamic spec parameters of `TAtMostOnceClickHouseSink`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAtMostOnceClickHouseSink.md) %}

To enable the bounded at-most-once queue, set `at_most_once_strategy.enabled = true` in the static spec. Configure its byte limit dynamically with the nested parameter `at_most_once_strategy.total_queue_bytes_limit`.

### The async_insert option {#async-insert}

`async_insert` is a dynamic parameter. When it is enabled, all sink classes emit `async_insert=1` and `wait_for_async_insert=1`. The batching sinks with the exactly-once guarantee additionally pass the deduplication token and emit `async_insert_deduplicate=1`; the at-least-once and at-most-once sinks intentionally do not pass a deduplication token and do not set `async_insert_deduplicate`.

- `async_insert_deduplicate=1` is required for exactly-once (with the default `0` there is no deduplication).
- `wait_for_async_insert=1` lets the sink learn about an insert error before the outbox advances.

Asynchronous deduplication uses the separate windows `replicated_deduplication_window_for_async_inserts` and `replicated_deduplication_window_seconds_for_async_inserts`. Keep in mind that asynchronous inserts are incompatible with deduplication for materialized views.

## Example {#example}

The repository contains an [integration test]({{source-root}}/yt/yt/flow/extensions/clickhouse/test) with a real local ClickHouse: the pipeline emits typed rows into a pre-created `ReplicatedMergeTree` table and checks the guarantees of every sink class under induced instability.

Sink spec fragment:

```yson
{
    "sink_class_name" = "NYT::NFlow::TClickHouseBatchingSink";
    "input_stream_ids" = ["rows"];
    "parameters" = {
        "host" = "localhost";
        "port" = 9000;
        "database" = "default";
        "table" = "flow_sink";
    };
}
```

A sharded exactly-once sink on two shards with co-location by `user_id`:

```yson
{
    "sink_class_name" = "NYT::NFlow::TShardedClickHouseBatchingSink";
    "input_stream_ids" = ["rows"];
    "parameters" = {
        "shard_hosts" = {
            "a" = ["ch-a-1"; "ch-a-2"];
            "b" = ["ch-b-1"; "ch-b-2"];
        };
        "sharding_key_columns" = ["user_id"];
        "port" = 9000;
        "database" = "default";
        "table" = "flow_sink";
    };
}
```

## See also

- [List of Extensions](../../../flow/extensions/about.md)
- [Processing guarantees: ClickHouse](../../../flow/concepts/guarantees.md#clickhouse-guarantees)
