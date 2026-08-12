# Static tables in {{product-name}} Flow

Connector to [static tables in {{product-name}}](../../../user-guide/storage/static-tables.md).

The connector code is [here]({{source-root}}/yt/yt/flow/library/cpp/connectors/static_table).

Static tables are a special type of source. They don’t have [partitions](../../../flow/concepts/glossary.md#partition) initially, their rows don’t have write timestamps, and the tables themselves are usually immutable with no incremental writes. At the same time, you often need to read an endless sequence of tables. Also, “writes” to this source come in very large blocks, so you must read from it with rate limiting to avoid taking resources away from more important [sources](../../../flow/concepts/glossary.md#source) in the [pipeline](../../../flow/concepts/glossary.md#pipeline).

Because of this, the main challenge with this source lies in the [controller](../../../flow/concepts/glossary.md#controller), which needs to figure out which tables to read, which timestamps ([SystemTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks), [EventTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks)) to choose for them, what to do if a table unexpectedly disappears, and so on.

### Source settings

Source class: `NYT::NFlow::NStaticTableConnector::TSource`.

##### Static spec:

{% include [NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource.md) %}

##### Dynamic spec:

{% include [NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource.md) %}

### Writing to static tables in arrival order

Sink class: `NYT::NFlow::NStaticTableConnector::TArrivalOrderTableSink`.

The sink creates a continuous sequence of tables with a fixed `table_period` step. A non-empty current slot is closed either at the time boundary or when `max_row_count`/`max_data_weight` is reached; closing by limit also shifts the next logical timestamp by one period, so the sequence can run ahead of the wall clock. While the sequence is ahead of the wall clock, closing by time doesn’t happen: an incomplete batch is written only once the wall clock catches up with the current logical timestamp, and a restart doesn’t reset this, because the lag is kept in external state. The size of the delay is proportional to the burst: the number of slots closed by limit multiplied by `table_period`. An empty slot `T` is created strictly in order, only if `T <= wall clock` and the known non-zero system watermark of the input stream is strictly greater than `T + table_period`. A watermark equal to the boundary is therefore not enough, and empty tables aren’t created in the future.

The table and the progress are committed in a single master transaction. The progress is kept in the `@progress` attribute of the `output_directory` itself and contains the owner `(pipeline, computation, sink id)`, the shared table sequence, and a separate frontier `(system_timestamp, message_id)` for each partition. All partitions share one table sequence, and the frontier is used for deduplication during replay: with a partially covered replay, the sink writes only the uncovered tail without restarting the job. The delivery callback is invoked only after a successful external commit and the next Flow commit. Progress writers are separated by a shared lock with the `progress` attribute key, so creating output tables in the same directory isn’t blocked.

Each sink needs its own `output_directory`: if a different owner is recorded in the attribute, the sink fails with an error and asks you to delete the attribute manually. Before handing the directory over to another pipeline, stop the writing pipeline; the new owner continues the grid after the most recent table in the directory. The sink doesn’t accept a directory with children that have no `table_timestamp` attribute: it fails with an error until they are removed. In particular, the main and DLQ sinks of one reader must write to different directories. A partition’s frontier is deleted as soon as its `system_timestamp` drops below the system watermark of the input stream: by that moment the partition has certainly delivered everything it produced, so its frontier is no longer needed.

The `output_directory` can be located on a cluster other than the pipeline’s cluster. The mandatory `table_ttl` parameter bounds the lifetime of the output tables: the Cypress `expiration_time` set on each table is `max(table_timestamp, table-creation time) + table_ttl`, not simply `table_timestamp + table_ttl` — a catch-up table whose `table_timestamp` already lies further in the past than `table_ttl` is clamped to expire `table_ttl` after it’s created instead of expiring immediately. While the table sequence tracks wall-clock time, this keeps the directory to at most `table_ttl / table_period` tables; a burst of catch-up tables created close together can keep more than that alive at once, since the clamp gives them all roughly the same expiration. A spec with a `table_ttl / table_period` ratio greater than 40000 is rejected because of the Cypress limit on the number of children.

The sink requires exactly one input stream with increasing `MessageId` values within it, so it applies to `Transform` and `SwiftOrderedSource` computations. A partition is identified by `SourceKey` if it is present, and by `PartitionId` otherwise. The schema of the output tables is taken from the input stream. By default, the message weight for the `max_data_weight` limit equals the message size; the optional `data_weight_column` (type `int64` or `uint64`, non-negative values) sets a custom weight, and a `null` in it means the message size. The initialization and commit transaction is retried until it succeeds or the job is aborted. For empty tables to be created even without input messages, your source computation must initialize each sink in advance in `DoInit`.

##### Static spec:

{% include [NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink.md) %}

##### Dynamic spec:

{% include [NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink.md) %}

## Importing a static table into state {#import-into-state}

A typical scenario is that a batch process (YQL{% if audience == "internal" %}, Nirvana{% endif %}, …) periodically rebuilds a reference dataset as a static YT table, and the real-time pipeline needs to join its stream against the latest version of this reference. The `static_table` connector supports this scenario out of the box, letting you load such a table into the pipeline’s [state](../../../flow/concepts/stateful.md) with custom business logic applied to each row before writing.

### How it works

The setup is: one computation reads the reference via the `static_table` source and writes it to state (loader); a second computation on the input stream retrieves a value by key from the same state and emits an enriched message (enricher).

1. The `static_table` source streams the table rows as regular messages. The controller automatically determines which table version is latest and feeds its rows to the pipeline as a separate stream.
2. The loader-computation (stateful) accepts each row, optionally validates or normalizes it (`trim`, `lower-case`, enrichment from other sources, filtering, etc.), and writes the result to state by key.
3. The enricher-computation, which sits on the event stream, reads values from the same state by the event’s key using a read-only [joiner](../../../flow/concepts/stateful.md#external-state-joiner) and emits the enriched message to the sink.

The main advantage of this approach is the **ability to implement business logic for processing each row** after the table is built but before it lands in state. This is handy when the data format on disk doesn’t match what the join needs, or when you must filter or enrich some rows on the fly from other sources.

### Reload semantics

Each new version of a static table is a **full reload** of the entire reference dataset into state. This scheme doesn’t automatically clean up deleted rows: if a row is missing in the new version, you must explicitly remove it from state — for example, by tagging rows with a version and periodically deleting outdated ones using the [key visit](../../../flow/concepts/key_visitor.md) mechanism.

The controller picks the latest version according to the rules defined in `TTableTimestampLocatorSpec` (see the source’s static spec above) — most often, this is the newest table in the directory whose name parses as an ISO8601 timestamp. A new table appearing in the directory automatically triggers a new run.

### When to choose this approach

- You need **business logic** on each row (normalization, validation, enrichment).
- You can tolerate a **full reload** on every rebuild.

These conditions aren’t strict: the approach works even without row-level business logic, and the complexities of full reloads are often manageable (see key visit above). Still, consider other options — see the [Alternatives](#alternatives) section.

### Configuration (outline)

The full spec is in the examples below; here we show only the key pipeline wiring. The loader writes to state via a **manager** (read-write), and the enricher reads from it via a **joiner** (read-only).

{% cut "Spec outline" %}

```yson
{
    "spec" = {
        "computations" = {
            "reference_reader" = {
                "computation_class_name" = "...";
                "output_stream_ids" = ["reference"];
                "source_streams" = {
                    "reference_table" = {
                        "source_class_name" = "NYT::NFlow::NStaticTableConnector::TSource";
                        "parameters" = { "tables_path" = "<cluster=primary>//path/to/reference"; };
                    };
                };
            };
            "reference_loader" = {
                "computation_class_name" = "...";
                "input_stream_ids" = ["reference"];
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"; required = %true;};
                    {"name" = "key"; "type" = "uint64";};
                ];
                "external_state_managers" = {
                    "/reference_state" = {
                        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                        "parameters" = { "path" = "<cluster=primary>//path/to/state"; };
                    };
                };
            };
            "enricher" = {
                "computation_class_name" = "...";
                "input_stream_ids" = ["event"];
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"; required = %true;};
                    {"name" = "key"; "type" = "uint64";};
                ];
                "external_state_joiners" = {
                    "/reference_state" = {
                        "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
                        "parameters" = { "path" = "<cluster=primary>//path/to/state"; };
                    };
                };
            };
        };
    };
}
```

{% endcut %}

Full working configs, binaries, `yt_sync`, and integration tests are in the examples.

### Examples

Fully working pipelines with tests that demonstrate this approach:

- C++: [`examples/cpp/static_table_join`]({{source-root}}/yt/yt/flow/examples/cpp/static_table_join)
- Python: [`examples/python/static_table_join`]({{source-root}}/yt/yt/flow/examples/python/static_table_join)
- Java: [`examples/java/static_table_join`]({{source-root}}/yt/yt/flow/examples/java/static_table_join)
- Kotlin: [`examples/kotlin/static_table_join`]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join)
- Go: [`examples/go/static_table_join`]({{source-root}}/yt/yt/flow/examples/go/static_table_join)

## Alternatives {#alternatives}

Using the `static_table` extension to load into state isn’t the only way to join with a reference dataset; the alternatives and their trade-offs are in the table below.

#|
|| **Approach** | **When to choose** | **Cost** ||
|| [#1 `static_table` extension → state](#import-into-state) (above) | you need row-level business logic before writing to state; full reloads are acceptable | cost of reloading within the pipeline on each version ||
|| [#2 Convert to a dynamic table + symlink for external state](#alt-dyntable-symlink) | large volume, request-based lookup, need atomic version switching | network lookup to the dynamic table + symlink maintenance ||
|| [#3 Embedded DB and deploy via Resource](#alt-embedded-db) | zero network calls on join, volume limited by worker memory/disk | complex maintenance, DB format and delivery to workers ||
{% if audience == "internal" %}|| [#4 Plutonium KV](#alt-plutonium) | large reference dataset, high lookup RPS, low runtime cost | complex to operate (MDS quota, infractl, metadata) ||
{% endif %}|#

### Convert to a dynamic table + symlink for external state {#alt-dyntable-symlink}

In this approach, the dynamic table itself acts as the state: a batch process builds the new version of the reference dataset as a sorted dynamic table `…/reference.vN`, mounts it, and the pipeline accesses it via a **Cypress symlink** `…/current → …/reference.vN`. After the next version is built, the symlink is atomically redirected to `…/reference.v(N+1)` (`yt link --force …/reference.v(N+1) …/current` or `set @target_path`), and the pipeline starts receiving new values.

In the pipeline, the `path` of the external-state connector points to the symlink, not to a specific version. The lookup is performed by a read-only [joiner](../../../flow/concepts/stateful.md#external-state-joiner) (`TSimpleExternalStateJoiner`); by default, the joiner re-reads the value from YT on each lookup, so the symlink switch is immediately visible to the pipeline without a restart. For more details on preparing the dynamic table and its schema, see [sorted-dynamic-table.md](../../../flow/connectors/sorted-dynamic-table.md).

#### Examples

- C++: [`examples/cpp/external_state_join`]({{source-root}}/yt/yt/flow/examples/cpp/external_state_join)
- Python: [`examples/python/external_state_join`]({{source-root}}/yt/yt/flow/examples/python/external_state_join)
- Java: [`examples/java/external_state_join`]({{source-root}}/yt/yt/flow/examples/java/external_state_join)
- Kotlin: [`examples/kotlin/external_state_join`]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join)
- Go: [`examples/go/external_state_join`]({{source-root}}/yt/yt/flow/examples/go/external_state_join)

In the example tests, `yt_sync` first builds `reference.v1` and links `current → reference.v1`; the pipeline enriches the event with value `v1`. Then `reference.v2` is built, the symlink is atomically redirected to the new version, and for **the same key**, the pipeline starts returning value `v2`.

### Embedded DB and deploy via Resource {#alt-embedded-db}

If the entire reference dataset fits in the worker’s memory or on its local SSD, and you need access with absolutely no network calls, it makes sense to build it as an **embedded DB**: the batch process deploys a ready file or directory, and the pipeline loads it as a local lookup engine directly inside the job process.

{% if audience == "internal" %}One such format in Arcadia is **vinyl**. An example of building and using it is in [`ads/core/library/cpp/vinyl`]({{source-root}}/ads/core/library/cpp/vinyl): a separate batch process builds a vinyl file on YT, deploys it to the required location, and the pipeline gets the latest file version via the [Resource](../../../flow/concepts/spec.md) mechanism (per worker) and reads from it on each message.{% endif %}

Advantages: **zero network calls** on the join, minimal lookup latency (only local disk/memory), and complete independence from external KV services. Disadvantages: volume is limited by the worker’s resources; the DB format, its schema, and version compatibility are the team’s responsibility; you must manage the delivery of new versions (Resource, redeployment, integrity checks) yourself. This approach suits relatively small reference datasets (a few gigabytes) that don’t update often.

{% if audience == "internal" %}

### Plutonium KV {#alt-plutonium}

[Plutonium KV](https://docs.yandex-team.ru/plutonium/reference/indexes/kv/overview) is a storage format for key-value data optimized for reads, plus a runtime that lets you look up values over the network. A lookup from the pipeline is a regular HTTP request to the runtime (typical response time is a few milliseconds).

Choose this when: the reference dataset is large, lookup load is high, and a full reload into state is unrealistic. Data in the runtime is stored in three replicas regardless of the number of consumers, the runtime scales separately by database volume and request count, and when part of the keys change, the index is rebuilt partially — for these workloads, Plutonium KV is significantly cheaper than dynamic tables in terms of hardware.

The cost is **operational complexity**. You need:

- A batch process to build the index via Plutonium’s build interfaces.
- An MDS/S3 quota for the index files.
- An installation spec in [infractl](https://docs.yandex-team.ru/plutonium/reference/infractl/installation) (runtime layers, sharding scheme).
- Monitoring for the delivery of new index generations.

For an example of deployment and operation, see the Plutonium documentation linked above; in Flow, a regular HTTP client from a computation is enough for lookups.

{% endif %}

## See also

- [List of connectors](../../../flow/connectors/about.md)
- [Sorted Dynamic Table](../../../flow/connectors/sorted-dynamic-table.md)
- [Stateful computations](../../../flow/concepts/stateful.md)