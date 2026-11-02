# Static tables in {{product-name}} Flow

Connector to [static tables in {{product-name}}](../../../user-guide/storage/static-tables.md).

The connector code is [here]({{source-root}}/yt/yt/flow/library/cpp/connectors/static_table).

Static tables are a special type of source. They don’t have [partitions](../../../flow/concepts/glossary.md#partition) initially, their rows don’t have write timestamps, and the tables themselves are usually immutable with no incremental writes. At the same time, you often need to read an endless sequence of tables. Also, “writes” to this source come in very large blocks, so you must read from it with rate limiting to avoid taking resources away from more important [sources](../../../flow/concepts/glossary.md#source) in the [pipeline](../../../flow/concepts/glossary.md#pipeline).

Because of this, the main challenge with this source lies in the [controller](../../../flow/concepts/glossary.md#controller), which needs to figure out which tables to read, which timestamps ([SystemTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks), [EventTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks)) to choose for them, what to do if a table unexpectedly disappears, and so on.

### Planned read timestamps {#planned-timestamps}

Set `use_planned_timestamps = %true` in the source's static parameters to set both `SystemTimestamp` and `EventTimestamp` to the planned start of each row range. The timestamp is proportional to the fraction of rows already assigned for reading. The controller fixes the start time and total read duration when it starts a table, using the current read limits. Process restarts preserve the schedule. Switching replicas, returning to a replica, or rereading a table replans the remaining ranges from the current time and read limits, preserving the existing rereading behavior. Timestamps may be in the future.

Minimum timestamps track unfinished ranges, including those not yet distributed. The option is disabled by default. Enabling or disabling it takes effect from the next table. After disabling the option, the next table uses locator timestamps again. Messages with timestamps below the published watermark are considered late; this is expected.

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

The sink requires exactly one input stream with increasing `MessageId` values within it, so it applies to `Transform` and `SwiftOrderedSource` modes. A partition is identified by `SourceKey` if present, and by `PartitionId` otherwise. The schema of the output tables is taken from the input stream. By default, the message weight for the `max_data_weight` limit equals the message size; the optional `data_weight_column` (type `int64` or `uint64`, non-negative values) sets a custom weight, and a `null` in it means the message size. The initialization and commit transaction is retried until it succeeds or the job is aborted. Creating empty tables before any input arrives requires a sink-initialization API that process functions don’t currently expose; if that behavior is mandatory, file a separate API-extension request instead of creating a raw Computation.

##### Static spec:

{% include [NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink.md) %}

##### Dynamic spec:

{% include [NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TArrivalOrderTableSink.md) %}

## Reading a sequence of static tables {#static-table-sequence}

The `static_table` source reads a sequence of static tables and emits their rows as ordinary input messages to a computation, just like a queue source. Set `tables_path` to the directory containing the tables, or provide an explicit list in `tables`. Source settings determine the read order and timestamps; the dynamic spec controls the read rate (see above).

## Periodic full-state reloads (full states) {#reference-datasets} {#alternatives} {#import-into-state} {#alt-key-visitor}

Periodic full-state reloads (full states) are a scenario where a batch process periodically prepares the complete state of a reference dataset in a static table, and a streaming pipeline uses it when processing events. Below, each complete version is called a snapshot. Each snapshot fully defines the reference values: a key absent from it no longer has a value from this dataset. If the data is copied into the pipeline’s own state, an absent key requires clearing the corresponding field while preserving other state fields.

Below are several suggested ways to implement this scenario. This list is not exhaustive; for most tasks, we recommend key visitor synchronization.

#|
|| **Method** | **How data is updated** | **What happens to a missing key** | **Applicability and cost** ||
|| Loading snapshots through a stream<br>Examples: [C++]({{source-root}}/yt/yt/flow/examples/cpp/static_table_join), [Python]({{source-root}}/yt/yt/flow/examples/python/static_table_join), [Java]({{source-root}}/yt/yt/flow/examples/java/static_table_join), [Kotlin]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join), [Go]({{source-root}}/yt/yt/flow/examples/go/static_table_join) | a `static_table` source emits rows; user logic writes the field into state | the application separately clears the field after loading or expires it | each row needs processing; no reload work without new snapshots, but expiration cleanup typically needs a key visitor over the computation’s own state by its primary key ||
|| [Key visitor with an external static table](../../../flow/concepts/key_visitor.md#static-table-joiner) | a periodic scan updates a field in the computation’s own state from a completed table | the visit handler clears the field if the row is absent from the table | recommended for most tasks; predictable scan cost that continues even without new snapshots ||
|| [Dynamic table lookup through a symlink](#alt-dyntable-symlink) | the user prepares a dynamic table from the snapshot and switches the symlink; the pipeline reads it as external state | the next read from the new table returns an absent row; no field copy is maintained in the computation’s own state | lookups during stream processing are sufficient; the supplier needs a tablet cell bundle and a table resharded in advance for the expected load ||
|| [Embedded DB and deploy via Resource](#alt-embedded-db) | a batch process builds a local DB; Resource delivers a new version to workers | once the new DB version is loaded, a key absent from it is no longer found | no network lookup calls; size is limited by worker resources, and the user manages DB building and delivery ||
{% if audience == "internal" %}|| [Plutonium KV](#alt-plutonium) | a batch process builds a new index generation; the pipeline performs network lookups in the runtime | after switching to the new generation, lookups do not find keys absent from it | large datasets and high RPS; requires MDS/S3 quota, runtime operation, and generation delivery monitoring ||
{% endif %}
|#

Loading snapshots through a stream and synchronizing through key visitor update the field in the computation’s own state gradually. Dynamic table lookup through a symlink switches the external data source; when processing sees the new table depends in part on caching.

### Conversion to a dynamic table + symlink for external state {#alt-dyntable-symlink}

A user-managed batch process prepares a separate **sorted dynamic table** from the complete static snapshot, for example `…/reference.vN`, and mounts it for reading. Once the new version is fully prepared, the user switches the Cypress symlink `…/current` to that table. The pipeline accesses the fixed `…/current` path through the read-only external state joiner `TSimpleExternalStateJoiner`.

When processing an incoming event, the computation obtains the external table row for the event’s key. If the key is absent from the new table, reading that version returns empty external state. This scheme does not require reloading the reference dataset into the computation’s own state or separately deleting missing values from it. If the application stores a copy of the field anyway, it must manage updates and cleanup separately; keys without incoming events will not update themselves.

The drawback of this method is that the data supplier must prepare the dynamic table for the pipeline’s lookup load. The supplier needs a [tablet cell bundle](../../../user-guide/dynamic-tables/concepts.md), and the table must be resharded in advance and provisioned for the expected request load. Merely converting a static table to a dynamic one and mounting it is not sufficient.

The table’s key schema must match `group_by_schema`, including computed column expressions. All versions must retain compatible schemas. The pipeline reads the table only; the user manages table preparation, mounting, and symlink switching.

By default, the joiner’s TTL cache is disabled (`cache.ttl = 0`): a key is loaded when needed, the loaded value is reused within the epoch, and a new read is required after the epoch ends. Set `external_state_joiners.<name>.parameters.cache.ttl` in the dynamic spec to reuse values across epochs. After a symlink switch, cached values may then remain unchanged until the TTL expires and the next read occurs. Switching the symlink does not switch all keys and partitions to the new version simultaneously.

Joiner configuration within the computation:

```yson
"external_state_joiners" = {
    "/reference" = {
        "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
        "parameters" = {
            "path" = "<cluster=primary>//path/to/reference/current";
        };
    };
};
```

See [TSimpleExternalStateJoiner](../../../flow/cpp/state.md#simple-external-state-joiner) for interface and caching details. Examples of stream processing with external state lookup:

- C++: [`examples/cpp/external_state_join`]({{source-root}}/yt/yt/flow/examples/cpp/external_state_join)
- Python: [`examples/python/external_state_join`]({{source-root}}/yt/yt/flow/examples/python/external_state_join)
- Java: [`examples/java/external_state_join`]({{source-root}}/yt/yt/flow/examples/java/external_state_join)
- Kotlin: [`examples/kotlin/external_state_join`]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join)
- Go: [`examples/go/external_state_join`]({{source-root}}/yt/yt/flow/examples/go/external_state_join)

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
- [Reading through the job proxy in vanilla](../../../flow/devops/vanilla/initial-deploy.md#job-proxy-readers)
