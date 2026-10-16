# Sorted Dynamic Table Sink in {{product-name}} Flow

Use this connector to write data to [sorted dynamic tables in {{product-name}}](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md).

{% note info "Attention" %}

In most cases, use [StateManager](../../../flow/concepts/stateful.md) to work with sorted dynamic tables, not this connector.

Key differences:

- **StateManager** supports read-modify-write operations, row deletion based on non-trivial rules, and guarantees a single modification point for each row. The `group_by_schema` computation must match (or be a prefix of) the state’s key columns.
- **This connector** is a low-level primitive that supports only write-only operations (inserting or deleting rows). There are no restrictions on `group_by_schema`, which lets you write to the table from multiple computations with different keys. However, the order of modifications to a single row isn’t controlled.

{% endnote %}

You can find the connector code [here]({{source-root}}/yt/yt/flow/library/cpp/connectors/sorted_dynamic_table).

## Write modes

The sink accepts messages and writes their rows to a sorted dynamic table. The message schema must be compatible with the target table’s schema.

The synchronous sink (`NYT::NFlow::NSortedDynamicTable::TSyncSink`) writes rows within the main epoch transaction. The write is atomic together with the other epoch changes, but the table must reside on the main processing cluster — the same cluster as the pipeline.

The asynchronous sink (`NYT::NFlow::NSortedDynamicTable::TAsyncSink`) writes each batch in a separate tablet transaction. The target table may reside on another cluster specified in `table_path`. This write is not atomic with the main epoch transaction: the sink acknowledges messages after the separate transaction commits successfully.

The asynchronous sink reports the latest YT write error in `/async_write` and retries every error except `Canceled` until the write succeeds or the job is cancelled. Retry delays use exponential backoff with jitter. The delay before jitter starts at `backoff_duration` and is capped at one minute (or at `backoff_duration` when it is greater). Pending messages remain accounted for in the output buffers, so the regular output backpressure applies while the sink is retrying.

Within one sink instance, batches are written sequentially. This ordering does not extend across job restarts or partition handoffs: a tablet transaction already started by the old job may commit after the replacement job writes the replayed batch and subsequent batches. As a result, an older modification may overwrite a newer modification of the same key. Use the asynchronous sink only when both replay and this reordering are safe, for example when every key is always written with the same value or the logical version is part of the key. Aggregate writes are unsupported.

The number of receiver channels automatically adjusts to match the number of tablets in the target table. For a chaos replicated table, the controller reads the tablet count from the enabled data replica with the lexicographically smallest replica id; its sync/async mode does not affect the choice. Resharding that replica may change the number of receiver channels.

### Synchronous sink parameters

Sink class: `NYT::NFlow::NSortedDynamicTable::TSyncSink`.

#### Static spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NSortedDynamicTable_TSyncSink.md) %}

#### Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NSortedDynamicTable_TSyncSink.md) %}

### Asynchronous sink parameters

Sink class: `NYT::NFlow::NSortedDynamicTable::TAsyncSink`.

#### Static spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NSortedDynamicTable_TAsyncSink.md) %}

#### Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NSortedDynamicTable_TAsyncSink.md) %}
