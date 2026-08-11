# Sorted Dynamic Table Sink in {{product-name}} Flow

Use this connector to write data to [sorted dynamic tables in {{product-name}}](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md).

{% note info "Attention" %}

In most cases, use [StateManager](../../../flow/concepts/stateful.md) to work with sorted dynamic tables, not this connector.

Key differences:

- **StateManager** supports read-modify-write operations, row deletion based on non-trivial rules, and guarantees a single modification point for each row. The `group_by_schema` computation must match (or be a prefix of) the state’s key columns.
- **This connector** is a low-level primitive that supports only write-only operations (inserting or deleting rows). There are no restrictions on `group_by_schema`, which lets you write to the table from multiple computations with different keys. However, the order of modifications to a single row isn’t controlled.

{% endnote %}

You can find the connector code [here]({{source-root}}/yt/yt/flow/library/cpp/connectors/sorted_dynamic_table).

## Writing to a sorted dynamic table

The sink accepts messages and writes their rows to a sorted dynamic table. The message schema must be compatible with the target table’s schema.

You write data synchronously within the main epoch transaction (`NYT::NFlow::NSortedDynamicTable::TSyncSink`). This means the write is atomic together with the other epoch changes. However, the table must reside on the main processing cluster — the same cluster as the pipeline.

The number of receiver channels automatically adjusts to match the number of tablets in the target table. The controller periodically polls the table and updates this value.

### Sink parameters

Sink class: `NYT::NFlow::NSortedDynamicTable::TSyncSink`.

#### Static spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NSortedDynamicTable_TSyncSink.md) %}

#### Dynamic spec:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NSortedDynamicTable_TSyncSink.md) %}