# Multiplexer in {{product-name}} Flow (C++)

Multiplexer is a process-function pattern that reads a set of records associated with an input key and sends each record as a separate output message. A typical example: key X arrives at the input, and you need to output all rows of a sorted dynamic table where X is a key prefix.

Here’s what the base class provides:

- **Uniform progress** across multiple active keys (a single large key doesn’t block the processing of others).
- **Collapse handling** — if a new input message arrives for a key that’s already being processed, the iteration restarts so that all rows are output with the latest version of the payload from the input.

## How the iteration works

The base class stores per-key state with a cursor (`Offset`). On each timer tick, it calls `FetchBatch` on the derived class, passing the current `startOffsetExclusive`. The derived class returns the next cursor; if there’s no more data, it returns `nullopt`.

When a collapse happens (a repeated input message for an active key), the base class saves the current position (`InitialStartOffset`) and reads the data in two passes:

1. It reads the remainder from the current position to the end (phase 1).
2. It goes back to the start and reads up to the saved collapse point (phase 2).

This ensures that after a collapse, all rows with the new payload version are output — including those that were already emitted before the collapse with the old version.

## Ready-made class: `TDynamicTableMultiplexerProcessFunction` {#dynamic-table-multiplexer}

This class covers a typical scenario: you have a sorted dynamic table, and input messages arrive with a key and a payload. For each input message, you need to output one row for every table record with that key.

[Class header]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/dynamic_table_multiplexer_process_function.h)

### Parameters

```yson
{
    "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
    "processing_function" = "TMyMultiplexerProcessFunction";
    "processing_function_parameters" = {
        "table_path" = "<cluster=primary>//path/to/lookup_table";
    };
}
```

Register the derived function with the same static and dynamic parameter types:

```cpp
YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TMyMultiplexerProcessFunction,
    TDynamicTableMultiplexerParameters,
    TDynamicMultiplexerParameters);
```

`table_path` is a required cluster-qualified path. Before the first batch, the class reads the set of columns for iteration (key columns after `group_by_schema`) and payload columns from the table schema and caches them.

The computation’s `group_by_schema` must match the leading key columns of the table, and the table must have at least one secondary sort-key column after that prefix. The base class persists the secondary-key schema with the offset and resets the offset when that schema changes between pipeline runs. Changes only to non-key payload columns don’t trigger this reset. The table schema is cached for the lifetime of a process-function instance.

The configuration fragment above shows only the process-function fields. The computation also needs `group_by_schema`, input and output streams, a current-time timer stream, a timer dependency on the timer and input streams, and `allow_timer_self_dependency = %true`. See the [function implementation]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/tests/pipeline/main.cpp) and its [pipeline spec]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/tests/pipeline/pipeline.yson).

### What you need to implement

Inherit from the class and expose its constructor with `using TDynamicTableMultiplexerProcessFunction::TDynamicTableMultiplexerProcessFunction`, or define a constructor that accepts `TProcessFunctionContextPtr`. Override `BuildOutputForRow` to build an output message for one selected row. The method receives the input key, row payload and schema, user state, output collector, and runtime context. If you need to pass data from an input message to the output, define your own `TUserState` and override `OnInputMessage`. Otherwise, keep the default `TEmptyMultiplexerUserState` and don’t override `OnInputMessage`.

```cpp
void BuildOutputForRow(
    const TKey& key,
    const TPayload& rowPayload,
    const NTableClient::TTableSchemaPtr& rowSchema,
    TStateAccessor<TUserState>& userState,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context) override;
```

The complete example linked above uses input `(key, payload)`, lookup table `[hash, key, secondary_key, region]`, and output `(key, secondary_key, region, payload)`.

`rowPayload` is a full row of the table (without group_by columns) as [`TPayload`](../../../../flow/cpp/state.md). `rowSchema` describes its columns (names and types). You retrieve columns by name using `GetColumnValue<T>` ([`payload.h`]({{source-root}}/yt/yt/flow/library/cpp/common/payload.h)).

### Dynamic parameters

These are inherited from the base class:

- `timer_period` (default 5 seconds) — how often the per-key timer triggers.
- `batch_size` (default 1000) — the size of a single batch; it’s passed to the `LIMIT` clause of the query.

Place both fields in the dynamic computation spec under `processing_function_parameters`:

```yson
{
    "processing_function_parameters" = {
        "timer_period" = "10s";
        "batch_size" = 1000;
    };
}
```

## Base class: `TMultiplexerProcessFunction` {#multiplexer-process-function}

If your data source isn’t a sorted dynamic table but something else (an in-memory structure, a custom RPC service, etc.), inherit directly from [`TMultiplexerProcessFunction<TUserState>`]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/multiplexer_process_function.h) and implement `FetchBatch`.

The `FetchBatch` contract:

- **Offset** (`TKey`) — must be comparable and monotonically increasing within a single iteration. The base class checks monotonicity and fails if the contract is violated.
- **`startOffsetExclusive`** — read data strictly **after** this position. `nullopt` means “from the very beginning”.
- **`endOffsetInclusive`** — is set only in phase 2 (after a collapse). Your implementation must not exceed this boundary — the base class checks this.
- **`limit`** — the recommended maximum number of rows in a single batch.
- **Returning `nullopt`** — indicates there’s no more data in the current range. The base class will either switch to phase 2 or finish the iteration.

`OnInputMessage` is called for every input message for a key — both when a new key appears and when a collapse occurs. If the implementation needs to distinguish these cases, store an explicit initialization marker in `TUserState`; don’t rely on `userState.IsEmpty()`, because default-valued state remains empty. If you don’t need to save anything from the input, don’t override this method.

## See also

- [Process functions (C++)](../../../../flow/cpp/process-functions.md)
- [Working with states (C++)](../../../../flow/cpp/state.md)
