# Multiplexer in {{product-name}} Flow (C++)

Multiplexer is a [computation](../../../../flow/concepts/glossary.md#stream-and-computation) pattern that reads a set of records associated with an input key and sends each record as a separate output message. A typical example: key X arrives at the input, and you need to output all rows of a sorted dynamic table where X is a key prefix.

Here’s what the base class provides:

- **Uniform progress** across multiple active keys (a single large key doesn’t block the processing of others).
- **Collapse handling** — if a new input message arrives for a key that’s already being processed, the iteration restarts so that all rows are output with the latest version of the payload from the input.

## How the iteration works

The base class stores per-key state with a cursor (`Offset`). On each timer tick, it calls `DoFetchBatch` on the derived class, passing the current `startOffsetExclusive`. The derived class returns the next cursor; if there’s no more data, it returns `nullopt`.

When a collapse happens (a repeated input message for an active key), the base class saves the current position (`InitialStartOffset`) and reads the data in two passes:

1. It reads the remainder from the current position to the end (phase 1).
2. It goes back to the start and reads up to the saved collapse point (phase 2).

This ensures that after a collapse, all rows with the new payload version are output — including those that were already emitted before the collapse with the old version.

## Ready-made class: `TDynamicTableMultiplexerComputation`

This class covers a typical scenario: you have a sorted dynamic table, and input messages arrive with a key and a payload. For each input message, you need to output one row for every table record with that key.

[Class header]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/dynamic_table_multiplexer_computation.h)

### Parameters

```yson
{
    "table_path" = "<cluster=primary>//path/to/lookup_table";
}
```

There’s one parameter — the table path. The class reads the set of columns for iteration (key columns after group_by) and payload columns from the table schema during initialization (`Get(table_path/@schema)`) and caches them.

The computation’s `group_by_schema` must match the leading key columns of the table. If the table schema changes between pipeline runs, the iteration for the affected keys automatically restarts from the beginning — the base class stores the schema of the current offset in the state and compares it before each tick.

### What you need to implement

You only need to inherit from the class and override `DoBuildOutputForRow` — this defines how to build an output message for a single row. If you need to pass something from the input message to the output, define your own `TUserState` and override `DoOnInputMessage`. Otherwise, keep `TUserState` as the default (`TEmptyMultiplexerUserState`) and don’t modify `DoOnInputMessage`.

[Full example]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/tests/pipeline/main.cpp): input `(key, payload)`, lookup table `[hash, key, secondary_key, region]`, output `(key, secondary_key, region, payload)`.

`rowPayload` is a full row of the table (without group_by columns) as [`TPayload`](../../../../flow/cpp/state.md). `rowSchema` describes its columns (names and types). You retrieve columns by name using `GetColumnValue<T>` ([`payload.h`]({{source-root}}/yt/yt/flow/library/cpp/common/payload.h)).

### Dynamic parameters

These are inherited from the base class:

- `timer_period` (default 5 seconds) — how often the per-key timer triggers.
- `batch_size` (default 1000) — the size of a single batch; it’s passed to the `LIMIT` clause of the query.

## Base class: `TMultiplexerComputation`

If your data source isn’t a sorted dynamic table but something else (an in-memory structure, a custom RPC service, etc.), inherit directly from [`TMultiplexerComputation<TUserState>`]({{source-root}}/yt/yt/flow/library/cpp/multiplexer/multiplexer_computation.h) and implement `DoFetchBatch` manually.

The `DoFetchBatch` contract:

- **Offset** (`TKey`) — must be comparable and monotonically increasing within a single iteration. The base class checks monotonicity and fails if the contract is violated.
- **`startOffsetExclusive`** — read data strictly **after** this position. `nullopt` means “from the very beginning”.
- **`endOffsetInclusive`** — is set only in phase 2 (after a collapse). Your implementation must not exceed this boundary — the base class checks this.
- **`limit`** — the recommended maximum number of rows in a single batch.
- **Returning `nullopt`** — indicates there’s no more data in the current range. The base class will either switch to phase 2 or finish the iteration.

`DoOnInputMessage` is called for every input message for a key — both when a new key appears and when a collapse occurs. You can distinguish these cases using `userState.IsEmpty()`. If you don’t need to save anything from the input, don’t override this method.

## See also

- [Computation (C++)](../../../../flow/cpp/computation.md)
- [Working with states (C++)](../../../../flow/cpp/state.md)