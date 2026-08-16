# Shuffle Service Architecture

## Overview

The Shuffle Service provides an API for partitioned data exchange: writers produce rows tagged with partition indices, and readers fetch all rows for a given partition. It runs as an RPC service (in the RPC proxy or optionally in the job proxy) and uses regular table chunks as the storage format.

Used by Spark Over YT for distributed data shuffling.

## Components

### Server Side (`yt/server/lib/shuffle_server/`)

**`TShuffleService`** (`shuffle_service.cpp`) — RPC service exposing `StartShuffle`, `RegisterChunks`, `FetchChunks`, plus `RegisterMapper` and `GetPartitionWriteSession` for the push-based path. Delegates to `TShuffleManager`.

**`TShuffleManager`** (`shuffle_manager.cpp`) — manages shuffle session lifecycle. Each shuffle is identified by a transaction ID. Creates a child transaction per shuffle; monitors commit/abort to clean up. Maintains a map of transaction ID to `IShuffleController` (the mode-agnostic storage handle) and hands it out via `GetController`; the service downcasts to the mode-specific interface via `ToPullBasedOrThrow` / `ToPushBasedOrThrow` (which throw if the controller is of the other mode).

**Shuffle controllers** (`shuffle_controller.cpp`) — per-shuffle state, one class per mode behind a separate interface, created via the `CreatePullBasedShuffleController` / `CreatePushBasedShuffleController` factories:
- `IShuffleController` is an empty base used only for uniform storage/lifetime in the manager.
- `TPullBasedShuffleController` (`IPullBasedShuffleController`) — tracks all registered chunks, indexed by partition. Supports writer epochs: when `overwrite_existing_writer_data` is set, the writer's epoch is incremented and chunks from previous epochs are filtered out on read. Serves `RegisterChunks` and `FetchChunks` (chunk slices).
- `TPushBasedShuffleController` (`IPushBasedShuffleController`) — owns the L1 distributed chunk session pool and the valid-mapper bookkeeping (see Push-Based Mode). Serves `RegisterMapper`, `GetPartitionWriteSession`, and `FetchChunks` (journal chunks + valid mapper ids).

### Client Side (`yt/ytlib/api/native/client_shuffle_impl.cpp`)

**Writer** — `CreateShuffleWriter(shuffleHandle, partitionColumn, writerIndex, options)`:
1. Creates a `TPartitionMultiChunkWriter` under the shuffle's transaction.
2. Partitions rows by extracting the partition column value.
3. On close, collects written chunk specs and calls `RegisterChunks` RPC to register them with the coordinator.

**Reader** — `CreateShuffleReader(shuffleHandle, partitionIndex, writerIndexRange)`:
1. Calls `FetchChunks` RPC with the desired partition index.
2. The coordinator returns chunk specs filtered by partition and writer range.
3. Creates a `SchemalessSequentialMultiReader` over the returned chunks.

### Public API (`yt/client/api/shuffle_client.h`)

```cpp
struct IShuffleClient {
    virtual TFuture<TSignedShuffleHandlePtr> StartShuffle(...) = 0;
    virtual TFuture<IRowBatchReaderPtr> CreateShuffleReader(...) = 0;
    virtual TFuture<IRowBatchWriterPtr> CreateShuffleWriter(...) = 0;
};
```

## Data Model

- Each shuffle has a fixed `partitionCount` set at creation.
- Writers produce regular table chunks. Each chunk stores a `PartitionsExt` proto extension with per-partition row counts and uncompressed data sizes.
- The coordinator uses `PartitionsExt` to create partition-specific `TInputChunkSlice` objects on read, with adjusted row counts and sizes.
- Chunks are created under the shuffle's transaction, inheriting its account, medium, and replication factor.

## Shuffle Handle

```cpp
struct TShuffleHandle {
    TTransactionId TransactionId;
    std::string CoordinatorAddress;  // RPC address of the shuffle service
    std::string Account;
    std::string Medium;
    int PartitionCount;
    int ReplicationFactor;
    bool UsePushBasedShuffle;        // selects pull-based or push-based path
    TTableSchemaPtr Schema;          // required for push-based; pull-based schemaless for now
};
```

Serialized as YSON and cryptographically signed (`TSignedShuffleHandlePtr`) for integrity.

## Push-Based Mode

When `start_shuffle(use_push_based_shuffle=true)` is called, the manager creates a `TPushBasedShuffleController` (otherwise a `TPullBasedShuffleController`):

- Owns an `IDistributedChunkSessionPool` (L1) with one slot per partition. The pool's journal chunks are created with read/write quorums derived from the shuffle's replication factor via `ComputeDefaultJournalQuorums` (write quorum = majority = RF/2+1, read quorum = RF − write + 1; RF=3 → read 2 / write 2, RF=2 → read 1 / write 2), not a fixed 2/2.
- Tracks per-`mapper_id` valid-set bookkeeping with overwrite-invalidation semantics: every `CreateShuffleWriter` allocates a fresh `mapper_id` from a monotonic counter; if `overwrite_existing_writer_data=true`, all prior `mapper_id`s for the same `writer_index` are removed from the valid set.
- Exposes two additional RPC methods: `RegisterMapper` and `GetPartitionWriteSession`. `FetchChunks` response is extended with `valid_mapper_ids` (returned unsorted; the reader uses it as a set).

Push-based shuffle is **schemaful**: `start_shuffle` carries a required `TTableSchema` that must be **strict** (a non-strict schema is rejected with `"Push-based shuffle requires a strict schema"`; a missing schema with `"Push-based shuffle requires a schema"`), stored on the handle. The schema is the single source of the column name-to-id mapping shared by all writers and the reader (column index = id), so records need no per-writer name-table reconciliation. The L2 writer/reader stay schema-agnostic — only the client-side L3 writer/reader read the schema: the writer builds its name table from the schema and validates each row strictly against it (rejecting unknown columns and type mismatches), and the reader builds the same name table and reads records directly.

The first `FetchChunks` call on the controller fires `Pool_->FinalizeSlot(p)` for every partition (fire-and-forget hint) and flips an internal `ReadPhaseStarted_` flag. Subsequent `GetPartitionWriteSession` calls throw `"Shuffle read phase has started; new writes are not allowed"` and `RegisterMapper` calls throw `"Shuffle read phase has started; cannot register a new mapper"`. This enforces the writes-before-reads invariant — a contract that holds for SPYT in practice because (a) `ShuffleDriverComponents.supportsReliableStorage=true` suppresses executor-loss-driven map re-runs, and (b) `YTsaurusShuffleReader` never throws `FetchFailedException`, eliminating the SPARK-25341 indeterminate-stage-retry trigger.

Pull-based mode is unchanged. The two paths coexist; the `UsePushBasedShuffle` field on the signed handle dispatches writer/reader.

## RPC Protocol (`yt/ytlib/shuffle_client/proto/shuffle_service.proto`)

**`StartShuffle`** — creates a new shuffle session. Request: `account`, `partition_count`, `parent_transaction_id`, optional `medium` and `replication_factor`, optional `use_push_based_shuffle`, and a `schema` that is required (and must be strict) when `use_push_based_shuffle` is set. Response: signed shuffle handle.

**`RegisterChunks`** — registers written chunks with the coordinator. Request: shuffle handle, chunk specs, optional `writer_index`, `overwrite_existing_writer_data`. The coordinator stores the chunks and optionally increments the writer's epoch.

**`FetchChunks`** — fetches chunks for a partition. Request: shuffle handle, `partition_index`, optional `writer_index_range`. Response: chunk specs (partition slices with adjusted sizes). Chunks from overwritten writer epochs are filtered out.

## Writer Epochs

Multiple writers can write to the same shuffle, each identified by `writerIndex`. When a writer calls `RegisterChunks` with `overwrite_existing_writer_data = true`, its epoch is incremented. On subsequent `FetchChunks`, chunks from previous epochs of that writer are excluded. This enables retries: a writer can re-write its data, and the old data is logically replaced.

## Session Lifecycle

1. Client calls `StartShuffle` → coordinator creates a child transaction and returns a shuffle handle.
2. Writers write chunks under the shuffle's transaction, then call `RegisterChunks`.
3. Readers call `FetchChunks` to get partition data.
4. When the parent transaction commits or aborts, the shuffle transaction follows, and the coordinator removes the shuffle from its active map.

## Deployment

- **RPC Proxy**: primary deployment, enabled via `enable_shuffle_service` config flag.
- **Job Proxy**: optional, enabled via `enable_shuffle_service_in_job_proxy` (mutually exclusive with RPC proxy in job proxy).
- Client discovers the shuffle service address via `connection.RegisterShuffleService()` / `connection.GetShuffleServiceChannelOrThrow()`.

## Key Files

| Component | Location |
|---|---|
| Server (service, manager, controller) | `yt/server/lib/shuffle_server/` |
| Client (writer, reader) | `yt/ytlib/api/native/client_shuffle_impl.cpp` |
| Proto definitions | `yt/ytlib/shuffle_client/proto/shuffle_service.proto` |
| Public API | `yt/client/api/shuffle_client.h` |
| Driver commands | `yt/client/driver/shuffle_commands.cpp` |
| Shuffle chunk pool | `yt/server/lib/chunk_pools/shuffle_chunk_pool.cpp` |
