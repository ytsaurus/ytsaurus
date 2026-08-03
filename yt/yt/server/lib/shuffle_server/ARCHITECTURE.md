# Shuffle Service Architecture

## Overview

The Shuffle Service provides an API for partitioned data exchange: writers produce rows tagged with partition indices, and readers fetch all rows for a given partition. It runs as an RPC service (in the RPC proxy or optionally in the job proxy) and uses regular table chunks as the storage format.

Used by Spark Over YT for distributed data shuffling.

## Components

### Server Side (`yt/server/lib/shuffle_server/`)

**`TShuffleService`** (`shuffle_service.cpp`) — RPC service exposing three methods: `StartShuffle`, `RegisterChunks`, `FetchChunks`. Delegates to `TShuffleManager`.

**`TShuffleManager`** (`shuffle_manager.cpp`) — manages shuffle session lifecycle. Each shuffle is identified by a transaction ID. Creates a child transaction per shuffle; monitors commit/abort to clean up. Maintains a map of transaction ID to `IShuffleController`.

**`TShuffleController`** (`shuffle_controller.cpp`) — per-shuffle state. Tracks all registered chunks, indexed by partition. Supports writer epochs: when `overwrite_existing_writer_data` is set, the writer's epoch is incremented and chunks from previous epochs are filtered out on read.

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
};
```

Serialized as YSON and cryptographically signed (`TSignedShuffleHandlePtr`) for integrity.

## RPC Protocol (`yt/ytlib/shuffle_client/proto/shuffle_service.proto`)

**`StartShuffle`** — creates a new shuffle session. Request: `account`, `partition_count`, `parent_transaction_id`, optional `medium` and `replication_factor`. Response: signed shuffle handle.

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
