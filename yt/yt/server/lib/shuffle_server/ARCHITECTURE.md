# Shuffle Service Architecture

## Overview

The Shuffle Service provides partitioned row exchange behind the public
`IShuffleClient` reader and writer API. It runs as an RPC service in an RPC
proxy or, optionally, a job proxy, and coordinates one child transaction per
shuffle.

Two storage modes share the API and manager but use different controllers and
client-side data paths:

| | Pull-based | Push-based |
|---|---|---|
| Storage | Regular table chunks written by each writer | Journal chunks shared by writers, one session-pool slot per partition |
| Write registration | Register completed chunk specs | Register a writer id and obtain partition write sessions |
| Retry selection | Writer epochs hide chunks from older epochs | `ValidWriterIds` hide records from superseded writer registrations |
| Read description | Partition-specific table chunk slices | Journal chunk ids/replicas plus valid writer ids |
| Client reader | `SchemalessSequentialMultiReader` | `IPushBasedPartitionReader` plus row-batch adapter |

Push-based mode is opt-in through `use_push_based_shuffle`; pull-based behavior
is unchanged. The cross-component motivation and correctness model are in the
[push-based shuffle design](../../../../design-docs/YT-27781-push-based-shuffle/README.md).

## Components

### Server side

`TShuffleService` (`shuffle_service.cpp`) exposes `StartShuffle`,
`RegisterChunks`, `RegisterWriter` (plus the legacy `RegisterMapper` alias),
`GetPartitionWriteSession`, and `FetchChunks`. It resolves the shuffle
controller through `TShuffleManager`
and rejects a mode-specific RPC when the transaction's stored controller has
the other mode.

`TShuffleManager` (`shuffle_manager.cpp`) starts a child master transaction,
stores an `IShuffleController` under its transaction id, and removes the
controller after that transaction commits or aborts. The controller holds the
transaction object so shuffle data remains alive for the same lifetime.

`shuffle_controller.cpp` contains two implementations behind the marker
`IShuffleController`:

- `TPullBasedShuffleController` implements chunk registration, partition-slice
  construction, logical-writer-index ranges, and writer epochs.
- `TPushBasedShuffleController` owns a distributed chunk session pool, writer
  validity state, and the global write-to-read phase transition.

All controller mutation runs through a serialized invoker.

### Native client side

`yt/yt/ytlib/api/native/client_shuffle_impl.cpp` decodes the signed-handle
payload and dispatches `CreateShuffleWriter` and `CreateShuffleReader` by
`UsePushBasedShuffle`.

The pull path creates existing table multi-writers/readers. The push path
adapts the common components in
[`push_based_shuffle_client`](../../../ytlib/push_based_shuffle_client/ARCHITECTURE.md)
to `IRowBatchWriter` and `IRowBatchReader` and implements the remote partition
session provider used by the common writer.

### Public API

`yt/yt/client/api/shuffle_client.h` defines:

- `StartShuffle(account, partitionCount, parentTransactionId, options)`;
- `CreateShuffleWriter(handle, partitionColumn, logicalWriterIndex, options)`;
- `CreateShuffleReader(handle, partitionIndex, logicalWriterIndexRange, options)`.

The mode is internal to the signed handle. Callers use the same row-batch
interfaces in both modes.

## Shuffle Handle and Configuration

`TShuffleHandle` is YSON-serialized and cryptographically signed. It carries:

- child transaction id and coordinator address;
- account, medium, partition count, and replication factor;
- `UsePushBasedShuffle`;
- an optional schema, currently required and strict in push-based mode;
- optional YSON-serialized `TPushShuffleConfig`.

`TPushShuffleConfig` contains four independent component configs:

- `WriterConfig` for the map-side Layer 2 writer;
- `ReaderConfig` for the Layer 2 partition reader;
- `JournalWriterConfig` for sequencer batching and flushing;
- `SessionPoolConfig` for the server-side distributed-session pool.

These settings travel in the signed handle so server and native client use the
same shuffle-specific configuration. The ordinary table reader/writer config
in `TShuffleReaderOptions` / `TShuffleWriterOptions` remains a pull-path
setting and is not translated to the push components.

## Pull-Based Mode

### Data model and write path

Each writer creates regular table chunks under the shuffle transaction with
`TPartitionMultiChunkWriter`. Rows are partitioned by the named partition
column. Each chunk contains a `PartitionsExt` with row counts and uncompressed
data sizes for every partition.

On writer close, the native client sends the produced chunk specs through
`RegisterChunks`. The controller stores each chunk together with its optional
logical writer index and the writer's current epoch.

### Read path

`FetchChunks(partition_index, logical_writer_index_range)` selects registered chunks,
discards chunks from superseded writer epochs, and uses `PartitionsExt` to
construct one partition-specific `TInputChunkSlice` for every non-empty chunk.
The slice overrides row count and size estimates for the requested partition.

The native client passes those slices to `SchemalessSequentialMultiReader`.
Pull-based shuffle is schemaless for backward compatibility; a handle schema,
if present, is not required by this path.

### Writer epochs

When `overwrite_existing_writer_data` is used, `logical_writer_index` is required. The
controller increments that writer's epoch before registering the new chunks.
Later reads ignore chunks from all earlier epochs of the same logical writer index.
This gives pull-based retries logical replacement without deleting old chunks.

## Push-Based Mode

### Schema and data model

Push-based `StartShuffle` requires a strict `TTableSchema`. The schema is the
single source of the name-to-id mapping: the native writer and reader both
construct a name table from schema order, so record value ids need no
cross-writer remapping. The writer adapter rejects columns outside the schema
and validates value types.

Each partition maps one-to-one to a distributed-session pool slot. Every
session creates a journal chunk containing compressed Layer 2 records. A
partition may acquire several chunks after session replacement, but a chunk
never mixes partitions.

The controller derives journal quorums from the handle's replication factor
with `ComputeDefaultJournalQuorums`: write quorum is a majority and read quorum
is `replication_factor - write_quorum + 1`, so the two intersect. For example,
replication factor 3 gives read/write quorums 2/2, while factor 2 gives 1/2.

### Session preparation and write path

The controller eagerly calls `Pool_->GetSession(partition)` for every partition
when the shuffle starts. Creation is asynchronous; failures are logged and a
later request can retry.

Creating a push writer first performs writer registration. During the rolling
upgrade it calls the legacy `RegisterMapper` name; new servers expose the
equivalent `RegisterWriter` name as well. The legacy alias can be removed after
the 26.2 branch is created:

1. The controller allocates a fresh monotonic `writer_id` for this concrete
   registration.
2. It updates logical-writer-index validity and returns the writer id together with
   `Pool_->GetReadySessions()`: at most one already-started session per
   non-finalized partition.
3. The native client derives a name table and partitioner from the strict
   schema and seeds the Layer 2 writer with those ready sessions.
4. `TRemotePartitionWriteSessionProvider` calls
   `GetPartitionWriteSession` for an unseeded partition or after a write
   failure. An `excluded_session_id` is a hint to prefer another active
   session, not a command to retire the old one globally.
5. The Layer 2 writer buffers rows per partition, builds immutable records, and
   writes them directly to the returned sequencer nodes.

Closing the client writer flushes its records. It does not finalize pool slots;
the server owns the global shuffle lifecycle.

### Writer validity

Every writer registration creates a distinct writer id and initially marks it
valid. The optional `logical_writer_index` is the stable caller-assigned identity
shared by retries. With `overwrite_existing_writer_data=true`, that index is
required, and registration removes **all** earlier writer ids associated with it
from the valid set before adding the new one. Their records remain in the journal
chunks but become logically invisible.

Without overwrite, several registrations can remain valid. A
`logical_writer_index_range` on `FetchChunks` restricts the returned valid-id set; it
does not slice journal chunks because each chunk contains interleaved records
from many writers.

### Read path and deduplication

The push `FetchChunks` result contains every `ChunkId + Replicas` pair for the
requested partition and the applicable `valid_writer_ids`. The native client:

1. derives the read quorum from the same replication factor;
2. creates an `IPushBasedPartitionReader` with `ReaderConfig`;
3. installs a header filter backed by the valid-id set;
4. adds all returned journal chunks, calls `SetNoMoreChunks()`, and asks the
   reader to finish at the current committed record count;
5. flattens accepted record rows into the public row-batch reader format.

The partition reader first drops a repeated immutable wire record by
`(writer_id, start_row)`, then applies writer validity, and only then
decompresses and parses it. These checks have separate purposes: the first
handles an ambiguous send retried through another session; the second removes
output from overwritten writer registrations.

### Write-to-read transition and sealing

The first push-based `FetchChunks` call performs a global phase transition:

1. `ReadPhaseStarted_` is set.
2. `Pool_->FinalizeSlot` is fired for every partition, closing active sessions
   and initiating background sealing through `ScheduleChunkSeal`.
3. The requested chunk list is returned immediately; the Layer 1 reader can
   read chunks that have not finished sealing.

After the transition, writer registration and `GetPartitionWriteSession` reject
new work. This is a writes-before-reads contract, not a general concurrent
Spark shuffle protocol: the caller must not start the first read while a valid
writer can still publish rows through a session it already holds.

SPYT currently satisfies the contract through its map-before-reduce stage
ordering and reliable-storage integration. In particular, it does not use
`FetchFailedException` to trigger map reruns after reduce starts. The server
checks the phase boundary but cannot reconstruct rows omitted by an early
caller transition.

## RPC Protocol

The protocol is defined in
`yt/yt/ytlib/shuffle_client/proto/shuffle_service.proto`.

| RPC | Mode | Contract |
|---|---|---|
| `StartShuffle` | Both | Creates the child transaction/controller and returns a signed handle; push mode also validates the strict schema and parses `push_config` |
| `RegisterChunks` | Pull | Registers completed regular chunk specs, logical writer index, and overwrite flag |
| `RegisterWriter` / `RegisterMapper` | Push | Allocates a writer id and returns currently ready partition sessions; `RegisterMapper` is retained for rolling-upgrade compatibility |
| `GetPartitionWriteSession` | Push | Returns a session for one partition, optionally preferring one other than an excluded id |
| `FetchChunks` | Both | Pull returns partition slices; push returns journal chunks and valid writer ids |

Mode-specific handlers downcast the controller stored for the transaction and
report an error when called for the wrong mode. The public API carries the
handle in a signed wrapper; native client adapters decode its payload and send
the internal YSON handle on service RPCs.

The `logical_writer_index` and `logical_writer_index_range` fields retain the
protobuf tags of their former names, preserving wire compatibility with older
clients and servers.

## Lifecycle

1. `StartShuffle` starts a child master transaction under the caller's parent
   transaction and creates the selected controller.
2. Writers use the pull registration flow or the push writer/session flow.
3. Readers fetch a partition description and construct the corresponding
   native reader.
4. In push mode, the first fetch also freezes writing and starts sealing all
   partition slots.
5. Commit or abort of the child transaction removes the controller from the
   manager. The shuffle's chunks follow transaction lifetime.

The manager state itself is in-memory; distributed-session/controller restart
recovery is planned separately and is not implemented by the current service.

## Deployment

- **RPC proxy:** primary deployment, enabled by `enable_shuffle_service`.
- **Job proxy:** optional deployment, enabled by
  `enable_shuffle_service_in_job_proxy` and mutually exclusive with the RPC
  proxy service inside that job proxy.

The native client discovers a service address through connection shuffle
service registration and then uses the coordinator address embedded in the
signed handle for subsequent calls.

## Key Files

| Responsibility | Location |
|---|---|
| Service, manager, and controllers | `yt/server/lib/shuffle_server/` |
| Native client adapters | `yt/ytlib/api/native/client_shuffle_impl.cpp` |
| RPC protocol and proxy | `yt/ytlib/shuffle_client/` |
| Public shuffle API and handle | `yt/client/api/shuffle_client.h` |
| Distributed-session client | `yt/ytlib/distributed_chunk_session_client/` |
| Common push writer and reader | `yt/ytlib/push_based_shuffle_client/` |
