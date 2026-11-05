# Push-Based Shuffle Client Architecture

## Overview

`yt/yt/ytlib/push_based_shuffle_client/` contains the common record, writer,
and reader layer for push-based shuffle: the typed header, wire layout, row
builder, compression pair, partition writer, and multi-chunk partition reader.
Those components are independent of the integration that supplies sessions,
chunks, schemas, and writer validity.

The module also contains reusable, MapReduce-oriented sort and sorted-merge
readers. They live next to the common reader because they depend directly on
its identity-preserving mode, but production controller-agent and job wiring is
outside this component.

See the cross-component
[design overview](../../../design-docs/YT-27781-push-based-shuffle/README.md)
and the
[MapReduce integration](../../../design-docs/YT-27781-push-based-shuffle/mapreduce-integration.md).

## Components

### `TRecordHeader`

A 16-byte fixed POD struct that sits at the front of every wire shuffle
record:

| Field | Type | Offset |
|---|---|---|
| `RowCount` | i32 | 0 |
| `WriterId` | i32 | 4 |
| `StartRow` | i64 | 8 |

The two i32 fields precede the i64 so the struct is naturally aligned to 16
bytes with no padding. `static_assert(sizeof(TRecordHeader) == 16)` enforces
this.

The header is **not** compressed -- it sits in front of the compressed payload
on the wire. This lets the partition reader inspect `(writer_id, start_row)`
for retry deduplication and integration-provided `ValidIds` filtering without
invoking the codec.

### `TShuffleRecord`, `TParsedRecord`

Two structs representing the intermediate and final shapes of a record on
the read/write paths:

- `TShuffleRecord` -- header plus uncompressed payload as
  `std::vector<TSharedRef> UncompressedPayload`. `UncompressedPayload`
  contains the row data only (offset table + varint-encoded rows); the
  16-byte `Header` is a separate field and is never serialized into
  `UncompressedPayload`. The vector form lets the builder hand
  `THorizontalBlockWriter::FlushBlock`'s chunk parts straight through to
  the codec (`ICodec::Compress(const std::vector<TSharedRef>&)`) without
  a payload-sized merge copy.
- `TParsedRecord` -- what `ParseShuffleRecord()` returns. Header, the
  uncompressed payload as a single `TSharedRef` (retained to keep string
  bytes alive), plus a pool-allocated `TRange<TUnversionedRow>`.

### `TShuffleRecordBuilder`

Pure serializer. Holds an `std::optional<THorizontalBlockWriter>`, an
immutable `WriterId`, and a `NextRowId` counter. Each `AddRow` delegates
to the block writer.
Each `FlushRecord` reads the row count, captures the header
(`WriterId`, current `NextRowId`, row count), moves the block writer's
chunk refs into `TShuffleRecord::UncompressedPayload` (zero payload copy -- the vector
elements are just `TSharedRef` handles), advances `NextRowId`, and recreates
the block writer (which is single-use -- `FlushBlock()` sets
`Closed_ = true`).

The block writer is constructed with a zero-column schema (`New<TTableSchema>()`).
This is safe: `ColumnCount_ = 0` means `IsInlineHunkValue` always returns
false and the hunk-handling code path is never exercised -- intermediate
shuffle data has no hunk columns.

### `ReadShuffleRecordHeader`

Reads the 16-byte header from the front of a wire record. Overloads accept a
single `TSharedRef` or `TRange<TSharedRef>`; the range form walks refs while
accumulating bytes and permits the header to straddle boundaries. The current
writer merges each record into one contiguous ref before its Layer 1 write, so
production partition reads use the single-ref form. The range overload keeps
the format utility symmetric with compression and supports future multi-part
transport without changing the format.

No codec is involved. The partition reader uses this header peek for exact
record-retry deduplication and optional `ValidIds` filtering before paying for
decompression.

### `CompressShuffleRecord` and `DecompressShuffleRecord`

Symmetric compression pair. Both are free functions, neither bundled into
the builder or the parser -- this keeps the format layer
(`TShuffleRecordBuilder` and `ParseShuffleRecord`) codec-free.

- `CompressShuffleRecord(TShuffleRecord, codec)` -- serializes the header into
  a 16-byte ref, compresses `UncompressedPayload` via the multi-ref
  `NCompression::ICodec::Compress(const std::vector<TSharedRef>&)` overload,
  and returns the wire form as `std::vector<TSharedRef>` `[header_ref,
  compressed_payload_ref]`. No memcpy of the compressed payload happens in
  this function: the codec's output ref is moved into the vector directly.
  The current shuffle writer then calls `MergeRefsToRef` because
  `IDistributedChunkWriter::WriteRecord` accepts one `TSharedRef`. Avoiding that
  final merge requires a future multi-ref Layer 1 write API.
- `DecompressShuffleRecord(wire, codec)` -- accepts the wire as
  `TRange<TSharedRef>` to handle multi-part input. Reads the 16-byte header
  via `ReadShuffleRecordHeader`, builds the post-header suffix as a
  `std::vector<TSharedRef>` (slicing the boundary ref if the header
  straddles it), and feeds the suffix to the codec's multi-ref `Decompress`
  overload. Returns `TShuffleRecord`.

### `ParseShuffleRecord`

Codec-free. Coalesces `record.UncompressedPayload` into a single `TSharedRef` --
zero-copy for the single-ref case (move), one merge copy for the
multi-ref case. Production input is always single-ref (post-decompression);
the merge branch is purely a robustness path for callers that bypass
`DecompressShuffleRecord`. Then delegates row-walking to
`NTableClient::THorizontalBlockReader` -- the block format produced by
`THorizontalBlockWriter` and the format we emit are byte-identical, so the
reader's offset-table + `ReadRowValue` loop just works. We pass an empty
schema, no hunk metadata, no sort orders, and `unpackAny=false`;
`GetRow(pool, /*remapIds=*/false)` produces rows whose string/Any/Composite
values point into the coalesced payload buffer rather than being copied
into the pool. The payload ref is moved into the returned `TParsedRecord`
so the buffer outlives the row range -- same lifetime convention as
`THorizontalBlockReader` itself.

When identity column ids are supplied, the reader reserves two extra value
slots and appends the record's writer id and `StartRow + rowIndex`. Optional
validation rejects input values that already use either id.

**Trust model.** v1 trusts the decompressed payload to be well-formed
(matches `THorizontalBlockReader`'s contract for chunk reads). The parser
validates only `Header.RowCount >= 0`; any other malformed shape -- short
offset table, out-of-range offset, truncated value -- relies on the reader's
internal `YT_VERIFY`s rather than producing `TError`s. Since both the writer
and the data nodes are cluster-internal, this is the same trust level
applied to existing journal / table reads. Stronger throw-on-malformed
behavior is a v2 hardening item.

## Shuffle Writer

`IPushBasedShuffleWriter` (`shuffle_writer.{h,cpp}`) is the writer-side component that
routes rows by partitioner, buffers per partition, evicts on memory pressure,
compresses, and ships records through Layer 1 `IDistributedChunkWriter`s. Its
API is two methods:

- `TFuture<void> Write(TRange<TUnversionedRow> rows)` -- callers wait on the
  returned future, then call again (sequential contract). Backpressure is
  baked into the future (it stays unset while in-flight bytes exceed cap).
- `TFuture<void> Close()` -- flushes every non-empty builder, drains every
  pending and in-flight record. Does not call `FinalizeSlot` and does not
  expose chunk lists -- the pool-owning integration (the Shuffle Service
  controller, the controller agent for sort and map-reduce operations, or a
  test harness) is responsible for both.

All state lives on a `CreateSerializedInvoker(...)` invoker. There are no
locks in the writer and no `WaitFor` in any writer method or callback;
internal flow uses future chains posted `.Via(SerializedInvoker_)`.

### Memory budget

The configured `MemoryBudget` is partitioned into two named buckets:

- `BuildersBudget = MemoryBudget * BuildersBudgetFraction` (default 0.1).
  Tracks the sum of `TShuffleRecordBuilder::GetAllocatedDataSize()` over
  non-empty partitions. A hard cap: each `Write` evicts until the bucket is
  back under cap.
- `InFlightBudget = MemoryBudget - BuildersBudget`. Tracks the compressed
  size of every record that has left a builder but is not yet acked
  (combines Pending + on-wire). A soft cap: aggressive eviction may push
  past it by at most one compressed record; backpressure (deferred Write
  future) kicks in when exceeded.

The fraction sets the per-partition send pipelining depth, independently of
the partition count: eviction emits records of roughly
`BuildersBudget / partition_count` bytes, so the in-flight bucket holds about
`(1 - fraction) / fraction` records per partition. The default fraction 0.1
targets a depth of nine; at depth ~1 (fraction 0.5 and above) every
partition's journal write degenerates into stop-and-wait rounds and write
throughput drops by about a third.

### Eviction

When `BuildersBytes > BuildersBudget` after a `Write` batch, the writer
flushes builders one at a time until the cap is satisfied. The budget
trigger is keyed on capacity (the `GetAllocatedDataSize()` deltas above),
but the eviction *victim* is the partition with the most buffered data
(`TShuffleRecordBuilder::GetDataSize()`). Selecting the victim by actual
data rather than quantized capacity keeps per-partition record sizes
uniform. The victim sits at the front of `EvictionHeap_`, an addressable
heap of partition indices built on `core/misc/heap.h`, so selection and
removal are O(log P); each partition caches its data key in
`BufferedDataSize` and its heap slot in `HeapIndex`, and the heap doubles
as the set of non-empty partitions.

### Session lifecycle

Sessions are obtained from a writer-side `IPartitionWriteSessionProvider`
(`session_provider.{h,cpp}` in `push_based_shuffle_client/`, namespace
`NPushBasedShuffleClient`). The common module provides the interface and a
Direct in-process implementation that wraps an
`IDistributedChunkSessionPool` plus a partition-to-slot mapping for tests.

The production Shuffle Service client implements
`TRemotePartitionWriteSessionProvider` in
`yt/ytlib/api/native/client_shuffle_impl.cpp`. Writer registration supplies the
currently ready sessions used to seed the writer's per-partition cache; the
remote provider calls `GetPartitionWriteSession` for partitions that were not
ready and for replacements after write failure.

The writer holds at most one active session per partition. Sessions are
requested proactively in `DoWrite` for any non-empty partition that does
not yet have one (so the session is usually ready by the time the builder
is flushed). On `WriteRecord` failure, the writer retires the current session
**locally** (resets `Session` and `Writer`) and requests a replacement with
`excludedSessionId = failed_session_id`. Exclusion is a per-request hint: the
pool does not treat one writer's ambiguous write as proof of global session
failure, and at its active-session cap it may return the same session. The
failed record is requeued in the partition's `Pending` queue with
`SendAttempts` preserved.

When the new session arrives, `OnSessionResolved` sweeps any in-flight
records that were still pending on the retired session into `Pending`
(their bytes stay accounted in `InFlightBytes` -- same bucket) and then
calls `DrainPending` to ship everything on the fresh writer. Stale acks
for swept cookies arrive later and no-op via the
`InFlight.find(cookie) == end()` guard in `OnWriteResponse`.

### Retry cap and terminal failure

`MaxSendAttempts` (default 3) bounds the number of physical send attempts
per record. The cap is checked in both `OnWriteResponse` (when a record's own ack
returns an error) and `DrainPending` (when a record was repeatedly swept
without any of its own acks ever firing -- the retire-move loop). Hitting
the cap fires `FailWriter`, which sets a terminal `TerminalError_`, fails
any waiting `BackpressurePromise_` / `ClosePromise_`, and short-circuits
subsequent `Write` and `Close` calls. The first error wins; later
failure callbacks become no-ops.

### v1 limitations

- `THorizontalBlockWriter`'s 64 KiB+1 reserve floor -- same constraint
  noted for the record format. With many partitions, eviction may flush
  small records.
- Single active session per partition. Multi-session-per-partition
  fanout for slow sequencers is future work.
- No `IMemoryUsageTracker` integration; budget tracked internally.
- No throughput throttler.
- Fixed Builders/InFlight fraction at construction; runtime tuning is
  future work.
- The writer calls `MergeRefsToRef` on `CompressShuffleRecord`'s output
  because `IDistributedChunkWriter::WriteRecord` takes a single
  `TSharedRef`. This re-introduces a payload-sized memcpy that the
  multi-ref codec output was designed to avoid. Future work: switch
  `WriteRecord` to `TRange<TSharedRef>` and drop the merge.
- `Write` processes a batch synchronously. In-row eviction bounds
  `BuildersBytes` to roughly `BuildersBudget + one_reserve_growth`,
  but compressed records from each eviction are added to
  `InFlightBytes` without throttling within the batch. A large
  `Write` call can therefore transiently exceed `InFlightBudget` by
  the sum of compressed sizes of records evicted during the call.
  Backpressure between calls is still enforced. Callers that want a
  hard memory cap should bound batch size. Adding mid-batch pause /
  resume (so the writer can yield the invoker, await acks, and
  continue) is future work.

## Wire format

```
+--------------------------------------+
| Header (16 bytes, host order)        |  <- uncompressed
|   row_count : i32   (offset 0)       |
|   writer_id : i32   (offset 4)       |
|   start_row : i64   (offset 8)       |
+--------------------------------------+
| Payload (variable, codec-compressed) |
|                                      |
|   When decompressed:                 |
|     offsets : u32[row_count]         |
|     data    : concatenated rows      |
|       each row:                      |
|         value_count : varint u32     |
|         per value: id+type+payload   |
|         (see ReadRowValue)           |
+--------------------------------------+
```

The data section uses the same encoding as `THorizontalBlockWriter`: an
offset table followed by per-row varint-encoded `(value_count, [id, type,
data]*)` tuples. `Composite` types are normalized to `Any` on write
(matches `THorizontalBlockWriter::WriteRow`'s behavior).

Codec for a session is fixed and known out-of-band to both writer and reader:
it is a field of the signed shuffle handle, which both sides read. There is no
codec marker in the record. There is also no version field -- intermediate
shuffle data does not cross major YT versions, so format-evolution machinery
is intentionally omitted.

## Format Design Rationale

### Row-based schemaless payload

The payload reuses YT's horizontal, schemaless row encoding rather than
introducing a columnar block format. Push records are deliberately small enough
to bound writer buffering, and MapReduce rows from many jobs can contain
different schemaless value sets. A columnar format would need per-record schema
or column metadata whose fixed cost is material at that size.

Horizontal rows carry each value's numeric id and type, need no chunk meta, and
reuse the established `WriteRowValue` / `ReadRowValue` representation.
Compression still applies to the complete offset table and row payload. The
schemaful Shuffle Service uses the same wire format; its strict schema supplies
a common id mapping outside Layer 2 rather than creating a second encoding.

### Full-row materialization on read

`ParseShuffleRecord` materializes every value descriptor in a record instead
of parsing only sort keys and revisiting the payload later. Both current
consumers ultimately need complete rows: the Shuffle Service returns them, and
MapReduce sorting must emit non-key values after ordering. A key-only first pass
would require another parser or retained per-value offsets and would complicate
identity-column insertion.

Materialization does not copy string-like payload bytes. Value descriptors
point into the retained decompressed ref, and the batch holder extends that
ref's lifetime for as long as a consumer retains the rows. The design therefore
pays descriptor allocation once while preserving payload ownership without a
deep copy.

## v1 limitations

### 64 KiB+1 reserve floor

The internal `THorizontalBlockWriter` clamps its reserve size to at least
64 KiB+1. This is a deliberate choice for `THorizontalBlockWriter`'s typical
allocation profile (hundreds of KiB to MiB blocks must exceed lf_alloc's
small-block size limit). It is the *opposite* of what push-based shuffle wants:
with median record size ~1.5 KiB, each per-partition builder holds ~62 KiB
of slack, multiplied by the number of concurrently-buffering partitions.

This is the same constraint pull-based shuffle has lived with for years.
Pull-based shuffle works around it with multi-level shuffle (caps each
level's partition count at ~200). Lifting the floor in push-based shuffle is
a v2 feature: replace `THorizontalBlockWriter` with a custom small-buffer
serializer that reuses `WriteRowValue`/`ReadRowValue` directly.

### Memory tracker not plumbed

`TShuffleRecordBuilder` does not currently feed YT's memory accounting
infrastructure (`IMemoryUsageTrackerPtr`). The implemented shuffle writer
enforces its own builder/in-flight budget, but that memory is not reported to
the surrounding job tracker. Integrating the tracker remains future work.

## Testing

`yt/yt/ytlib/unittests/push_based_shuffle_record_format_ut.cpp` contains
focused unit tests:

- `EmptyFlushReturnsNullopt` -- `FlushRecord()` on an empty builder.
- `MultiFlushAdvancesNextRowId` -- `StartRow` of record N+1 equals
  `StartRow + RowCount` of record N.
- `RoundTripMixedTypesLz4` -- build -> compress -> header peek -> decompress ->
  parse, with mixed value types and `Lz4` codec.
- `CompositeNormalizesToAny` -- confirms the `Composite -> Any` normalization
  contract we inherit from `THorizontalBlockWriter`.
- `ReadHeaderRejectsShortRecord` -- throws when the wire's combined size is
  below `sizeof(TRecordHeader)`.
- `ReadHeaderHandlesSplitHeader` -- header bytes split across two refs in
  the wire are walked and reconstructed correctly.
- `ParseHandlesMultiRefPayload` -- `ParseShuffleRecord` coalesces a
  multi-ref `UncompressedPayload` and produces correct rows.
- `ParseAppendsIdentityValues` -- optional identity ids append writer and row
  identities during decoding.
- `ParseRejectsNegativeRowCount` -- throws on `Header.RowCount < 0`.

The layout invariant `sizeof(TRecordHeader) == 16` is enforced at compile
time by `static_assert` in `record_format.h`, not at runtime.

`THorizontalBlockWriter`, `ReadRowValue`/`WriteRowValue`, and the `NCompression`
codecs are well-tested in their own modules; tests here focus on our wrapping
logic.

## Multi-Chunk Reader

`IPushBasedPartitionReader` (`partition_reader.{h,cpp}`) is the reducer-side component that opens one Layer 1 `IDistributedChunkSessionReader` per chunk for a partition, decompresses + parses blobs into `(TRecordHeader, TSharedRange<TUnversionedRow>)` pairs, and presents them as a unified `Finished`-flagged stream. Dynamic chunk discovery is supported: callers `AddChunk` new chunks as the integration learns about them, and `SetNoMoreChunks` says that no additional chunks will be added. It does not assert that every chunk is already sealed. `FinishAtCurrentCommittedRecordCount` separately tells the Layer 1 readers to finish at a snapshot of their current quorum-confirmed record counts; without it, they remain active until they observe sealing.

State lives on a serialized invoker. Each chunk holds an L1 reader, one
in-flight future, an optional ready result, and a cursor. Completed reads
enqueue their chunk id, so draining advances cursors without scanning all chunks
or erasing vector prefixes. Duplicate `(writer_id, start_row)` headers and an
optional caller filter are checked before decompression. Rejected blobs still
count toward `MaxBytesPerRead`.

The factory optionally accepts writer and row identity column ids. When supplied,
decoded rows receive both header-derived values. Other readers allocate no extra
row slots.

Canceling a pending `Read()` terminally cancels the reader, cancels every
in-flight Layer 1 read, and releases the Layer 1 readers. Subsequent reads
return the same cancellation.

**Test seam.** A `CreatePushBasedPartitionReaderForTesting(config, codec, createDistributedChunkSessionReader, invoker, recordHeaderFilter, identityColumnIds)` overload lives in `partition_reader.cpp` only (not declared in the public header) and lets unit tests inject a mock `IDistributedChunkSessionReader` -- mirrors the writer's `CreatePushBasedShuffleWriterForTesting` pattern. Tests forward-declare it inside their own translation unit.

### Lifetime model -- design rationale

Three alternatives were considered:

| | Cleared pool | Accumulating pool | Per-batch (chosen) |
|---|---|---|---|
| **Holder** | `MakeStrong(reader)` | `MakeStrong(reader)` | private `TBatchPayloadsHolder` (one per batch) |
| **Pool lifecycle** | one pool, cleared each Read | one pool, grows forever, freed at reader death | one pool per batch, freed when batch dies |
| **Allocs / Read (warm)** | ~0 | ~0 | ~3 small + ~1 chunk (~250 ns + ~64 KiB at default RowBufferStartChunkSize) |
| **Retention granularity** | "current batch only" | "whole reader" | "per batch" |
| **MR sort memory peak** | 2x (forced deep-copy of strings) | 1x | 1x |
| **Shuffle Service row-batch memory peak** | 1x current batch | partition-size (pool never frees) | 1x current batch |
| **L3 contract** | strict: consume before next Read | hold reader for whole sort | drop batches you're done with |
| **Failure mode if misused** | dangling string reads | unbounded memory growth | none |
| **YT precedent** | `THorizontalSchemalessRangeChunkReader` | `TPartitionChunkReader` | none |

Per-batch wins for both current consumers: the MapReduce-oriented sort reader
can retain input without copying string payloads, while the Shuffle Service
row-batch adapter can release each consumed batch. Batch-level granularity
matches how both paths retain data; per-batch allocation cost is negligible
beside decompression and parsing, and ownership does not depend on the next
`Read()` call.

### Tuning

`RowBufferStartChunkSize` and `MaxBytesPerRead` size *different* buffers. The former is the initial chunk size for the per-batch `TRowBuffer` (pool grows as parsing fills it; defaults to 64 KiB). The latter is the wire-byte cap on how much one `Read()` will drain across all ready chunks (defaults to 64 MiB); checked between every blob, so overshoot is at most one blob's compressed size. Keep `RowBufferStartChunkSize` close to the expected post-decompression size of a typical batch -- too small forces extra pool growth allocations; too large wastes memory on tiny batches. The two are independent knobs.

### Logging

Logger category is `PushBasedShuffleClient`, shared with the writer. If a deployment needs reader-only filter levels (e.g. heavier diagnostics on the reducer side without affecting writer logs), splitting into `PushBasedShuffleReader` / `PushBasedShuffleWriter` is a localized change to `PushBasedShuffleLogger()` and one log-config entry.

### v1 limitations

- One L1 read per chunk may be pending or ready; there is no global count or
  memory cap.
- Single-threaded decompression + parsing on the serialized invoker.
- Batch-granularity retention via a single `TBatchPayloadsHolder` per batch.
- No `IMemoryUsageTracker` integration.
- No `IUnversionedRowsetReader` adapter.
- No per-chunk `SetChunkFinished(chunkId, finalRecordCount)` signal. `FinishAtCurrentCommittedRecordCount()` applies one snapshot boundary to every live Layer 1 reader.
- No `TPushBasedPartitionReaderStatistics`. The writer ships without a stats struct too; add when an L3 caller needs counters.

## Sort Readers

`ISortReader` (`sort_reader.{h,cpp}`) is the job-side Layer 3 reader. It owns a
partition reader, sorts a partition or partition fragment in memory, and emits
sorted row batches. `CreateSortReader` selects the mode with
`TSortReaderMode`:

| | Identity-preserving | Identity-free |
|---|---|---|
| Mode value | `TIdentityColumnIds` | `TValidWriterIds` |
| Partition-reader setup | append writer and row identities | filter invalid writer ids |
| Sort key | `(key..., writer_id, row_id)` | `(key...)` |
| Output | rows with the identity suffix | original rows |

Both modes rely on the partition reader's `(writer_id, start_row)`
deduplication. The sort reader does not revalidate or deduplicate its input.

Input rows begin with `comparator.GetLength()` key values in comparator order.
Ingestion starts before the factory returns and retains each non-empty input batch.
Rows are grouped into buckets containing at most `BucketRowCount` rows. A bucket stores shallow row
handles, a flat copy of its key-value descriptors, and an `i32` row
permutation. Sealing a bucket dispatches its permutation sort to the sort
invoker while ingestion continues.

After ingestion and all bucket sorts finish, the serialized invoker performs a
k-way heap merge. It prepares at most one output batch ahead and stops a batch
at `MaxRowsPerRead` or the soft `MaxDataWeightPerRead` limit.
`MergeYieldPeriod` bounds the CPU interval between cooperative merge yields.

An empty range marks the end of stream and remains idempotent. Reads must not
overlap. Canceling a pending read terminally cancels the sort reader and its
in-flight partition read; subsequent reads return the same error. Sort tasks
are weak-bound, and retained input is released after output drains or the last
running sort finishes after an error.

Output ranges reuse the input row descriptors and share an input-batch holder,
so they keep string payloads alive without retaining the sort reader.

### v1 limitations

- The whole partition is retained until sorting finishes.
- One asynchronous task is dispatched per bucket.
- No `IMemoryUsageTracker` or statistics.
- No late valid-writer finalization for identity-preserving input.
- Schemaless name-table normalization must happen before the sort reader.

## Sorted Merging Reader

`CreateIdentityAwareSortedMergingReader`
(`sorted_merging_reader.{h,cpp}`) reads regular table chunks produced by
intermediate identity-preserving sort jobs. Its comparator covers the complete
physical key `(SortBy..., writer_id, row_id)` and is derived from the
intermediate output schema. Such a schema places all key columns first; table
schema validation enforces this prefix invariant, and the schemaless chunk
writer serializes schema columns in schema order.

The factory requires at least three comparator columns and moves the physical
comparator into the stock merging reader. Since interruptions are unsupported,
it passes an empty merge comparator.

Its pipeline is exact:

```text
stock merge -> valid-writer filtering -> adjacent identity deduplication -> identity projection
```

Identity values are never returned downstream. An output batch may be empty
and non-null when every merged row in one source batch is rejected. Validation
errors become terminal readiness errors.

Interruption is unsupported: splitting before every copy of an identity or a
user-key group is processed would break global deduplication. Nontrivial reduce
combiners are not supported.

## Table Interface Adapters

The writer and the sort readers speak narrow domain interfaces, while a
Layer 3 caller drives a job through `ISchemalessMultiChunkWriter` and
`ISchemalessMultiChunkReader`. Two adapters translate between them.

`CreateShuffleWriterAdapter` (`shuffle_writer_adapter.{h,cpp}`) presents a
shuffle writer as a table writer. Partitioning and the record payload address
values by position, so the adapter materializes every row against the shuffled
schema before handing it over: values are placed at their schema position, an
absent column becomes null, and a null row, a duplicate value id, a value that
violates the schema column's type or requiredness, or a value outside the
schema is an error. The name table is derived from the schema, so
a value id is the schema column index by construction. The materialized rows
are captured into the adapter's own row buffer and held until the underlying
write resolves, so the caller may reuse its row memory as soon as `Write`
returns. A rejected batch is reported through the ready event, which keeps the
error for every later call. `Close` must not be issued with a write in flight:
the shuffle writer requires serialized calls, so the adapter asserts that the
last write has resolved. The shuffle chunks belong to
the controller-owned sessions, so the adapter reports no written chunk specs,
no replicas and no digest; its data statistics count each logical row once.

`CreateSortReaderAdapter` (`sort_reader_adapter.{h,cpp}`) presents a sort
reader as a table reader. A read that the sort reader has not finished yet
yields an empty batch and a pending ready event; a failed one yields an empty
batch and keeps the error in the ready event; an empty range from the sort
reader ends the stream. The adapter carries no data slices, so interruption and
data slice introspection are unsupported.
