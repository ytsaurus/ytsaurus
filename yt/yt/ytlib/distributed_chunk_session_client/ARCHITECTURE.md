# Distributed Chunk Session Client Architecture

## Overview

`yt/yt/ytlib/distributed_chunk_session_client/` contains the current client-side implementation of Layer 1 distributed chunk sessions.

Today this layer is intentionally small:

- `TDistributedChunkSessionController` creates the backing journal chunk, allocates write targets, starts the remote sequencer session, and keeps it alive with periodic pings.
- `TDistributedChunkSessionPool` multiplexes several controller-backed sessions per logical slot, exposes sessions that are ready for immediate use, and seals their chunks through master-side `ScheduleChunkSeal`.
- `TDistributedChunkWriter` sends opaque records to that sequencer over RPC.
- `TDistributedChunkSessionReader` reads records from a single chunk that may still be receiving writes; it transitions from a probe-driven active phase to a quorum-info-driven final phase once writers signal completion or the chunk is observed sealed.
- `FetchDistributedChunkSessionSealSummaries` performs one throttled Object Service batch that fetches master-side seal summaries.
- `IDistributedChunkSessionSealMonitor` globally batches chunks from operation-scoped subscriptions and polls master until they are sealed.

A session pool created with a seal monitor subscribes to it to recover terminal session progress after a sequencer failure; the controller agent supplies one, the Shuffle Service does not.

## Components

### `IDistributedChunkSessionController` / `TDistributedChunkSessionController`

The controller owns session lifecycle on the client side.

- `StartSession()` creates a `JournalChunk` on master.
- It allocates write targets via `AllocateWriteTargets`.
- It shuffles the targets and chooses the first replica host as the sequencer node.
- It sends `StartSession` to that node, passing the session id, timeout, target replicas and their node descriptors, and YSON-serialized journal writer options/config.
- After the remote session starts, it launches a periodic `PingSession` executor.
- `Close()` sends `FinishSession` to the sequencer and resolves the controller's closed future after the ping executor stops.
- `GetClosedFuture()` reports session termination for both clean shutdown and error paths.
- `GetSessionId()` is only valid once `StartSession()` has succeeded, including while the session is closing; it returns a null session id until then.
- `StartSession()` now returns `TStartedSessionInfo {SessionId, SequencerNode, Replicas}` so the pool can retain chunk metadata.
- `ProgressUpdated` reports the session's own progress as a `TControllerSessionProgress`:
  zero or more `TSessionInFlightProgress` updates, then exactly one terminal
  alternative -- `TSessionFinalProgress` on a clean close, `TSessionCloseFailed`
  otherwise. The terminal is raised before the closed future resolves and is
  replayed to a subscriber that arrives after the session ended, so every
  subscriber receives it exactly once regardless of who called `Close()`.

Internally the controller uses the state machine `Created -> Starting -> Running -> Closing -> Closed`.
Entering `Closed` is what raises the terminal alternative. Successful ping responses
carrying strictly newer progress raise the in-flight alternative; stale or duplicate
values are ignored, a response from a pre-26.2 sequencer without progress is accepted
without publishing an update, and progress that is negative or inconsistent with what
was already confirmed fails the session.

### `IDistributedChunkSessionPool` / `TDistributedChunkSessionPool`

The pool owns write-side slot lifecycle above individual controllers.

- The slot cookie range is fixed at construction: a cookie outside `[0, slotCount)` is rejected before any slot is created, so a cookie the caller never owned cannot start a session on a chunk nothing finalizes. `GetSession` and `GetSlotChunks` report it in the returned future; `FinalizeSlot` throws.
- `GetSession(slotCookie)` creates the first controller-backed session for a slot or reuses one of the active sessions.
- `GetSession(slotCookie, excludedSessionId)` creates another session while the slot is below `max_active_sessions_per_slot`; otherwise it returns another active or pending session when possible.
- Each slot keeps every created session id in `AllSessionIds` and keeps only currently writable ones in `ActiveSessionIds`; `Sessions` stores the per-session state.
- The pool subscribes to each controller's closed future. Any closure removes the session from the active set and schedules `ScheduleChunkSeal` for the chunk exactly once.
- `FinalizeSlot(slotCookie)` closes every active session for the slot, marks the slot finalized, and schedules sealing for every chunk produced by that slot.
- `GetSlotChunks(slotCookie)` returns a future with `ChunkId + Replicas` for all sessions ever created for the slot.
- `GetReadySessions()` returns at most one currently active session for each non-finalized slot. It skips slots whose sessions are still starting. The Shuffle Service uses this current set when registering a mapper so the mapper can seed its partition-session cache without waiting for an extra per-partition RPC. The controller agent does the same for its push-based shuffle: each partition job spec carries the pool's ready sessions.

### `FetchDistributedChunkSessionSealSummaries` / `IDistributedChunkSessionSealMonitor`

The free function fetches one Object Service batch from one master cell and uses a per-cell throttler manager supplied by its caller. It returns `ChunkId + RecordCount + CompressedDataSize` only for chunks observed as sealed. Unsealed and missing chunks are omitted. Other Object Service subrequest errors fail the fetch so the monitor can back off that master cell.

The controller agent owns one seal monitor and shares it across operation-scoped, move-only subscriptions rather than giving every session pool its own polling loop. `TrackChunks()` adds globally unique chunk ids asynchronously. The monitor runs an independent pump for each master cell, with at most one fetch in flight per cell. Each fetch fairly selects at most `max_chunks_per_fetch` chunks from the active subscriptions. A successful response is processed immediately: sealed chunks are delivered, unsealed chunks become eligible again after `poll_period`, and another eligible batch may start without waiting for other cells. The supplied per-cell-throttled fetcher controls the aggregate request rate. To avoid an accumulated-token burst, the throttler's `limit * period` bucket capacity should not exceed one full fetch batch. A failed fetch backs off only its cell. Destroying the subscription unregisters all of its pending chunks, and an in-flight result is ignored once the subscription is inactive.

### `IDistributedChunkWriter` / `TDistributedChunkWriter`

The writer is a thin RPC wrapper.

- It is constructed from the selected sequencer node, session id, native connection, and RPC timeout config.
- `WriteRecord(TSharedRef)` sends exactly one `WriteRecord` RPC with the raw record bytes attached.
- The writer does not serialize rows, batch records, or own session lifecycle.

### `IDistributedChunkSessionReader` / `TDistributedChunkSessionReader`

The reader consumes records from one chunk produced by a distributed chunk session. It is intentionally narrower than the journal client's quorum reader because it is designed to keep up with an actively-written chunk and then converge on the sealed-chunk view without coordination from the writer host.

State machine: a one-way transition from `Active` (Phase 1) to `Final` (Phase 2). All mutation runs on a serialized invoker built from one supplied to the factory.

- `Read()` returns at most one batch of records starting from the current cursor; `Finished` indicates `Cursor >= EffectiveEnd`.
- `SetAllWritersFinished()` posts a closure that sets the phase to `Final`, leaving the record count to be learned lazily by Phase 2. The overload `SetAllWritersFinished(finalRecordCount, finalCompressedDataSize)` additionally records both values so Phase 2 skips the `ComputeQuorumInfo` round trip; the compressed data size sizes Phase-2 reads, and the two-argument overload makes supplying a count without it impossible at compile time.
- `GetStatistics()` returns `TDistributedChunkSessionReaderStatistics` (atomic counters: master refreshes, probe rounds, sealed detections, quorum-info attempts/successes, read-block calls, error attempts, Phase 1 poll iterations, successful prefetch reads, and prefetch retries (`PrefetchReadCount`, `PrefetchRetryCount`)).

Until Phase-2 prefetch starts, missing replica descriptors trigger `LocateChunks` against
master. The reader merges the returned node directory and retries with the
located replicas. Unchanged replica ids still count as successful recovery when
their descriptors become available. Failed resolution consumes the existing
error budget and backoff; a complete directory needs no master refresh.

Phase 1 step:
1. `Probe` issues `GetChunkMeta` (with `MiscExt` only, under the configured workload descriptor) in parallel to all known replicas, with first-good-response semantics. Outcomes: `Sealed` (any replica reports `MiscExt.sealed = true`), `HasData` (any replica reports `RowCount > Cursor`), `NoNewData` (>=1 successful probe but none ahead of cursor), `AllNoSuchChunk`, `AllOtherErrors`.
2. On `HasData`, a single-replica `TReplicationReader` (with `AllowFetchingSeedsFromMaster = false`) reads `[Cursor, min(PickedRowCount, RangeEndIndex))`; the cursor advances by the number of returned blocks.
3. On `NoNewData`, the reader sleeps `PollInterval` (does not consume the error budget).
4. On `Sealed`, the reader transitions to Phase 2.
5. On `AllNoSuchChunk`, the reader runs `Refresh()` (direct `LocateChunks` against master). If the replica set changed, it transitions to Phase 2; otherwise (refresh failed or returned the same set) an attempt is consumed.
6. On `AllOtherErrors`, an attempt is consumed.

Phase 2 (sealed-chunk prefetch):

1. If `ChunkRecordCount` is unset, `ComputeQuorumInfo` is invoked once over the current replica set, yielding both the record count and the compressed data size; on failure the reader runs `Refresh()` and consumes one attempt. When `SetAllWritersFinished` already supplied a final record count and data size, this round trip is skipped.
2. With `ChunkRecordCount` and `CompressedDataSize` known, the sealed range `[Cursor, EffectiveEnd)` (`EffectiveEnd = min(RangeEnd, ChunkRecordCount)`) is served by an autonomous, work-stealing window pool over a single reused `TReplicationReader`:
   - The range is split into up to `prefetch_window_count` contiguous windows (fewer for a small range, so a window is never smaller than one read). Each read targets `sequential_read_size` bytes, converted to a block count via the average record size (`CompressedDataSize / RecordCount`).
   - Each window reads sequentially, advancing its cursor by the number of records the data node actually returns. Because a `ReadBlocks` response is capped server-side (`MaxBlocksPerRead` / `MaxBytesPerRead`), a truncated read is simply continued by the next one -- no slivers, no gap re-reads. A window keeps at most one read in flight and buffers up to `prefetch_depth` reads ahead of the consumer, then pauses (back-pressure).
   - A window that exhausts its range steals the upper half of the window with the largest remainder (when that half is worth at least one read), so fast windows take over slow windows' work.
   - The pool is a self-rescheduling step on the serialized invoker, independent of `Read()`: it harvests completions, rebalances, and issues reads on its own, up to the `prefetch_window_count x prefetch_depth` buffered cap, with a single step ever in flight. Each step runs to completion without a fiber context switch (enforced by `TForbidContextSwitchGuard`), so it is atomic against `Read()` and the recovery fibers below. It reschedules on a single wakeup that every progress source pings -- a completed read, a completed recovery, or `Read()` freeing a depth slot; the atomicity is what makes that single promise race-free. `Read()` consumes the pool -- it returns the next ready batch (from the window holding the most buffered batches, round-robin on ties) and frees a depth slot, or parks until the pool produces one. Records are delivered out of order; no index is delivered twice.
3. A read failure or empty response starts a per-window recovery on its own fiber, so the step itself never blocks on it: the recovery refreshes replicas from master (rebuilding the shared reader if the set changed), waits out the backoff, then lets the window retry. Each window carries its own consecutive-failure budget (`max_read_attempts`) and backoff, both reset by any successful read, so transient failures separated by progress do not accumulate into a lifetime limit; a window that exhausts its budget fails the reader. Simultaneous failures do not all query master -- a replica epoch stamped on each read at issue time lets only the first recovery of a generation refresh, and the rest reuse its result. A data node returning more records than requested is a protocol violation: the reader logs `YT_LOG_ALERT` and fails without retrying.

Every consumed attempt advances `ErrorBackoff` (exponential-with-jitter: starts at `MinBackoff`, grows by `BackoffMultiplier`, capped at `MaxBackoff`) and sleeps for the resulting interval before retrying.

Concurrent `Read()` calls are not supported -- the reader verifies single-in-flight on the serialized invoker.

### Config

`TDistributedChunkSessionControllerConfig` contains:

- session ping period,
- server-side session timeout,
- account / medium / vitality for the created chunk,
- RPC timeouts,
- maximum consecutive ping failures before the controller self-closes.

`TDistributedChunkSessionPoolConfig` currently contains:

- `max_active_sessions_per_slot`.
- `chunk_seal_retry_backoff`.
- `chunk_seal_rpc_timeout`.

`TDistributedChunkSessionSealMonitorConfig` contains:

- the per-request chunk limit,
- the minimum interval before an unsealed chunk is checked again,
- per-cell retry backoff for failed fetches.

`TDistributedChunkWriterConfig` currently contains only the write RPC timeout.

`TDistributedChunkSessionReaderConfig` contains:

- the underlying replication reader config,
- the inherited `workload_descriptor` (`TWorkloadConfig`, default `user_batch`), applied to the probe, the record reads and `ComputeQuorumInfo`,
- per-RPC timeouts (`probe_timeout`, `quorum_probe_timeout`, `refresh_timeout`),
- Phase 1 idle poll interval,
- maximum read attempts (`max_read_attempts`) and the error backoff strategy,
- the replica-lag limit forwarded to `ComputeQuorumInfo`,
- Phase-2 prefetch tuning: parallel window count, per-read size in bytes, and per-window depth (`prefetch_window_count`, `sequential_read_size`, `prefetch_depth`).

### RPC Contract

The current protocol is defined in `proto/session_service.proto`.

- `StartSession` starts a remote sequencer for an already-created chunk.
- `PingSession` only renews server-side liveness.
- `WriteRecord` carries the session id plus one attachment with the opaque record bytes.
- `FinishSession` requests sequencer shutdown.

Ping and finish responses carry the sequencer's progress. Both fields are optional for rolling protocol compatibility: a pre-26.2 sequencer omits them, and the controller then terminates the session without any logical counters.

## Session Lifecycle

1. The caller creates a controller with journal writer options/config and a master transaction id.
2. `StartSession()` creates the journal chunk on master.
3. The controller allocates chunk replicas and chooses a sequencer node from that replica set.
4. The controller sends `StartSession` to the chosen node.
5. Writers send `WriteRecord` RPCs directly to that sequencer.
6. The controller periodically renews the session lease with `PingSession`.
7. `Close()` sends `FinishSession`, waits for the RPC pipeline to complete, stops the ping executor, and resolves the closed future.

With the pool enabled:

1. The caller asks the pool for a session for a logical slot.
2. The pool either reuses an existing active session or creates a new controller-backed session.
3. Writers use the returned `SessionId` and `SequencerNode`.
4. Retry requests can expand one slot into several active sessions up to the configured cap.
5. `GetReadySessions()` can advertise one already-started session per writable slot to a batch of new writers.
6. Controller closure or explicit `FinalizeSlot()` schedules asynchronous chunk sealing via `ScheduleChunkSeal`.

## Failure Handling

The controller handles three distinct failure classes:

- Start failure: if chunk creation, target allocation, or remote session startup fails, the controller transitions from `Starting` to `Closed` and resolves the closed future with that error.
- Session expiry: if `PingSession` returns `NoSuchSession`, the controller closes immediately.
- Transport or node failure: non-`NoSuchSession` ping failures are counted, and the controller closes after `MaxConsecutivePingFailures`.

Write failures are not hidden by the client layer. `TDistributedChunkWriter` returns the underlying RPC error to its caller unchanged.

Pool-level failure handling is intentionally conservative in v1:

- mapper retry does not retire the excluded session globally;
- controller closed future is the only global liveness signal;
- sealing always goes through `ScheduleChunkSeal`, both for unexpected closure and for clean slot finalization.

## Tested Behavior

`yt/tests/cpp/test_distributed_chunk_sessions/test_distributed_chunk_sessions.cpp` currently exercises the main client-side behavior:

- `MasterSealSummaryFetchWaitsForThrottler`, `MasterSealSummaryFetchOmitsUnsealedChunk`, and `MasterSealSummaryFetchOmitsMissingChunk` verify throttled single-batch fetching and omission of unsealed or unavailable chunks.
- `SingleWriter` and `MultipleWriters` verify successful session start and record delivery.
- `SessionTimeout` and `GetClosedFutureResolvedOnSessionExpiry` verify lease expiry handling.
- `DoubleClose` verifies repeated `Close()` calls are safe.
- `SequencerNodeDiesAfterWrite` verifies the controller closes after repeated ping failures while already-written data remains readable at quorum.
- `ReadWithFreshNodeDirectory` and `ReadWithFreshNodeDirectoryAndKnownRecordCount` verify descriptor recovery with empty or partially populated directories across reader phases; `FreshNodeDirectoryRefreshFailureIsBounded` checks that failed resolution exhausts the retry budget.
- `ReadAllRecordsFromSealedChunk`, `ReadInterleavedWithWriter`, `SealedDetectedViaProbe`, `SetAllWritersFinishedNoCount`, `BoundedReaderRespectsRangeEnd`, `ChunkShorterThanRange`, `EmptyRange`, `EmptyChunk`, `Phase2DoesNotProbe`, `NoQuorumInfoWhenCountProvided`, `SlowWriterDoesNotExhaustBudget`, and `SetAllWritersFinishedMidRead` exercise the reader's Phase 1/Phase 2 paths, range bounds, statistics invariants, and phase-transition concurrency.
- `PrefetchAdvancesWithoutRead` confirms the pool keeps prefetching with no `Read()` in flight; `Phase2PerWindowBudgetTerminatesUnreadableWindow` confirms a window stuck on an unreadable range exhausts its per-window budget and fails the reader while sibling windows still deliver; `RecoveryCancellationDoesNotTerminate` confirms a recovery fiber canceled during backoff unwinds instead of terminating the process; window-count, read-size, and depth permutations cover the work-stealing and back-pressure paths.

`yt/tests/cpp/test_distributed_chunk_sessions/test_node_directory.cpp` verifies
session startup with request-supplied descriptors or descriptors already known to
the sequencer, shared-directory population, missing-descriptor rejection, and
actual write/finish/read behavior.
Controlled master responses also verify that reader failures distinguish missing
replicas from unresolved node descriptors and exhaust the retry budget.

`yt/ytlib/unittests/distributed_chunk_session_controller_ut.cpp` runs the controller against
fake master and sequencer services: final progress on a clean close, an empty final
alternative from a pre-26.2 sequencer, terminal replay to a late subscriber, in-flight
updates preceding the single terminal one, a lost session raising a close failure, a start
failure raising one too, and the session id staying observable while closing.
The fake sequencer also checks that startup supplies the allocated targets' node
descriptors and expected addresses.

`yt/ytlib/unittests/distributed_chunk_session_seal_monitor_ut.cpp` verifies cross-subscription batching, repeated polling of unsealed chunks, bounded non-overlapping fetches, per-cell retry isolation, and RAII unregistration while a fetch is in flight.

## Current Limitations

- One RPC is sent per record.
- Write target allocation still lives inside the controller.
- There are no resume/recovery helpers such as `ResumeDistributedChunkSession`.

The planned protocol and recovery extensions are described in
[Distributed Chunk Session Lifecycle Extensions](../../../design-docs/YT-27781-push-based-shuffle/session-lifecycle-extensions.md).
