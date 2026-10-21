# Distributed Chunk Session Server Architecture

## Overview

`yt/yt/server/lib/distributed_chunk_session_server/` implements the current sequencer side of Layer 1 distributed chunk sessions.

This module is built around three responsibilities:

- `TDistributedChunkSessionService` exposes the RPC interface.
- `TDistributedChunkSessionManager` owns active sessions and their leases.
- `TDistributedChunkSessionSequencer` wraps `TJournalChunkWriter` for a single session.

The server exposes live and final progress counters. It does not retain results
after a session is removed and has no restart recovery helpers.

## Components

### `TDistributedChunkSessionService`

The service is the public RPC entry point.

- `StartSession` deserializes the target replicas and journal writer config, merges the supplied target node descriptors into the connection's node directory, rejects targets that still have no descriptor, then asks the manager to create a sequencer.
- `PingSession` renews the corresponding session lease and returns the
  sequencer's current progress.
- `WriteRecord` validates that there is exactly one attachment, reads the
  caller-supplied logical statistics, looks up the sequencer, and forwards both
  the statistics and opaque record bytes.
- `FinishSession` looks up the sequencer, closes it, and returns final progress
  only when close succeeds.

`WriteRecord` is registered as a heavy RPC method. All methods use the client-side distributed chunk session service descriptor and proto definitions.

### `TDistributedChunkSessionManager`

The manager owns active server-side sessions.

- Sessions are keyed by `TSessionId`.
- Each entry stores a pair of `(sequencer, lease)`.
- `StartSession` rejects duplicate session ids with `SessionAlreadyExists`.
- `RenewSessionLeaseAndGetSequencerOrThrow` renews the stored lease and returns
  the sequencer from the same session-map lookup.
- `GetSequencerOrThrow` returns `NoSuchSession` for sessions absent from the map.

The manager subscribes to each sequencer's closed future and removes it from the map when the sequencer finishes, closing the stored lease with it.

### `TDistributedChunkSessionSequencer`

The sequencer wraps `TJournalChunkWriter` and owns progress accounting.

- It is created with the session id, preallocated targets, journal writer options/config, connection, and invoker.
- `Open()` opens the underlying journal writer.
- `WriteRecord(TSharedRef, TDistributedChunkSessionWriteStatistics)` validates
  nonnegative logical statistics, refuses a record whose statistics would
  overflow the session counters, and forwards one opaque record to the journal
  writer. Compressed size is derived from the attachment itself.
- Each accepted write is associated with the record index returned by the
  journal writer. Successful completions advance progress only across the
  largest contiguous prefix, so a later write cannot hide an earlier pending
  write.
- Progress contains journal `RecordCount`, table `RowCount`, `DataWeight`,
  `CompressedDataSize`, and `UncompressedDataSize` as distinct cumulative
  counters.
- `Close()` must not precede `Open()`. It closes the underlying writer as soon
  as every record has been submitted, so the last partial batch is flushed at
  once, and waits for both that close and every accepted write callback. It
  returns final progress only if close succeeds and no accepted write failed.

If writer open or write fails, or the writer reports failure through its `Failed` signal, the sequencer logs the failure and triggers self-close.

## RPC Contract

The current protocol is defined in `yt/yt/ytlib/distributed_chunk_session_client/proto/session_service.proto`.

- `StartSession` request contains the session id, lease timeout, target replicas and their node descriptors, and serialized journal writer options/config.
- `PingSession` carries the session id and returns current progress.
- `WriteRecord` carries the session id and caller-supplied `DataWeight`,
  `UncompressedDataSize`, and table `RowCount`; the compressed payload is one
  RPC attachment.
- `FinishSession` carries the session id and returns final progress after a
  successful sequencer close.

The outer write-statistics and progress fields are optional for rolling
protocol compatibility. A request without statistics contributes zero logical
counters while still advancing journal `RecordCount` and compressed size. The
server always fills both progress responses; the client side that supplies
statistics and consumes progress comes in a follow-up. The individual counters
are optional for future protocol evolution.

The target node directory is optional: an older controller omits it, and startup
then succeeds only if the sequencer already knows the targets. Supplied descriptors
are merged into the connection's directory before opening the journal writer.

Unresolved targets fail session startup with an RPC error; they cannot be located
as chunk replicas before the journal writer opens and confirms them on master.

## Session Lifecycle

1. The client chooses a sequencer node and sends `StartSession`.
2. The service merges the supplied node descriptors, rejects unresolved targets, and asks the manager to start the session.
3. The manager reserves the session id, creates a new sequencer, and calls `Open()`; once the writer is open it creates the lease and stores the pair in the active-session map.
4. Writers send `WriteRecord` requests to the same node.
5. Periodic `PingSession` requests renew the lease and return the current
   quorum-confirmed prefix.
6. `FinishSession` closes the sequencer explicitly and returns final progress,
   or lease expiry closes it implicitly.
7. Once the sequencer finishes, the manager removes it from the map.

## Lease Management

Lease handling is centralized in the manager.

- One `TLease` per session, created once the writer is open.
- Lease expiry asynchronously invokes `OnSequencerLeaseExpired`.
- On expiry, the manager finds the sequencer and closes it.
- The closed-future subscription later erases the map entry and closes the lease.

Session liveness is tracked entirely by the manager and lease subsystem.

## Failure Handling

The current server code handles failures at four levels:

- Service level: unresolved targets fail startup with `NoSuchNode`.
  Missing sessions are reported by `GetSequencerOrThrow`;
  `RenewSessionLeaseAndGetSequencerOrThrow` additionally reports a mapped session
  whose lease has already expired.
- Manager level: duplicate registration is rejected, expired sessions disappear from the map, and lease expiry forces closure.
- Sequencer level: open/write failures log an error, trigger `Close()`, and propagate the failure to the caller.
- Progress level: negative logical counters and counter overflow fail the write
  path. Duplicate or regressing journal indexes and a noncontiguous final prefix
  violate the locally assigned writer-index invariant and are fatal.

The server relies on `TJournalChunkWriter` for data node session management and actual journal write semantics.

## Current Limitations

- Progress is kept only in the live sequencer and is lost when the manager
  removes the session.
- Progress does not include the physical journal disk space required for direct
  master sealing.
- There is no finished-session cache.
- There is no `GetSessionResult` RPC.
- There is no explicit restart recovery protocol.
