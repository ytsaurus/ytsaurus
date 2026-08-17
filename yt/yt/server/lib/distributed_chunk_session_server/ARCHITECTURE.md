# Distributed Chunk Session Server Architecture

## Overview

`yt/yt/server/lib/distributed_chunk_session_server/` implements the current sequencer side of Layer 1 distributed chunk sessions.

This module is built around three responsibilities:

- `TDistributedChunkSessionService` exposes the RPC interface.
- `TDistributedChunkSessionManager` owns active sessions and their leases.
- `TDistributedChunkSessionSequencer` wraps `TJournalChunkWriter` for a single session.

The server does not yet expose progress counters, retained final session results, or restart recovery helpers.

## Components

### `TDistributedChunkSessionService`

The service is the public RPC entry point.

- `StartSession` deserializes the target replicas and journal writer config, then asks the manager to create a sequencer.
- `PingSession` renews the corresponding session lease.
- `WriteRecord` validates that there is exactly one attachment, looks up the sequencer, and forwards the opaque record bytes.
- `FinishSession` looks up the sequencer and closes it.

`WriteRecord` is registered as a heavy RPC method. All methods use the client-side distributed chunk session service descriptor and proto definitions.

### `TDistributedChunkSessionManager`

The manager owns active server-side sessions.

- Sessions are keyed by `TSessionId`.
- Each entry stores a pair of `(sequencer, lease)`.
- `StartSession` rejects duplicate session ids with `SessionAlreadyExists`.
- `RenewSessionLease` renews the stored lease.
- `GetSequencerOrThrow` returns `NoSuchSession` for invalid or expired sessions.

The manager subscribes to each sequencer's closed future and removes it from the map when the sequencer finishes.

### `TDistributedChunkSessionSequencer`

The sequencer is a thin wrapper around `TJournalChunkWriter`.

- It is created with the session id, preallocated targets, journal writer options/config, connection, and invoker.
- `Open()` opens the underlying journal writer.
- `WriteRecord(TSharedRef)` forwards one opaque record to the journal writer.
- `Close()` closes the underlying writer and exposes the result via `GetClosedFuture()`.

If writer open or write fails, the sequencer logs the failure and triggers self-close.

## RPC Contract

The current protocol is defined in `yt/yt/ytlib/distributed_chunk_session_client/proto/distributed_chunk_session_service.proto`.

- `StartSession` request contains the session id, lease timeout, target replicas, and serialized journal writer options/config.
- `PingSession` carries only the session id.
- `WriteRecord` carries only the session id in the proto body; the actual payload is provided as one RPC attachment.
- `FinishSession` carries only the session id.

All success responses are currently empty. There are no progress fields in `TRspPingSession` and no final statistics in `TRspFinishSession`.

## Session Lifecycle

1. The client chooses a sequencer node and sends `StartSession`.
2. The service forwards the request to the manager.
3. The manager creates a lease and a new sequencer, stores them in the active-session map, and calls `Open()`.
4. Writers send `WriteRecord` requests to the same node.
5. Periodic `PingSession` requests renew the server-side lease.
6. `FinishSession` closes the sequencer explicitly, or lease expiry closes it implicitly.
7. Once the sequencer finishes, the manager removes it from the map.

## Lease Management

Lease handling is centralized in the manager.

- `StartSession` creates one `TLease` per session.
- Lease expiry asynchronously invokes `OnSequencerLeaseExpired`.
- On expiry, the manager finds the sequencer and closes it.
- The closed-future subscription later erases the map entry.

This means the service itself is stateless beyond its reference to the manager; session liveness is tracked entirely by the manager and lease subsystem.

## Failure Handling

The current server code handles failures at three levels:

- Service level: missing sessions are reported by `GetSequencerOrThrow`.
- Manager level: duplicate registration is rejected, expired sessions disappear from the map, and lease expiry forces closure.
- Sequencer level: open/write failures log an error, trigger `Close()`, and propagate the failure to the caller.

The server relies on `TJournalChunkWriter` for data node session management and actual journal write semantics.

## Current Limitations

- Ping responses do not expose row counts, data weight, or compressed data size.
- Finish responses do not expose final exact statistics.
- There is no finished-session cache.
- There is no `GetSessionResult` RPC.
- There is no explicit restart recovery protocol.
