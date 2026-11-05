#pragma once

#include <library/cpp/yt/error/error.h>

#include <util/generic/strbuf.h>

#include <functional>
#include <vector>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Commits one chunk of items; returns the error of a commit that did not land.
template <class T>
using TChunkCommitter = std::function<TError(const std::vector<T>&)>;

//! Invoked for every chunk that landed, in commit order.
template <class T>
using TChunkCommittedHandler = std::function<void(const std::vector<T>&)>;

//! Waits out a moving tablet between rounds. Defaults to #TDelayedExecutor::WaitForDuration,
//! which is what production wants and what a test wants to be rid of.
using TChunkRetryDelayer = std::function<void(TDuration)>;

//! Enough to split the widest chunk down to a single item and retry it a few times.
constexpr int MaxChunkedModificationRounds = 20;

//! Backoff between rounds that a moving tablet forced, growing linearly with the round.
//! Sized against the move itself (seconds), not against the transaction that failed.
constexpr auto ChunkedModificationTransientRetryBackoff = TDuration::MilliSeconds(200);
constexpr auto MaxChunkedModificationTransientRetryBackoff = TDuration::Seconds(2);

//! Commits |items| through |commitChunk| in chunks of at most |itemsPerChunk| items each, calling
//! |onCommitted| for every chunk that lands and returning the errors of those that never did.
//!
//! Two kinds of failure are retried here, and only these two:
//!
//! A write-write conflict is retried on a halved chunk when |splitOnConflict| says the conflict
//! can happen only once per row — which holds for the lease revocation phases, where the worker
//! behind the conflict can never start another transaction after phase 1. Halving then isolates
//! the guilty rows and the rounds converge after at most log2(chunk size) splits. It does NOT hold
//! for a grant: nothing has revoked the superseded worker at that point, so it keeps committing
//! until the grant lands, and splitting would only multiply transactions.
//!
//! A tablet in the middle of a smooth movement is retried whole, after a delay: it rejected the
//! chunk regardless of its contents and comes back within seconds, so the rounds have to outlast
//! the move rather than race it.
//!
//! Anything else is returned to the caller untried: a tablet that is genuinely down stays down for
//! longer than an iteration.
//!
//! An item is passed to |commitChunk| again only as part of a chunk that failed, so a committed
//! item is never committed twice.
template <class T>
std::vector<TError> ModifyInChunks(
    TStringBuf phase,
    const std::vector<T>& items,
    ssize_t itemsPerChunk,
    bool splitOnConflict,
    const TChunkCommitter<T>& commitChunk,
    const TChunkCommittedHandler<T>& onCommitted = {},
    const TChunkRetryDelayer& delay = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController

#define CHUNKED_MODIFICATION_INL_H_
#include "chunked_modification-inl.h"
#undef CHUNKED_MODIFICATION_INL_H_
