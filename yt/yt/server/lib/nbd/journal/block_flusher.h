#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Moves dirty blocks from the pool to the store.
/*!
 *  Drains the dirty pool down to the configured resident fraction, writing the excess to the store
 *  one batch at a time.
 *
 *  Thread affinity: any
 */
struct IBlockFlusher
    : public TRefCounted
{
    //! Starts the periodic flush loop. Subscribe to the signals below before calling this.
    virtual void Start() = 0;

    //! Stops the periodic flush loop; the dirty pool and any pending flush barrier are failed
    //! asynchronously. Store writes already in flight are not awaited.
    virtual void Stop() = 0;

    //! Nudges the flusher to run immediately instead of waiting for the next periodic tick. The write
    //! path calls this after each put, so draining starts as soon as there is excess rather than up to
    //! a full period later; #force skips the resident-target check for callers that must make progress
    //! regardless (e.g. a put blocked on a full pool).
    virtual void RequestFlush(bool force = false) = 0;

    //! Nudges the flusher to eagerly drain every block enqueued as of this call, down to the pool's
    //! current tail.
    /*!
     *  The future is set once every one of them is in the store; it fails if a flush fails or the
     *  flusher is stopped, and a barrier requested after either is refused outright.
     */
    virtual TFuture<void> RequestFlushBarrier() = 0;

    //! Fired once per block a flush has durably written to the store, in reservation order.
    /*!
     *  The device uses it to publish the block as clean (repointing the block map and populating
     *  the clean-block cache). Fired inline on the flush path, so handlers must be cheap and
     *  non-blocking.
     */
    DECLARE_INTERFACE_SIGNAL(void(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId), BlockFlushed);
};

DEFINE_REFCOUNTED_TYPE(IBlockFlusher)

////////////////////////////////////////////////////////////////////////////////

IBlockFlusherPtr CreateBlockFlusher(
    TJournalBlockFlusherConfigPtr config,
    IDirtyBlockPoolPtr dirtyPool,
    IBlockStorePtr blockStore,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
