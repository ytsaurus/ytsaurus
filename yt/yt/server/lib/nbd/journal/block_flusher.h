#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Background component that moves dirty blocks from the pool to the store.
/*!
 *  It drains the dirty pool down to the configured resident fraction and writes the excess to the
 *  store, keeping several flushes in flight so their store round-trips overlap. Completed flushes
 *  are retired in reservation order.
 *
 *  Thread affinity: any
 */
struct IBlockFlusher
    : public TRefCounted
{
    //! Starts the periodic flush loop. Subscribe to the signals below before calling this.
    virtual void Start() = 0;

    //! Stops the periodic flush loop. Store writes already in flight are not awaited.
    virtual void Stop() = 0;

    //! Nudges the flusher to run immediately instead of waiting for the next periodic tick; used by
    //! the write path when the pool fills up.
    virtual void RequestFlush() = 0;

    //! Fired once per block a flush has durably written to the store, in reservation order.
    /*!
     *  The device uses it to publish the block as clean (repointing the block map and populating
     *  the clean-block cache). Fired inline on the flush path, so handlers must be cheap and
     *  non-blocking.
     */
    DECLARE_INTERFACE_SIGNAL(void(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId), BlockFlushed);

    //! Fired once if flushing fails for good (the store is persistently unwritable).
    /*!
     *  The flusher then gives up permanently; the device uses this to fail itself so the error
     *  surfaces to clients.
     */
    DECLARE_INTERFACE_SIGNAL(void(const TError& error), Failed);
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
