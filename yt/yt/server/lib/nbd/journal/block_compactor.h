#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Reclaims space by relocating the surviving blocks out of mostly-dead retired chunks.
/*!
 *  Once a retired chunk's garbage ratio crosses the configured threshold, its still-live blocks are read
 *  and rewritten into fresh chunks and the block map is repointed at the copies; the emptied chunk then
 *  goes fully dead and the store unstages it. Compactions run up to a configured concurrency and the relocated bytes
 *  are throttled.
 *
 *  Thread affinity: any
 */
struct IBlockCompactor
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockCompactor)

////////////////////////////////////////////////////////////////////////////////

IBlockCompactorPtr CreateBlockCompactor(
    TJournalBlockCompactorConfigPtr config,
    IBlockMapPtr blockMap,
    IBlockStorePtr blockStore,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

IBlockCompactorPtr GetNullBlockCompactor();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
