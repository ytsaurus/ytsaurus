#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NCellMaster {

///////////////////////////////////////////////////////////////////////////////

//! A helper for syncing with other master cells in multiple phases.
/*!
 *  Stores the set of already synced-with cells, thus avoiding syncing with the
 *  same cell twice.
 *
 *  For use in those situations when, after one sync is done, there may arise a
 *  need to sync with some additional cells (and so on).
 */
class TMultiPhaseCellSyncSession
    : public TRefCounted
{
public:
    TMultiPhaseCellSyncSession(
        TBootstrap* bootstrap,
        NLogging::TLogger logger);

    void SetSyncWithUpstream(bool syncWithUpstream);

    // NB: the #additionalFutures is just to save some allocations and avoid doing this all the time:
    //   auto syncFuture = session->Sync(); // Already calls #AllSucceeded internally.
    //   additionalFutures.push_back(std::move(syncFuture));
    //   AllSucceeded(std::move(additionalFutures)); // Second call to #AllSucceeded.
    TFuture<void> Sync(const NObjectClient::TCellTagList& cellTags, std::vector<TFuture<void>> additionalFutures = {});
    TFuture<void> Sync(const NObjectClient::TCellTagList& cellTags, TFuture<void> additionalFuture);

private:
    TBootstrap* const Bootstrap_;
    const NLogging::TLogger Logger;

    int PhaseNumber_ = 0;
    bool SyncWithUpstream_ = false;
    NObjectClient::TCellTagList SyncedWithCellTags_;

    bool RegisterCellToSyncWith(NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TMultiPhaseCellSyncSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
