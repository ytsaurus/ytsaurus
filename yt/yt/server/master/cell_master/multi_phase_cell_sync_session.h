#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

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

    void ScheduleSyncWithUpstream();
    void ScheduleSyncWithSequoiaTransactions();

    TFuture<void> Sync(const NObjectClient::TCellTagList& cellTags, TFuture<void> additionalFuture = {});
    TFuture<void> Sync(const NObjectClient::TCellTagList& cellTags, std::vector<TFuture<void>> additionalFutures);

private:
    TBootstrap* const Bootstrap_;
    const NLogging::TLogger Logger;

    enum class ESyncRequest {
        DontNeed,
        Need,
        Done,
    };

    ESyncRequest SyncWithUpstream_ = ESyncRequest::DontNeed;
    ESyncRequest SyncWithSequoiaTransactions_ = ESyncRequest::DontNeed;
    NObjectClient::TCellTagList SyncedWithCellTags_;

    bool RegisterCellToSyncWith(NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TMultiPhaseCellSyncSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
