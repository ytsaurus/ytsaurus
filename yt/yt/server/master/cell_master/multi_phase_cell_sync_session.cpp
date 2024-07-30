#include "multi_phase_cell_sync_session.h"

#include "private.h"
#include "bootstrap.h"
#include "multicell_manager.h"

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NCellMaster {

using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TMultiPhaseCellSyncSession::TMultiPhaseCellSyncSession(
    TBootstrap* bootstrap,
    NLogging::TLogger logger)
    : Bootstrap_(bootstrap)
    , Logger(std::move(logger))
{ }

void TMultiPhaseCellSyncSession::SetSyncWithUpstream(bool syncWithUpstream)
{
    SyncWithUpstream_ = syncWithUpstream;
}

TFuture<void> TMultiPhaseCellSyncSession::Sync(const TCellTagList& cellTags, std::vector<TFuture<void>> additionalFutures)
{
    YT_ASSERT(std::find(additionalFutures.begin(), additionalFutures.end(), TFuture<void>()) == additionalFutures.end());

    ++PhaseNumber_;

    auto syncWithUpstream = SyncWithUpstream_ && PhaseNumber_ == 1;

    auto& syncFutures = additionalFutures; // Just a better name.
    syncFutures.reserve(syncFutures.size() + cellTags.size() + (syncWithUpstream ? 1 : 0));

    auto addAsyncResult = [&] (TFuture<void> future) {
        if (!future.IsSet() || !future.Get().IsOK()) {
            syncFutures.push_back(std::move(future));
        }
    };

    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    if (syncWithUpstream) {
        addAsyncResult(multicellManager->SyncWithUpstream());
    }

    const auto& hiveManager = Bootstrap_->GetHiveManager();
    TCellTagList syncCellTags;
    for (auto cellTag : cellTags) {
        if (!RegisterCellToSyncWith(cellTag)) {
            continue;
        }

        auto cellId = multicellManager->GetCellId(cellTag);
        addAsyncResult(hiveManager->SyncWith(cellId, true));
        syncCellTags.push_back(cellTag);
    }

    if (syncFutures.empty()) {
        return VoidFuture;
    }

    YT_LOG_DEBUG_UNLESS(syncCellTags.empty(), "Request will synchronize with other cells (CellTags: %v)",
        syncCellTags);

    return AllSucceeded(std::move(syncFutures));
}

TFuture<void> TMultiPhaseCellSyncSession::Sync(const TCellTagList& cellTags, TFuture<void> additionalFuture)
{
    YT_ASSERT(additionalFuture);

    std::vector<TFuture<void>> additionalFutures;
    additionalFutures.push_back(std::move(additionalFuture));
    return Sync(cellTags, std::move(additionalFutures));
}

bool TMultiPhaseCellSyncSession::RegisterCellToSyncWith(TCellTag cellTag)
{
    if (std::find(SyncedWithCellTags_.begin(), SyncedWithCellTags_.end(), cellTag) != SyncedWithCellTags_.end()) {
        // Already synced with this cell.
        return false;
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (cellTag == multicellManager->GetCellTag()) {
        // No need to sync with self.
        return false;
    }

    if (SyncWithUpstream_ &&
        multicellManager->IsSecondaryMaster() && cellTag == multicellManager->GetPrimaryCellTag())
    {
        // IHydraManager::SyncWithUpstream will take care of this.
        return false;
    }

    SyncedWithCellTags_.push_back(cellTag);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
