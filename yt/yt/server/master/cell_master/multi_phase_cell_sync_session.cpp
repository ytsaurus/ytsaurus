#include "multi_phase_cell_sync_session.h"

#include "private.h"
#include "bootstrap.h"
#include "multicell_manager.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

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

void TMultiPhaseCellSyncSession::ScheduleSyncWithUpstream()
{
    if (SyncWithUpstream_ == ESyncRequest::DontNeed) {
        SyncWithUpstream_ = ESyncRequest::Need;
    }
}

void TMultiPhaseCellSyncSession::ScheduleSyncWithSequoiaTransactions()
{
    if (SyncWithSequoiaTransactions_ == ESyncRequest::DontNeed) {
        SyncWithSequoiaTransactions_ = ESyncRequest::Need;
    }
}

TFuture<void> TMultiPhaseCellSyncSession::Sync(const TCellTagList& cellTags, TFuture<void> additionalFuture)
{
    std::vector<TFuture<void>> additionalFutures;
    if (additionalFuture) {
        additionalFutures.push_back(std::move(additionalFuture));
    }
    return Sync(cellTags, std::move(additionalFutures));
}

TFuture<void> TMultiPhaseCellSyncSession::Sync(const TCellTagList& cellTags, std::vector<TFuture<void>> additionalFutures)
{
    YT_ASSERT(std::ranges::find(additionalFutures, TFuture<void>()) == additionalFutures.end());

    auto& syncFutures = additionalFutures; // Just a better name.
    syncFutures.reserve(cellTags.size()
        + additionalFutures.size()
        + (SyncWithUpstream_ == ESyncRequest::Need ? 1 : 0)
        + (SyncWithSequoiaTransactions_ == ESyncRequest::Need ? 1 : 0));

    auto addAsyncResult = [&] (TFuture<void> future) {
        if (!future.IsSet() || !future.GetOrCrash().IsOK()) {
            syncFutures.push_back(std::move(future));
        }
    };

    if (SyncWithSequoiaTransactions_ == ESyncRequest::Need) {
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        addAsyncResult(hydraManager->SyncWithLeader().Apply(BIND([transactionSupervisor] {
            return transactionSupervisor->WaitUntilPreparedTransactionsFinished();
        })));
        SyncWithSequoiaTransactions_ = ESyncRequest::Done;
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    if (SyncWithUpstream_ == ESyncRequest::Need) {
        addAsyncResult(multicellManager->SyncWithUpstream());
        SyncWithUpstream_ = ESyncRequest::Done;
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
        return OKFuture;
    }

    YT_LOG_DEBUG_UNLESS(syncCellTags.empty(), "Request will synchronize with other cells (CellTags: %v)",
        syncCellTags);

    return AllSucceeded(std::move(syncFutures));
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

    if (SyncWithUpstream_ != ESyncRequest::DontNeed &&
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
