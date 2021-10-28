#include "backup_manager.h"
#include "tablet.h"
#include "private.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/tablet_server/proto/backup_manager.pb.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/table_client/proto/table_ypath.pb.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectServer;
using namespace NTableServer;
using namespace NTransactionClient;
using namespace NTransactionServer;

using NTransactionServer::TTransaction;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBackupManager
    : public IBackupManager
    , public TMasterAutomatonPart
{
public:
    explicit TBackupManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TabletManager)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(
                EAutomatonThreadQueue::Default),
            AutomatonThread);

        RegisterMethod(BIND(&TBackupManager::HydraFinishBackup, Unretained(this)));
        RegisterMethod(BIND(&TBackupManager::HydraFinishRestore, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionAborted(
            BIND(&TBackupManager::OnTransactionAborted, MakeWeak(this)));
    }

    void SetBackupBarrier(
        TTableNode* table,
        TTimestamp /*timestamp*/,
        TTransaction* transaction) override
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Setting backup barrier (TableId: %v, TransactionId: %v)",
            table->GetId(),
            transaction->GetId());

        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            if (tablet->GetBackupState() != ETabletBackupState::None) {
                THROW_ERROR_EXCEPTION("Cannot set backup barrier since tablet %v "
                    "is in invalid backup state: expected %Qlv, got %Qlv",
                    tablet->GetId(),
                    ETabletBackupState::None,
                    tablet->GetBackupState());
            }
        }

        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            tablet->CheckedSetBackupState(
                ETabletBackupState::None,
                ETabletBackupState::BarrierRequested);

            // FIXME(ifsmirnov): send messages to tablet cells and wait
            // for confirmation.
            tablet->CheckedSetBackupState(
                ETabletBackupState::BarrierRequested,
                ETabletBackupState::BarrierConfirmed);
        }

        YT_VERIFY(transaction->TablesWithBackupBarriers().insert(table).second);
        UpdateAggregatedBackupState(table);
    }

    void ReleaseBackupBarrier(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction) override
    {
        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            if (tablet->GetBackupState() != ETabletBackupState::BarrierRequested &&
                tablet->GetBackupState() != ETabletBackupState::BarrierConfirmed)
            {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Attempted to release backup barrier from a tablet "
                    "in wrong backup state (TableId: %v, TabletId: %v, TransactionId: %v, "
                    "BackupState: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    transaction->GetId(),
                    tablet->GetBackupState());
                continue;
            }

            tablet->SetBackupState(ETabletBackupState::None);
            // FIXME(ifsmirnov): send messages to tablet cells.
            EraseOrCrash(transaction->TablesWithBackupBarriers(), table);
        }
        UpdateAggregatedBackupState(table);
    }

    void CheckBackupBarrier(
        TTableNode* /*table*/,
        NTableClient::NProto::TRspCheckBackupBarrier* response) override
    {
        response->set_confirmed(true);
    }

    TFuture<void> FinishBackup(TTableNode* table) override
    {
        return FinishBackupTask<NProto::TReqFinishBackup>(table, "backup");
    }

    TFuture<void> FinishRestore(TTableNode* table) override
    {
        return FinishBackupTask<NProto::TReqFinishRestore>(table, "restore");
    }

    void SetClonedTabletBackupState(
        TTablet* clonedTablet,
        const TTablet* sourceTablet,
        ENodeCloneMode mode) override
    {
        if (mode == ENodeCloneMode::Backup) {
            if (sourceTablet->GetBackupState() == ETabletBackupState::BarrierConfirmed) {
                clonedTablet->SetBackupState(ETabletBackupState::BackupStarted);
            } else {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Failed to clone tablet in backup mode since it is "
                    "in invalid backup state (SourceTabletId: %v, DestinationTabletId: %v, "
                    "SourceTableId: %v, DestinationTableId: %v, ExpectedState: %v, "
                    "ActualState: %v)",
                    sourceTablet->GetId(),
                    clonedTablet->GetId(),
                    sourceTablet->GetTable()->GetId(),
                    clonedTablet->GetTable()->GetId(),
                    ETabletBackupState::BarrierConfirmed,
                    sourceTablet->GetBackupState());
                clonedTablet->SetBackupState(ETabletBackupState::BackupFailed);
            }
        } else if (mode == ENodeCloneMode::Restore) {
            if (sourceTablet->GetBackupState() == ETabletBackupState::BackupCompleted) {
                clonedTablet->SetBackupState(ETabletBackupState::RestoreStarted);
            } else {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Failed to clone tablet in restore mode since it is "
                    "in invalid backup state (SourceTabletId: %v, DestinationTabletId: %v, "
                    "SourceTableId: %v, DestinationTableId: %v, ExpectedState: %v, "
                    "ActualState: %v)",
                    sourceTablet->GetId(),
                    clonedTablet->GetId(),
                    sourceTablet->GetTable()->GetId(),
                    clonedTablet->GetTable()->GetId(),
                    ETabletBackupState::BackupCompleted,
                    sourceTablet->GetBackupState());
                clonedTablet->SetBackupState(ETabletBackupState::RestoreFailed);
            }
        } else {
            switch (sourceTablet->GetBackupState()) {
                // If source table was the backup source, cloned tablets are clean.
                case ETabletBackupState::None:
                case ETabletBackupState::BarrierRequested:
                case ETabletBackupState::BarrierConfirmed:
                    clonedTablet->SetBackupState(ETabletBackupState::None);
                    break;

                // If source table was partially or unsuccessfully backed up,
                // cloned tablets are invalid.
                case ETabletBackupState::BackupStarted:
                case ETabletBackupState::BackupFailed:
                    clonedTablet->SetBackupState(ETabletBackupState::BackupFailed);
                    break;

                // Same for restore.
                case ETabletBackupState::RestoreStarted:
                case ETabletBackupState::RestoreFailed:
                    clonedTablet->SetBackupState(ETabletBackupState::RestoreFailed);

                // If source table is a backup table, so is cloned one.
                case ETabletBackupState::BackupCompleted:
                    clonedTablet->SetBackupState(ETabletBackupState::BackupCompleted);
                    break;

                case ETabletBackupState::Mixed:
                    YT_ABORT();
            }
        }
    }

    void UpdateAggregatedBackupState(TTableNode* table) override
    {
        YT_VERIFY(!table->IsExternal());

        table = table->GetTrunkNode();

        std::optional<ETabletBackupState> aggregate;
        for (auto backupState : TEnumTraits<ETabletBackupState>::GetDomainValues()) {
            if (table->TabletCountByBackupState()[backupState] == 0) {
                continue;
            }

            // Failed states prevail over other.
            if (backupState == ETabletBackupState::BackupFailed ||
                backupState == ETabletBackupState::RestoreFailed)
            {
                aggregate = backupState;
                break;
            }

            if (backupState != aggregate.value_or(backupState)) {
                aggregate = ETabletBackupState::Mixed;
            } else {
                aggregate = backupState;
            }
        }

        YT_VERIFY(aggregate);

        if (table->GetAggregatedTabletBackupState() == aggregate) {
            return;
        }

        table->SetAggregatedTabletBackupState(*aggregate);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Updated aggregated backup state (TableId: %v, BackupState: %v)",
            table->GetId(),
            *aggregate);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void HydraFinishBackup(NProto::TReqFinishBackup* request)
    {
        auto transactionId = FromProto<TTabletId>(request->transaction_id());

        TTableNode* table = nullptr;

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        for (const auto& protoTabletId : request->tablet_ids()) {
            auto tabletId = FromProto<TTabletId>(protoTabletId);

            auto* tablet = tabletManager->FindTablet(tabletId);
            if (!IsObjectAlive(tablet)) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Cannot finish backup since tablet is missing (TabletId: %v, TransactionId: %v)",
                    tabletId,
                    transactionId);
                continue;
            }

            if (!tablet->GetTable()) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Cannot finish backup since tablet lacks table "
                    "(TabletId: %v, TransactionId: %v)",
                    tabletId,
                    transactionId);
                continue;
            }

            if (table) {
                YT_VERIFY(table == tablet->GetTable());
            } else {
                table = tablet->GetTable();
            }

            if (tablet->GetBackupState() != ETabletBackupState::BackupStarted) {
                YT_LOG_WARNING_IF(IsMutationLoggingEnabled(),
                    "Attempted to finish backup of the tablet in invalid state "
                    "(TableId: %v, TabletId: %v, BackupState: %v, TransactionId: %v)",
                    table->GetId(),
                    tabletId,
                    tablet->GetBackupState(),
                    transactionId);
                continue;
            }

            tabletManager->WrapWithBackupChunkViews(tablet);

            tablet->CheckedSetBackupState(ETabletBackupState::BackupStarted, ETabletBackupState::BackupCompleted);
        }

        if (table) {
            UpdateAggregatedBackupState(table);
        }
    }

    void HydraFinishRestore(NProto::TReqFinishRestore* request)
    {
        auto transactionId = FromProto<TTabletId>(request->transaction_id());

        TTableNode* table = nullptr;

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        for (const auto& protoTabletId : request->tablet_ids()) {
            auto tabletId = FromProto<TTabletId>(protoTabletId);

            auto* tablet = tabletManager->FindTablet(tabletId);
            if (!IsObjectAlive(tablet)) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Cannot finish restore since tablet is missing (TabletId: %v, TransactionId: %v)",
                    tabletId,
                    transactionId);
                return;
            }

            if (!tablet->GetTable()) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Cannot finish restore since tablet lacks table "
                    "(TabletId: %v, TransactionId: %v)",
                    tabletId,
                    transactionId);
                continue;
            }

            if (table) {
                YT_VERIFY(table == tablet->GetTable());
            } else {
                table = tablet->GetTable();
            }

            if (tablet->GetBackupState() != ETabletBackupState::RestoreStarted) {
                YT_LOG_WARNING_IF(IsMutationLoggingEnabled(),
                    "Attempted to finish restore of the tablet in invalid state "
                    "(TableId: %v, TabletId: %v, BackupState: %v, TransactionId: %v)",
                    table->GetId(),
                    tabletId,
                    tablet->GetBackupState(),
                    transactionId);
                return;
            }

            auto error = tabletManager->PromoteFlushedDynamicStores(tablet);

            if (error.IsOK()) {
                tablet->CheckedSetBackupState(ETabletBackupState::RestoreStarted, ETabletBackupState::None);
            } else {
                // TODO(ifsmirnov): store error somewhere.
                tablet->CheckedSetBackupState(ETabletBackupState::RestoreStarted, ETabletBackupState::RestoreFailed);
            }
        }

        UpdateAggregatedBackupState(table);
    }

    void OnAfterSnapshotLoaded() override
    {
        // TODO(ifsmirnov): This should be done conditionally upon master reign,
        // waiting for it to be available in OnAfterSnapshotLoaded.
        //
        // RecomputeTableBackupStates is idempotent though so it should be safe
        // to call it whatsoever.
        RecomputeTableBackupStates();
    }

    template <class TRequest>
    TFuture<void> FinishBackupTask(TTableNode* table, TStringBuf taskName)
    {
        auto transaction = table->GetTransaction();
        YT_VERIFY(transaction);

        std::vector<TFuture<TMutationResponse>> asyncCommitResults;
        TRequest currentReq;
        int storeCount = 0;

        auto maybeFlush = [&] (bool force) {
            if (storeCount < MaxStoresPerBackupMutation && !force) {
                return;
            }

            ToProto(currentReq.mutable_transaction_id(), transaction->GetId());

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            auto asyncResult = CreateMutation(hydraManager, currentReq)
                ->CommitAndLog(Logger);
            asyncCommitResults.push_back(std::move(asyncResult));

            storeCount = 0;
            currentReq = {};
        };

        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            YT_LOG_DEBUG("Schedule backup task for tablet (Task: %v, TabletId: %v)",
                taskName,
                tablet->GetId());
            ToProto(currentReq.add_tablet_ids(), tablet->GetId());
            storeCount += tablet->GetChunkList()->Statistics().ChunkCount;

            maybeFlush(false);
        }

        maybeFlush(true);

        return AllSucceeded(asyncCommitResults).AsVoid();
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        // NB: ReleaseBackupBarrier modifies transaction->TablesWithBackupBarriers.
        for (auto* table : GetValuesSortedByKey(transaction->TablesWithBackupBarriers())) {
            if (!IsObjectAlive(table)) {
                continue;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Releasing backup barrier on transaction abort (TableId: %v, "
                "TransactionId: %v)",
                table->GetId(),
                transaction->GetId());
            ReleaseBackupBarrier(table, transaction);
        }
    }

    void RecomputeTableBackupStates()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        THashSet<TTableNode*> clearedTables;

        for (auto [id, tablet] : tabletManager->Tablets()) {
            if (!IsObjectAlive(tablet)) {
                continue;
            }
            if (auto* table = tablet->GetTable()) {
                YT_VERIFY(IsObjectAlive(table));

                if (clearedTables.insert(table).second) {
                    table->MutableTabletCountByBackupState() = {};
                }

                ++table->MutableTabletCountByBackupState()[tablet->GetBackupState()];
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IBackupManagerPtr CreateBackupManager(TBootstrap* bootstrap)
{
    return New<TBackupManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
