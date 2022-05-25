#include "backup_manager.h"
#include "tablet.h"
#include "private.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/table_server/replicated_table_node.h>
#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>

#include <yt/yt/server/lib/hydra_common/mutation_context.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/proto/backup_manager.pb.h>

#include <yt/yt/ytlib/table_client/proto/table_ypath.pb.h>

namespace NYT::NTabletServer {

using namespace NApi;
using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectServer;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NTabletNode::NProto;

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
        RegisterMethod(BIND(&TBackupManager::HydraResetBackupMode, Unretained(this)));
        RegisterMethod(BIND(&TBackupManager::HydraOnBackupCheckpointPassed, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionAborted(
            BIND(&TBackupManager::OnTransactionAborted, MakeWeak(this)));
        transactionManager->SubscribeTransactionCommitted(
            BIND(&TBackupManager::OnTransactionCommitted, MakeWeak(this)));
    }

    void StartBackup(
        TTableNode* table,
        TTimestamp timestamp,
        TTransaction* transaction,
        EBackupMode backupMode,
        TTableReplicaId upstreamReplicaId,
        std::optional<TClusterTag> clockClusterTag,
        std::vector<TTableReplicaBackupDescriptor> replicaBackupDescriptors) override
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Setting backup checkpoint (TableId: %v, TransactionId: %v, CheckpointTimestamp: %llx, "
            "BackupMode: %v, ClockClusterTag: %v, BackupableReplicaIds: %v)",
            table->GetId(),
            transaction->GetId(),
            timestamp,
            backupMode,
            clockClusterTag,
            MakeFormattableView(replicaBackupDescriptors, [] (auto* builder, const auto& descriptor) {
                builder->AppendFormat("%v", descriptor.ReplicaId);
            }));

        if (timestamp == NullTimestamp) {
            THROW_ERROR_EXCEPTION("Checkpoint timestamp cannot be null");
        }

        // TODO(ifsmirnov): YT-15032 - backups for tables with hunks.
        const auto& schema = table->GetSchema()->AsTableSchema();
        if (schema->HasHunkColumns()) {
            THROW_ERROR_EXCEPTION("Cannot backup table with hunk columns")
                << TErrorAttribute("table_id", table->GetId());
        }

        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            if (tablet->GetBackupState() != ETabletBackupState::None) {
                THROW_ERROR_EXCEPTION("Cannot set backup checkpoint since tablet %v "
                    "is in invalid backup state: expected %Qlv, got %Qlv",
                    tablet->GetId(),
                    ETabletBackupState::None,
                    tablet->GetBackupState());
            }

            switch (tablet->GetState()) {
                case ETabletState::Unmounted:
                    break;

                case ETabletState::Mounted:
                case ETabletState::Frozen:
                    if (!table->GetMountedWithEnabledDynamicStoreRead()) {
                        THROW_ERROR_EXCEPTION("Dynamic store read must be enabled in order to backup a mounted table")
                            << TErrorAttribute("table_id", table->GetId());
                    }
                    break;

                default:
                    THROW_ERROR_EXCEPTION("Cannot set backup checkpoint since tablet %v "
                        "is in unstable state: expected one of %Qlv, %Qlv, %Qlv, got %Qlv",
                        tablet->GetId(),
                        ETabletState::Unmounted,
                        ETabletState::Mounted,
                        ETabletState::Frozen,
                        tablet->GetState());
            }
        }

        // NB: This validation should not fail given that native client
        // is consistent with master.
        ValidateBackupMode(table, backupMode);

        if (table->GetUpstreamReplicaId() != upstreamReplicaId) {
            THROW_ERROR_EXCEPTION("Invalid upstream replica id: expected %v, got %v",
                upstreamReplicaId,
                table->GetUpstreamReplicaId());
        }

        if (table->GetUpstreamReplicaId() && !clockClusterTag) {
            THROW_ERROR_EXCEPTION("Clock cluster tag must be specified for a replica table")
                << TErrorAttribute("table_id", table->GetId());
        }

        if (!table->GetUpstreamReplicaId() && clockClusterTag) {
            THROW_ERROR_EXCEPTION("Clock cluster tag can be specified only for replica tables")
                << TErrorAttribute("table_id", table->GetId())
                << TErrorAttribute("clock_cluster_tag", clockClusterTag);
        }

        if (table->IsReplicated()) {
            ValidateReplicasAssociation(table, replicaBackupDescriptors, /*validateModes*/ true);
        }

        table->SetBackupCheckpointTimestamp(timestamp);
        table->SetBackupMode(backupMode);
        table->MutableReplicaBackupDescriptors() = replicaBackupDescriptors;

        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            if (auto* cell = tablet->GetCell()) {
                tablet->CheckedSetBackupState(
                    ETabletBackupState::None,
                    ETabletBackupState::CheckpointRequested);

                TReqSetBackupCheckpoint req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                req.set_mount_revision(tablet->GetMountRevision());
                req.set_timestamp(timestamp);
                req.set_backup_mode(ToProto<int>(backupMode));

                if (clockClusterTag) {
                    req.set_clock_cluster_tag(*clockClusterTag);
                }

                ToProto(req.mutable_replicas(), replicaBackupDescriptors);

                const auto& hiveManager = Bootstrap_->GetHiveManager();
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, req);
            } else {
                tablet->CheckedSetBackupState(
                    ETabletBackupState::None,
                    ETabletBackupState::CheckpointConfirmed);
            }
        }

        YT_VERIFY(transaction->TablesWithBackupCheckpoints().insert(table).second);
        UpdateAggregatedBackupState(table);
    }

    virtual void StartRestore(
        TTableNode* table,
        TTransaction* transaction,
        std::vector<TTableReplicaBackupDescriptor> replicaBackupDescriptors) override
    {
        YT_LOG_DEBUG("Starting restore from backup (TableId: %v, TransactionId: %v, "
            "BackupableReplicaIds: %v)",
            table->GetId(),
            transaction->GetId(),
            MakeFormattableView(replicaBackupDescriptors, [] (auto* builder, const auto& descriptor) {
                builder->AppendFormat("%v", descriptor.ReplicaId);
            }));

        if (table->IsReplicated()) {
            ValidateReplicasAssociation(table, replicaBackupDescriptors, /*validateModes*/ false);
        }

        table->MutableReplicaBackupDescriptors() = replicaBackupDescriptors;
    }

    void ReleaseBackupCheckpoint(
        TTableNode* table,
        TTransaction* transaction) override
    {
        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            tablet->BackupCutoffDescriptor() = std::nullopt;
            tablet->BackedUpReplicaInfos().clear();

            if (tablet->GetBackupState() != ETabletBackupState::CheckpointRequested &&
                tablet->GetBackupState() != ETabletBackupState::CheckpointConfirmed &&
                tablet->GetBackupState() != ETabletBackupState::CheckpointRejected)
            {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Attempted to release backup checkpoint from a tablet "
                    "in wrong backup state (TableId: %v, TabletId: %v, TransactionId: %v, "
                    "BackupState: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    transaction->GetId(),
                    tablet->GetBackupState());
                continue;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Releasing backup checkpoint (TabletId: %v, BackupState: %v, TransactionId: %v)",
                tablet->GetId(),
                tablet->GetBackupState(),
                transaction->GetId());

            if (auto* cell = tablet->GetCell()) {
                TReqReleaseBackupCheckpoint req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                req.set_mount_revision(tablet->GetMountRevision());

                const auto& hiveManager = Bootstrap_->GetHiveManager();
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, req);
            }

            tablet->SetBackupState(ETabletBackupState::None);
        }

        if (!transaction->TablesWithBackupCheckpoints().contains(table)) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                "Attempted to remove unknown backup checkpoint table from "
                "a transaction (TableId: %v, TransactionId: %v)",
                table->GetId(),
                transaction->GetId());
        } else {
            EraseOrCrash(transaction->TablesWithBackupCheckpoints(), table);
        }

        UpdateAggregatedBackupState(table);
    }

    void CheckBackup(
        TTableNode* table,
        NTableClient::NProto::TRspCheckBackup* response) override
    {
        const auto& tabletCountByState = table->TabletCountByBackupState();
        int pendingCount = tabletCountByState[ETabletBackupState::CheckpointRequested];
        int confirmedCount = tabletCountByState[ETabletBackupState::CheckpointConfirmed];
        int rejectedCount = tabletCountByState[ETabletBackupState::CheckpointRejected];

        YT_LOG_DEBUG("Backup checkpoint checked (TableId: %v, "
            "PendingCount: %v, ConfirmedCount: %v, RejectedCount: %v)",
            table->GetId(),
            pendingCount,
            confirmedCount,
            rejectedCount);

        if (rejectedCount > 0) {
            auto error = TError(
                NTabletClient::EErrorCode::BackupCheckpointRejected,
                "Backup checkpoint rejected");
            if (!table->BackupError().IsOK()) {
                error.MutableInnerErrors()->push_back(table->BackupError());
            }
            THROW_ERROR error;
        }

        response->set_pending_tablet_count(pendingCount);
        response->set_confirmed_tablet_count(confirmedCount);
    }

    TFuture<void> FinishBackup(TTableNode* table) override
    {
        YT_VERIFY(!HasMutationContext());

        return FinishBackupTask<NProto::TReqFinishBackup>(table, "backup");
    }

    TFuture<void> FinishRestore(TTableNode* table) override
    {
        YT_VERIFY(!HasMutationContext());

        return FinishBackupTask<NProto::TReqFinishRestore>(table, "restore");
    }

    void SetClonedTabletBackupState(
        TTablet* clonedTablet,
        const TTablet* sourceTablet,
        ENodeCloneMode mode) override
    {
        if (mode == ENodeCloneMode::Backup) {
            if (sourceTablet->GetBackupState() == ETabletBackupState::CheckpointConfirmed) {
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
                    ETabletBackupState::CheckpointConfirmed,
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
                case ETabletBackupState::CheckpointRequested:
                case ETabletBackupState::CheckpointConfirmed:
                case ETabletBackupState::CheckpointRejected:
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
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Finishing table backup (TableId: %v, TransactionId: %v, Timestamp: %llx, BackupMode: %v)",
                    table->GetId(),
                    transactionId,
                    table->GetBackupCheckpointTimestamp(),
                    table->GetBackupMode());
            }

            if (tablet->GetBackupState() != ETabletBackupState::BackupStarted) {
                YT_LOG_WARNING_IF(IsMutationLoggingEnabled(),
                    "Attempted to finish backup of the tablet in invalid state "
                    "(TableId: %v, TabletId: %v, BackupState: %v, TransactionId: %v, BackupMode: %v)",
                    table->GetId(),
                    tabletId,
                    tablet->GetBackupState(),
                    transactionId,
                    table->GetBackupMode());
                continue;
            }

            switch (table->GetBackupMode()) {
                case EBackupMode::Sorted:
                case EBackupMode::SortedSyncReplica:
                    tabletManager->WrapWithBackupChunkViews(tablet, table->GetBackupCheckpointTimestamp());
                    break;

                case EBackupMode::SortedAsyncReplica: {
                    auto error = tabletManager->ApplyBackupCutoff(tablet);
                    YT_VERIFY(error.IsOK());
                    break;
                }

                case EBackupMode::OrderedStrongCommitOrdering:
                case EBackupMode::OrderedExact:
                case EBackupMode::OrderedAtLeast:
                case EBackupMode::OrderedAtMost:
                case EBackupMode::ReplicatedSorted: {
                    auto error = tabletManager->ApplyBackupCutoff(tablet);

                    if (!error.IsOK()) {
                        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), error,
                            "Failed to apply cutoff row index to tablet (TabletId: %v)",
                            tablet->GetId());
                        RegisterBackupError(table, error.Sanitize());
                        tablet->SetBackupState(ETabletBackupState::BackupFailed);
                    }
                    break;
                }

                default:
                    YT_ABORT();
            }

            tablet->CheckedSetBackupState(
                ETabletBackupState::BackupStarted,
                ETabletBackupState::BackupCompleted);
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
                tablet->CheckedSetBackupState(
                    ETabletBackupState::RestoreStarted,
                    ETabletBackupState::None);
            } else {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), error,
                    "Failed to restore the tablet from backup (TableId: %v, TabletId: %v, TransactionId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    transactionId);
                tablet->CheckedSetBackupState(
                    ETabletBackupState::RestoreStarted,
                    ETabletBackupState::RestoreFailed);
                RegisterBackupError(table, error.Sanitize());
            }
        }

        UpdateAggregatedBackupState(table);
    }

    void HydraResetBackupMode(NProto::TReqResetBackupMode* request)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto tableId = FromProto<TTableId>(request->table_id());
        auto* node = cypressManager->FindNode(TVersionedNodeId(tableId, /*transactionId*/{}));
        if (!node) {
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Table backup mode reset (TableId: %v)",
            tableId);
        auto* tableNode = node->As<TTableNode>();
        tableNode->SetBackupMode(EBackupMode::None);
    }

    void HydraOnBackupCheckpointPassed(NProto::TReqReportBackupCheckpointPassed* response)
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        if (tablet->GetMountRevision() != response->mount_revision()) {
            return;
        }

        if (tablet->GetBackupState() != ETabletBackupState::CheckpointRequested) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Backup checkpoint passage reported to a tablet in "
                "wrong backup state, ignored (TabletId: %v, BackupState: %v)",
                tablet->GetId(),
                tablet->GetBackupState());
            return;
        }

        if (response->confirmed()) {
            if (response->has_cutoff_descriptor()) {
                tablet->BackupCutoffDescriptor() = FromProto<TBackupCutoffDescriptor>(
                    response->cutoff_descriptor());
            }

            std::vector<TString> replicaLogStrings;

            if (tablet->GetTable()->IsReplicated()) {
                for (const auto& protoReplicaInfo : response->replicas()) {
                    auto replicaId = FromProto<TTableReplicaId>(protoReplicaInfo.replica_id());
                    auto& replicaInfo = EmplaceOrCrash(
                        tablet->BackedUpReplicaInfos(),
                        replicaId,
                        TTableReplicaInfo{})
                        ->second;
                    replicaInfo.MergeFrom(protoReplicaInfo.replica_statistics());
                    replicaLogStrings.push_back(Format("%v: %v/%llx",
                        replicaId,
                        replicaInfo.GetCommittedReplicationRowIndex(),
                        replicaInfo.GetCurrentReplicationTimestamp()));
                }
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Backup checkpoint confirmed by the tablet cell "
                "(TableId: %v, TabletId: %v, CutoffDescriptor: %v, Replicas: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId(),
                tablet->BackupCutoffDescriptor(),
                replicaLogStrings);
            tablet->CheckedSetBackupState(
                ETabletBackupState::CheckpointRequested,
                ETabletBackupState::CheckpointConfirmed);
        } else {
            auto error = FromProto<TError>(response->error());
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), error,
                "Backup checkpoint rejected by the tablet cell "
                "(TableId: %v, TabletId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId());
            RegisterBackupError(tablet->GetTable(), error);
            tablet->CheckedSetBackupState(
                ETabletBackupState::CheckpointRequested,
                ETabletBackupState::CheckpointRejected);
        }

        UpdateAggregatedBackupState(tablet->GetTable());
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

        auto commitMutation = [=] (auto mutation) {
            // TODO(aleksandra-zh, gritukan): Mutation commit from non-automaton thread
            // should not be a problem for new Hydra.
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            return BIND([=, mutation = std::move(mutation)] {
                return CreateMutation(hydraManager, mutation)
                    ->CommitAndLog(Logger);
            })
                .AsyncVia(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService))
                .Run();
        };

        auto maybeFlush = [&] (bool force) {
            if (storeCount < MaxStoresPerBackupMutation && !force) {
                return;
            }

            ToProto(currentReq.mutable_transaction_id(), transaction->GetId());

            asyncCommitResults.push_back(commitMutation(std::move(currentReq)));
            storeCount = 0;
            currentReq = {};
        };

        for (auto* tablet : table->GetTrunkNode()->Tablets()) {
            YT_LOG_DEBUG("Schedule backup task for tablet (Task: %v, TabletId: %v)",
                taskName,
                tablet->GetId());
            ToProto(currentReq.add_tablet_ids(), tablet->GetId());

            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                if (auto* chunkList = tablet->GetChunkList(contentType)) {
                    storeCount += chunkList->Statistics().ChunkCount;
                }
            }

            maybeFlush(false);
        }

        maybeFlush(true);

        if constexpr (std::is_same_v<TRequest, NProto::TReqFinishBackup>) {
            NProto::TReqResetBackupMode req;
            ToProto(req.mutable_table_id(), table->GetId());
            asyncCommitResults.push_back(commitMutation(std::move(req)));
        }

        return AllSucceeded(asyncCommitResults).AsVoid();
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        if (transaction->TablesWithBackupCheckpoints().empty()) {
            return;
        }

        // NB: ReleaseBackupCheckpoint modifies transaction->TablesWithBackupCheckpoints.
        for (auto* table : GetValuesSortedByKey(transaction->TablesWithBackupCheckpoints())) {
            if (!IsObjectAlive(table)) {
                continue;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Releasing backup checkpoint on transaction abort (TableId: %v, "
                "TransactionId: %v)",
                table->GetId(),
                transaction->GetId());
            ReleaseBackupCheckpoint(table, transaction);
            table->MutableBackupError() = {};
        }
    }

    void OnTransactionCommitted(TTransaction* transaction)
    {
        if (transaction->TablesWithBackupCheckpoints().empty()) {
            return;
        }

        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
            "Table backup transaction was committed manually before cloning (TransactionId: %v)",
            transaction->GetId());

        for (auto* table : GetValuesSortedByKey(transaction->TablesWithBackupCheckpoints())) {
            if (!IsObjectAlive(table)) {
                continue;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Releasing backup checkpoint on manual transaction commit (TableId: %v, "
                "TransactionId: %v)",
                table->GetId(),
                transaction->GetId());
            ReleaseBackupCheckpoint(table, transaction);
            table->MutableBackupError() = {};
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
                if (clearedTables.insert(table).second) {
                    table->MutableTabletCountByBackupState() = {};
                }

                ++table->MutableTabletCountByBackupState()[tablet->GetBackupState()];
            }
        }
    }

    void RegisterBackupError(TTableNode* table, const TError& error)
    {
        // Store at most one error per table.
        if (!table->BackupError().IsOK()) {
            return;
        }

        // Should be already sanitized.
        table->MutableBackupError() = error;
    }

    void ValidateBackupMode(TTableNode* table, EBackupMode mode)
    {
        if (mode == EBackupMode::ReplicatedSorted) {
            if (!table->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Can backup only replicated tables in mode %Qlv", mode);

            }
        } else {
            if (table->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Cannot backup replicated table in mode %Qlv", mode);
            }
        }

        auto validateUpstreamReplica = [&] {
            if (!table->GetUpstreamReplicaId()) {
                THROW_ERROR_EXCEPTION("Cannot backup replica table in mode %Qlv", mode);
            }
        };

        auto validateNoUpstreamReplica = [&] {
            if (table->GetUpstreamReplicaId()) {
                THROW_ERROR_EXCEPTION("Can backup only replica tables in mode %Qlv", mode);
            }
        };

        auto validateSorted = [&] {
            if (!table->IsSorted()) {
                THROW_ERROR_EXCEPTION("Cannot backup ordered table in mode %Qlv", mode);
            }
        };

        auto validateOrdered = [&] {
            if (table->IsSorted()) {
                THROW_ERROR_EXCEPTION("Cannot backup sorted table in mode %Qlv", mode);
            }
        };

        switch (mode) {
            case EBackupMode::Sorted:
                validateSorted();
                validateNoUpstreamReplica();
                break;

            case EBackupMode::SortedSyncReplica:
            case EBackupMode::SortedAsyncReplica:
                validateSorted();
                validateUpstreamReplica();
                break;

            case EBackupMode::OrderedStrongCommitOrdering:
                validateOrdered();
                validateNoUpstreamReplica();
                if (table->GetCommitOrdering() != ECommitOrdering::Strong) {
                    THROW_ERROR_EXCEPTION("Cannot backup table with commit ordering %Qlv "
                        "in mode %Qlv",
                        table->GetCommitOrdering(),
                        mode);
                }
                break;

            case EBackupMode::OrderedAtLeast:
                validateOrdered();
                validateNoUpstreamReplica();
                if (table->GetCommitOrdering() != ECommitOrdering::Weak) {
                    THROW_ERROR_EXCEPTION("Cannot backup table with commit ordering %Qlv "
                        "in mode %Qlv",
                        table->GetCommitOrdering(),
                        mode);
                }
                break;

            case EBackupMode::OrderedExact:
            case EBackupMode::OrderedAtMost:
                THROW_ERROR_EXCEPTION("Backup mode %Qlv is not supported", mode);

            case EBackupMode::ReplicatedSorted:
                validateSorted();
                break;

            default:
                THROW_ERROR_EXCEPTION("Invalid backup mode %Qlv", mode);
        }
    }

    void ValidateReplicasAssociation(
        TTableNode* table,
        const std::vector<TTableReplicaBackupDescriptor>& replicaBackupDescriptors,
        bool validateModes)
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& descriptor : replicaBackupDescriptors) {
            const auto* replica = tabletManager->FindTableReplica(descriptor.ReplicaId);
            if (!replica || replica->GetTable() != table) {
                THROW_ERROR_EXCEPTION("Table replica %v does not belong to the table")
                    << TErrorAttribute("table_id", table->GetId());
            }
            if (!validateModes) {
                continue;
            }

            if (replica->GetMode() != descriptor.Mode) {
                THROW_ERROR_EXCEPTION("Table replica %v has unexpected mode: "
                    "expected %Qlv, actual %Qlv",
                    replica->GetId(),
                    descriptor.Mode,
                    replica->GetMode())
                    << TErrorAttribute("table_id", table->GetId());
            }

            if (replica->GetState() != ETableReplicaState::Enabled &&
                replica->GetState() != ETableReplicaState::Disabled)
            {
                THROW_ERROR_EXCEPTION("Table replica %v is in transient state %Qlv",
                    replica->GetId(),
                    replica->GetState())
                    << TErrorAttribute("table_id", table->GetId());
            }

            if (replica->GetState() == ETableReplicaState::Disabled &&
                replica->GetMode() == ETableReplicaMode::Sync)
            {
                THROW_ERROR_EXCEPTION("Sync replica %v is disabled; it must be enabled "
                    "or switched to async mode to be backed up",
                    replica->GetId())
                    << TErrorAttribute("table_id", table->GetId());
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
