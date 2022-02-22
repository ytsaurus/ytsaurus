#include "backup_manager.h"

#include "automaton.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "bootstrap.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "store_manager.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/proto/backup_manager.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NClusterNode;
using namespace NYTree;
using namespace NTransactionClient;

using NYT::ToProto;
using NYT::FromProto;

using NLsm::EStoreRotationReason;

////////////////////////////////////////////////////////////////////////////////

class TBackupManager
    : public TTabletAutomatonPart
    , public IBackupManager
{
public:
    TBackupManager(
        ITabletSlotPtr slot,
        IBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot->GetCellId(),
            slot->GetSimpleHydraManager(),
            slot->GetAutomaton(),
            slot->GetAutomatonInvoker())
        , Slot_(std::move(slot))
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(BIND(&TBackupManager::HydraSetBackupCheckpoint, Unretained(this)));
        RegisterMethod(BIND(&TBackupManager::HydraReleaseBackupCheckpoint, Unretained(this)));
        RegisterMethod(BIND(&TBackupManager::HydraConfirmBackupCheckpointFeasibility, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "BackupManager",
            BIND(&TBackupManager::Save, Unretained(this)));

        RegisterLoader(
            "BackupManager",
            BIND(&TBackupManager::Load, Unretained(this)));

        const auto& configManager = Bootstrap_->GetDynamicConfigManager();
        Config_ = configManager->GetConfig()->TabletNode->BackupManager;
        configManager->SubscribeConfigChanged(
            BIND(&TBackupManager::OnDynamicConfigChanged, MakeWeak(this))
                .Via(Slot_->GetAutomatonInvoker()));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();
        transactionManager->SubscribeTransactionBarrierHandled(
            BIND(&TBackupManager::OnTransactionBarrierHandled, MakeStrong(this)));
        transactionManager->SubscribeBeforeTransactionSerialized(
            BIND(&TBackupManager::OnBeforeTransactionSerialized, MakeStrong(this)));
    }

private:
    const ITabletSlotPtr Slot_;
    IBootstrap* const Bootstrap_;

    struct TTabletWithCheckpoint
    {
        TTabletId TabletId;
        TRevision MountRevision;
        TTimestamp CheckpointTimestamp;

        bool operator<(const auto& other) const
        {
            return std::tie(CheckpointTimestamp, TabletId) <
                std::tie(other.CheckpointTimestamp, other.TabletId);
        }

        TTabletWithCheckpoint() = default;

        TTabletWithCheckpoint(const TTablet* tablet)
            : TabletId(tablet->GetId())
            , MountRevision(tablet->GetMountRevision())
            , CheckpointTimestamp(tablet->GetBackupCheckpointTimestamp())
        { }

        void Persist(const TStreamPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, TabletId);
            Persist(context, MountRevision);
            Persist(context, CheckpointTimestamp);
        }
    };

    // Tablets that received checkpoint timestamp and are waiting for it
    // to be persistently confirmed.
    // Transient.
    std::vector<TTabletWithCheckpoint> TabletsAwaitingFeasibilityCheck_;

    // Tablets with confirmed checkpoint timestamp.
    // They are waiting for one of the events:
    //  - a transaction with commit timestamp greater than their chekpoint
    //    timestamp is serialized (commit_ordering=strong only).
    //  - barrier timestamp overruns ther checkpoint timestamp and no transaction
    //    affecting that tablet is pending serialization.
    // Persistent.
    std::set<TTabletWithCheckpoint> TabletsAwaitingCheckpointPassing_;

    bool CheckpointFeasibilityCheckScheduled_ = false;

    TBackupManagerDynamicConfigPtr Config_;

    void OnDynamicConfigChanged(
        TClusterNodeDynamicConfigPtr /*oldConfig*/,
        TClusterNodeDynamicConfigPtr newConfig)
    {
        Config_ = newConfig->TabletNode->BackupManager;
    }

    void OnLeaderActive() override
    {
        TTabletAutomatonPart::OnLeaderActive();

        CheckpointFeasibilityCheckScheduled_ = false;

        TabletsAwaitingFeasibilityCheck_.clear();

        const auto& tabletManager = Slot_->GetTabletManager();
        for (auto [id, tablet] : tabletManager->Tablets()) {
            if (tablet->GetBackupStage() == EBackupStage::TimestampReceived) {
                TabletsAwaitingFeasibilityCheck_.emplace_back(tablet);
            }

            if (tablet->GetBackupStage() == EBackupStage::FeasibilityConfirmed) {
                TTabletWithCheckpoint holder(tablet);
                YT_VERIFY(TabletsAwaitingCheckpointPassing_.contains(holder));
            }
        }

        if (!TabletsAwaitingFeasibilityCheck_.empty()) {
            ScheduleCheckpointFeasibilityCheck(TDuration::Zero());
        }
    }

    void Clear() override
    {
        TTabletAutomatonPart::Clear();

        TabletsAwaitingFeasibilityCheck_.clear();
        TabletsAwaitingCheckpointPassing_.clear();
    }

    void Save(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, TabletsAwaitingCheckpointPassing_);
    }

    void Load(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, TabletsAwaitingCheckpointPassing_);
    }

    void HydraSetBackupCheckpoint(NProto::TReqSetBackupCheckpoint* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto timestamp = request->timestamp();

        const auto& tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Backup checkpoint set (TabletId: %v, CheckpointTimestamp: %llx)",
            tabletId,
            timestamp);

        tablet->SetBackupCheckpointTimestamp(timestamp);

        tablet->CheckedSetBackupStage(EBackupStage::None, EBackupStage::TimestampReceived);

        TabletsAwaitingFeasibilityCheck_.emplace_back(tablet);
        ScheduleCheckpointFeasibilityCheck(Config_->CheckpointFeasibilityCheckBatchPeriod);
    }

    void HydraReleaseBackupCheckpoint(NProto::TReqReleaseBackupCheckpoint* request)
    {
        const auto& tabletManager = Slot_->GetTabletManager();
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Backup checkpoint released (TabletId: %v)",
            tabletId);
        YT_VERIFY(tablet->GetBackupCheckpointTimestamp());

        if (tablet->GetBackupStage() == EBackupStage::FeasibilityConfirmed) {
            TTabletWithCheckpoint holder(tablet);
            EraseOrCrash(TabletsAwaitingCheckpointPassing_, holder);
        }

        tablet->SetBackupCheckpointTimestamp(NullTimestamp);
        tablet->SetBackupStage(EBackupStage::None);
    }

    void HydraConfirmBackupCheckpointFeasibility(
        NProto::TReqConfirmBackupCheckpointFeasibility* request)
    {
        const auto& tabletManager = Slot_->GetTabletManager();

        // Confirmed.
        for (const auto& confirmedInfo : request->confirmed_tablets()) {
            auto tabletId = FromProto<TTabletId>(confirmedInfo.tablet_id());
            auto timestamp = confirmedInfo.timestamp();

            auto* tablet = tabletManager->FindTablet(tabletId);
            if (!tablet) {
                continue;
            }
            if (tablet->GetBackupCheckpointTimestamp() != timestamp) {
                continue;
            }

            // Checkpoint may have been already rejected elsewhere, e.g.
            // by a serialized transaction with later timestamp.
            if (tablet->GetBackupStage() != EBackupStage::TimestampReceived) {
                continue;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Persistently confirmed backup checkpoint feasibility "
                "(TabletId: %v, CheckpointTimestamp: %llx)",
                tablet->GetId(),
                timestamp);

            tablet->CheckedSetBackupStage(EBackupStage::TimestampReceived, EBackupStage::FeasibilityConfirmed);

            TTabletWithCheckpoint holder(tablet);
            InsertOrCrash(TabletsAwaitingCheckpointPassing_, holder);
        }

        // Rejected.
        for (const auto& rejectedInfo : request->rejected_tablets()) {
            auto tabletId = FromProto<TTabletId>(rejectedInfo.tablet_id());
            auto* tablet = tabletManager->FindTablet(tabletId);
            if (!tablet) {
                continue;
            }

            // Checkpoint may have been already rejected elsewhere, e.g.
            // by a serialized transaction with later timestamp.
            if (tablet->GetBackupStage() != EBackupStage::TimestampReceived) {
                continue;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Persistently rejected backup checkpoint feasibility "
                "(TabletId: %v, CheckpointTimestamp: %llx)",
                tablet->GetId(),
                tablet->GetBackupCheckpointTimestamp());

            tablet->CheckedSetBackupStage(EBackupStage::TimestampReceived, EBackupStage::RespondedToMaster);

            NTabletServer::NProto::TReqReportBackupCheckpointPassed req;
            ToProto(req.mutable_tablet_id(), tabletId);
            req.set_mount_revision(tablet->GetMountRevision());
            req.set_confirmed(false);
            req.mutable_error()->CopyFrom(rejectedInfo.error());
            Slot_->PostMasterMessage(tabletId, req);
        }
    }

    void ScheduleCheckpointFeasibilityCheck(TDuration delay)
    {
        if (!IsLeader()) {
            return;
        }

        if (!CheckpointFeasibilityCheckScheduled_) {
            CheckpointFeasibilityCheckScheduled_ = true;

            TDelayedExecutor::Submit(
                BIND(&TBackupManager::CheckCheckpointFeasibility, MakeWeak(this)),
                delay,
                Slot_->GetEpochAutomatonInvoker());
        }
    }

    void CheckCheckpointFeasibility()
    {
        YT_VERIFY(!HasMutationContext());

        try {
            DoCheckCheckpointFeasibility();

            // NB: Reset the flag after the call since DoCheckCheckpointFeasibility may yield
            // and that can lead to another check scheduled while the first one is still in progress.
            CheckpointFeasibilityCheckScheduled_ = false;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to check backup checkpoint feasibility, will reschedule");

            CheckpointFeasibilityCheckScheduled_ = false;
            ScheduleCheckpointFeasibilityCheck(Config_->CheckpointFeasibilityCheckBackoff);
        }
    }

    void DoCheckCheckpointFeasibility()
    {
        if (TabletsAwaitingFeasibilityCheck_.empty()) {
            return;
        }

        const auto& timestampProvider = Bootstrap_
            ->GetMasterConnection()
            ->GetTimestampProvider();
        auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
            .ValueOrThrow();

        NProto::TReqConfirmBackupCheckpointFeasibility req;

        const auto& tabletManager = Slot_->GetTabletManager();

        for (auto tabletWithCheckpoint : TabletsAwaitingFeasibilityCheck_) {
            auto* tablet = tabletManager->FindTablet(tabletWithCheckpoint.TabletId);
            if (!tablet) {
                continue;
            }

            if (tablet->GetBackupStage() != EBackupStage::TimestampReceived) {
                YT_LOG_DEBUG("Checked checkpoint feasibility for a tablet "
                    "in a wrong stage (TabletId: %v, BackupStage: %v)",
                    tablet->GetId(),
                    tablet->GetBackupStage());
                continue;
            }

            if (tablet->GetBackupCheckpointTimestamp() > currentTimestamp) {
                YT_LOG_DEBUG("Transiently confirmed backup checkpoint feasibility "
                    "(TabletId: %v, CheckpointTimestamp: %llx)",
                    tablet->GetId(),
                    tablet->GetBackupCheckpointTimestamp());

                auto* confirmedInfo = req.add_confirmed_tablets();
                ToProto(confirmedInfo->mutable_tablet_id(), tablet->GetId());
                confirmedInfo->set_timestamp(tablet->GetBackupCheckpointTimestamp());
            } else {
                YT_LOG_DEBUG("Transiently rejected checkpoint feasibility "
                    "(TabletId: %v, CheckpointTimestamp: %llx, CurrentTimestamp: %llx)",
                    tablet->GetId(),
                    tablet->GetBackupCheckpointTimestamp(),
                    currentTimestamp);

                auto rejectedInfo = req.add_rejected_tablets();
                ToProto(rejectedInfo->mutable_tablet_id(), tablet->GetId());
                auto error = TError("Checkpoint timestamp is too late")
                    << TErrorAttribute("checkpoint_timestamp", tablet->GetBackupCheckpointTimestamp())
                    << TErrorAttribute("current_timestamp", currentTimestamp)
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("cell_id", Slot_->GetCellId());
                ToProto(rejectedInfo->mutable_error(), error.Sanitize());
            }
        }

        TabletsAwaitingFeasibilityCheck_.clear();

        if (req.confirmed_tablets().size() + req.rejected_tablets().size() > 0) {
            Slot_->CommitTabletMutation(req);
        }
    }

    void ReportCheckpointPassage(TTablet* tablet)
    {
        YT_VERIFY(HasMutationContext());

        NTabletServer::NProto::TReqReportBackupCheckpointPassed req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(tablet->GetMountRevision());

        auto respond = [&] {
            Slot_->PostMasterMessage(tablet->GetId(), req);
            tablet->CheckedSetBackupStage(EBackupStage::FeasibilityConfirmed, EBackupStage::RespondedToMaster);
        };

        if (tablet->IsPhysicallyOrdered() && tablet->GetCommitOrdering() == ECommitOrdering::Strong) {
            auto* cutoffDescriptor = req.mutable_row_index_cutoff_descriptor();
            cutoffDescriptor->set_cutoff_row_index(tablet->GetTotalRowCount());

            // Active store is non-empty, should rotate.
            if (const auto& activeStore = tablet->GetActiveStore();
                activeStore && activeStore->GetRowCount() > 0)
            {
                if (tablet->GetState() != ETabletState::Mounted) {
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                        "Tablet with nonempty active store is not mounted "
                        "during backup checkpoint passing (%v, TabletState: %v)",
                        tablet->GetLoggingTag(),
                        tablet->GetState());
                    auto error = TError("Tablet %v has nonempty dynamic store and is not mounted "
                        "during backup checkpoint passing",
                        tablet->GetId())
                        << TErrorAttribute("tablet_state", tablet->GetState())
                        << TErrorAttribute("cell_id", Slot_->GetCellId());
                    ToProto(req.mutable_error(), error.Sanitize());

                    respond();
                    return;
                }

                if (tablet->DynamicStoreIdPool().empty()) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                        "Cannot perform backup cutoff due to empty "
                        "dynamic store id pool (%v)",
                        tablet->GetLoggingTag());
                    auto error = TError("Tablet %v cannot perform backup cutoff due to empty "
                        "dynamic store id pool",
                        tablet->GetId())
                        << TErrorAttribute("cell_id", Slot_->GetCellId());
                    ToProto(req.mutable_error(), error.Sanitize());

                    respond();
                    return;
                }

                tablet->GetStoreManager()->Rotate(/*createNewStore*/ true, EStoreRotationReason::None);

                const auto& tabletManager = Slot_->GetTabletManager();
                tabletManager->UpdateTabletSnapshot(tablet);

                if (tabletManager->AllocateDynamicStoreIfNeeded(tablet)) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                        "Dynamic store id for ordered tablet allocated "
                        "after backup cutoff (%v)",
                        tablet->GetLoggingTag());
                }
            }

            if (const auto& activeStore = tablet->GetActiveStore()) {
                ToProto(cutoffDescriptor->mutable_next_dynamic_store_id(), activeStore->GetId());
            }
        }

        req.set_confirmed(true);
        respond();
    }

    void OnTransactionBarrierHandled(TTimestamp barrierTimestamp)
    {
        YT_VERIFY(HasMutationContext());

        const auto& tabletManager = Slot_->GetTabletManager();

        auto& set = TabletsAwaitingCheckpointPassing_;
        for (auto it = set.begin(); it != set.end(); it = set.erase(it)) {
            auto* tablet = tabletManager->FindTablet(it->TabletId);
            if (!tablet) {
                continue;
            }

            if (tablet->GetMountRevision() != it->MountRevision) {
                continue;
            }

            YT_VERIFY(tablet->GetBackupCheckpointTimestamp() == it->CheckpointTimestamp);
            YT_VERIFY(tablet->GetBackupStage() == EBackupStage::FeasibilityConfirmed);

            if (tablet->GetBackupCheckpointTimestamp() >= barrierTimestamp) {
                break;
            }

            ReportCheckpointPassage(tablet);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Reported backup checkpoint passage by barrier timestamp "
                "(TabletId: %v, CheckpointTimestamp: %llx, BarrierTimestamp: %llx)",
                tablet->GetId(),
                tablet->GetBackupCheckpointTimestamp(),
                barrierTimestamp);
        }
    }

    void OnBeforeTransactionSerialized(TTransaction* transaction)
    {
        YT_VERIFY(HasMutationContext());

        for (auto* tablet : transaction->LockedTablets()) {
            if (tablet->GetCommitOrdering() != ECommitOrdering::Strong) {
                continue;
            }

            auto checkpointTimestamp = tablet->GetBackupCheckpointTimestamp();
            if (!checkpointTimestamp) {
                continue;
            }

            auto commitTimestamp = transaction->GetCommitTimestamp();
            if (commitTimestamp <= checkpointTimestamp) {
                continue;
            }

            if (tablet->GetBackupStage() == EBackupStage::FeasibilityConfirmed) {
                auto holder = TTabletWithCheckpoint(tablet);
                EraseOrCrash(TabletsAwaitingCheckpointPassing_, holder);

                ReportCheckpointPassage(tablet);

                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Reported backup checkpoint passage due to a transaction "
                    "with later timestamp (%v, CheckpointTimestamp: %llx, "
                    "NextTransactionCommitTimestamp: %llx)",
                    tablet->GetLoggingTag(),
                    checkpointTimestamp,
                    commitTimestamp);

            } else if (tablet->GetBackupStage() == EBackupStage::TimestampReceived) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Rejected backup checkpoint timestamp due to a transaction "
                    "with later timestamp (%v, CheckpointTimestamp: %llx, "
                    "CommitTimestamp: %llx)",
                    tablet->GetLoggingTag(),
                    checkpointTimestamp,
                    commitTimestamp);

                tablet->CheckedSetBackupStage(
                    EBackupStage::TimestampReceived,
                    EBackupStage::RespondedToMaster);

                auto error = TError("Failed to confirm checkpoint timestamp in time "
                    "due to a transaction with later timestamp")
                    << TErrorAttribute("checkpoint_timestamp", checkpointTimestamp)
                    << TErrorAttribute("commit_timestamp", commitTimestamp)
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("cell_id", Slot_->GetCellId());

                NTabletServer::NProto::TReqReportBackupCheckpointPassed req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                req.set_mount_revision(tablet->GetMountRevision());
                req.set_confirmed(false);
                ToProto(req.mutable_error(), error.Sanitize());
                Slot_->PostMasterMessage(tablet->GetId(), req);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IBackupManagerPtr CreateBackupManager(ITabletSlotPtr slot, IBootstrap* bootstrap)
{
    return New<TBackupManager>(std::move(slot), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
