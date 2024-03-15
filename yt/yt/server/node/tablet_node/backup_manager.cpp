#include "backup_manager.h"

#include "automaton.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "bootstrap.h"
#include "tablet_cell_write_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "store_manager.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/proto/backup_manager.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NClusterNode;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NApi;
using namespace NTabletClient;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

using NLsm::EStoreRotationReason;

////////////////////////////////////////////////////////////////////////////////
/**

    Overview

Typical lifetime of a tablet being backed up is as follows (all actions are
persistent by default and transient if marked with (T)):
- receive backup checkpoint timestamp from master. At this moment some activities
  are restricted, e.g. merge_rows_on_flush is disabled until backup finishes;
- (T) schedule checkpoint feasibility check: that is, validate that checkpoint
  timestamp has not yet occurred;
- (T) generate timestamp and schedule confirmation mutation;
- confirm checkpoint timestamp and start waiting for it to come;
- when checkpoint timestamp has passed (that is noticed by either more recent
  serialized transaction or by barrier timestamp):
    - if tablet does not participates in replication from either side, report
      checkpoint passage to master immediately;
    - if it does, wait for all replication transactions to finish and report
      only after that;
- release checkpoint timestamp upon request from master. All restrictions become
  void at this moment, the backup is finished.


    Cutoffs for different table kinds

Backup is determined by a certain timestamp, so-called checkpoint timestamp.
Generally we want to have only transactions committed before checkpoint timestamp
(inclusive) to be included into the backup. This applies to both write and
replication transactions. Description of how tables of different kinds deal with
checkpoint timestamp follows.

- Sorted tables: we are waiting for all acceptable transactions to be committed
    (this is guaranteed by barrier timestamp). Later chunk views with clip
    timestamp is added by master, so we don't care about committed rows with
    greater timestamps.
- Ordered tables, commit_ordering=strong: we are waiting for all acceptable
    transactions to be serialized and perform store rotation at the moment when
    last such transaction is serialized. This moment is determined either by
    barrier timestamp or by trying to serialize a transaction with commit timestamp
    exceeding barrier timestamp. The position of the rotated store is sent to
    master (see TRowIndexCutoffDescriptor).
- Sorted replicated tables: regarding log part, replicated tables are similar
    to ordered ones. Sync replicas do not require additional care (we require
    for them to be in sync to start the backup).
      As for async replicas, only replication transactions committed before
    checkpoint should be accounted.  We ensure that no replication transaction
    overlaps checkpoint timestamp with its start and commit timestamps (and
    if it does, backup is aborted).  When checkpoint timestamp has passed for
    the log, we additionally wait for all prepared replication transactions,
    save replication progress and report success only after that.
      Replication remains disabled until checkpoint is released.
- Sorted sync replicas: similar to regular sorted table expect for clock source.
- Sorted async replicas: after checkpoint has passed, we wait for all prepared
    replication transactions to finish. After that the tablet rotates active
    store and reports success, sending the list of backupable dynamic stores
    to the master. Replication is disabled until table is cloned: it guarantees
    that no sequence of flushes and compactions can construct a chunk with rows
    committed after the checkpoint.


    Stages (denoted with EBackupStage)

None: tablet is not participating in the backup.
TimestampReceived: tablet has started participating in the backup.
FeasibilityConfirmed: tablet has confirmed that checkpoint timestamp was received
  before it happened and is waiting for checkpoint passage.
AwaitingReplicationFinish: checkpoint timestamp has passed, the tablet is waiting
  for replication in progress to finish.
RespondedToMasterSuccess: tablet's active articipation in the backup is finished
  by passing checkpoint and confirming it.
RespondedToMasterFailure: tablet's active articipation in the backup is finished
  by rejecting the checkpoint.


    Transitions between stages

All stages and transitions are persistent.

None:
  -> TimestampReceived: timestamp is received from master, start backup.

TimestampReceived:
  -> None: backup was cancelled for any reason.
  -> RespondedToMasterFailure: reject immediately if replicas have unexpected
      statuses.
  -> RespondedToMasterFailure: reject in feasibility check if fresh timestamp
      exceeds checkpoint timestamp.
  -> RespondedToMasterFailure: reject if transaction with greater timestamp is
      serialized before feasibility check mutation was committed.
  -> FeasibilityConfirmed: confirm feasibility check if fresh timestamp is fine.

FeasibilityConfirmed:
  -> None: backup was cancelled for any reason.
  -> RespondedToMasterSuccess: backup checkpoint has passed successfully.
      success or was not ready and reported failure (e.g. in case of lacking spare
      dynamic store id necessary for store rotation).
  -> RespondedToMasterFailure: backup checkpoint has passed, tablet was not ready
      and reported failure (e.g. in case of lacking spare dynamic store id necessary
      for store rotation).
  -> AwaitingReplicationFinish: backup checkpoint has passed but some replication
      transactions are still pending (prepared but not committed). Applicable for
      replicated tables and async replicas.

AwaitingReplicationFinish:
  -> None: backup was cancelled for any reason.
  -> RespondedToMasterSuccess/RespondedToMasterFailure: all replication transactions
      have finished, tablet reports success (or failure, was it not ready).
  -> FeasibilityConfirmed: technical transition for sorted async replicas
      that immediately results in RespondedToMasterSuccess/RespondedToMasterFailure.

RespondedToMasterSuccess:
  -> None: backup was either finished or cancelled for any reason.

RespondedToMasterFailure:
  -> None: backup was cancelled.

**/
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
            slot->GetAutomatonInvoker(),
            slot->GetMutationForwarder())
        , Slot_(std::move(slot))
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TBackupManager::HydraSetBackupCheckpoint, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TBackupManager::HydraReleaseBackupCheckpoint, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TBackupManager::HydraConfirmBackupCheckpointFeasibility, Unretained(this)));

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
            BIND_NO_PROPAGATE(&TBackupManager::OnDynamicConfigChanged, MakeWeak(this))
                .Via(Slot_->GetAutomatonInvoker()));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();
        transactionManager->SubscribeTransactionBarrierHandled(
            BIND_NO_PROPAGATE(&TBackupManager::OnTransactionBarrierHandled, MakeStrong(this)));
        transactionManager->SubscribeBeforeTransactionSerialized(
            BIND_NO_PROPAGATE(&TBackupManager::OnBeforeTransactionSerialized, MakeStrong(this)));

        const auto& tabletManager = Slot_->GetTabletManager();
        tabletManager->SubscribeReplicationTransactionFinished(
            BIND_NO_PROPAGATE(&TBackupManager::OnReplicationTransactionFinished, MakeStrong(this)));
    }

    void ValidateReplicationTransactionCommit(
        TTablet* tablet,
        TTransaction* transaction) override
    {
        auto checkpointTimestamp = tablet->GetBackupCheckpointTimestamp();
        if (!checkpointTimestamp) {
            return;
        }

        bool overlaps = transaction->GetStartTimestamp() <= checkpointTimestamp &&
            transaction->GetCommitTimestamp() > checkpointTimestamp;
        if (!overlaps) {
            return;
        }

        YT_LOG_DEBUG(
            "Replication transaction overlaps backup checkpoint timestamp, aborting backup "
            "(%v, StartTimestamp: %v, CheckpointTimestamp: %v, CommitTimestamp: %v)",
            tablet->GetLoggingTag(),
            transaction->GetStartTimestamp(),
            checkpointTimestamp,
            transaction->GetCommitTimestamp());

        auto error = TError("Backup aborted due to overlapping replication transaction")
            << TErrorAttribute("tablet_id", tablet->GetId())
            << TErrorAttribute("table_path", tablet->GetTablePath())
            << TErrorAttribute("start_timestamp", transaction->GetStartTimestamp())
            << TErrorAttribute("checkpoint_timestamp", checkpointTimestamp)
            << TErrorAttribute("commit_timestamp", transaction->GetCommitTimestamp())
            << TErrorAttribute("transaction_id", transaction->GetId());

        OnBackupFailed(tablet, std::move(error));
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

    struct TTabletAwaitingReplicationFinish
    {
        int PendingAsyncReplicaCount = 0;
        NTabletServer::NProto::TReqReportBackupCheckpointPassed Request;

        void Persist(const TStreamPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, PendingAsyncReplicaCount);
            Persist(context, Request);
        }
    };

    // Tablets that received checkpoint timestamp and are waiting for it
    // to be persistently confirmed.
    // Transient.
    std::vector<TTabletWithCheckpoint> TabletsAwaitingFeasibilityCheck_;

    // Tablets with confirmed checkpoint timestamp.
    // They are waiting for one of the events:
    //  - a transaction with commit timestamp greater than their checkpoint
    //    timestamp is serialized (commit_ordering=strong only).
    //  - barrier timestamp overruns their checkpoint timestamp and no transaction
    //    affecting that tablet is pending serialization.
    // Persistent.
    std::set<TTabletWithCheckpoint> TabletsAwaitingCheckpointPassing_;

    // Tablets that have passed checkpoint but still have have async replicas
    // with prepared replicated rows.
    // Persistent.
    THashMap<TTabletId, TTabletAwaitingReplicationFinish> TabletsAwaitingReplicationFinish_;

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
        TabletsAwaitingReplicationFinish_.clear();
    }

    void Save(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, TabletsAwaitingCheckpointPassing_);
        Save(context, TabletsAwaitingReplicationFinish_);
    }

    void Load(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, TabletsAwaitingCheckpointPassing_);
        Load(context, TabletsAwaitingReplicationFinish_);
    }

    void HydraSetBackupCheckpoint(NProto::TReqSetBackupCheckpoint* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto timestamp = request->timestamp();
        auto mode = FromProto<EBackupMode>(request->backup_mode());
        auto clockClusterTag = request->has_clock_cluster_tag()
            ? std::make_optional(FromProto<TClusterTag>(request->clock_cluster_tag()))
            : std::nullopt;
        auto replicaDescriptors = FromProto<std::vector<TTableReplicaBackupDescriptor>>(
            request->replicas());
        TDynamicStoreId allocatedDynamicStoreId;
        if (request->has_dynamic_store_id()) {
            FromProto(&allocatedDynamicStoreId, request->dynamic_store_id());
        }

        // COMPAT(ifsmirnov)
        if (static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign) < ETabletReign::SendDynamicStoreInBackup) {
            allocatedDynamicStoreId = {};
        }

        const auto& tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        tablet->SetBackupCheckpointTimestamp(timestamp);
        tablet->CheckedSetBackupStage(EBackupStage::None, EBackupStage::TimestampReceived);
        if (allocatedDynamicStoreId) {
            tablet->PushDynamicStoreIdToPool(allocatedDynamicStoreId);
        }

        YT_LOG_DEBUG(
            "Backup checkpoint set (TabletId: %v, CheckpointTimestamp: %v, BackupMode: %v, "
            "BackupableReplicaIds: %v, AllocatedDynamicStoreId: %v)",
            tabletId,
            timestamp,
            mode,
            MakeFormattableView(replicaDescriptors, [&] (auto* builder, const auto& descriptor) {
                builder->AppendFormat("%v", descriptor.ReplicaId);
            }),
            allocatedDynamicStoreId);

        if (tablet->IsReplicated()) {
            if (auto error = CheckReplicaStatuses(tablet, replicaDescriptors);
                !error.IsOK())
            {
                error = error
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("table_path", tablet->GetTablePath());
                YT_LOG_DEBUG(error,
                    "Replica statuses do not allow backup");
                RejectCheckpoint(tablet, error, EBackupStage::TimestampReceived);
                return;
            }
        }

        auto& backupMetadata = tablet->BackupMetadata();
        backupMetadata.SetBackupMode(mode);
        backupMetadata.SetClockClusterTag(clockClusterTag);
        backupMetadata.ReplicaBackupDescriptors() = std::move(replicaDescriptors);

        TabletsAwaitingFeasibilityCheck_.emplace_back(tablet);
        ScheduleCheckpointFeasibilityCheck(Config_->CheckpointFeasibilityCheckBatchPeriod);
    }

    void HydraReleaseBackupCheckpoint(NProto::TReqReleaseBackupCheckpoint* request)
    {
        const auto& tabletManager = Slot_->GetTabletManager();
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        YT_LOG_DEBUG(
            "Backup checkpoint released (%v, BackupMode: %v, BackupStage: %v)",
            tablet->GetLoggingTag(),
            tablet->GetBackupMode(),
            tablet->GetBackupStage());

        if (!tablet->GetBackupCheckpointTimestamp()) {
            YT_LOG_ALERT("Backup checkpoint released from a tablet that does not participate "
                "in backup (%v)",
                tablet->GetLoggingTag());
            return;
        }

        if (tablet->GetBackupStage() == EBackupStage::FeasibilityConfirmed) {
            TTabletWithCheckpoint holder(tablet);
            EraseOrCrash(TabletsAwaitingCheckpointPassing_, holder);
        }

        if (tablet->GetBackupStage() == EBackupStage::AwaitingReplicationFinish) {
            if (tablet->GetBackupMode() == EBackupMode::ReplicatedSorted) {
                EraseOrCrash(TabletsAwaitingReplicationFinish_, tablet->GetId());
            }
        }

        tablet->BackupMetadata() = {};
        tablet->SetBackupCheckpointTimestamp(NullTimestamp);
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

            YT_LOG_DEBUG(
                "Persistently confirmed backup checkpoint feasibility "
                "(TabletId: %v, CheckpointTimestamp: %v)",
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

            YT_LOG_DEBUG(
                "Persistently rejected backup checkpoint feasibility "
                "(TabletId: %v, CheckpointTimestamp: %v)",
                tablet->GetId(),
                tablet->GetBackupCheckpointTimestamp());

            RejectCheckpoint(
                tablet,
                FromProto<TError>(rejectedInfo.error()),
                EBackupStage::TimestampReceived);
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
            while (!TabletsAwaitingFeasibilityCheck_.empty()) {
                DoCheckCheckpointFeasibility();
            }

            // NB: Reset the flag after the call since DoCheckCheckpointFeasibility may yield
            // and that can lead to another check scheduled while the first one is still in progress.
            CheckpointFeasibilityCheckScheduled_ = false;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to check backup checkpoint feasibility, will reschedule");

            CheckpointFeasibilityCheckScheduled_ = false;
            ScheduleCheckpointFeasibilityCheck(Config_->CheckpointFeasibilityCheckBackoff);
        }
    }

    std::vector<TTimestamp> GenerateTimestamps(const std::vector<std::optional<TClusterTag>>& clockClusterTags)
    {
        std::vector<TFuture<TTimestamp>> asyncTimestamps;
        const auto& localConnection = Bootstrap_->GetConnection();
        const auto& clusterDirectory = localConnection->GetClusterDirectory();

        for (auto clusterTag : clockClusterTags) {
            NNative::IConnectionPtr connection;
            if (clusterTag) {
                connection = MakeStrong(
                    dynamic_cast<NNative::IConnection*>(
                        clusterDirectory->GetConnectionOrThrow(*clusterTag).Get()));
            } else {
                connection = localConnection;
            }

            const auto& timestampProvider = connection->GetTimestampProvider();
            asyncTimestamps.push_back(timestampProvider->GenerateTimestamps());
        }

        return WaitFor(AllSucceeded(asyncTimestamps))
            .ValueOrThrow();
    }

    void DoCheckCheckpointFeasibility()
    {
        const auto& tabletManager = Slot_->GetTabletManager();

        auto tabletsAwaitingCheck = std::move(TabletsAwaitingFeasibilityCheck_);
        TabletsAwaitingFeasibilityCheck_.clear();

        std::vector<std::optional<TClusterTag>> clockClusterTags;
        for (const auto& tabletWithCheckpoint : tabletsAwaitingCheck) {
            auto* tablet = tabletManager->FindTablet(tabletWithCheckpoint.TabletId);
            if (!tablet) {
                continue;
            }
            auto clusterTag = tablet->BackupMetadata().GetClockClusterTag();
            if (std::count(clockClusterTags.begin(), clockClusterTags.end(), clusterTag) == 0) {
                clockClusterTags.push_back(clusterTag);
            }
        }

        auto currentTimestamps = GenerateTimestamps(clockClusterTags);

        auto timestampByClusterTag = [&] (std::optional<TClusterTag> clusterTag) {
            auto it = std::find(clockClusterTags.begin(), clockClusterTags.end(), clusterTag);
            YT_VERIFY(it != clockClusterTags.end());
            return currentTimestamps[it - clockClusterTags.begin()];
        };

        NProto::TReqConfirmBackupCheckpointFeasibility req;

        for (auto tabletWithCheckpoint : tabletsAwaitingCheck) {
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

            auto clusterTag = tablet->BackupMetadata().GetClockClusterTag();
            auto currentTimestamp = timestampByClusterTag(clusterTag);

            if (tablet->GetDynamicStoreCount() >= DynamicStoreCountLimit) {
                YT_LOG_DEBUG("Backup rejected since dynamic store count limit is exceeded"
                    "(%v, DynamicStoreCount: %v, Limit: %v)",
                    tablet->GetLoggingTag(),
                    tablet->GetDynamicStoreCount(),
                    DynamicStoreCountLimit);

                auto rejectedInfo = req.add_rejected_tablets();
                ToProto(rejectedInfo->mutable_tablet_id(), tablet->GetId());
                auto error = TError("Dynamic store count limit is exceeded")
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("cell_id", Slot_->GetCellId())
                    << TErrorAttribute("dynamic_store_count", tablet->GetDynamicStoreCount())
                    << TErrorAttribute("dynamic_store_count_limit", DynamicStoreCountLimit);
                ToProto(rejectedInfo->mutable_error(), error);
                continue;
            }

            if (tablet->GetBackupCheckpointTimestamp() > currentTimestamp) {
                YT_LOG_DEBUG("Transiently confirmed backup checkpoint feasibility "
                    "(TabletId: %v, CheckpointTimestamp: %v, ClockClusterTag: %v)",
                    tablet->GetId(),
                    tablet->GetBackupCheckpointTimestamp(),
                    clusterTag);

                auto* confirmedInfo = req.add_confirmed_tablets();
                ToProto(confirmedInfo->mutable_tablet_id(), tablet->GetId());
                confirmedInfo->set_timestamp(tablet->GetBackupCheckpointTimestamp());
            } else {
                YT_LOG_DEBUG("Transiently rejected backup checkpoint feasibility "
                    "(TabletId: %v, CheckpointTimestamp: %v, CurrentTimestamp: %v, "
                    "ClusterTag: %v)",
                    tablet->GetId(),
                    tablet->GetBackupCheckpointTimestamp(),
                    currentTimestamp,
                    clusterTag);

                auto rejectedInfo = req.add_rejected_tablets();
                ToProto(rejectedInfo->mutable_tablet_id(), tablet->GetId());
                auto error = TError("Checkpoint timestamp is too late")
                    << TErrorAttribute("checkpoint_timestamp", tablet->GetBackupCheckpointTimestamp())
                    << TErrorAttribute("current_timestamp", currentTimestamp)
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("cell_id", Slot_->GetCellId())
                    << TErrorAttribute("clock_cluster_tag", clusterTag);
                ToProto(rejectedInfo->mutable_error(), error);
            }
        }

        if (req.confirmed_tablets().size() + req.rejected_tablets().size() > 0) {
            Slot_->CommitTabletMutation(req);
        }
    }

    void OnBackupFailed(TTablet* tablet, TError error)
    {
        YT_VERIFY(HasMutationContext());

        auto stage = tablet->GetBackupStage();

        if (stage == EBackupStage::None || stage == EBackupStage::RespondedToMasterFailure) {
            return;
        }

        if (stage == EBackupStage::RespondedToMasterSuccess) {
            YT_LOG_ALERT(error,
                "Attempted to abort tablet backup when it has already reported success "
                "to master (%v)",
                tablet->GetLoggingTag());
            return;
        }

        if (stage == EBackupStage::FeasibilityConfirmed) {
            TTabletWithCheckpoint holder(tablet);
            EraseOrCrash(TabletsAwaitingCheckpointPassing_, tablet);
        }

        if (tablet->GetBackupStage() == EBackupStage::AwaitingReplicationFinish &&
            tablet->GetBackupMode() == EBackupMode::ReplicatedSorted)
        {
            EraseOrCrash(TabletsAwaitingReplicationFinish_, tablet->GetId());
        }

        RejectCheckpoint(tablet, std::move(error), stage);
    }

    void RejectCheckpoint(TTablet* tablet, TError error, EBackupStage expectedStage)
    {
        NTabletServer::NProto::TReqReportBackupCheckpointPassed req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(tablet->GetMountRevision());
        req.set_confirmed(false);
        ToProto(req.mutable_error(), error);
        Slot_->PostMasterMessage(tablet->GetId(), req);

        tablet->CheckedSetBackupStage(expectedStage, EBackupStage::RespondedToMasterFailure);
        tablet->BackupMetadata().SetLastPassedCheckpointTimestamp(
            tablet->GetBackupCheckpointTimestamp());
    }

    static bool NeedsImmediateCutoffRotation(const TTablet* tablet)
    {
        return NeedsRowIndexCutoff(tablet) || NeedsDynamicStoreListCutoff(tablet);
    }

    static bool NeedsRowIndexCutoff(const TTablet* tablet)
    {
        auto backupMode = tablet->GetBackupMode();
        return backupMode == EBackupMode::ReplicatedSorted ||
            backupMode == EBackupMode::OrderedStrongCommitOrdering ||
            backupMode == EBackupMode::OrderedExact ||
            backupMode == EBackupMode::OrderedAtLeast ||
            backupMode == EBackupMode::OrderedAtMost;
    }

    static bool NeedsDynamicStoreListCutoff(const TTablet* tablet)
    {
        auto backupMode = tablet->GetBackupMode();
        return backupMode == EBackupMode::SortedAsyncReplica;
    }

    void OnCheckpointPassed(TTablet* tablet)
    {
        YT_VERIFY(HasMutationContext());

        NTabletServer::NProto::TReqReportBackupCheckpointPassed req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(tablet->GetMountRevision());

        auto respond = [&] (bool success) {
            Slot_->PostMasterMessage(tablet->GetId(), req);
            tablet->CheckedSetBackupStage(
                EBackupStage::FeasibilityConfirmed,
                success
                    ? EBackupStage::RespondedToMasterSuccess
                    : EBackupStage::RespondedToMasterFailure);
            tablet->BackupMetadata().SetLastPassedCheckpointTimestamp(
                tablet->GetBackupCheckpointTimestamp());
        };

        if (tablet->GetBackupMode() == EBackupMode::SortedAsyncReplica) {
            YT_VERIFY(tablet->PreparedReplicatorTransactionIds().empty());
        }

        std::vector<TDynamicStoreId> capturedDynamicStoreIds;
        if (tablet->IsPhysicallySorted()) {
            for (const auto& store : tablet->GetEden()->Stores()) {
                if (store->IsDynamic() &&
                    (store->GetStoreState() != EStoreState::ActiveDynamic || store->GetRowCount() > 0))
                {
                    capturedDynamicStoreIds.push_back(store->GetId());
                }
            }
        }

        if (const auto& activeStore = tablet->GetActiveStore();
            activeStore && activeStore->GetRowCount() > 0)
        {
            // Active store is non-empty, should rotate.
            if (NeedsImmediateCutoffRotation(tablet)) {
                if (tablet->GetState() != ETabletState::Mounted) {
                    // Tablet may be in freeze/unmount workflow. We abort the backup in this case.
                    YT_LOG_DEBUG(
                        "Tablet with nonempty active store is not mounted "
                        "during backup checkpoint passing (%v, TabletState: %v)",
                        tablet->GetLoggingTag(),
                        tablet->GetState());
                    auto error = TError("Tablet %v has nonempty dynamic store and is not mounted "
                        "during backup checkpoint passing",
                        tablet->GetId())
                        << TErrorAttribute("tablet_state", tablet->GetState())
                        << TErrorAttribute("cell_id", Slot_->GetCellId());
                    ToProto(req.mutable_error(), error);
                    req.set_confirmed(false);

                    respond(false);
                    return;
                }

                if (tablet->DynamicStoreIdPool().empty()) {
                    YT_LOG_DEBUG(
                        "Cannot perform backup cutoff due to empty "
                        "dynamic store id pool (%v)",
                        tablet->GetLoggingTag());
                    auto error = TError("Tablet %v cannot perform backup cutoff due to empty "
                        "dynamic store id pool",
                        tablet->GetId())
                        << TErrorAttribute("cell_id", Slot_->GetCellId());
                    ToProto(req.mutable_error(), error);
                    req.set_confirmed(false);

                    respond(false);
                    return;
                }

                auto previousStoreId = activeStore->GetId();

                tablet->GetStoreManager()->Rotate(/*createNewStore*/ true, EStoreRotationReason::None);

                YT_LOG_DEBUG(
                    "Store rotated to perform backup cutoff (%v, PreviousStoreId: %v, "
                    "NextStoreId: %v, BackupMode: %v)",
                    tablet->GetLoggingTag(),
                    previousStoreId,
                    tablet->GetActiveStore()->GetId(),
                    tablet->GetBackupMode());

                const auto& tabletManager = Slot_->GetTabletManager();
                tabletManager->UpdateTabletSnapshot(tablet);

                if (tabletManager->AllocateDynamicStoreIfNeeded(tablet)) {
                    YT_LOG_DEBUG(
                        "Dynamic store id for ordered tablet allocated "
                        "after backup cutoff (%v)",
                        tablet->GetLoggingTag());
                }
            } else {
                tablet->SetOutOfBandRotationRequested(true);
            }
        }

        if (NeedsRowIndexCutoff(tablet)) {
            auto* cutoffDescriptor = req.mutable_cutoff_descriptor()->mutable_row_index_descriptor();
            cutoffDescriptor->set_cutoff_row_index(tablet->GetTotalRowCount());

            TDynamicStoreId nextDynamicStoreId;
            if (const auto& activeStore = tablet->GetActiveStore()) {
                ToProto(cutoffDescriptor->mutable_next_dynamic_store_id(), activeStore->GetId());
                nextDynamicStoreId = activeStore->GetId();
            }

            YT_LOG_DEBUG(
                "Backup row index cutoff descriptor generated (%v, RowIndex: %v, NextDynamicStoreId: %v)",
                tablet->GetLoggingTag(),
                tablet->GetTotalRowCount(),
                nextDynamicStoreId);
        } else {
            auto* cutoffDescriptor = req.mutable_cutoff_descriptor()->mutable_dynamic_store_list_descriptor();
            ToProto(cutoffDescriptor->mutable_dynamic_store_ids_to_keep(), capturedDynamicStoreIds);

            YT_LOG_DEBUG(
                "Backup dynamic store list cutoff descriptor generated (%v, DynamicStoreIdsToKeep: %v)",
                tablet->GetLoggingTag(),
                capturedDynamicStoreIds);
        }

        if (tablet->GetBackupMode() == EBackupMode::ReplicatedSorted) {
            int pendingAsyncReplicaCount = 0;

            for (const auto& descriptor : tablet->BackupMetadata().ReplicaBackupDescriptors()) {
                const auto& replicaInfo = tablet->Replicas()[descriptor.ReplicaId];

                auto* protoReplicaInfo = req.add_replicas();
                ToProto(protoReplicaInfo->mutable_replica_id(), descriptor.ReplicaId);

                auto* statistics = protoReplicaInfo->mutable_replica_statistics();
                switch (replicaInfo.GetMode()) {
                    case ETableReplicaMode::Sync:
                        statistics->set_committed_replication_row_index(tablet->GetTotalRowCount());
                        break;

                    case ETableReplicaMode::Async:
                        if (replicaInfo.GetPreparedReplicationTransactionId()) {
                            ++pendingAsyncReplicaCount;
                        } else {
                            YT_ASSERT(replicaInfo.GetCommittedReplicationRowIndex() ==
                                replicaInfo.GetCurrentReplicationRowIndex());
                            statistics->set_committed_replication_row_index(
                                replicaInfo.GetCommittedReplicationRowIndex());
                        }
                        break;

                    default:
                        YT_ABORT();
                }
            }

            if (pendingAsyncReplicaCount > 0) {
                YT_LOG_DEBUG(
                    "Backup checkpoint confirmation is delayed by replication in progress "
                    "(%v, PendingAsyncReplicaCount: %v)",
                    tablet->GetLoggingTag(),
                    pendingAsyncReplicaCount);
                tablet->CheckedSetBackupStage(
                    EBackupStage::FeasibilityConfirmed,
                    EBackupStage::AwaitingReplicationFinish);
                EmplaceOrCrash(
                    TabletsAwaitingReplicationFinish_,
                    tablet->GetId(),
                    TTabletAwaitingReplicationFinish{
                        .PendingAsyncReplicaCount = pendingAsyncReplicaCount,
                        .Request = std::move(req),
                    });
                return;
            }
        }

        req.set_confirmed(true);
        respond(true);
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

            if (tablet->GetBackupMode() == EBackupMode::SortedAsyncReplica &&
                !tablet->PreparedReplicatorTransactionIds().empty())
            {
                tablet->CheckedSetBackupStage(
                    EBackupStage::FeasibilityConfirmed,
                    EBackupStage::AwaitingReplicationFinish);
                YT_LOG_DEBUG(
                    "Tablet has passed backup checkpoint but still has replicator writes "
                    "in progress (%v, PreparedTransactionIds: %v)",
                    tablet->GetLoggingTag(),
                    MakeFormattableView(
                        tablet->PreparedReplicatorTransactionIds(),
                        TDefaultFormatter{}));
                continue;
            }

            OnCheckpointPassed(tablet);

            YT_LOG_DEBUG(
                "Reported backup checkpoint passage by barrier timestamp "
                "(TabletId: %v, CheckpointTimestamp: %v, BarrierTimestamp: %v)",
                tablet->GetId(),
                tablet->GetBackupCheckpointTimestamp(),
                barrierTimestamp);
        }
    }

    void OnBeforeTransactionSerialized(TTransaction* transaction)
    {
        YT_VERIFY(HasMutationContext());

        const auto& tabletManager = Slot_->GetTabletManager();
        for (auto tabletId : transaction->PersistentAffectedTabletIds()) {
            auto* tablet = tabletManager->FindTablet(tabletId);
            if (!tablet) {
                continue;
            }

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

                OnCheckpointPassed(tablet);

                YT_LOG_DEBUG(
                    "Reported backup checkpoint passage due to a transaction "
                    "with later timestamp (%v, CheckpointTimestamp: %v, "
                    "NextTransactionCommitTimestamp: %v)",
                    tablet->GetLoggingTag(),
                    checkpointTimestamp,
                    commitTimestamp);

            } else if (tablet->GetBackupStage() == EBackupStage::TimestampReceived) {
                YT_LOG_DEBUG(
                    "Rejected backup checkpoint timestamp due to a transaction "
                    "with later timestamp (%v, CheckpointTimestamp: %v, "
                    "CommitTimestamp: %v)",
                    tablet->GetLoggingTag(),
                    checkpointTimestamp,
                    commitTimestamp);

                auto error = TError("Failed to confirm checkpoint timestamp in time "
                    "due to a transaction with later timestamp")
                    << TErrorAttribute("checkpoint_timestamp", checkpointTimestamp)
                    << TErrorAttribute("commit_timestamp", commitTimestamp)
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("cell_id", Slot_->GetCellId());

                RejectCheckpoint(tablet, error, EBackupStage::TimestampReceived);
            }
        }
    }

    void OnReplicatorWriteTransactionFinished(TTablet* tablet) override
    {
        if (!tablet->GetBackupCheckpointTimestamp()) {
            return;
        }

        if (tablet->GetBackupStage() != EBackupStage::AwaitingReplicationFinish) {
            return;
        }

        if (!tablet->PreparedReplicatorTransactionIds().empty()) {
            return;
        }

        YT_LOG_DEBUG(
            "All replicator write transactions finished (%v)",
            tablet->GetLoggingTag());

        YT_VERIFY(tablet->GetBackupMode() == EBackupMode::SortedAsyncReplica);
        tablet->CheckedSetBackupStage(
            EBackupStage::AwaitingReplicationFinish,
            EBackupStage::FeasibilityConfirmed);
        OnCheckpointPassed(tablet);
    }

    void OnReplicationTransactionFinished(
        TTablet* tablet,
        const TTableReplicaInfo* replicaInfo)
    {
        if (!tablet->GetBackupCheckpointTimestamp()) {
            return;
        }

        if (tablet->GetBackupStage() != EBackupStage::AwaitingReplicationFinish) {
            return;
        }

        YT_VERIFY(tablet->GetBackupMode() == EBackupMode::ReplicatedSorted);
        YT_VERIFY(!replicaInfo->GetPreparedReplicationTransactionId());

        auto it = TabletsAwaitingReplicationFinish_.find(tablet->GetId());
        YT_VERIFY(it != TabletsAwaitingReplicationFinish_.end());

        int replicaIndex = -1;
        for (const auto& [index, replicaDescriptor] :
            Enumerate(tablet->BackupMetadata().ReplicaBackupDescriptors()))
        {
            if (replicaInfo->GetId() == replicaDescriptor.ReplicaId) {
                replicaIndex = index;
                break;
            }
        }

        if (replicaIndex == -1) {
            return;
        }

        auto& request = it->second.Request;
        auto* statistics = request
            .mutable_replicas(replicaIndex)
            ->mutable_replica_statistics();

        YT_VERIFY(!statistics->has_committed_replication_row_index());
        YT_ASSERT(replicaInfo->GetCurrentReplicationRowIndex() ==
            replicaInfo->GetCommittedReplicationRowIndex());

        statistics->set_committed_replication_row_index(
            replicaInfo->GetCommittedReplicationRowIndex());

        if (--it->second.PendingAsyncReplicaCount == 0) {
            request.set_confirmed(true);
            Slot_->PostMasterMessage(tablet->GetId(), request);
            TabletsAwaitingReplicationFinish_.erase(tablet->GetId());

            tablet->CheckedSetBackupStage(
                EBackupStage::AwaitingReplicationFinish,
                EBackupStage::RespondedToMasterSuccess);
            tablet->BackupMetadata().SetLastPassedCheckpointTimestamp(
                tablet->GetBackupCheckpointTimestamp());
        }
    }

    TError CheckReplicaStatuses(
        const TTablet* tablet,
        const std::vector<TTableReplicaBackupDescriptor>& replicaDescriptors)
    {
        const auto& replicas = tablet->Replicas();
        for (const auto& descriptor : replicaDescriptors) {
            auto it = replicas.find(descriptor.ReplicaId);
            if (it == replicas.end()) {
                return TError("Unknown replica %v", descriptor.ReplicaId);
            }

            const auto& replica = it->second;

            switch (descriptor.Mode) {
                case ETableReplicaMode::Sync:
                    if (replica.GetStatus() != ETableReplicaStatus::SyncInSync) {
                        return TError("Sync replica %v is not in sync",
                            replica.GetId())
                            << TErrorAttribute("replica_status", replica.GetStatus());
                    }
                    break;

                case ETableReplicaMode::Async:
                    if (replica.GetStatus() != ETableReplicaStatus::AsyncInSync &&
                        replica.GetStatus() != ETableReplicaStatus::AsyncCatchingUp)
                    {
                        return TError("Async replica %v has synchronous writes in progress",
                            replica.GetId())
                            << TErrorAttribute("replica_status", replica.GetStatus());

                    }
                    break;

                default:
                    return TError("Unsupported replica mode %Qlv", descriptor.Mode);
            }
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IBackupManagerPtr CreateBackupManager(ITabletSlotPtr slot, IBootstrap* bootstrap)
{
    return New<TBackupManager>(std::move(slot), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
