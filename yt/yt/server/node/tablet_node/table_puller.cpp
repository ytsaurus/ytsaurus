#include "table_puller.h"

#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"
#include "private.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NChaosClient;
using namespace NObjectClient;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const int TabletRowsPerRead = 1000;

////////////////////////////////////////////////////////////////////////////////

class TTablePuller
    : public ITablePuller
{
public:
    TTablePuller(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        NNative::IConnectionPtr localConnection,
        ITabletSlotPtr slot,
        ITabletSnapshotStorePtr tabletSnapshotStore,
        IInvokerPtr workerInvoker,
        IThroughputThrottlerPtr nodeInThrottler)
        : Config_(std::move(config))
        , LocalConnection_(std::move(localConnection))
        , Slot_(std::move(slot))
        , TabletSnapshotStore_(std::move(tabletSnapshotStore))
        , WorkerInvoker_(std::move(workerInvoker))
        , TabletId_(tablet->GetId())
        , MountRevision_(tablet->GetMountRevision())
        , TableSchema_(tablet->GetTableSchema())
        , NameTable_(TNameTable::FromSchema(*TableSchema_))
        , MountConfig_(tablet->GetSettings().MountConfig)
        , ReplicaId_(tablet->GetUpstreamReplicaId())
        , Logger(TabletNodeLogger
            .WithTag("%v, UpstreamReplicaId: %v",
                tablet->GetLoggingTag(),
                ReplicaId_))
        , Throttler_(CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            std::move(nodeInThrottler),
            CreateReconfigurableThroughputThrottler(MountConfig_->ReplicationThrottler, Logger)
        }))
    { }

    void Enable() override
    {
        Disable();

        FiberFuture_ = BIND(&TTablePuller::FiberMain, MakeWeak(this))
            .AsyncVia(Slot_->GetHydraManager()->GetAutomatonCancelableContext()->CreateInvoker(WorkerInvoker_))
            .Run();

        YT_LOG_INFO("Puller fiber started");
    }

    void Disable() override
    {
        if (FiberFuture_) {
            FiberFuture_.Cancel(TError("Puller disabled"));
            YT_LOG_INFO("Puller fiber stopped");
        }
        FiberFuture_.Reset();
    }

private:
    const TTabletManagerConfigPtr Config_;
    const NNative::IConnectionPtr LocalConnection_;
    const ITabletSlotPtr Slot_;
    const ITabletSnapshotStorePtr TabletSnapshotStore_;
    const IInvokerPtr WorkerInvoker_;

    const TTabletId TabletId_;
    const TRevision MountRevision_;
    const TTableSchemaPtr TableSchema_;
    const TNameTablePtr NameTable_;
    const TTableMountConfigPtr MountConfig_;
    const TReplicaId ReplicaId_;

    const NLogging::TLogger Logger;

    const IThroughputThrottlerPtr NodeInThrottler_;
    const IThroughputThrottlerPtr Throttler_;

    TFuture<void> FiberFuture_;

    void FiberMain()
    {
        while (true) {
            NProfiling::TWallTimer timer;
            FiberIteration();
            TDelayedExecutor::WaitForDuration(MountConfig_->ReplicationTickPeriod - timer.GetElapsedTime());
        }
    }

    void FiberIteration()
    {
        TTabletSnapshotPtr tabletSnapshot;

        try {
            tabletSnapshot = TabletSnapshotStore_->FindTabletSnapshot(TabletId_, MountRevision_);
            if (!tabletSnapshot) {
                THROW_ERROR_EXCEPTION("No tablet snapshot is available")
                    << HardErrorAttribute;
            }

            if (!tabletSnapshot->TabletChaosData->ReplicationCard) {
                THROW_ERROR_EXCEPTION("No replication card")
                    << HardErrorAttribute;
            }

            auto counters = tabletSnapshot->TableProfiler->GetTablePullerCounters();
            auto countError = Finally([&] {
                if (std::uncaught_exception()) {
                    counters.ErrorCount.Increment();
                }
            });

            if (auto writeMode = tabletSnapshot->TabletRuntimeData->WriteMode.load(); writeMode != ETabletWriteMode::Pull) {
                YT_LOG_DEBUG("Will not pull rows since tablet write mode does not imply pulling (WriteMode: %v)",
                    writeMode);
                return;
            }

            // NB: There can be uncommitted sync transactions from previous era but they should be already in prepared state and will be committed anyway.

            auto replicationCard = tabletSnapshot->TabletChaosData->ReplicationCard;
            auto replicationProgress = tabletSnapshot->TabletRuntimeData->ReplicationProgress.Load();
            auto [queueReplica, upperTimestamp] = PickQueueReplica(tabletSnapshot, replicationCard, replicationProgress);
            if (!queueReplica) {
                return;
            }

            auto* selfReplica = replicationCard->FindReplica(tabletSnapshot->UpstreamReplicaId);
            if (!selfReplica) {
                return;
            }

            const auto& cluster = queueReplica->Cluster;
            const auto& tablePath = queueReplica->TablePath;
            auto upstreamReplicaId = queueReplica->ReplicaId;
            auto replicationRound = tabletSnapshot->TabletChaosData->ReplicationRound;
            TPullRowsResult result;

            {
                TEventTimerGuard timerGuard(counters.PullRowsTime);

                auto foreignConnection = LocalConnection_->GetClusterDirectory()->FindConnection(cluster);
                auto foreignClient = foreignConnection->CreateClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));

                TPullRowsOptions options;
                options.TabletRowsPerRead = TabletRowsPerRead;
                options.ReplicationProgress = *replicationProgress;
                options.StartReplicationRowIndexes = tabletSnapshot->TabletChaosData->CurrentReplicationRowIndexes;
                options.UpperTimestamp = upperTimestamp;
                options.UpstreamReplicaId = upstreamReplicaId;
                options.OrderRowsByTimestamp = selfReplica->ContentType == EReplicaContentType::Queue;

                YT_LOG_DEBUG("Pulling rows (Cluster: %v, Path: %v, ReplicationProgress: %v, ReplicationRowIndexes: %v, UpperTimestamp: %llx)",
                    cluster,
                    tablePath,
                    options.ReplicationProgress,
                    options.StartReplicationRowIndexes,
                    upperTimestamp);

                result = WaitFor(foreignClient->PullRows(tablePath, options))
                    .ValueOrThrow();
            }

            auto rowCount = result.RowCount;
            auto dataWeight = result.DataWeight;
            const auto& endReplicationRowIndexes = result.EndReplicationRowIndexes;
            const auto& resultRows = result.Rows;
            const auto& progress = result.ReplicationProgress;

            YT_LOG_DEBUG("Pulled rows (RowCount: %v, DataWeight: %v, NewProgress: %v, EndReplictionRowIndexes: %v)",
                rowCount,
                dataWeight,
                progress,
                endReplicationRowIndexes);

            // Update progress even if no rows pulled.
            if (IsReplicationProgressGreaterOrEqual(*replicationProgress, progress)) {
                YT_VERIFY(resultRows.empty());
                return;
            }

            {
                TEventTimerGuard timerGuard(counters.WriteTime);

                auto localClient = LocalConnection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
                auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
                    .ValueOrThrow();

                // Set options to avoid nested writes to other replicas.
                TModifyRowsOptions modifyOptions;
                modifyOptions.ReplicationCard = replicationCard;
                modifyOptions.UpstreamReplicaId = tabletSnapshot->UpstreamReplicaId;
                modifyOptions.TopmostTransaction = false;

                localTransaction->WriteRows(
                    tabletSnapshot->TablePath,
                    NameTable_,
                    resultRows,
                    modifyOptions);

                {
                    NProto::TReqWritePulledRows req;
                    ToProto(req.mutable_tablet_id(), TabletId_);
                    req.set_replication_round(replicationRound);
                    ToProto(req.mutable_new_replication_progress(), progress);
                    for (const auto [tabletId, endReplicationRowIndex] : endReplicationRowIndexes) {
                        auto protoEndReplicationRowIndex = req.add_new_replication_row_indexes();
                        ToProto(protoEndReplicationRowIndex->mutable_tablet_id(), tabletId);
                        protoEndReplicationRowIndex->set_replication_row_index(endReplicationRowIndex);
                    }
                    localTransaction->AddAction(Slot_->GetCellId(), MakeTransactionActionData(req));
                }

                YT_LOG_DEBUG("Commiting pull rows write transaction (TransactionId: %v)",
                    localTransaction->GetId());

                // NB: 2PC is used here to correctly process transaction signatures (sent by both rows and actions).
                // TODO(savrus) Discard 2PC.
                TTransactionCommitOptions commitOptions;
                commitOptions.CoordinatorCellId = Slot_->GetCellId();
                commitOptions.Force2PC = true;
                WaitFor(localTransaction->Commit(commitOptions))
                    .ThrowOnError();

                YT_LOG_DEBUG("Pull rows write transaction committed (TransactionId: %v)",
                    localTransaction->GetId());
            }

            counters.RowCount.Increment(rowCount);
            counters.DataWeight.Increment(dataWeight);
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Pull);
            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Pull].Store(error);
            YT_LOG_ERROR(error, "Error pulling rows, backing off");

            if (error.Attributes().Get<bool>("hard", false)) {
                DoHardBackoff(error);
            } else {
                DoSoftBackoff(error);
            }
        }
    }

    std::pair<NChaosClient::TReplicaInfo*, TTimestamp> PickQueueReplica(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TReplicationCardPtr& replicationCard,
        const TRefCountedReplicationProgressPtr& replicationProgress)
    {
        // If our progress is less than any queue replica progress, pull from that replica.
        // Otherwise pull from sync replica of oldest era corresponding to our progress.

        YT_LOG_DEBUG("Pick replica to pull from (Replicas: %v)",
            MakeFormattableView(replicationCard->Replicas,
                [] (TStringBuilderBase* builder, const NChaosClient::TReplicaInfo& replicaInfo) {
                    builder->AppendFormat("(%v,%v,%v,%v)", replicaInfo.Cluster, replicaInfo.TablePath,
                        replicaInfo.Mode, replicaInfo.ContentType);
                }));

        auto findFreshQueueReplica = [&] () -> NChaosClient::TReplicaInfo* {
            for (auto& replica : replicationCard->Replicas) {
                if (replica.ContentType == EReplicaContentType::Queue &&
                    replica.State == EReplicaState::Enabled &&
                    !IsReplicationProgressGreaterOrEqual(*replicationProgress, replica.ReplicationProgress))
                {
                    return &replica;
                }
            }
            return {};
        };

        auto findSyncQueueReplica = [&] (auto* selfReplica, auto timestamp) -> std::pair<NChaosClient::TReplicaInfo*, TTimestamp> {
            for (auto& replica : replicationCard->Replicas) {
                if (replica.ContentType != EReplicaContentType::Queue || replica.State != EReplicaState::Enabled) {
                    continue;
                }

                auto historyItemIndex = replica.FindHistoryItemIndex(timestamp);
                if (historyItemIndex == -1) {
                    continue;
                }
                if (const auto& item = replica.History[historyItemIndex]; !IsReplicaReallySync(item.Mode, item.State)) {
                    continue;
                }

                // Pull from (past) sync replica until it changed mode or we became sync.
                TTimestamp upperTimestamp = NullTimestamp;
                if (historyItemIndex + 1 < std::ssize(replica.History)) {
                    upperTimestamp = replica.History[historyItemIndex + 1].Timestamp;
                } else if (IsReplicaReallySync(selfReplica->Mode, selfReplica->State)) {
                    upperTimestamp = selfReplica->History.back().Timestamp;
                }

                return {&replica, upperTimestamp};
            }

            return {};
        };

        auto* selfReplica = replicationCard->FindReplica(tabletSnapshot->UpstreamReplicaId);
        if (!selfReplica) {
            YT_LOG_DEBUG("Will not pull rows since replication card does not contain us");
            return {};
        }

        if (selfReplica->State != EReplicaState::Enabled) {
            YT_LOG_DEBUG("Will not pull rows since replica is not enabled (ReplicaState: %v)",
                selfReplica->State);
            return {};
        }

        auto oldestTimestamp = GetReplicationProgressMinTimestamp(*replicationProgress);
        {
            auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);
            YT_VERIFY(historyItemIndex >= 0 && historyItemIndex < std::ssize(selfReplica->History));
            const auto& historyItem = selfReplica->History[historyItemIndex];
            if (IsReplicaReallySync(historyItem.Mode, historyItem.State)) {
                YT_LOG_DEBUG("Will not pull rows since oldest progress timestamp corresponds to sync history item (OldestTimestamp: %v, HistoryItem: %v)",
                    oldestTimestamp,
                    historyItem);
                return {};
            }
        }

        if (selfReplica->Mode != EReplicaMode::Async) {
            YT_LOG_DEBUG("Pulling rows while replica is not async (ReplicaMode: %v)",
                selfReplica->Mode);
            // NB: Allow this since sync replica could be catching up.
        }

        auto* queueReplica = findFreshQueueReplica();
        if (queueReplica) {
            YT_LOG_DEBUG("Pull rows from fresh replica (ReplicaId: %v)",
                queueReplica->ReplicaId);
            return {queueReplica, NullTimestamp};
        }

        TTimestamp upperTimestamp = NullTimestamp;
        std::tie(queueReplica, upperTimestamp) = findSyncQueueReplica(selfReplica, oldestTimestamp);
        if (queueReplica) {
            YT_LOG_DEBUG("Pull rows from sync replica (ReplicaId: %v, OldestTimestamp: %llx, UpperTimestamp: %llx)",
                queueReplica->ReplicaId,
                oldestTimestamp,
                upperTimestamp);
            return {queueReplica, upperTimestamp};
        }

        YT_LOG_DEBUG("Will not pull rows since no in-sync queue found");
        return {};
    }

    void DoSoftBackoff(const TError& error)
    {
        YT_LOG_INFO(error, "Doing soft backoff");
        TDelayedExecutor::WaitForDuration(Config_->ReplicatorSoftBackoffTime);
    }

    void DoHardBackoff(const TError& error)
    {
        YT_LOG_INFO(error, "Doing hard backoff");
        TDelayedExecutor::WaitForDuration(Config_->ReplicatorHardBackoffTime);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITablePullerPtr CreateTablePuller(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    NNative::IConnectionPtr localConnection,
    ITabletSlotPtr slot,
    ITabletSnapshotStorePtr tabletSnapshotStore,
    IInvokerPtr workerInvoker,
    IThroughputThrottlerPtr nodeInThrottler)
{
    return New<TTablePuller>(
        std::move(config),
        tablet,
        std::move(localConnection),
        std::move(slot),
        std::move(tabletSnapshotStore),
        std::move(workerInvoker),
        std::move(nodeInThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
