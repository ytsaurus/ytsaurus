#include "table_puller.h"

#include "chaos_agent.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"
#include "private.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const int TabletRowsPerRead = 1000;

////////////////////////////////////////////////////////////////////////////////

class TBannedReplicaTracker
{
public:
    explicit TBannedReplicaTracker(NLogging::TLogger logger)
        : Logger(std::move(logger))
    { }

    bool IsReplicaBanned(TReplicaId replicaId)
    {
        auto it = BannedReplicas_.find(replicaId);
        bool result = it != BannedReplicas_.end() && it->second > 0;

        YT_LOG_INFO("Banned replica tracker checking replica (ReplicaId: %v, Result: %v)",
            replicaId,
            result);

        return result;
    }

    void BanReplica(TReplicaId replicaId)
    {
        BannedReplicas_[replicaId] = std::ssize(BannedReplicas_);

        YT_LOG_DEBUG("Banned replica tracker has banned replica (ReplicaId: %v, ReplicasSize: %v)",
            replicaId,
            BannedReplicas_.size());
    }

    void SyncReplicas(const TReplicationCardPtr& replicationCard)
    {
        auto replicaIds = GetKeys(BannedReplicas_);
        for (auto replicaId : replicaIds) {
            if (!replicationCard->Replicas.contains(replicaId)) {
                EraseOrCrash(BannedReplicas_, replicaId);
            }
        }

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
            if (!BannedReplicas_.contains(replicaId) &&
                replicaInfo.ContentType == ETableReplicaContentType::Queue &&
                IsReplicaEnabled(replicaInfo.State))
            {
                InsertOrCrash(BannedReplicas_, std::pair(replicaId, 0));
            }
        }

        DecreaseCounters();
    }

private:
    const NLogging::TLogger Logger;

    THashMap<TReplicaId, int> BannedReplicas_;

    void DecreaseCounters()
    {
        for (auto& [_, counter] : BannedReplicas_) {
            if (counter > 0) {
                --counter;
            }
        }
    }
};

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
        , MountConfig_(tablet->GetSettings().MountConfig)
        , ReplicaId_(tablet->GetUpstreamReplicaId())
        , PivotKey_(tablet->GetPivotKey())
        , NextPivotKey_(tablet->GetNextPivotKey())
        , Logger(TabletNodeLogger
            .WithTag("%v, UpstreamReplicaId: %v",
                tablet->GetLoggingTag(),
                ReplicaId_))
        , Throttler_(CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            std::move(nodeInThrottler),
            CreateReconfigurableThroughputThrottler(MountConfig_->ReplicationThrottler, Logger)
        }))
        , BannedReplicaTracker_(Logger)
        , LastReplicationProgressAdvance_(*tablet->RuntimeData()->ReplicationProgress.Acquire())
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
    const TTableMountConfigPtr MountConfig_;
    const TReplicaId ReplicaId_;
    const TLegacyOwningKey PivotKey_;
    const TLegacyOwningKey NextPivotKey_;

    const NLogging::TLogger Logger;

    const IThroughputThrottlerPtr Throttler_;

    TBannedReplicaTracker BannedReplicaTracker_;
    ui64 ReplicationRound_ = 0;
    TReplicationProgress LastReplicationProgressAdvance_;

    TFuture<void> FiberFuture_;

    void FiberMain()
    {
        while (true) {
            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("TablePuller"));
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

            auto replicationCard = tabletSnapshot->TabletRuntimeData->ReplicationCard.Acquire();
            if (!replicationCard) {
                THROW_ERROR_EXCEPTION("No replication card");
            }

            auto* selfReplica = replicationCard->FindReplica(tabletSnapshot->UpstreamReplicaId);
            if (!selfReplica) {
                THROW_ERROR_EXCEPTION("Table unable to identify self replica in replication card")
                    << TErrorAttribute("upstream_replica_id", tabletSnapshot->UpstreamReplicaId)
                    << HardErrorAttribute;
            }

            if (IsReplicaDisabled(selfReplica->State)) {
                YT_LOG_DEBUG("Will not pull rows since replica is not enabled (ReplicaState: %v)",
                    selfReplica->State);
                return;
            }

            // There can be unserialized sync transactions from previous era or pull rows write transaction from previous iteration.
            if (auto delayedLocklessRowCount = tabletSnapshot->TabletRuntimeData->DelayedLocklessRowCount.load();
                delayedLocklessRowCount > 0)
            {
                YT_LOG_DEBUG("Will not pull rows since some transactions are not serialized yet (DelayedLocklessRowCount: %v)",
                    delayedLocklessRowCount);
                return;
            }

            auto replicationRound = tabletSnapshot->TabletChaosData->ReplicationRound.load();
            if (replicationRound < ReplicationRound_) {
                YT_LOG_DEBUG("Will not pull rows since previous pull rows transaction is not fully serialized yet (ReplicationRound: %v)",
                    replicationRound);
                return;
            }
            ReplicationRound_ = replicationRound;

            if (auto writeMode = tabletSnapshot->TabletRuntimeData->WriteMode.load(); writeMode != ETabletWriteMode::Pull) {
                YT_LOG_DEBUG("Will not pull rows since tablet write mode does not imply pulling (WriteMode: %v)",
                    writeMode);
                tabletSnapshot->TabletRuntimeData->Errors
                    .BackgroundErrors[ETabletBackgroundActivity::Pull].Store(TError());
                return;
            }

            auto replicationProgress = tabletSnapshot->TabletRuntimeData->ReplicationProgress.Acquire();

            if (!IsReplicationProgressGreaterOrEqual(*replicationProgress, LastReplicationProgressAdvance_)) {
                YT_LOG_DEBUG("Skipping chaos agent iteration because last progress advance is not there yet "
                    "(TabletReplicationProgress: %v, LastReplicationProgress: %v)",
                    static_cast<TReplicationProgress>(*replicationProgress),
                    LastReplicationProgressAdvance_);
                return;
            }

            if (auto newProgress = MaybeAdvanceReplicationProgress(selfReplica, replicationProgress)) {
                if (!IsReplicationProgressGreaterOrEqual(*newProgress, LastReplicationProgressAdvance_)) {
                    YT_LOG_ALERT("Trying to advance replication progress behind last attempt (LastReplicationProgress: %v, NewReplicationProgress: %v)",
                        LastReplicationProgressAdvance_,
                        newProgress);
                }

                bool committed = AdvanceTabletReplicationProgress(
                    LocalConnection_,
                    Logger,
                    Slot_->GetCellId(),
                    TabletId_,
                    std::move(*newProgress),
                    /*validateStrictAdvance*/ true,
                    ReplicationRound_);

                if (committed) {
                    ++ReplicationRound_;
                    LastReplicationProgressAdvance_ = std::move(*newProgress);
                }
            } else {
                BannedReplicaTracker_.SyncReplicas(replicationCard);
                DoPullRows(
                    tabletSnapshot,
                    replicationCard,
                    selfReplica,
                    replicationProgress);
            }

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Pull].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Pull);
            YT_LOG_ERROR(error, "Error pulling rows, backing off");
            if (tabletSnapshot) {
                tabletSnapshot->TabletRuntimeData->Errors
                    .BackgroundErrors[ETabletBackgroundActivity::Pull].Store(error);
            }
            if (error.Attributes().Get<bool>("hard", false)) {
                DoHardBackoff(error);
            } else {
                DoSoftBackoff(error);
            }
        }
    }

    std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*, TTimestamp> PickQueueReplica(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TReplicationCardPtr& replicationCard,
        const TRefCountedReplicationProgressPtr& replicationProgress)
    {
        // If our progress is less than any queue replica progress, pull from that replica.
        // Otherwise pull from sync replica of oldest era corresponding to our progress.

        YT_LOG_DEBUG("Pick replica to pull from");

        auto* selfReplica = replicationCard->FindReplica(tabletSnapshot->UpstreamReplicaId);
        if (!selfReplica) {
            YT_LOG_DEBUG("Will not pull rows since replication card does not contain us");
            return {};
        }

        if (!IsReplicationProgressGreaterOrEqual(*replicationProgress, selfReplica->ReplicationProgress)) {
            YT_LOG_DEBUG("Will not pull rows since actual replication progress is behind replication card replica progress"
                " (ReplicationProgress: %v, ReplicaInfo: %v)",
                static_cast<TReplicationProgress>(*replicationProgress),
                *selfReplica);
            return {};
        }

        auto oldestTimestamp = GetReplicationProgressMinTimestamp(*replicationProgress);
        auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);
        if (historyItemIndex == -1) {
            YT_LOG_DEBUG("Will not pull rows since replica history does not cover replication progress (OldestTimestamp: %v, History: %v)",
                oldestTimestamp,
                selfReplica->History);
            return {};
        }

        YT_VERIFY(historyItemIndex >= 0 && historyItemIndex < std::ssize(selfReplica->History));
        const auto& historyItem = selfReplica->History[historyItemIndex];
        if (historyItem.IsSync()) {
            YT_LOG_DEBUG("Will not pull rows since oldest progress timestamp corresponds to sync history item (OldestTimestamp: %v, HistoryItem: %v)",
                oldestTimestamp,
                historyItem);
            return {};
        }

        if (!IsReplicaAsync(selfReplica->Mode)) {
            YT_LOG_DEBUG("Pulling rows while replica is not async (ReplicaMode: %v)",
                selfReplica->Mode);
            // NB: Allow this since sync replica could be catching up.
        }

        auto findFreshQueueReplica = [&] () -> std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*> {
            for (auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
                if (BannedReplicaTracker_.IsReplicaBanned(replicaId)) {
                    continue;
                }

                if (replicaInfo.ContentType != ETableReplicaContentType::Queue ||
                    !IsReplicaEnabled(replicaInfo.State) ||
                    replicaInfo.FindHistoryItemIndex(oldestTimestamp) == -1)
                {
                    continue;
                }

                if (selfReplica->ContentType == ETableReplicaContentType::Data) {
                    if (!IsReplicationProgressGreaterOrEqual(*replicationProgress, replicaInfo.ReplicationProgress)) {
                        return {replicaId, &replicaInfo};
                    }
                } else {
                    YT_VERIFY(selfReplica->ContentType == ETableReplicaContentType::Queue);
                    auto replicaOldestTimestamp = GetReplicationProgressMinTimestamp(
                        replicaInfo.ReplicationProgress,
                        replicationProgress->Segments[0].LowerKey,
                        replicationProgress->UpperKey);
                    if (replicaOldestTimestamp > oldestTimestamp) {
                        return {replicaId, &replicaInfo};
                    }
                }
            }
            return {};
        };

        auto findSyncQueueReplica = [&] () -> std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*, TTimestamp> {
            for (auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
                if (BannedReplicaTracker_.IsReplicaBanned(replicaId)) {
                    continue;
                }

                if (replicaInfo.ContentType != ETableReplicaContentType::Queue || !IsReplicaEnabled(replicaInfo.State)) {
                    continue;
                }

                auto historyItemIndex = replicaInfo.FindHistoryItemIndex(oldestTimestamp);
                if (historyItemIndex == -1) {
                    continue;
                }

                const auto& historyItem = replicaInfo.History[historyItemIndex];
                if (!historyItem.IsSync()) {
                    continue;
                }

                YT_LOG_DEBUG("Found sync replica corresponding history item (ReplicaId %v, HistoryItem: %v)",
                    replicaId,
                    historyItem);

                // Pull from (past) sync replica until it changed mode or we became sync.
                // AsyncToSync -> SyncToAsync transition is possible, so check the previous state
                // when in SyncToAsync mode
                auto upperTimestamp = NullTimestamp;
                if (historyItemIndex + 1 < std::ssize(replicaInfo.History)) {
                    upperTimestamp = replicaInfo.History[historyItemIndex + 1].Timestamp;
                } else if (IsReplicaReallySync(selfReplica->Mode, selfReplica->State, selfReplica->History.back())) {
                    upperTimestamp = selfReplica->History.back().Timestamp;
                }

                return {replicaId, &replicaInfo, upperTimestamp};
            }

            return {};
        };

        if (auto [queueReplicaId, queueReplica] = findFreshQueueReplica(); queueReplica) {
            YT_LOG_DEBUG("Pull rows from fresh replica (ReplicaId: %v)",
                queueReplicaId);
            return {queueReplicaId, queueReplica, NullTimestamp};
        }

        if (auto [queueReplicaId, queueReplicaInfo, upperTimestamp] = findSyncQueueReplica(); queueReplicaInfo) {
            YT_LOG_DEBUG("Pull rows from sync replica (ReplicaId: %v, OldestTimestamp: %v, UpperTimestamp: %v)",
                queueReplicaId,
                oldestTimestamp,
                upperTimestamp);
            return {queueReplicaId, queueReplicaInfo, upperTimestamp};
        }

        YT_LOG_DEBUG("Will not pull rows since no in-sync queue found");
        return {};
    }

    void DoPullRows(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TReplicationCardPtr& replicationCard,
        const TReplicaInfo* selfReplica,
        const TRefCountedReplicationProgressPtr& replicationProgress)
    {
        const auto& tableProfiler = tabletSnapshot->TableProfiler;
        auto* counters = tableProfiler->GetTablePullerCounters();

        TDuration throttleTime;
        {
            if (auto throttleFuture = Throttler_->Throttle(1); !throttleFuture.IsSet()) {
                auto timerGuard = TEventTimerGuard(counters->ThrottleTime);
                YT_LOG_DEBUG("Started waiting for replication throttling");

                WaitFor(throttleFuture)
                    .ThrowOnError();

                YT_LOG_DEBUG("Finished waiting for replication throttling");
                throttleTime = timerGuard.GetElapsedTime();
            }
        }

        auto [queueReplicaId, queueReplicaInfo, upperTimestamp] = PickQueueReplica(tabletSnapshot, replicationCard, replicationProgress);
        if (!queueReplicaInfo) {
            THROW_ERROR_EXCEPTION("Unable to pick a queue replica to replicate from");
        }
        YT_VERIFY(queueReplicaId);

        auto finally = Finally([this, queueReplicaId=queueReplicaId, tableProfiler, counters] {
            if (std::uncaught_exception()) {
                BannedReplicaTracker_.BanReplica(queueReplicaId);
                counters->ErrorCount.Increment();
            }
        });

        const auto& clusterName = queueReplicaInfo->ClusterName;
        const auto& replicaPath = queueReplicaInfo->ReplicaPath;
        TPullRowsResult result;
        {
            TEventTimerGuard timerGuard(counters->PullRowsTime);

            auto alienConnection = LocalConnection_->GetClusterDirectory()->FindConnection(clusterName);
            if (!alienConnection) {
                THROW_ERROR_EXCEPTION("Queue replica cluster %Qv is not known", clusterName)
                    << HardErrorAttribute;
            }
            auto alienClient = alienConnection->CreateClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));

            TPullRowsOptions options;
            options.TabletRowsPerRead = TabletRowsPerRead;
            options.ReplicationProgress = *replicationProgress;
            options.StartReplicationRowIndexes = tabletSnapshot->TabletChaosData->CurrentReplicationRowIndexes.Load();
            options.UpperTimestamp = upperTimestamp;
            options.UpstreamReplicaId = queueReplicaId;
            options.OrderRowsByTimestamp = selfReplica->ContentType == ETableReplicaContentType::Queue;
            options.TableSchema = TableSchema_;

            YT_LOG_DEBUG("Pulling rows (ClusterName: %v, ReplicaPath: %v, ReplicationProgress: %v, ReplicationRowIndexes: %v, UpperTimestamp: %v)",
                clusterName,
                replicaPath,
                options.ReplicationProgress,
                options.StartReplicationRowIndexes,
                upperTimestamp);

            result = WaitFor(alienClient->PullRows(replicaPath, options))
                .ValueOrThrow();
        }

        if (result.Versioned != TableSchema_->IsSorted()) {
            THROW_ERROR_EXCEPTION("Could not pull from queue since it has unexpected replication log format")
                << TErrorAttribute("queue_cluster", clusterName)
                << TErrorAttribute("queue_path", replicaPath)
                << TErrorAttribute("versioned_result", result.Versioned)
                << HardErrorAttribute;
        }

        auto rowCount = result.RowCount;
        auto dataWeight = result.DataWeight;
        const auto& endReplicationRowIndexes = result.EndReplicationRowIndexes;
        auto resultRows = result.Rowset->GetRows();
        const auto& progress = result.ReplicationProgress;
        const auto& nameTable = result.Rowset->GetNameTable();

        YT_LOG_DEBUG("Pulled rows "
            "(RowCount: %v, DataWeight: %v, NewProgress: %v, EndReplicationRowIndexes: %v, ThrottleTime: %v)",
            rowCount,
            dataWeight,
            progress,
            endReplicationRowIndexes,
            throttleTime);

        Throttler_->Acquire(dataWeight);

        // TODO(savrus) Remove this sanity check when pull rows is mature enough.
        if (result.Versioned) {
            auto versionedRows = ReinterpretCastRange<TVersionedRow>(resultRows);
            for (auto row : versionedRows) {
                YT_VERIFY(row.GetWriteTimestampCount() + row.GetDeleteTimestampCount() == 1);
                auto rowTimestamp = row.GetWriteTimestampCount() > 0
                    ? row.WriteTimestamps()[0]
                    : row.DeleteTimestamps()[0];
                auto progressTimestamp = FindReplicationProgressTimestampForKey(*replicationProgress, row.Keys());
                if (!progressTimestamp || progressTimestamp >= rowTimestamp) {
                    YT_LOG_ALERT("Received inappropriate row timestamp in pull rows response (RowTimestamp: %v, ProgressTimestamp: %v, Row: %v, Progress: %v)",
                        rowTimestamp,
                        progressTimestamp,
                        row,
                        static_cast<TReplicationProgress>(*replicationProgress));

                    THROW_ERROR_EXCEPTION("Inappropriate row timestamp in pull rows response")
                        << TErrorAttribute("row_timestamp", rowTimestamp)
                        << TErrorAttribute("progress_timestamp", progressTimestamp)
                        << HardErrorAttribute;
                }
            }
        } else {
            auto progressTimestamp = replicationProgress->Segments[0].Timestamp;
            auto unversionedRows = ReinterpretCastRange<TUnversionedRow>(resultRows);

            auto timestampColumnIndex = nameTable->FindId(TimestampColumnName);
            if (!timestampColumnIndex) {
                THROW_ERROR_EXCEPTION("Invalid pulled rows result: %Qv column is absent",
                    TimestampColumnName)
                    << HardErrorAttribute;
            }

            for (auto row : unversionedRows) {
                if (row[*timestampColumnIndex].Id != *timestampColumnIndex) {
                    YT_LOG_ALERT("Could not identify timestamp column in pulled row. Timestamp validation disabled (Row: %v, TimestampColumnIndex: %v)",
                        row,
                        *timestampColumnIndex);
                }
                if (auto rowTimestamp = row[*timestampColumnIndex].Data.Uint64; progressTimestamp >= rowTimestamp) {
                    YT_LOG_ALERT("Received inappropriate timestamp in pull rows response (RowTimestamp: %v, ProgressTimestamp: %v, Row: %v, Progress: %v)",
                        rowTimestamp,
                        progressTimestamp,
                        row,
                        static_cast<TReplicationProgress>(*replicationProgress));

                    THROW_ERROR_EXCEPTION("Inappropriate row timestamp in pull rows response")
                        << TErrorAttribute("row_timestamp", rowTimestamp)
                        << TErrorAttribute("progress_timestamp", progressTimestamp)
                        << HardErrorAttribute;
                }
            }
        }

        // Update progress even if no rows pulled.
        if (IsReplicationProgressGreaterOrEqual(*replicationProgress, progress)) {
            YT_VERIFY(resultRows.empty());
            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Pull].Store(TError());
            return;
        }

        {
            TEventTimerGuard timerGuard(counters->WriteTime);

            auto localClient = LocalConnection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
            auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
                .ValueOrThrow();

            // Set options to avoid nested writes to other replicas.
            TModifyRowsOptions modifyOptions;
            modifyOptions.ReplicationCard = replicationCard;
            modifyOptions.UpstreamReplicaId = tabletSnapshot->UpstreamReplicaId;
            modifyOptions.TopmostTransaction = false;
            modifyOptions.AllowMissingKeyColumns = true;

            std::vector<TRowModification> rowModifications;
            rowModifications.reserve(resultRows.size());
            for (auto row : resultRows) {
                rowModifications.push_back({ERowModificationType::VersionedWrite, row, TLockMask()});
            }

            localTransaction->ModifyRows(
                tabletSnapshot->TablePath,
                nameTable,
                MakeSharedRange(std::move(rowModifications)),
                modifyOptions);

            {
                NProto::TReqWritePulledRows req;
                ToProto(req.mutable_tablet_id(), TabletId_);
                req.set_replication_round(ReplicationRound_);
                ToProto(req.mutable_new_replication_progress(), progress);
                for (const auto [tabletId, endReplicationRowIndex] : endReplicationRowIndexes) {
                    auto protoEndReplicationRowIndex = req.add_new_replication_row_indexes();
                    ToProto(protoEndReplicationRowIndex->mutable_tablet_id(), tabletId);
                    protoEndReplicationRowIndex->set_replication_row_index(endReplicationRowIndex);
                }
                localTransaction->AddAction(Slot_->GetCellId(), MakeTransactionActionData(req));
            }

            YT_LOG_DEBUG("Committing pull rows write transaction (TransactionId: %v, ReplicationRound: %v)",
                localTransaction->GetId(),
                ReplicationRound_);

            // NB: 2PC is used here to correctly process transaction signatures (sent by both rows and actions).
            // TODO(savrus) Discard 2PC.
            TTransactionCommitOptions commitOptions;
            commitOptions.CoordinatorCellId = Slot_->GetCellId();
            commitOptions.Force2PC = true;
            commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
            WaitFor(localTransaction->Commit(commitOptions))
                .ThrowOnError();

            ++ReplicationRound_;

            YT_LOG_DEBUG("Pull rows write transaction committed (TransactionId: %v, NewReplicationRound: %v)",
                localTransaction->GetId(),
                ReplicationRound_);
        }

        counters->RowCount.Increment(rowCount);
        counters->DataWeight.Increment(dataWeight);
    }

    std::optional<TReplicationProgress> MaybeAdvanceReplicationProgress(
        const TReplicaInfo* selfReplica,
        const TRefCountedReplicationProgressPtr& progress)
    {
        if (progress->Segments.size() == 1 && progress->Segments[0].Timestamp == MinTimestamp) {
            auto historyTimestamp = selfReplica->History[0].Timestamp;
            auto progressTimestamp = GetReplicationProgressMinTimestamp(
                selfReplica->ReplicationProgress,
                PivotKey_.Get(),
                NextPivotKey_.Get());

            YT_LOG_DEBUG("Checking that replica has been added in non-catchup mode (ReplicationCardMinProgressTimestamp: %v, HistoryMinTimestamp: %v)",
                progressTimestamp,
                historyTimestamp);

            if (progressTimestamp == historyTimestamp && progressTimestamp != MinTimestamp) {
                YT_LOG_DEBUG("Advance replication progress to first history item. (ReplicationProgress: %v, Replica: %v, Timestamp: %v)",
                    static_cast<TReplicationProgress>(*progress),
                    selfReplica,
                    historyTimestamp);

                return AdvanceReplicationProgress(
                    *progress,
                    historyTimestamp);
            }

            YT_LOG_DEBUG("Checking that replication card contains further progress (ReplicationProgress: %v, Replica: %v)",
                static_cast<TReplicationProgress>(*progress),
                selfReplica);

            if (!IsReplicationProgressGreaterOrEqual(*progress, selfReplica->ReplicationProgress)) {
                YT_LOG_DEBUG("Advance replication progress to replica progress from replication card");
                return ExtractReplicationProgress(
                    selfReplica->ReplicationProgress,
                    PivotKey_.Get(),
                    NextPivotKey_.Get());
            }
        }

        auto oldestTimestamp = GetReplicationProgressMinTimestamp(*progress);
        auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);

        YT_LOG_DEBUG("Replica is in pulling mode, consider jumping (ReplicaMode: %v, OldestTimestamp: %v, HistoryItemIndex: %v)",
            ETabletWriteMode::Pull,
            oldestTimestamp,
            historyItemIndex);

        if (historyItemIndex == -1) {
            YT_LOG_WARNING("Invalid replication card: replica history does not cover its progress (ReplicationProgress: %v, Replica: %v, Timestamp: %v)",
                static_cast<TReplicationProgress>(*progress),
                *selfReplica,
                oldestTimestamp);
        } else {
            if (selfReplica->History[historyItemIndex].IsSync()) {
                ++historyItemIndex;
                if (historyItemIndex >= std::ssize(selfReplica->History)) {
                    YT_LOG_DEBUG("Will not advance replication progress to the next era because current history item is the last one (HistoryItemIndex: %v, Replica: %v)",
                        historyItemIndex,
                        *selfReplica);
                    return {};
                }

                YT_LOG_DEBUG("Advance replication progress to next era (Era: %v, Timestamp: %v)",
                    selfReplica->History[historyItemIndex].Era,
                    selfReplica->History[historyItemIndex].Timestamp);
                return AdvanceReplicationProgress(
                    *progress,
                    selfReplica->History[historyItemIndex].Timestamp);
            }
        }

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
