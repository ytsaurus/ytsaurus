#include "table_puller.h"

#include "alien_cluster_client_cache_base.h"
#include "alien_cluster_client_cache.h"
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

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/tablet_client/config.h>

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

DEFINE_ENUM(EPullerErrorKind,
    (Default)
    (UnableToPickQueueReplica)
);

////////////////////////////////////////////////////////////////////////////////

static const int TabletRowsPerRead = 1000;
static const TString PullerErrorKindAttribute = "puller_error_kind";

////////////////////////////////////////////////////////////////////////////////

class TBannedReplicaTracker
{
public:
    struct TBanInfo
    {
        size_t Counter;
        TError LastError;
    };

    explicit TBannedReplicaTracker(NLogging::TLogger logger, std::optional<size_t> replicaBanDuration)
        : Logger(std::move(logger))
        , ReplicaBanDuration_(replicaBanDuration)
    { }

    bool IsReplicaBanned(TReplicaId replicaId)
    {
        auto it = BannedReplicas_.find(replicaId);
        bool result = it != BannedReplicas_.end() && it->second.Counter > 0;

        YT_LOG_INFO("Banned replica tracker checking replica (ReplicaId: %v, Result: %v)",
            replicaId,
            result);

        return result;
    }

    void BanReplica(TReplicaId replicaId, TError error)
    {
        BannedReplicas_[replicaId] = TBanInfo{ReplicaBanDuration_.value_or(std::size(BannedReplicas_)), std::move(error)};

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
                InsertOrCrash(BannedReplicas_, std::pair(replicaId, TBanInfo{0, TError()}));
            }
        }

        DecreaseCounters();
    }

    const THashMap<TReplicaId, TBanInfo>& GetBannedReplicas() const
    {
        return BannedReplicas_;
    }

private:
    const NLogging::TLogger Logger;
    const std::optional<size_t> ReplicaBanDuration_;

    THashMap<TReplicaId, TBanInfo> BannedReplicas_;

    void DecreaseCounters()
    {
        for (auto& [_, info] : BannedReplicas_) {
            if (info.Counter > 0) {
                --info.Counter;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPerFiberClusterClientCache
    : public TAlienClusterClientCacheBase
{
public:
    explicit TPerFiberClusterClientCache(
        IAlienClusterClientCachePtr underlying)
        : TAlienClusterClientCacheBase(underlying->GetEvictionPeriod())
        , Underlying_(std::move(underlying))
    { }

    const NNative::IClientPtr& GetClient(const std::string& clusterName)
    {
        CheckAndRemoveExpired(TInstant::Now(), false);

        auto entryIt = CachedClients_.find(clusterName);
        if (entryIt == CachedClients_.end()) {
            if (auto client = Underlying_->GetClient(clusterName)) {
                return EmplaceOrCrash(CachedClients_, clusterName, std::move(client))->second;
            }

            return NullClient;
        }

        if (entryIt->second->GetConnection()->IsTerminated()) {
            if (entryIt->second = Underlying_->GetClient(clusterName); !entryIt->second) {
                CachedClients_.erase(entryIt);
                return NullClient;
            }
        }

        return entryIt->second;
    }

    const NNative::IClientPtr& GetLocalClient() const
    {
        return Underlying_->GetLocalClient();
    }

private:
    inline static const NNative::IClientPtr NullClient = nullptr;
    const IAlienClusterClientCachePtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

class TTablePuller
    : public ITablePuller
{
public:
    TTablePuller(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        IAlienClusterClientCachePtr replicatorClientCache,
        ITabletSlotPtr slot,
        ITabletSnapshotStorePtr tabletSnapshotStore,
        IInvokerPtr workerInvoker,
        IThroughputThrottlerPtr nodeInThrottler,
        IMemoryUsageTrackerPtr memoryTracker)
        : Config_(std::move(config))
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
        , Logger(TabletNodeLogger()
            .WithTag("%v, UpstreamReplicaId: %v",
                tablet->GetLoggingTag(),
                ReplicaId_))
        , Throttler_(CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            std::move(nodeInThrottler),
            CreateReconfigurableThroughputThrottler(MountConfig_->ReplicationThrottler, Logger)
        }))
        , MemoryTracker_(std::move(memoryTracker))
        , ChaosAgent_(tablet->GetChaosAgent())
        , BannedReplicaTracker_(Logger, MountConfig_->Testing.TablePullerReplicaBanIterationsCount)
        , LastReplicationProgressAdvance_(*tablet->RuntimeData()->ReplicationProgress.Acquire())
        , ReplicatorClientCache_(std::move(replicatorClientCache))
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
    const IMemoryUsageTrackerPtr MemoryTracker_;

    IChaosAgentPtr ChaosAgent_;
    TBannedReplicaTracker BannedReplicaTracker_;
    ui64 ReplicationRound_ = 0;
    TReplicationProgress LastReplicationProgressAdvance_;
    TInstant NextPermittedTimeForProgressBehindAlert_ = Now();
    TPerFiberClusterClientCache ReplicatorClientCache_;

    TFuture<void> FiberFuture_;

    using TReplicaOrError = TErrorOr<std::tuple<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo*, TTimestamp>>;

    void FiberMain()
    {
        while (true) {
            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("TablePuller"));
            NProfiling::TWallTimer timer;
            FiberIteration();
            TDelayedExecutor::WaitForDuration(MountConfig_->ReplicationTickPeriod - timer.GetElapsedTime());
        }
    }

    void UpdatePullerErrors(TTabletErrors& tabletErrors, TError currentPullError)
    {
        TError combinedError;

        if (!currentPullError.IsOK()) {
            auto kind = currentPullError.Attributes().Get<EPullerErrorKind>(
                PullerErrorKindAttribute,
                EPullerErrorKind::Default);

            if (kind == EPullerErrorKind::UnableToPickQueueReplica) {
                TError aggregatedError = TError("Some queue replicas are banned");

                for (const auto& [replicaId, banInfo] : BannedReplicaTracker_.GetBannedReplicas()) {
                    if (banInfo.Counter > 0) {
                        aggregatedError = aggregatedError
                            << (TError(banInfo.LastError)
                                << TErrorAttribute("replica_id", replicaId));
                    }
                }

                currentPullError = currentPullError
                    << aggregatedError;
            }

            combinedError = TError("Pull iteration failed")
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Pull);


            combinedError = combinedError
                << currentPullError;
        }

        tabletErrors.BackgroundErrors[ETabletBackgroundActivity::Pull].Store(combinedError);
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

            auto configGuard = ChaosAgent_->TryGetConfigLockGuard();
            if (!configGuard) {
                YT_LOG_DEBUG("Tablet is being reconfigured right now, skipping replication iteration");
                return;
            }

            ChaosAgent_->ReconfigureTablet();

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

            auto localConnection = ReplicatorClientCache_.GetLocalClient()->GetNativeConnection();
            const auto& clusterName = localConnection->GetClusterName().value();
            if (!IsReplicaLocationValid(selfReplica, tabletSnapshot->TablePath, clusterName)) {
                THROW_ERROR_EXCEPTION("Upstream replica id corresponds to another table")
                    << TErrorAttribute("upstream_replica_id", tabletSnapshot->UpstreamReplicaId)
                    << TErrorAttribute("table_path", tabletSnapshot->TablePath)
                    << TErrorAttribute("expected_path", selfReplica->ReplicaPath)
                    << TErrorAttribute("table_cluster", clusterName)
                    << TErrorAttribute("expected_cluster", selfReplica->ClusterName)
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
                UpdatePullerErrors(tabletSnapshot->TabletRuntimeData->Errors, TError());
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
                    ReplicatorClientCache_.GetLocalClient(),
                    Logger,
                    Slot_->GetCellId(),
                    Slot_->GetOptions()->ClockClusterTag,
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

            UpdatePullerErrors(tabletSnapshot->TabletRuntimeData->Errors, TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_ERROR(error, "Error pulling rows, backing off");
            if (tabletSnapshot) {
                UpdatePullerErrors(tabletSnapshot->TabletRuntimeData->Errors, error);
            }

            if (error.Attributes().Get<bool>("hard", false)) {
                DoHardBackoff(error);
            } else {
                DoSoftBackoff(error);
            }
        }
    }

    TReplicaOrError PickQueueReplica(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TReplicationCardPtr& replicationCard,
        const TRefCountedReplicationProgressPtr& replicationProgress)
    {
        // If our progress is less than any queue replica progress, pull from that replica.
        // Otherwise pull from sync replica of oldest era corresponding to our progress.

        YT_LOG_DEBUG("Pick replica to pull from");

        auto* selfReplica = replicationCard->FindReplica(tabletSnapshot->UpstreamReplicaId);
        if (!selfReplica) {
            return TError("Will not pull rows since replication card does not contain us");
        }

        if (!IsReplicationProgressGreaterOrEqual(*replicationProgress, selfReplica->ReplicationProgress)) {
            constexpr auto message = "Will not pull rows since actual replication progress is behind replication card replica progress";

            // TODO(ponasenko-rs): Remove alerts after testing period.
            if (Now() >= NextPermittedTimeForProgressBehindAlert_) {
                YT_LOG_ALERT("%s (ReplicationProgress: %v, ReplicaInfo: %v)",
                    message,
                    static_cast<TReplicationProgress>(*replicationProgress),
                    *selfReplica);
                NextPermittedTimeForProgressBehindAlert_ = Now() + TDuration::Days(1);
            }

            return TError(message)
                << TErrorAttribute("replication_progress", static_cast<TReplicationProgress>(*replicationProgress))
                << TErrorAttribute("replica_info", *selfReplica);
        }

        auto oldestTimestamp = GetReplicationProgressMinTimestamp(*replicationProgress);
        auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);
        if (historyItemIndex == -1) {
            return TError("Will not pull rows since replica history does not cover replication progress")
                << TErrorAttribute("oldest_timestamp", oldestTimestamp)
                << TErrorAttribute("history", selfReplica->History);
        }

        YT_VERIFY(historyItemIndex >= 0 && historyItemIndex < std::ssize(selfReplica->History));
        const auto& historyItem = selfReplica->History[historyItemIndex];
        if (historyItem.IsSync()) {
            return TError("Will not pull rows since oldest progress timestamp corresponds to sync history item")
                << TErrorAttribute("oldest_timestamp", oldestTimestamp)
                << TErrorAttribute("history_item", historyItem);
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
                } else if (IsReplicaReallySync(selfReplica->Mode, selfReplica->State, selfReplica->History)) {
                    upperTimestamp = selfReplica->History.back().Timestamp;
                }

                return {replicaId, &replicaInfo, upperTimestamp};
            }

            return {};
        };

        if (auto [queueReplicaId, queueReplica] = findFreshQueueReplica(); queueReplica) {
            YT_LOG_DEBUG("Pull rows from fresh replica (ReplicaId: %v)",
                queueReplicaId);
            return std::tuple{queueReplicaId, queueReplica, NullTimestamp};
        }

        if (auto [queueReplicaId, queueReplicaInfo, upperTimestamp] = findSyncQueueReplica(); queueReplicaInfo) {
            YT_LOG_DEBUG("Pull rows from sync replica (ReplicaId: %v, OldestTimestamp: %v, UpperTimestamp: %v)",
                queueReplicaId,
                oldestTimestamp,
                upperTimestamp);
            return std::tuple{queueReplicaId, queueReplicaInfo, upperTimestamp};
        }

        return TError("Will not pull rows since no in-sync queue found");
    }

    void DoPullRows(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TReplicationCardPtr& replicationCard,
        const TReplicaInfo* selfReplica,
        const TRefCountedReplicationProgressPtr& replicationProgress)
    {
        const auto& tableProfiler = tabletSnapshot->TableProfiler;
        auto* counters = tableProfiler->GetTablePullerCounters();

        if (MemoryTracker_->IsExceeded()) {
            YT_LOG_DEBUG("Skipping pull rows iteration due to puller memory limit exceeded (MemoryLimit: %v)",
                MemoryTracker_->GetLimit());
            THROW_ERROR_EXCEPTION("Skipping pull rows iteration due to puller memory limit exceeded")
                << TErrorAttribute("memory_limit", MemoryTracker_->GetLimit());
        }

        auto reservingTracker = CreateReservingMemoryUsageTracker(MemoryTracker_, counters->MemoryUsage);
        TDuration throttleTime;
        {
            auto throttleFuture = Throttler_->Throttle(1);
            if (throttleFuture.IsSet()) {
                throttleFuture.Get().ThrowOnError();
            } else {
                auto timerGuard = TEventTimerGuard(counters->ThrottleTime);
                YT_LOG_DEBUG("Started waiting for replication throttling");

                WaitFor(throttleFuture)
                    .ThrowOnError();

                YT_LOG_DEBUG("Finished waiting for replication throttling");
                throttleTime = timerGuard.GetElapsedTime();
            }
        }

        auto queueReplicaOrError = PickQueueReplica(tabletSnapshot, replicationCard, replicationProgress);
        if (!queueReplicaOrError.IsOK()) {
            // This form of logging accepts only string literals.
            YT_LOG_DEBUG(queueReplicaOrError, "Unable to pick a queue replica to replicate from");
            THROW_ERROR_EXCEPTION("Unable to pick a queue replica to replicate from")
                << queueReplicaOrError
                << TErrorAttribute(PullerErrorKindAttribute, EPullerErrorKind::UnableToPickQueueReplica);
        }

        auto [queueReplicaId, queueReplicaInfo, upperTimestamp] = queueReplicaOrError
            .ValueOrThrow();
        YT_VERIFY(queueReplicaId);
        YT_VERIFY(queueReplicaInfo);

        try {
            const auto& clusterName = queueReplicaInfo->ClusterName;
            const auto& replicaPath = queueReplicaInfo->ReplicaPath;
            TPullRowsResult result;
            {
                TEventTimerGuard timerGuard(counters->PullRowsTime);

                const auto& alienClient = ReplicatorClientCache_.GetClient(clusterName);
                if (!alienClient) {
                    THROW_ERROR_EXCEPTION("Queue replica cluster %Qv is not known", clusterName)
                        << HardErrorAttribute;
                }

                TPullRowsOptions options;
                options.TabletRowsPerRead = TabletRowsPerRead;
                options.ReplicationProgress = *replicationProgress;
                options.StartReplicationRowIndexes = tabletSnapshot->TabletChaosData->CurrentReplicationRowIndexes.Load();
                options.UpperTimestamp = upperTimestamp;
                options.UpstreamReplicaId = queueReplicaId;
                options.OrderRowsByTimestamp = selfReplica->ContentType == ETableReplicaContentType::Queue;
                options.TableSchema = TableSchema_;
                options.MemoryTracker = reservingTracker;

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
                UpdatePullerErrors(tabletSnapshot->TabletRuntimeData->Errors, TError());
                return;
            }

            {
                TEventTimerGuard timerGuard(counters->WriteTime);

                TTransactionStartOptions startOptions;
                startOptions.ClockClusterTag = Slot_->GetOptions()->ClockClusterTag;
                const auto& localClient = ReplicatorClientCache_.GetLocalClient();
                auto localTransaction = WaitFor(
                    localClient->StartNativeTransaction(ETransactionType::Tablet, startOptions))
                    .ValueOrThrow();

                // Set options to avoid nested writes to other replicas.
                TModifyRowsOptions modifyOptions;
                modifyOptions.ReplicationCard = replicationCard;
                modifyOptions.UpstreamReplicaId = tabletSnapshot->UpstreamReplicaId;
                modifyOptions.TopmostTransaction = false;
                modifyOptions.AllowMissingKeyColumns = true;

                std::vector<TRowModification> rowModifications;

                auto memoryGuard = TMemoryUsageTrackerGuard::Acquire(
                    reservingTracker,
                    resultRows.size() * sizeof(TRowModification));
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
        } catch (const std::exception& ex) {
            BannedReplicaTracker_.BanReplica(queueReplicaId, TError(ex));
            counters->ErrorCount.Increment();
            throw;
        }
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
    IAlienClusterClientCachePtr replicatorClientCache,
    ITabletSlotPtr slot,
    ITabletSnapshotStorePtr tabletSnapshotStore,
    IInvokerPtr workerInvoker,
    IThroughputThrottlerPtr nodeInThrottler,
    IMemoryUsageTrackerPtr memoryTracker)
{
    return New<TTablePuller>(
        std::move(config),
        tablet,
        std::move(replicatorClientCache),
        std::move(slot),
        std::move(tabletSnapshotStore),
        std::move(workerInvoker),
        std::move(nodeInThrottler),
        std::move(memoryTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
