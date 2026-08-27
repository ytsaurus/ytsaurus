#include "chaos_agent.h"

#include "private.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chaos_client/replication_card_updates_batcher.h>

#include <yt/yt/ytlib/api/native/chaos_helpers.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TChaosAgent
    : public IChaosAgent
{
public:
    TChaosAgent(
        TTablet* tablet,
        ITabletSlotPtr slot,
        TReplicationCardId replicationCardId,
        NNative::IClientPtr localClient,
        IReplicationCardUpdatesBatcherPtr replicationCardUpdatesBatcher)
        : Tablet_(tablet)
        , Slot_(std::move(slot))
        , MountConfig_(tablet->GetSettings().MountConfig)
        , ReplicationCardId_(replicationCardId)
        , LocalClient_(std::move(localClient))
        , ReplicationCardUpdatesBatcher_(std::move(replicationCardUpdatesBatcher))
        , Logger(TabletNodeLogger()
            .WithTags(tablet->GetLoggingTags())
            .WithTag("ReplicationCardId", replicationCardId))
        , ConfigurationLock_(New<TAsyncSemaphore>(1))
        , SelfInvoker_(Tablet_->GetEpochAutomatonInvoker())
    { }

    void Enable() override
    {
        const auto& epochAutomatonInvoker = Tablet_->GetEpochAutomatonInvoker();
        SelfInvoker_.Store(epochAutomatonInvoker);
        UpdateReplicationCardExecutor_ = New<TPeriodicExecutor>(
            epochAutomatonInvoker,
            BIND(&TChaosAgent::OnUpdateReplicationCardTick, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = MountConfig_->ReplicationTickPeriod,
                .DelayMode = EPeriodicExecutorDelayMode::FromPreviousStart,
            });
        UpdateReplicationCardExecutor_->Start();

        ReplicationProgressExecutor_ = New<TPeriodicExecutor>(
            epochAutomatonInvoker,
            BIND(&TChaosAgent::OnReplicationProgressTick, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = MountConfig_->ReplicationProgressUpdateTickPeriod,
                .DelayMode = EPeriodicExecutorDelayMode::FromPreviousStart,
            });
        ReplicationProgressExecutor_->Start();

        YT_TLOG_INFO("Chaos agent enabled")
            .With("ReplicationTickPeriod", MountConfig_->ReplicationTickPeriod)
            .With("ReplicationProgressUpdateTickPeriod", MountConfig_->ReplicationProgressUpdateTickPeriod);
    }

    void Disable() override
    {
        if (auto executor = std::exchange(UpdateReplicationCardExecutor_, nullptr)) {
            YT_UNUSED_FUTURE(executor->Stop());
            YT_TLOG_INFO("Chaos agent fiber stopped");
        }
        if (auto executor = std::exchange(ReplicationProgressExecutor_, nullptr)) {
            YT_UNUSED_FUTURE(executor->Stop());
            YT_TLOG_INFO("Chaos agent progress reporter fiber stopped");
        }
        SelfInvoker_.Store(nullptr);

        YT_TLOG_INFO("Chaos agent disabled");
    }

    TAsyncSemaphoreGuard TryGetConfigLockGuard() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return TAsyncSemaphoreGuard::TryAcquire(ConfigurationLock_);
    }

    void ReconfigureTablet() override
    {
        if (auto invoker = SelfInvoker_.Read(&TWeakPtr<IInvoker>::Lock)) {
            WaitFor(BIND(&TChaosAgent::ReconfigureTabletWriteMode, MakeWeak(this))
                .AsyncVia(invoker)
                .Run())
            .ThrowOnError();
        }
    }

    TFuture<void> GetFutureEra(
        TReplicationEra currentEra,
        const TTabletSnapshotPtr& tabletSnapshot) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto snapshotEra = tabletSnapshot->TabletRuntimeData->ReplicationEra.load();
        if (currentEra < snapshotEra) {
            return OKFuture;
        }

        return UpdateEraPromise_.Read([currentEra, tabletSnapshot] (const TPromise<void>& promise) {
            auto snapshotEra = tabletSnapshot->TabletRuntimeData->ReplicationEra.load();
            if (currentEra < snapshotEra) {
                return OKFuture;
            }

            return promise.ToFuture();
        });
    }

private:
    TTablet* const Tablet_;
    const ITabletSlotPtr Slot_;
    const TTableMountConfigPtr MountConfig_;
    const TReplicationCardId ReplicationCardId_;
    const NNative::IClientPtr LocalClient_;
    // Nullable.
    const IReplicationCardUpdatesBatcherPtr ReplicationCardUpdatesBatcher_;

    const NLogging::TLogger Logger;

    TReplicationCardPtr ReplicationCard_;
    bool ReplicationCardReconfigured_ = false;

    TPeriodicExecutorPtr UpdateReplicationCardExecutor_;
    TPeriodicExecutorPtr ReplicationProgressExecutor_;
    TAsyncSemaphorePtr ConfigurationLock_;
    TAtomicObject<TWeakPtr<IInvoker>> SelfInvoker_;

    TAtomicObject<TFuture<void>> RefreshEraFuture_;
    TAtomicObject<TPromise<void>> UpdateEraPromise_ = NewPromise<void>();

    void OnUpdateReplicationCardTick()
    {
        TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ChaosAgent"));

        if (!Tablet_->IsActiveServant()) {
            return;
        }

        UpdateReplicationCardAndReconfigure();
    }

    void UpdateReplicationCardAndReconfigure(TReplicationEra newEra = InvalidReplicationEra)
    {
        UpdateReplicationCard(newEra);

        if (ReplicationCardReconfigured_) {
            return;
        }

        if (auto guard = TAsyncSemaphoreGuard::TryAcquire(ConfigurationLock_)) {
            try {
                ReconfigureTabletWriteMode();
            } catch (std::exception& ex) {
                auto error = TError(ex)
                    .With("tablet_id", Tablet_->GetId())
                    .With("table_path", Tablet_->GetTablePath());
                YT_TLOG_ERROR("Failed to reconfigure tablet write mode")
                    .With(error);
            }
        } else {
            YT_TLOG_DEBUG("Skipping reconfiguration because configuration lock is held");
        }
    }

    void UpdateReplicationCard(TReplicationEra newEra = InvalidReplicationEra)
    {
        try {
            YT_TLOG_DEBUG("Updating tablet replication card");

            const auto& replicationCardCache = LocalClient_->GetNativeConnection()->GetReplicationCardCache();

            auto key = TReplicationCardCacheKey{
                .CardId = ReplicationCardId_,
                .FetchOptions = {
                    .IncludeProgress = true,
                    .IncludeHistory = true,
                },
            };

            auto watchedKey = TReplicationCardCacheKey{
                .CardId = ReplicationCardId_,
                .FetchOptions = MinimalFetchOptions,
            };

            TReplicationCardPtr replicationCard;

            auto snapshotEra = Tablet_->RuntimeData()->ReplicationEra.load();
            if (snapshotEra == InvalidReplicationEra) {
                YT_TLOG_DEBUG("Getting replication card synchronously");

                replicationCard = NNative::GetSyncReplicationCard(
                    LocalClient_->GetNativeConnection(),
                    ReplicationCardId_);
            } else {
                auto futureWatchedReplicationCard = replicationCardCache->GetReplicationCard(watchedKey);

                replicationCard = WaitFor(replicationCardCache->GetReplicationCard(key))
                    .ValueOrThrow();

                auto watchedReplicationCard = WaitForFast(futureWatchedReplicationCard)
                    .ValueOrThrow();

                auto maxEra = std::max(snapshotEra, watchedReplicationCard->Era);
                if (newEra != InvalidReplicationEra) {
                    maxEra = std::max(maxEra, newEra);
                }

                if (replicationCard->Era < maxEra) {
                    key.RefreshEra = maxEra;
                    YT_TLOG_DEBUG("Forcing cached replication card update due to outdated copy obtained")
                        .With("FetchedEra", replicationCard->Era)
                        .With("SnapshotEra", snapshotEra)
                        .With("WatchedEra", watchedReplicationCard->Era)
                        .With("NewEra", newEra);

                    const auto& config = LocalClient_->GetNativeConnection()->GetStaticConfig();
                    int retriesCount = config->TableMountCache->OnErrorRetryCount;
                    for (int retryCount = 0; retryCount < retriesCount; ++retryCount) {
                        replicationCardCache->ForceRefresh(key, replicationCard);
                        auto replicationCardOrError = WaitFor(replicationCardCache->GetReplicationCard(key));

                        if (!replicationCardOrError.IsOK()) {
                            YT_TLOG_DEBUG("Failed to get replication card")
                                .With("Attempt", retryCount)
                                .With(replicationCardOrError);

                            continue;
                        }

                        replicationCard = replicationCardOrError.Value();
                        if (replicationCard->Era >= maxEra) {
                            break;
                        }

                        // Some other thread might be updating cache to the previous era, so it can happen.
                        YT_TLOG_DEBUG("Replication card era is outdated after forced refresh")
                            .With("FetchedEra", replicationCard->Era)
                            .With("SnapshotEra", snapshotEra)
                            .With("WatchedEra", watchedReplicationCard->Era)
                            .With("NewEra", newEra)
                            .With("Attempt", retryCount);
                    }
                }

                if (replicationCard->Era < snapshotEra) {
                    YT_TLOG_DEBUG("Replication card era is outdated after retries, skipping update")
                        .With("FetchedEra", replicationCard->Era)
                        .With("SnapshotEra", snapshotEra);
                    return;
                }
            }

            // Check if the replication card has changed during update, or we are looking at an old instance.
            if (ReplicationCard_.Get() != replicationCard.Get()) {
                ReplicationCard_ = std::move(replicationCard);

                Tablet_->RuntimeData()->ReplicationCard.Store(ReplicationCard_);
                ReplicationCardReconfigured_ = false;
            }

            YT_TLOG_DEBUG("Tablet replication card updated")
                .With("ReplicationCard", ToString(*ReplicationCard_, {{Tablet_->GetPivotKey(), Tablet_->GetNextPivotKey()}}))
                .With("ReplicationCardReconfigured", ReplicationCardReconfigured_);
        } catch (std::exception& ex) {
            YT_TLOG_DEBUG("Failed to update tablet replication card")
                .With(ex);
        }
    }

    void RefreshEra(TReplicationEra newEra) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_TLOG_DEBUG("Refreshing replication card era")
            .With("NewEra", newEra);

        auto future = RefreshEraFuture_.Load();
        if (!future || future.IsSet()) {
            future = RefreshEraFuture_.Transform([this, newEra] (auto& futureEra) {
                if (!futureEra || futureEra.IsSet()) {
                    futureEra = BIND(
                        &TChaosAgent::UpdateReplicationCardAndReconfigure,
                        MakeWeak(this),
                        newEra)
                        .AsyncVia(Tablet_->GetEpochAutomatonInvoker())
                        .Run();
                }

                return futureEra;
            });
        }

        WaitFor(std::move(future))
            .ThrowOnError();

        YT_TLOG_DEBUG("Finished refreshing replication card era")
            .With("NewEra", newEra);
    }

    void TryAdvanceReplicationEra(TReplicationEra newEra)
    {
        auto snapshotEra = Tablet_->RuntimeData()->ReplicationEra.load();
        if (snapshotEra != InvalidReplicationEra && snapshotEra >= newEra) {
            return;
        }

        NProto::TReqAdvanceReplicationEra req;
        ToProto(req.mutable_tablet_id(), Tablet_->GetId());
        req.set_new_replication_era(newEra);

        YT_TLOG_DEBUG("Committing replication era advance")
            .With("NewReplicationEra", newEra)
            .With("OldReplicationEra", snapshotEra);

        auto mutation = CreateMutation(Slot_->GetSimpleHydraManager(), req);
        WaitFor(mutation->Commit())
            .ThrowOnError();

        if (Tablet_->RuntimeData()->ReplicationEra.load() == newEra) {
            UpdateEraPromise_.Transform([] (TPromise<void>& futureEra) {
                futureEra.Set();
                futureEra = NewPromise<void>();
            });
        }

        YT_TLOG_DEBUG("Replication era advance finished")
            .With("NewReplicationEra", newEra);
    }

    void ReconfigureTabletWriteMode()
    {
        YT_VERIFY(ConfigurationLock_->GetFree() == 0);

        ReplicationCardReconfigured_ = true;

        auto replicationCard = ReplicationCard_;
        if (!replicationCard) {
            YT_TLOG_DEBUG("Replication card is not available");
            return;
        }

        auto* selfReplica = [&] () -> TReplicaInfo* {
            auto* selfReplica = replicationCard->FindReplica(Tablet_->GetUpstreamReplicaId());
            if (!selfReplica) {
                YT_TLOG_DEBUG("Could not find self replica in replication card");
                return nullptr;
            }
            if (selfReplica->History.empty()) {
                YT_VERIFY(!IsReplicaEnabled(selfReplica->State));
                YT_TLOG_DEBUG("Replica history list is empty");
                return nullptr;
            }

            const auto& localClusterName = LocalClient_->GetNativeConnection()->GetClusterName().value();
            if (!IsReplicaLocationValid(selfReplica, Tablet_->GetTablePath(), localClusterName)) {
                YT_TLOG_DEBUG("Upstream replica id corresponds to another table")
                    .With("TablePath", Tablet_->GetTablePath())
                    .With("ExpectedPath", selfReplica->ReplicaPath)
                    .With("TableCluster", localClusterName)
                    .With("ExpectedCluster", selfReplica->ClusterName);
                return nullptr;
            }
            return selfReplica;
        }();

        if (!selfReplica) {
            Tablet_->RuntimeData()->WriteMode = ETabletWriteMode::Pull;
            TryAdvanceReplicationEra(replicationCard->Era);
            return;
        }

        ETabletWriteMode writeMode = ETabletWriteMode::Pull;
        auto progress = Tablet_->RuntimeData()->ReplicationProgress.Acquire();

        const auto& lastHistoryItem = selfReplica->History.back();
        bool isProgressGreaterThanTimestamp =
            IsReplicationProgressGreaterOrEqual(*progress, lastHistoryItem.Timestamp);

        YT_TLOG_DEBUG("Checking self write mode")
            .With("ReplicationProgress", static_cast<TReplicationProgress>(*progress))
            .With("LastHistoryItemTimestamp", lastHistoryItem.Timestamp)
            .With("IsProgressGreaterThanTimestamp", isProgressGreaterThanTimestamp);

        // Mode can be switched from AsyncToSync to SyncToAsync without adding a history record. So while in
        // SyncToAsync mode check that previous mode was actually Sync before changing write mode to Direct
        if (IsReplicaEnabled(selfReplica->State) &&
            (selfReplica->Mode == ETableReplicaMode::Sync ||
                (selfReplica->Mode == ETableReplicaMode::SyncToAsync &&
                lastHistoryItem.Mode == ETableReplicaMode::Sync)) &&
            isProgressGreaterThanTimestamp)
        {
            writeMode = ETabletWriteMode::Direct;
        }

        // Should be updated before era not to race with logic in tablet service.
        Tablet_->RuntimeData()->WriteMode = writeMode;
        // ReplicationCard_ might change during this call so we are using a local reference.
        TryAdvanceReplicationEra(replicationCard->Era);

        YT_TLOG_DEBUG("Updated tablet write mode")
            .With("WriteMode", writeMode)
            .With("ReplicationEra", replicationCard->Era);

        if (IsReplicaDisabled(selfReplica->State)) {
            return;
        }

        if (!MountConfig_->EnableReplicationProgressAdvanceToBarrier) {
            return;
        }

        if (writeMode == ETabletWriteMode::Direct) {
            auto currentTimestamp = replicationCard->CurrentTimestamp;
            if (!IsReplicationProgressGreaterOrEqual(*progress, currentTimestamp)) {
                auto newProgress = AdvanceReplicationProgress(
                    *progress,
                    currentTimestamp);

                AdvanceTabletReplicationProgress(
                    LocalClient_,
                    Logger,
                    Slot_->GetTabletManager(),
                    Slot_->GetCellId(),
                    Slot_->GetOptions()->ClockClusterTag,
                    Tablet_,
                    std::move(newProgress));

                YT_TLOG_DEBUG("Advanced replication progress to replication card current timestamp")
                    .With("CurrentTimestamp", currentTimestamp);
            }
        }
    }

    void OnReplicationProgressTick()
    {
        TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ChaosAgent"));

        if (!Tablet_->IsActiveServant()) {
            return;
        }

        auto progress = Tablet_->RuntimeData()->ReplicationProgress.Acquire();
        auto* counters = Tablet_->GetTableProfiler()->GetTablePullerCounters();
        if (Tablet_->RuntimeData()->WriteMode == ETabletWriteMode::Direct) {
            counters->LagTime.Update(TDuration::Zero());
        } else {
            auto now = NProfiling::GetInstant();
            auto minTimestamp = TimestampToInstant(GetReplicationProgressMinTimestamp(*progress)).first;
            auto time = now > minTimestamp ? now - minTimestamp : TDuration::Zero();
            counters->LagTime.Update(time);
        }

        if (!ReplicationCardUpdatesBatcher_ || !ReplicationCardUpdatesBatcher_->Enabled()) {
            auto options = TUpdateChaosTableReplicaProgressOptions{
                .Progress = *progress,
            };
            auto future = LocalClient_->UpdateChaosTableReplicaProgress(
                Tablet_->GetUpstreamReplicaId(),
                options);
            auto resultOrError = WaitFor(future);

            if (resultOrError.IsOK()) {
                YT_TLOG_DEBUG("Replication progress updated successfully")
                    .With("ReplicationProgress", options.Progress);
            } else {
                YT_TLOG_ERROR("Failed to update replication progress")
                    .With(resultOrError);
            }
        } else {
            YT_TLOG_DEBUG("Updating replication progress with batching")
                .With("ReplicationProgressUpdate", *progress);

            auto future = ReplicationCardUpdatesBatcher_->AddTabletProgressUpdate(
                Tablet_->GetReplicationCardId(),
                Tablet_->GetUpstreamReplicaId(),
                *progress);

            auto resultOrError = WaitFor(std::move(future));
            if (resultOrError.IsOK()) {
                YT_TLOG_DEBUG("Replication progress updated successfully")
                    .With("ReplicationProgress", *progress);
            } else {
                YT_TLOG_ERROR("Failed to update replication progress")
                    .With(resultOrError);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosAgentPtr CreateChaosAgent(
    TTablet* tablet,
    ITabletSlotPtr slot,
    TReplicationCardId replicationCardId,
    NNative::IClientPtr localClient,
    IReplicationCardUpdatesBatcherPtr replicationCardUpdatesBatcher)
{
    return New<TChaosAgent>(
        tablet,
        std::move(slot),
        replicationCardId,
        std::move(localClient),
        std::move(replicationCardUpdatesBatcher));
}

////////////////////////////////////////////////////////////////////////////////

bool AdvanceTabletReplicationProgress(
    const NNative::IClientPtr& localClient,
    const NLogging::TLogger& Logger,
    const ITabletManagerPtr& tabletManager,
    TTabletCellId tabletCellId,
    NApi::TClusterTag clockClusterTag,
    std::variant<TTabletSnapshotPtr, TTablet*> tablet,
    const TReplicationProgress& progress,
    bool validateStrictAdvance,
    std::optional<ui64> replicationRound)
{
    TTransactionStartOptions startOptions;
    startOptions.ClockClusterTag = clockClusterTag;
    auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet, startOptions))
        .ValueOrThrow();

    auto tabletId = std::holds_alternative<TTabletSnapshotPtr>(tablet)
        ? std::get<TTabletSnapshotPtr>(tablet)->TabletId
        : std::get<TTablet*>(tablet)->GetId();

    std::visit(
        [&] (auto&& tablet) {
            tabletManager->ExternalizeTransactionIfNeeded(
                tablet,
                localTransaction,
                "advance_replication_progress");
        },
        tablet);

    {
        NProto::TReqAdvanceReplicationProgress req;
        ToProto(req.mutable_tablet_id(), tabletId);
        ToProto(req.mutable_new_replication_progress(), progress);
        req.set_validate_strict_advance(validateStrictAdvance);
        if (replicationRound) {
            req.set_replication_round(*replicationRound);
        }
        localTransaction->AddAction(tabletCellId, MakeTransactionActionData(req));
    }

    YT_TLOG_DEBUG("Committing replication progress advance transaction")
        .With("TransactionId", localTransaction->GetId())
        .With("ReplicationProgress", progress)
        .With("ReplicationRound", replicationRound);

    // TODO(savrus) Discard 2PC.
    TTransactionCommitOptions commitOptions;
    commitOptions.CoordinatorCellId = tabletCellId;
    commitOptions.Force2PC = true;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
    auto result = WaitFor(localTransaction->Commit(commitOptions));

    YT_TLOG_DEBUG("Replication progress advance transaction finished")
        .With("TransactionId", localTransaction->GetId())
        .With("ReplicationProgress", progress)
        .With(result);

    return result.IsOK();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
