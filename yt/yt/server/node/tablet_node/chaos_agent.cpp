#include "chaos_agent.h"

#include "config.h"
#include "private.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChaosClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NObjectClient;
using namespace NConcurrency;
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
        NNative::IClientPtr localClient)
        : Tablet_(tablet)
        , Slot_(std::move(slot))
        , MountConfig_(tablet->GetSettings().MountConfig)
        , ReplicationCardId_(replicationCardId)
        , LocalClient_(std::move(localClient))
        , Logger(TabletNodeLogger()
            .WithTag("%v, ReplicationCardId: %v",
                tablet->GetLoggingTag(),
                replicationCardId))
        , ConfigurationLock_(New<TAsyncSemaphore>(1))
        , SelfInvoker_(Tablet_->GetEpochAutomatonInvoker())
    { }

    void Enable() override
    {
        YT_LOG_DEBUG("Starting chaos agent (ReplicationTickPeriod: %v, ReplicationProgressUpdateTickPeriod: %v)",
            MountConfig_->ReplicationTickPeriod,
            MountConfig_->ReplicationProgressUpdateTickPeriod);

        SelfInvoker_ = Tablet_->GetEpochAutomatonInvoker();
        FiberFuture_ = BIND(
            &TChaosAgent::FiberMain,
            MakeWeak(this),
            BIND_NO_PROPAGATE(&TChaosAgent::FiberIteration, MakeWeak(this)),
            MountConfig_->ReplicationTickPeriod)
            .AsyncVia(Tablet_->GetEpochAutomatonInvoker())
            .Run();

        ProgressReporterFiberFuture_ = BIND(
            &TChaosAgent::FiberMain,
            MakeWeak(this),
            BIND_NO_PROPAGATE(&TChaosAgent::ReportUpdatedReplicationProgress,
            MakeWeak(this)),
            MountConfig_->ReplicationProgressUpdateTickPeriod)
            .AsyncVia(Tablet_->GetEpochAutomatonInvoker())
            .Run();

        YT_LOG_INFO("Chaos agent fiber started");
    }

    void Disable() override
    {
        if (FiberFuture_) {
            FiberFuture_.Cancel(TError("Chaos agent disabled"));
            YT_LOG_INFO("Chaos agent fiber stopped");
        }
        if (ProgressReporterFiberFuture_) {
            ProgressReporterFiberFuture_.Cancel(TError("Chaos agent progress reporter disabled"));
            YT_LOG_INFO("Chaos agent progress reporter fiber stopped");
        }
        FiberFuture_.Reset();
        ProgressReporterFiberFuture_.Reset();
        SelfInvoker_.Reset();
    }

    TAsyncSemaphoreGuard TryGetConfigLockGuard() override
    {
        return TAsyncSemaphoreGuard::TryAcquire(ConfigurationLock_);
    }

    void ReconfigureTablet() override
    {
        if (auto invoker = SelfInvoker_.Lock()) {
            WaitFor(BIND(&TChaosAgent::ReconfigureTabletWriteMode, MakeWeak(this))
                .AsyncVia(invoker)
                .Run())
            .ThrowOnError();
        }
    }

private:
    TTablet* const Tablet_;
    const ITabletSlotPtr Slot_;
    const TTableMountConfigPtr MountConfig_;
    const TReplicationCardId ReplicationCardId_;
    const NNative::IClientPtr LocalClient_;

    TReplicationCardPtr ReplicationCard_;

    const NLogging::TLogger Logger;

    TFuture<void> FiberFuture_;
    TFuture<void> ProgressReporterFiberFuture_;
    TAsyncSemaphorePtr ConfigurationLock_;
    TWeakPtr<IInvoker> SelfInvoker_;

    void FiberMain(TCallback<void()> callback, TDuration period)
    {
        while (true) {
            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ChaosAgent"));
            NProfiling::TWallTimer timer;
            callback();
            TDelayedExecutor::WaitForDuration(period - timer.GetElapsedTime());
        }
    }

    void FiberIteration()
    {
        UpdateReplicationCardAndReconfigure();
    }

    void UpdateReplicationCardAndReconfigure(TReplicationEra newEra = InvalidReplicationEra)
    {
        UpdateReplicationCard(newEra);

        if (auto guard = TAsyncSemaphoreGuard::TryAcquire(ConfigurationLock_)) {
            try {
                ReconfigureTabletWriteMode();
            } catch (std::exception& ex) {
                auto error = TError(ex)
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("table_path", Tablet_->GetTablePath());
                YT_LOG_ERROR(error, "Failed to reconfigure tablet write mode");
            }
        } else {
            YT_LOG_DEBUG("Skipping reconfiguration because configuration lock is held");
        }
    }

    void UpdateReplicationCard(TReplicationEra newEra = InvalidReplicationEra)
    {
        try {
            YT_LOG_DEBUG("Updating tablet replication card");

            const auto& replicationCardCache = LocalClient_->GetNativeConnection()->GetReplicationCardCache();

            auto key = TReplicationCardCacheKey{
                .CardId = ReplicationCardId_,
                .FetchOptions = {
                    .IncludeProgress = true,
                    .IncludeHistory = true,
                },
            };

            // Replication card can be null on start, so check it before getting the era.
            if (ReplicationCard_ && newEra != InvalidReplicationEra && ReplicationCard_->Era < newEra) {
                key.RefreshEra = newEra;
                YT_LOG_DEBUG("Forcing cached replication card update (OldEra: %v, NewEra: %v)",
                    ReplicationCard_->Era,
                    newEra);
                replicationCardCache->ForceRefresh(key, ReplicationCard_);
            }

            auto replicationCard = WaitFor(replicationCardCache->GetReplicationCard(key))
                .ValueOrThrow();

            if (auto snapshotEra = Tablet_->RuntimeData()->ReplicationEra.load();
                snapshotEra != InvalidReplicationEra && replicationCard->Era < snapshotEra)
            {
                key.RefreshEra = snapshotEra;
                YT_LOG_DEBUG(
                    "Forcing cached replication card update due to outdated copy obtained "
                    "(FetchedEra: %v, SnapshotEra: %v)",
                    replicationCard->Era,
                    snapshotEra);

                replicationCardCache->ForceRefresh(key, replicationCard);
                replicationCard = WaitFor(replicationCardCache->GetReplicationCard(key))
                    .ValueOrThrow();

                if (replicationCard->Era < snapshotEra) {
                    YT_LOG_ALERT(
                        "Replication card era is outdated after forced refresh "
                        "(FetchedEra: %v, SnapshotEra: %v)",
                        replicationCard->Era,
                        snapshotEra);
                }
            }

            ReplicationCard_ = std::move(replicationCard);

            Tablet_->RuntimeData()->ReplicationCard.Store(ReplicationCard_);

            YT_LOG_DEBUG("Tablet replication card updated (ReplicationCard: %v)",
                ToString(*ReplicationCard_, {{Tablet_->GetPivotKey(), Tablet_->GetNextPivotKey()}}));
        } catch (std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to update tablet replication card");
        }
    }

    void RefreshEra(TReplicationEra newEra) override
    {
        YT_LOG_DEBUG("Refreshing replication card era (NewEra: %v)",
            newEra);

        WaitFor(BIND_NO_PROPAGATE(&TChaosAgent::UpdateReplicationCardAndReconfigure, MakeWeak(this), newEra)
            .AsyncVia(Tablet_->GetEpochAutomatonInvoker())
            .Run())
        .ThrowOnError();

        YT_LOG_DEBUG("Finished refreshing replication card era (NewEra: %v)",
            newEra);
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

        YT_LOG_DEBUG("Committing replication era advance (NewReplicationEra: %v, OldReplicationEra: %v)",
            newEra,
            snapshotEra
        );

        auto mutation = CreateMutation(Slot_->GetSimpleHydraManager(), req);
        WaitFor(mutation->Commit())
            .ThrowOnError();

        YT_LOG_DEBUG("Replication era advance finished (NewReplicationEra: %v)",
            newEra);
    }

    void ReconfigureTabletWriteMode()
    {
        YT_VERIFY(ConfigurationLock_->GetFree() == 0);

        auto replicationCard = ReplicationCard_;
        if (!replicationCard) {
            YT_LOG_DEBUG("Replication card is not available");
            return;
        }

        auto* selfReplica = [&] () -> TReplicaInfo* {
            auto* selfReplica = replicationCard->FindReplica(Tablet_->GetUpstreamReplicaId());
            if (!selfReplica) {
                YT_LOG_DEBUG("Could not find self replica in replication card");
                return nullptr;
            }
            if (selfReplica->History.empty()) {
                YT_VERIFY(!IsReplicaEnabled(selfReplica->State));
                YT_LOG_DEBUG("Replica history list is empty");
                return nullptr;
            }

            const auto& localClusterName = LocalClient_->GetNativeConnection()->GetClusterName().value();
            if (!IsReplicaLocationValid(selfReplica, Tablet_->GetTablePath(), localClusterName)) {
                YT_LOG_DEBUG("Upstream replica id corresponds to another table (TablePath: %v, ExpectedPath: %v, TableCluster: %v, ExpectedCluster: %v)",
                    Tablet_->GetTablePath(),
                    selfReplica->ReplicaPath,
                    localClusterName,
                    selfReplica->ClusterName);
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

        YT_LOG_DEBUG("Checking self write mode (ReplicationProgress: %v, LastHistoryItemTimestamp: %v, IsProgressGreaterThanTimestamp: %v)",
            static_cast<TReplicationProgress>(*progress),
            lastHistoryItem.Timestamp,
            isProgressGreaterThanTimestamp);

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

        YT_LOG_DEBUG("Updated tablet write mode (WriteMode: %v, ReplicationEra: %v)",
            writeMode,
            replicationCard->Era);

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
                    Slot_->GetCellId(),
                    Slot_->GetOptions()->ClockClusterTag,
                    Tablet_->GetId(),
                    std::move(newProgress));

                YT_LOG_DEBUG("Advanced replication progress to replication card current timestamp (CurrentTimestamp: %v)",
                    currentTimestamp);
            }
        }
    }

    void ReportUpdatedReplicationProgress()
    {
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

        auto options = TUpdateChaosTableReplicaProgressOptions{
            .Progress = *progress
        };
        auto future = LocalClient_->UpdateChaosTableReplicaProgress(
            Tablet_->GetUpstreamReplicaId(),
            options);
        auto resultOrError = WaitFor(future);

        if (resultOrError.IsOK()) {
            YT_LOG_DEBUG("Replication progress updated successfully (ReplicationProgress: %v)",
                options.Progress);
        } else {
            YT_LOG_ERROR(resultOrError, "Failed to update replication progress");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosAgentPtr CreateChaosAgent(
    TTablet* tablet,
    ITabletSlotPtr slot,
    TReplicationCardId replicationCardId,
    NNative::IClientPtr localClient)
{
    return New<TChaosAgent>(
        tablet,
        std::move(slot),
        replicationCardId,
        std::move(localClient));
}

////////////////////////////////////////////////////////////////////////////////

bool AdvanceTabletReplicationProgress(
    const NNative::IClientPtr& localClient,
    const NLogging::TLogger& Logger,
    TTabletCellId tabletCellId,
    NApi::TClusterTag clockClusterTag,
    TTabletId tabletId,
    const TReplicationProgress& progress,
    bool validateStrictAdvance,
    std::optional<ui64> replicationRound)
{
    TTransactionStartOptions startOptions;
    startOptions.ClockClusterTag = clockClusterTag;
    auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet, startOptions))
        .ValueOrThrow();

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

    YT_LOG_DEBUG("Committing replication progress advance transaction (TransactionId: %v, ReplicationProgress: %v, ReplicationRound: %v)",
        localTransaction->GetId(),
        progress,
        replicationRound);

    // TODO(savrus) Discard 2PC.
    TTransactionCommitOptions commitOptions;
    commitOptions.CoordinatorCellId = tabletCellId;
    commitOptions.Force2PC = true;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
    auto result = WaitFor(localTransaction->Commit(commitOptions));

    YT_LOG_DEBUG(result, "Replication progress advance transaction finished (TransactionId: %v, ReplicationProgress: %v)",
        localTransaction->GetId(),
        progress);

    return result.IsOK();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
