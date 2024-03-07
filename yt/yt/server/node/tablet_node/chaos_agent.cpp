#include "chaos_agent.h"

#include "private.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/transaction_client/helpers.h>

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
        NNative::IConnectionPtr localConnection)
        : Tablet_(tablet)
        , Slot_(std::move(slot))
        , MountConfig_(tablet->GetSettings().MountConfig)
        , ReplicationCardId_(replicationCardId)
        , Connection_(std::move(localConnection))
        , Logger(TabletNodeLogger
            .WithTag("%v, ReplicationCardId: %v",
                tablet->GetLoggingTag(),
                replicationCardId))
    { }

    void Enable() override
    {
        YT_LOG_DEBUG("Starting chaos agent (ReplicationTickPeriod: %v, ReplicationProgressUpdateTickPeriod: %v)",
            MountConfig_->ReplicationTickPeriod,
            MountConfig_->ReplicationProgressUpdateTickPeriod);

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
    }

private:
    TTablet* const Tablet_;
    const ITabletSlotPtr Slot_;
    const TTableMountConfigPtr MountConfig_;
    const TReplicationCardId ReplicationCardId_;
    const NNative::IConnectionPtr Connection_;

    TReplicationCardPtr ReplicationCard_;

    const NLogging::TLogger Logger;

    TFuture<void> FiberFuture_;
    TFuture<void> ProgressReporterFiberFuture_;

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
        UpdateReplicationCard();
        ReconfigureTabletWriteMode();
    }

    void UpdateReplicationCard()
    {
        try {
            YT_LOG_DEBUG("Updating tablet replication card");

            const auto& replicationCardCache = Connection_->GetReplicationCardCache();

            ReplicationCard_ = WaitFor(replicationCardCache->GetReplicationCard({
                    .CardId = ReplicationCardId_,
                    .FetchOptions = {
                        .IncludeProgress = true,
                        .IncludeHistory = true,
                    }
                })).ValueOrThrow();

            Tablet_->RuntimeData()->ReplicationCard.Store(ReplicationCard_);

            YT_LOG_DEBUG("Tablet replication card updated (ReplicationCard: %v)",
                ToString(*ReplicationCard_, {{Tablet_->GetPivotKey(), Tablet_->GetNextPivotKey()}}));
        } catch (std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to update tablet replication card");
        }
    }

    void ReconfigureTabletWriteMode()
    {
        if (!ReplicationCard_) {
            YT_LOG_DEBUG("Replication card is not available");
            return;
        }

        auto* selfReplica = [&] () -> TReplicaInfo* {
            auto* selfReplica = ReplicationCard_->FindReplica(Tablet_->GetUpstreamReplicaId());
            if (!selfReplica) {
                YT_LOG_DEBUG("Could not find self replica in replication card");
                return nullptr;
            }
            if (selfReplica->History.empty()) {
                YT_VERIFY(!IsReplicaEnabled(selfReplica->State));
                YT_LOG_DEBUG("Replica history list is empty");
                return nullptr;
            }
            return selfReplica;
        }();

        if (!selfReplica) {
            Tablet_->RuntimeData()->WriteMode = ETabletWriteMode::Pull;
            Tablet_->RuntimeData()->ReplicationEra = ReplicationCard_->Era;
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

        Tablet_->RuntimeData()->WriteMode = writeMode;
        Tablet_->RuntimeData()->ReplicationEra = ReplicationCard_->Era;

        YT_LOG_DEBUG("Updated tablet write mode (WriteMode: %v, ReplicationEra: %v)",
            writeMode,
            ReplicationCard_->Era);

        if (IsReplicaDisabled(selfReplica->State)) {
            return;
        }

        if (!MountConfig_->EnableReplicationProgressAdvanceToBarrier) {
            return;
        }

        if (writeMode == ETabletWriteMode::Direct) {
            auto currentTimestamp = ReplicationCard_->CurrentTimestamp;
            if (!IsReplicationProgressGreaterOrEqual(*progress, currentTimestamp)) {
                auto newProgress = AdvanceReplicationProgress(
                    *progress,
                    currentTimestamp);
                AdvanceTabletReplicationProgress(
                    Connection_,
                    Logger,
                    Slot_->GetCellId(),
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

        auto client = Connection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));

        auto options = TUpdateChaosTableReplicaProgressOptions{
            .Progress = *progress
        };
        auto future = client->UpdateChaosTableReplicaProgress(
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
    NNative::IConnectionPtr localConnection)
{
    return New<TChaosAgent>(
        tablet,
        std::move(slot),
        replicationCardId,
        std::move(localConnection));
}

////////////////////////////////////////////////////////////////////////////////

bool AdvanceTabletReplicationProgress(
    NNative::IConnectionPtr connection,
    const NLogging::TLogger& Logger,
    TTabletCellId tabletCellId,
    TTabletId tabletId,
    const TReplicationProgress& progress,
    bool validateStrictAdvance,
    std::optional<ui64> replicationRound)
{
    auto localClient = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
    auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
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
