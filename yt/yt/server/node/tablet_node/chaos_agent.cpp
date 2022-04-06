#include "chaos_agent.h"

#include "private.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

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
        , LastReplicationProgressAdvance_(*Tablet_->RuntimeData()->ReplicationProgress.Load())
        , Logger(TabletNodeLogger
            .WithTag("%v, ReplicationCardId: %v",
                tablet->GetLoggingTag(),
                replicationCardId))
    { }

    void Enable() override
    {
        FiberFuture_ = BIND(&TChaosAgent::FiberMain, MakeWeak(this), BIND(&TChaosAgent::FiberIteration, MakeWeak(this)))
            .AsyncVia(Tablet_->GetEpochAutomatonInvoker())
            .Run();

        ProgressReporterFiberFuture_ = BIND(&TChaosAgent::FiberMain, MakeWeak(this), BIND(&TChaosAgent::ReportUpdatedReplicationProgress, MakeWeak(this)))
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
    TReplicationProgress LastReplicationProgressAdvance_;

    const NLogging::TLogger Logger;

    TFuture<void> FiberFuture_;
    TFuture<void> ProgressReporterFiberFuture_;

    void FiberMain(TCallback<void()> callback)
    {
        while (true) {
            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ChaosAgent"));
            NProfiling::TWallTimer timer;
            callback();
            TDelayedExecutor::WaitForDuration(MountConfig_->ReplicationTickPeriod - timer.GetElapsedTime());
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
                *ReplicationCard_);
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
                YT_VERIFY(selfReplica->State != ETableReplicaState::Enabled);
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
        auto progress = Tablet_->RuntimeData()->ReplicationProgress.Load();

        if (!IsReplicationProgressGreaterOrEqual(*progress, LastReplicationProgressAdvance_)) {
            YT_LOG_DEBUG("Skipping chaos agent iteration because last progress advance is not there yet "
                "(TabletReplicationProgress: %v, LastReplicationProgress: %v)",
                static_cast<TReplicationProgress>(*progress),
                LastReplicationProgressAdvance_);
            return;
        }

        YT_LOG_DEBUG("Checking self write mode (ReplicationProgress: %v, LastHistoryItemTimestamp: %llx, IsProgressGreaterThanTimestamp: %llx)",
            static_cast<TReplicationProgress>(*progress),
            selfReplica->History.back().Timestamp,
            IsReplicationProgressGreaterOrEqual(*progress, selfReplica->History.back().Timestamp));

        if (IsReplicaReallySync(selfReplica->Mode, selfReplica->State) &&
            IsReplicationProgressGreaterOrEqual(*progress, selfReplica->History.back().Timestamp))
        {
            writeMode = ETabletWriteMode::Direct;
        }

        Tablet_->RuntimeData()->WriteMode = writeMode;
        Tablet_->RuntimeData()->ReplicationEra = ReplicationCard_->Era;

        YT_LOG_DEBUG("Updated tablet write mode (WriteMode: %v, ReplicationEra: %v)",
            writeMode,
            ReplicationCard_->Era);

        if (selfReplica->State == ETableReplicaState::Disabled) {
            return;
        }

        auto initialReplicationProgressAdvance = [&] {
            if (progress->Segments.size() != 1 || progress->Segments[0].Timestamp != MinTimestamp) {
                return;
            }

            auto historyTimestamp = selfReplica->History[0].Timestamp;
            auto progressTimestamp = GetReplicationProgressMinTimestamp(
                selfReplica->ReplicationProgress,
                Tablet_->GetPivotKey().Get(),
                Tablet_->GetNextPivotKey().Get());

            YT_LOG_DEBUG("Checking that replica has been added in non-catchup mode (ReplicationCardMinProgressTimestamp: %llx, HistoryMinTimestamp: %llx)",
                progressTimestamp,
                historyTimestamp);

            if (progressTimestamp == historyTimestamp && progressTimestamp != MinTimestamp) {
                YT_LOG_DEBUG("Advance replication progress to first history item. (ReplicationProgress: %v, Replica: %v, Timestamp: %llx)",
                    static_cast<TReplicationProgress>(*progress),
                    selfReplica,
                    historyTimestamp);

                auto newProgress = AdvanceReplicationProgress(
                    *progress,
                    historyTimestamp);
                AdvanceTabletReplicationProgress(std::move(newProgress));
                return;
            }

            YT_LOG_DEBUG("Checking that replication card contains further progress (ReplicationProgress: %v, Replica: %v)",
                static_cast<TReplicationProgress>(*progress),
                selfReplica);

            if (!IsReplicationProgressGreaterOrEqual(*progress, selfReplica->ReplicationProgress)) {
                auto newProgress = ExtractReplicationProgress(
                    selfReplica->ReplicationProgress, 
                    Tablet_->GetPivotKey().Get(),
                    Tablet_->GetNextPivotKey().Get());
                AdvanceTabletReplicationProgress(std::move(newProgress));

                YT_LOG_DEBUG("Advanced replication progress to replica progress from replication card");
            }
        };

        auto forwardReplicationProgress = [&] (int historyItemIndex) {
            if (historyItemIndex >= std::ssize(selfReplica->History)) {
                YT_LOG_DEBUG("Will not advance replication progress to the next era because current history item is the last one (HistoryItemIndex: %v, Replica: %v)",
                    historyItemIndex,
                    *selfReplica);
                return;
            }

            auto newProgress = AdvanceReplicationProgress(
                *progress,
                selfReplica->History[historyItemIndex].Timestamp);
            AdvanceTabletReplicationProgress(std::move(newProgress));

            YT_LOG_DEBUG("Advanced replication progress to next era (Era: %v, Timestamp: %llx)",
                selfReplica->History[historyItemIndex].Era,
                selfReplica->History[historyItemIndex].Timestamp);
        };

        // TODO(savrus): Update write mode after advancing replication progress.

        if (writeMode == ETabletWriteMode::Pull) {
            auto oldestTimestamp = GetReplicationProgressMinTimestamp(*progress);
            auto historyItemIndex = selfReplica->FindHistoryItemIndex(oldestTimestamp);

            initialReplicationProgressAdvance();

            YT_LOG_DEBUG("Rplica is in pulling mode, consider jumping (ReplicaMode: %v, OldestTimestmap: %llx, HistoryItemIndex: %v)",
                ETabletWriteMode::Pull,
                oldestTimestamp,
                historyItemIndex);

            if (historyItemIndex == -1) {
                YT_LOG_WARNING("Invalid replication card: replica history does not cover its progress (ReplicationProgress: %v, Replica: %v, Timestamp: %llx)",
                    static_cast<TReplicationProgress>(*progress),
                    *selfReplica,
                    oldestTimestamp);
            } else {
                const auto& item = selfReplica->History[historyItemIndex];
                if (IsReplicaReallySync(item.Mode, item.State)) {
                    forwardReplicationProgress(historyItemIndex + 1);
                }
            }
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
                AdvanceTabletReplicationProgress(std::move(newProgress), /*validateStrictAdvance*/ false);

                YT_LOG_DEBUG("Advanced replication progres to replication card current timestamp (CurrentTimestamp: %llx)",
                    currentTimestamp);
            }
        }
    }

    void ReportUpdatedReplicationProgress()
    {
        auto client = Connection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));

        auto options = TUpdateChaosTableReplicaProgressOptions{
            .Progress = *Tablet_->RuntimeData()->ReplicationProgress.Load()
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

    void AdvanceTabletReplicationProgress(TReplicationProgress progress, bool validateStrictAdvance = true)
    {
        if (!IsReplicationProgressGreaterOrEqual(progress, LastReplicationProgressAdvance_)) {
            YT_LOG_ALERT("Trying to advance replication progress behind last attempt (LastReplicationProgress: %v, NewReplicationProgress: %v)",
                LastReplicationProgressAdvance_,
                progress);
        }

        auto localClient = Connection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
        auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        {
            NProto::TReqAdvanceReplicationProgress req;
            ToProto(req.mutable_tablet_id(), Tablet_->GetId());
            ToProto(req.mutable_new_replication_progress(), progress);
            req.set_validate_strict_advance(validateStrictAdvance);
            localTransaction->AddAction(Slot_->GetCellId(), MakeTransactionActionData(req));
        }

        YT_LOG_DEBUG("Commiting replication progress advance transaction (TransactionId: %v, ReplicationProgress: %v)",
            localTransaction->GetId(),
            progress);

        // TODO(savrus) Discard 2PC.
        TTransactionCommitOptions commitOptions;
        commitOptions.CoordinatorCellId = Slot_->GetCellId();
        commitOptions.Force2PC = true;
        commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
        auto result = WaitFor(localTransaction->Commit(commitOptions));

        YT_LOG_DEBUG(result, "Replication progress advance transaction finished (TransactionId: %v, ReplicationProgress: %v)",
            localTransaction->GetId(),
            progress);

        if (result.IsOK() && validateStrictAdvance) {
            LastReplicationProgressAdvance_ = std::move(progress);
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

} // namespace NYT::NTabletNode
