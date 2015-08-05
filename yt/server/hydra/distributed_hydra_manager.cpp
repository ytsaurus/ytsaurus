#include "stdafx.h"
#include "distributed_hydra_manager.h"
#include "hydra_manager.h"
#include "private.h"
#include "recovery.h"
#include "decorated_automaton.h"
#include "recovery.h"
#include "changelog.h"
#include "snapshot.h"
#include "config.h"
#include "automaton.h"
#include "lease_tracker.h"
#include "mutation_context.h"
#include "mutation_committer.h"
#include "checkpointer.h"
#include "snapshot_discovery.h"
#include "hydra_service.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/scheduler.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>
#include <core/rpc/response_keeper.h>

#include <core/ytree/fluent.h>

#include <core/logging/log.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

#include <server/election/election_manager.h>

#include <atomic>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

class TDistributedHydraManager;
typedef TIntrusivePtr<TDistributedHydraManager> TDistributedHydraManagerPtr;

class TDistributedHydraManager
    : public THydraServiceBase
    , public IHydraManager
{
public:
    class TElectionCallbacks
        : public IElectionCallbacks
    {
    public:
        explicit TElectionCallbacks(TDistributedHydraManagerPtr owner)
            : Owner_(owner)
            , CancelableControlInvoker_(owner->CancelableControlInvoker_)
        { }

        virtual void OnStartLeading() override
        {
            CancelableControlInvoker_->Invoke(BIND(&TDistributedHydraManager::OnElectionStartLeading, Owner_));
        }

        virtual void OnStopLeading() override
        {
            CancelableControlInvoker_->Invoke(BIND(&TDistributedHydraManager::OnElectionStopLeading, Owner_));
        }

        virtual void OnStartFollowing() override
        {
            CancelableControlInvoker_->Invoke(BIND(&TDistributedHydraManager::OnElectionStartFollowing, Owner_));
        }

        virtual void OnStopFollowing() override
        {
            CancelableControlInvoker_->Invoke(BIND(&TDistributedHydraManager::OnElectionStopFollowing, Owner_));
        }

        virtual TPeerPriority GetPriority() override
        {
            auto owner = Owner_.Lock();
            return owner ? owner->GetElectionPriority() : TPeerPriority();
        }

        virtual Stroka FormatPriority(TPeerPriority priority) override
        {
            auto version = TVersion::FromRevision(priority);
            return ToString(version);
        }

    private:
        const TWeakPtr<TDistributedHydraManager> Owner_;
        const IInvokerPtr CancelableControlInvoker_;

    };

    TDistributedHydraManager(
        TDistributedHydraManagerConfigPtr config,
        IInvokerPtr controlInvoker,
        IInvokerPtr automatonInvoker,
        IAutomatonPtr automaton,
        IServerPtr rpcServer,
        TCellManagerPtr cellManager,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        const TDistributedHydraManagerOptions& options)
        : THydraServiceBase(
            controlInvoker,
            NRpc::TServiceId(THydraServiceProxy::GetServiceName(), cellManager->GetCellId()),
            HydraLogger)
        , Config_(config)
        , RpcServer_(rpcServer)
        , CellManager_(cellManager)
        , ControlInvoker_(controlInvoker)
        , CancelableControlInvoker_(CancelableContext_->CreateInvoker(ControlInvoker_))
        , AutomatonInvoker_(automatonInvoker)
        , ChangelogStore_(changelogStore)
        , SnapshotStore_(snapshotStore)
        , Options_(options)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
        VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);

        Logger.AddTag("CellId: %v", CellManager_->GetCellId());

        DecoratedAutomaton_ = New<TDecoratedAutomaton>(
            Config_,
            CellManager_,
            automaton,
            AutomatonInvoker_,
            ControlInvoker_,
            SnapshotStore_,
            ChangelogStore_,
            Options_);

        ElectionManager_ = New<TElectionManager>(
            Config_,
            CellManager_,
            controlInvoker,
            New<TElectionCallbacks>(this));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LogMutations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceBuildSnapshot)
            .SetInvoker(DecoratedAutomaton_->GetDefaultGuardedUserInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RotateChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncWithLeader));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitMutation)
            .SetInvoker(DecoratedAutomaton_->GetDefaultGuardedUserInvoker()));
    }

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ != EPeerState::None)
            return;

        DecoratedAutomaton_->GetSystemInvoker()->Invoke(
            BIND(&TDecoratedAutomaton::Clear, DecoratedAutomaton_));

        RpcServer_->RegisterService(this);
        RpcServer_->RegisterService(ElectionManager_->GetRpcService());

        LOG_INFO("Hydra instance started (SelfAddress: %v, SelfId: %v)",
            CellManager_->GetSelfAddress(),
            CellManager_->GetSelfPeerId());

        ControlState_ = EPeerState::Elections;

        Participate();
    }

    virtual void Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped)
            return;

        CancelableContext_->Cancel();

        ElectionManager_->Stop();

        if (ControlState_ != EPeerState::None) {
            RpcServer_->UnregisterService(this);
            RpcServer_->UnregisterService(ElectionManager_->GetRpcService());
        }

        if (ControlEpochContext_) {
            StopEpoch();
        }

        ControlState_ = EPeerState::Stopped;

        LeaderLease_->Invalidate();
        LeaderRecovered_ = false;
        FollowerRecovered_ = false;

        SwitchTo(AutomatonInvoker_);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        switch (GetAutomatonState()) {
            case EPeerState::Leading:
            case EPeerState::LeaderRecovery:
                DecoratedAutomaton_->OnStopLeading();
                StopLeading_.Fire();
                break;

            case EPeerState::Following:
            case EPeerState::FollowerRecovery:
                DecoratedAutomaton_->OnStopFollowing();
                StopFollowing_.Fire();
                break;

            default:
                break;
        }

        AutomatonEpochContext_.Reset();

        LOG_INFO("Hydra instance stopped");
    }

    virtual EPeerState GetControlState() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ControlState_;
    }

    virtual EPeerState GetAutomatonState() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return DecoratedAutomaton_->GetState();
    }

    virtual IInvokerPtr CreateGuardedAutomatonInvoker(IInvokerPtr underlyingInvoker) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->CreateGuardedUserInvoker(underlyingInvoker);
    }

    virtual bool IsActiveLeader() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return LeaderRecovered_ && LeaderLease_->IsValid();
    }

    virtual bool IsActiveFollower() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return FollowerRecovered_;
    }

    virtual TCancelableContextPtr GetControlCancelableContext() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ControlEpochContext_ ? ControlEpochContext_->CancelableContext : nullptr;
    }

    virtual TCancelableContextPtr GetAutomatonCancelableContext() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_ ? AutomatonEpochContext_->CancelableContext : nullptr;
    }
    
    virtual TPeerId GetAutomatonLeaderId() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_ ? AutomatonEpochContext_->LeaderId : InvalidPeerId;
    }

    virtual bool GetReadOnly() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ReadOnly_;
    }

    virtual void SetReadOnly(bool value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (GetAutomatonState() != EPeerState::Leading) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Not a leader");
        }

        ReadOnly_ = value;
    }

    virtual TFuture<int> BuildSnapshot() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = AutomatonEpochContext_;

        if (!epochContext || !IsActiveLeader()) {
            return MakeFuture<int>(TError(
                NHydra::EErrorCode::InvalidState,
                "Not an active leader"));
        }

        if (!epochContext->Checkpointer->CanBuildSnapshot()) {
            return MakeFuture<int>(TError(
                NHydra::EErrorCode::InvalidState,
                "Cannot build a snapshot at the moment"));
        }

        return BuildSnapshotAndWatch(epochContext).Apply(
            BIND([] (const TRemoteSnapshotParams& params) -> int {
                return params.SnapshotId;
            }));
    }

    virtual TYsonProducer GetMonitoringProducer() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND([=, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
            VERIFY_THREAD_AFFINITY_ANY();
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("state").Value(ControlState_)
                    .Item("committed_version").Value(ToString(DecoratedAutomaton_->GetAutomatonVersion()))
                    .Item("logged_version").Value(ToString(DecoratedAutomaton_->GetLoggedVersion()))
                    .Item("elections").Do(ElectionManager_->GetMonitoringProducer())
                    .Item("active_leader").Value(IsActiveLeader())
                    .Item("active_follower").Value(IsActiveFollower())
                .EndMap();
        });
    }

    virtual TFuture<void> SyncWithLeader() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(!HasMutationContext());

        auto epochContext = AutomatonEpochContext_;
        if (!epochContext || !IsActiveLeader() && !IsActiveFollower()) {
            return MakeFuture(TError(
                NHydra::EErrorCode::InvalidState,
                "Not an active peer"));
        }

        if (GetAutomatonState() == EPeerState::Leading) {
            return VoidFuture;
        }

        if (!epochContext->PendingLeaderSyncPromise) {
            epochContext->PendingLeaderSyncPromise = NewPromise<void>();
            TDelayedExecutor::Submit(
                BIND(&TDistributedHydraManager::OnLeaderSyncDeadlineReached, MakeStrong(this), epochContext)
                    .Via(epochContext->EpochUserAutomatonInvoker),
                Config_->MaxLeaderSyncDelay);
        }

        return epochContext->PendingLeaderSyncPromise;
    }

    virtual TFuture<TMutationResponse> CommitMutation(const TMutationRequest& request) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(!HasMutationContext());

        if (ReadOnly_) {
            return MakeFuture<TMutationResponse>(TError(
                NHydra::EErrorCode::ReadOnly,
                "Read-only mode is active"));
        }

        auto epochContext = AutomatonEpochContext_;
        auto activeLeader = IsActiveLeader();
        auto activeFollower = IsActiveFollower();
        if (!epochContext || !activeLeader && !activeFollower) {
            return MakeFuture<TMutationResponse>(TError(
                NHydra::EErrorCode::InvalidState,
                "Not an active peer"));
        }

        if (!request.AllowLeaderForwarding && !activeLeader) {
            return MakeFuture<TMutationResponse>(TError(
                NHydra::EErrorCode::InvalidState,
                "Not an active leader"));
        }

        if (activeLeader) {
            return epochContext->LeaderCommitter->Commit(request);
        } else {
            return epochContext->FollowerCommitter->Forward(request);
        }
    }

    DEFINE_SIGNAL(void(), StartLeading);
    DEFINE_SIGNAL(void(), LeaderRecoveryComplete);
    DEFINE_SIGNAL(void(), LeaderActive);
    DEFINE_SIGNAL(void(), StopLeading);

    DEFINE_SIGNAL(void(), StartFollowing);
    DEFINE_SIGNAL(void(), FollowerRecoveryComplete);
    DEFINE_SIGNAL(void(), StopFollowing);

    DEFINE_SIGNAL(TFuture<void>(), LeaderLeaseCheck);

private:
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();

    const TDistributedHydraManagerConfigPtr Config_;
    const NRpc::IServerPtr RpcServer_;
    const TCellManagerPtr CellManager_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr CancelableControlInvoker_;
    const IInvokerPtr AutomatonInvoker_;
    const IChangelogStorePtr ChangelogStore_;
    const ISnapshotStorePtr SnapshotStore_;
    const TDistributedHydraManagerOptions Options_;

    std::atomic<bool> ReadOnly_ = {false};
    const TLeaderLeasePtr LeaderLease_ = New<TLeaderLease>();
    std::atomic<bool> LeaderRecovered_ = {false};
    std::atomic<bool> FollowerRecovered_ = {false};
    EPeerState ControlState_ = EPeerState::None;
    TSystemLockGuard SystemLockGuard_;

    TVersion ReachableVersion_;

    TElectionManagerPtr ElectionManager_;

    TDecoratedAutomatonPtr DecoratedAutomaton_;

    TEpochContextPtr ControlEpochContext_;
    TEpochContextPtr AutomatonEpochContext_;


    DECLARE_RPC_SERVICE_METHOD(NProto, LookupChangelog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int changelogId = request->changelog_id();

        context->SetRequestInfo("ChangelogId: %v", changelogId);

        auto changelog = OpenChangelogOrThrow(changelogId);
        int recordCount = changelog->GetRecordCount();
        response->set_record_count(recordCount);

        context->SetResponseInfo("RecordCount: %v", recordCount);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReadChangeLog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int changelogId = request->changelog_id();
        int startRecordId = request->start_record_id();
        int recordCount = request->record_count();

        context->SetRequestInfo("ChangelogId: %v, StartRecordId: %v, RecordCount: %v",
            changelogId,
            startRecordId,
            recordCount);

        YCHECK(startRecordId >= 0);
        YCHECK(recordCount >= 0);

        auto changelog = OpenChangelogOrThrow(changelogId);

        auto asyncRecordsData = changelog->Read(
            startRecordId,
            recordCount,
            Config_->MaxChangelogBytesPerRequest);
        auto recordsData = WaitFor(asyncRecordsData)
            .ValueOrThrow();

        // Pack refs to minimize allocations.
        response->Attachments().push_back(PackRefs(recordsData));

        context->SetResponseInfo("RecordCount: %v", recordsData.size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, LogMutations)
    {
        // LogMutations and RotateChangelog handling must start in Control Thread
        // since during recovery Automaton Thread may be busy for prolonged periods of time
        // and we must still be able to capture and postpone the relevant mutations.
        //
        // Additionally, it is vital for LogMutations, BuildSnapshot, and RotateChangelog handlers
        // to follow the same thread transition pattern (start in ControlThread, then switch to
        // Automaton Thread) to ensure consistent callbacks ordering.
        //
        // E.g. BulidSnapshot and RotateChangelog calls rely on the fact than all mutations
        // that were previously sent via LogMutations are accepted (and the logged version is
        // propagated appropriately).

        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto startVersion = TVersion::FromRevision(request->start_revision());
        auto committedVersion = TVersion::FromRevision(request->committed_revision());
        int mutationCount = static_cast<int>(request->Attachments().size());

        context->SetRequestInfo("StartVersion: %v, CommittedVersion: %v, EpochId: %v, MutationCount: %v",
            startVersion,
            committedVersion,
            epochId,
            mutationCount);

        if (ControlState_ != EPeerState::Following && ControlState_ != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot accept mutations in %Qlv state",
                ControlState_);
        }

        auto epochContext = GetEpochContext(epochId);

        switch (ControlState_) {
            case EPeerState::Following: {
                SwitchTo(epochContext->EpochUserAutomatonInvoker);
                VERIFY_THREAD_AFFINITY(AutomatonThread);

                CommitMutationsAtFollower(epochContext, committedVersion);

                try {
                    auto asyncResult = epochContext->FollowerCommitter->LogMutations(
                        startVersion,
                        request->Attachments());
                    WaitFor(asyncResult)
                        .ThrowOnError();
                    response->set_logged(true);
                } catch (const std::exception& ex) {
                    if (Restart(epochContext)) {
                        LOG_ERROR(ex, "Error logging mutations");
                    }
                    throw;
                }
                break;
            }

            case EPeerState::FollowerRecovery: {
                try {
                    CheckForInitialPing(startVersion);
                    epochContext->FollowerRecovery->PostponeMutations(
                        startVersion,
                        request->Attachments());
                    response->set_logged(false);
                } catch (const std::exception& ex) {
                    if (Restart(epochContext)) {
                        LOG_ERROR(ex, "Error postponing mutations during recovery");
                    }
                    throw;
                }
                break;
            }

            default:
                YUNREACHABLE();
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingFollower)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto loggedVersion = TVersion::FromRevision(request->logged_revision());
        auto committedVersion = TVersion::FromRevision(request->committed_revision());

        context->SetRequestInfo("LoggedVersion: %v, CommittedVersion: %v, EpochId: %v",
            loggedVersion,
            committedVersion,
            epochId);

        if (ControlState_ != EPeerState::Following && ControlState_ != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot handle follower ping in %Qlv state",
                ControlState_);
        }

        auto epochContext = GetEpochContext(epochId);

        switch (ControlState_) {
            case EPeerState::Following:
                epochContext->EpochUserAutomatonInvoker->Invoke(BIND(
                    &TDistributedHydraManager::CommitMutationsAtFollower,
                    MakeStrong(this),
                    std::move(epochContext),
                    committedVersion));
                break;

            case EPeerState::FollowerRecovery:
                CheckForInitialPing(loggedVersion);
                break;

            default:
                YUNREACHABLE();
        }

        response->set_state(static_cast<int>(ControlState_));

        // Reply with OK in any case.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, BuildSnapshot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        UNUSED(response);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %v, Version: %v",
            epochId,
            version);

        if (ControlState_ != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot build snapshot in %Qlv state",
                ControlState_);
        }

        auto epochContext = GetEpochContext(epochId);

        SwitchTo(epochContext->EpochUserAutomatonInvoker);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (DecoratedAutomaton_->GetLoggedVersion() != version) {
            Restart(epochContext);
            context->Reply(TError(
                NHydra::EErrorCode::InvalidVersion,
                "Invalid logged version: expected %v, actual %v",
                version,
                DecoratedAutomaton_->GetLoggedVersion()));
            return;
        }

        auto result = WaitFor(DecoratedAutomaton_->BuildSnapshot())
            .ValueOrThrow();

        response->set_checksum(result.Checksum);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ForceBuildSnapshot)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        bool setReadOnly = request->set_read_only();

        context->SetRequestInfo("SetReadOnly: %v",
            setReadOnly);

        SetReadOnly(setReadOnly);

        int snapshotId = WaitFor(BuildSnapshot())
            .ValueOrThrow();

        context->SetResponseInfo("SnapshotId: %v",
            snapshotId);

        response->set_snapshot_id(snapshotId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RotateChangelog)
    {
        // See LogMutations.
        VERIFY_THREAD_AFFINITY(ControlThread);
        UNUSED(response);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %v, Version: %v",
            epochId,
            version);

        if (ControlState_ != EPeerState::Following && ControlState_  != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot rotate changelog while in %Qlv state",
                ControlState_);
        }

        auto epochContext = GetEpochContext(epochId);

        switch (ControlState_) {
            case EPeerState::Following: {
                SwitchTo(epochContext->EpochUserAutomatonInvoker);
                VERIFY_THREAD_AFFINITY(AutomatonThread);

                try {
                    if (DecoratedAutomaton_->GetLoggedVersion() != version) {
                        THROW_ERROR_EXCEPTION(
                            NHydra::EErrorCode::InvalidVersion,
                            "Invalid logged version: expected %v, actual %v",
                            version,
                            DecoratedAutomaton_->GetLoggedVersion());
                    }

                    auto followerCommitter = epochContext->FollowerCommitter;
                    if (followerCommitter->IsLoggingSuspended()) {
                        THROW_ERROR_EXCEPTION(
                            NHydra::EErrorCode::InvalidState,
                            "Changelog is already being rotated");
                    }

                    followerCommitter->SuspendLogging();

                    WaitFor(DecoratedAutomaton_->RotateChangelog(epochContext))
                        .ThrowOnError();

                    followerCommitter->ResumeLogging();
                } catch (const std::exception& ex) {
                    if (Restart(epochContext)) {
                        LOG_ERROR(ex, "Error rotating changelog");
                    }
                    throw;
                }

                break;
            }

            case EPeerState::FollowerRecovery: {
                auto followerRecovery = epochContext->FollowerRecovery;
                if (!followerRecovery) {
                    // NB: No restart.
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::InvalidState,
                        "Initial ping is not received yet");
                }

                try {
                    followerRecovery->PostponeChangelogRotation(version);
                } catch (const std::exception& ex) {
                    if (Restart(epochContext)) {
                        LOG_ERROR(ex, "Error postponing changelog rotation during recovery");
                    }
                    throw;
                }

                break;
            }

            default:
                YUNREACHABLE();
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SyncWithLeader)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        context->SetRequestInfo("EpochId: %v",
            epochId);

        if (!IsActiveLeader()) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Not an active leader");
        }

        // Validate epoch id.
        GetEpochContext(epochId);

        auto version = DecoratedAutomaton_->GetAutomatonVersion();

        context->SetResponseInfo("CommittedVersion: %s",
            version);

        response->set_committed_revision(version.ToRevision());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitMutation)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMutationRequest mutationRequest;
        mutationRequest.Type = request->type();
        mutationRequest.Data = request->Attachments()[0];

        context->SetRequestInfo("Type: %v", mutationRequest.Type);

        CommitMutation(mutationRequest).Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
            if (!result.IsOK()) {
                context->Reply(result);
                return;
            }

            const auto& mutationResponse = result.Value();
            response->Attachments() = mutationResponse.Data.ToVector();
            context->Reply();
        }));
    }


    i64 GetElectionPriority() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto version = ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::Following
            ? DecoratedAutomaton_->GetAutomatonVersion()
            : ReachableVersion_;

        return version.ToRevision();
    }


    void Participate()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        CancelableControlInvoker_->Invoke(
            BIND(&TDistributedHydraManager::DoParticipate, MakeStrong(this)));
    }

    bool Restart(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        bool expected = false;
        if (epochContext->Restarted.compare_exchange_strong(expected, true)) {
            return false;
        }

        CancelableControlInvoker_->Invoke(BIND(
            &TDistributedHydraManager::DoRestart,
            MakeWeak(this),
            epochContext));

        return true;
    }


    void DoRestart(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ElectionManager_->Stop();
    }

    void DoParticipate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Computing reachable version");

        while (true) {
            try {
                auto asyncMaxSnapshotId = SnapshotStore_->GetLatestSnapshotId();
                int maxSnapshotId = WaitFor(asyncMaxSnapshotId)
                    .ValueOrThrow();

                if (maxSnapshotId == InvalidSegmentId) {
                    LOG_INFO("No snapshots found");
                    // Let's pretend we have snapshot 0.
                    maxSnapshotId = 0;
                } else {
                    LOG_INFO("The latest snapshot is %v", maxSnapshotId);
                }

                auto asyncMaxChangelog = ChangelogStore_->GetLatestChangelogId(maxSnapshotId);
                int maxChangelogId = WaitFor(asyncMaxChangelog)
                    .ValueOrThrow();

                if (maxChangelogId == InvalidSegmentId) {
                    LOG_INFO("No changelogs found");
                    ReachableVersion_ = TVersion(maxSnapshotId, 0);
                } else {
                    LOG_INFO("The latest changelog is %v", maxChangelogId);
                    auto changelog = OpenChangelogOrThrow(maxChangelogId);
                    ReachableVersion_ = TVersion(maxChangelogId, changelog->GetRecordCount());
                }
                break;
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error computing reachable version, backing off and retrying");
                WaitFor(TDelayedExecutor::MakeDelayed(Config_->RestartBackoffTime));
            }
        }

        LOG_INFO("Reachable version is %v", ReachableVersion_);
        DecoratedAutomaton_->SetLoggedVersion(ReachableVersion_);
        ElectionManager_->Start();
    }


    IChangelogPtr OpenChangelogOrThrow(int id)
    {
        return WaitFor(ChangelogStore_->OpenChangelog(id))
            .ValueOrThrow();
    }


    void OnCheckpointNeeded(TWeakPtr<TEpochContext> epochContext_)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = epochContext_.Lock();
        if (!epochContext || !IsActiveLeader())
            return;

        auto checkpointer = epochContext->Checkpointer;
        if (checkpointer->CanBuildSnapshot()) {
            BuildSnapshotAndWatch(epochContext);
        } else if (checkpointer->CanRotateChangelogs()) {
            LOG_WARNING("Snapshot is still being built, just rotating changlogs");
            RotateChangelogAndWatch(epochContext);
        } else {
            return;
        }
    }

    void OnCommitFailed(TWeakPtr<TEpochContext> epochContext_, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = epochContext_.Lock();
        if (!epochContext)
            return;

        DecoratedAutomaton_->CancelPendingLeaderMutations(error);

        if (Restart(epochContext)) {
            LOG_ERROR(error, "Error committing mutation, restarting");
        }
    }

    void OnLeaderLeaseLost(TWeakPtr<TEpochContext> epochContext_, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto epochContext = epochContext_.Lock();
        if (!epochContext)
            return;

        if (Restart(epochContext)) {
            LOG_ERROR(error, "Leader lease is lost, restarting");
        }
    }


    void RotateChangelogAndWatch(TEpochContextPtr epochContext)
    {
        auto changelogResult = epochContext->Checkpointer->RotateChangelog();
        WatchChangelogRotation(epochContext, changelogResult);
    }

    TFuture<TRemoteSnapshotParams> BuildSnapshotAndWatch(TEpochContextPtr epochContext)
    {
        TFuture<void> changelogResult;
        TFuture<TRemoteSnapshotParams> snapshotResult;
        std::tie(changelogResult, snapshotResult) = epochContext->Checkpointer->BuildSnapshot();
        WatchChangelogRotation(epochContext, changelogResult);
        return snapshotResult;
    }

    void WatchChangelogRotation(TEpochContextPtr epochContext, TFuture<void> result)
    {
        result.Subscribe(BIND(
            &TDistributedHydraManager::OnChangelogRotated,
            MakeWeak(this),
            epochContext));
    }

    void OnChangelogRotated(TEpochContextPtr epochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (error.IsOK()) {
            LOG_INFO("Distributed changelog rotation succeeded");
        } else {
            if (Restart(epochContext)) {
                LOG_ERROR(error, "Distributed changelog rotation failed");
            }
        }
    }


    void OnElectionStartLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting leader recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::LeaderRecovery;

        StartEpoch();
        auto epochContext = ControlEpochContext_;

        epochContext->LeaseTracker = New<TLeaseTracker>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext.Get(),
            LeaderLease_,
            LeaderLeaseCheck_.ToVector());
        epochContext->LeaseTracker->GetLeaseLost().Subscribe(
            BIND(&TDistributedHydraManager::OnLeaderLeaseLost, MakeWeak(this), MakeWeak(epochContext)));

        epochContext->LeaderCommitter = New<TLeaderCommitter>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            ChangelogStore_,
            epochContext.Get());
        epochContext->LeaderCommitter->SubscribeCheckpointNeeded(
            BIND(&TDistributedHydraManager::OnCheckpointNeeded, MakeWeak(this), MakeWeak(epochContext)));
        epochContext->LeaderCommitter->SubscribeCommitFailed(
            BIND(&TDistributedHydraManager::OnCommitFailed, MakeWeak(this), MakeWeak(epochContext)));

        epochContext->Checkpointer = New<TCheckpointer>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext->LeaderCommitter,
            SnapshotStore_,
            epochContext.Get());

        epochContext->LeaseTracker->Start();

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_ = epochContext;
        DecoratedAutomaton_->OnStartLeading();
        StartLeading_.Fire();

        SwitchTo(epochContext->EpochControlInvoker);
        VERIFY_THREAD_AFFINITY(ControlThread);

        RecoverLeader();
    }

    void RecoverLeader()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;

        try {
            epochContext->LeaderRecovery = New<TLeaderRecovery>(
                Config_,
                CellManager_,
                DecoratedAutomaton_,
                ChangelogStore_,
                SnapshotStore_,
                Options_.ResponseKeeper,
                epochContext.Get());

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            auto version = DecoratedAutomaton_->GetLoggedVersion();
            auto asyncRecoveryResult = epochContext->LeaderRecovery->Run(version);
            WaitFor(asyncRecoveryResult)
                .ThrowOnError();

            DecoratedAutomaton_->OnLeaderRecoveryComplete();
            LeaderRecoveryComplete_.Fire();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            YCHECK(ControlState_ == EPeerState::LeaderRecovery);
            ControlState_ = EPeerState::Leading;

            LOG_INFO("Leader recovery complete");

            LOG_INFO("Waiting for leader lease");

            WaitFor(epochContext->LeaseTracker->GetLeaseAcquired())
                .ThrowOnError();

            LOG_INFO("Leader lease acquired");

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            WaitFor(epochContext->Checkpointer->RotateChangelog())
                .ThrowOnError();

            LOG_INFO("Initial changelog rotated");

            LeaderRecovered_ = true;
            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }
            LeaderActive_.Fire();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            SystemLockGuard_.Release();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Leader recovery failed, backing off and restarting");
            WaitFor(TDelayedExecutor::MakeDelayed(Config_->RestartBackoffTime));
            Restart(epochContext);
        }
    }

    void OnElectionStopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Stopped leading");

        StopEpoch();

        YCHECK(ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery);
        ControlState_ = EPeerState::Elections;
        
        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_.Reset();
        DecoratedAutomaton_->OnStopLeading();
        StopLeading_.Fire();

        Participate();
    }


    void OnElectionStartFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting follower recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::FollowerRecovery;

        StartEpoch();
        auto epochContext = ControlEpochContext_;

        epochContext->FollowerCommitter = New<TFollowerCommitter>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext.Get());

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_ = epochContext;
        DecoratedAutomaton_->OnStartFollowing();
        StartFollowing_.Fire();
    }

    void RecoverFollower()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;

        try {
            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            auto asyncRecoveryResult = epochContext->FollowerRecovery->Run();
            WaitFor(asyncRecoveryResult)
                .ThrowOnError();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            YCHECK(ControlState_ == EPeerState::FollowerRecovery);
            ControlState_ = EPeerState::Following;

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            LOG_INFO("Follower recovery complete");

            DecoratedAutomaton_->OnFollowerRecoveryComplete();
            FollowerRecoveryComplete_.Fire();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            FollowerRecovered_ = true;
            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }

            SystemLockGuard_.Release();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Follower recovery failed, backing off and restarting");
            WaitFor(TDelayedExecutor::MakeDelayed(Config_->RestartBackoffTime));
            Restart(epochContext);
        }
    }

    void OnElectionStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Stopped following");

        StopEpoch();

        YCHECK(ControlState_ == EPeerState::Following || ControlState_ == EPeerState::FollowerRecovery);
        ControlState_ = EPeerState::Elections;

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_.Reset();
        DecoratedAutomaton_->OnStopFollowing();
        StopFollowing_.Fire();

        Participate();

        SystemLockGuard_ = TSystemLockGuard();
    }

    void CheckForInitialPing(TVersion version)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(ControlState_ == EPeerState::FollowerRecovery);

        auto epochContext = ControlEpochContext_;

        // Check if initial ping is already received.
        if (epochContext->FollowerRecovery)
            return;

        LOG_INFO("Received initial ping from leader (Version: %v)",
            version);

        epochContext->FollowerRecovery = New<TFollowerRecovery>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            ChangelogStore_,
            SnapshotStore_,
            Options_.ResponseKeeper,
            epochContext.Get(),
            version);

        epochContext->EpochControlInvoker->Invoke(
            BIND(&TDistributedHydraManager::RecoverFollower, MakeStrong(this)));
    }


    void StartEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto electionEpochContext = ElectionManager_->GetEpochContext();

        auto epochContext = New<TEpochContext>();
        epochContext->LeaderId = electionEpochContext->LeaderId;
        epochContext->EpochId = electionEpochContext->EpochId;
        epochContext->CancelableContext = electionEpochContext->CancelableContext;
        epochContext->EpochControlInvoker = epochContext->CancelableContext->CreateInvoker(CancelableControlInvoker_);
        epochContext->EpochSystemAutomatonInvoker = epochContext->CancelableContext->CreateInvoker(DecoratedAutomaton_->GetSystemInvoker());
        epochContext->EpochUserAutomatonInvoker = epochContext->CancelableContext->CreateInvoker(AutomatonInvoker_);

        YCHECK(!ControlEpochContext_);
        ControlEpochContext_ = epochContext;

        SystemLockGuard_ = TSystemLockGuard::Acquire(DecoratedAutomaton_);
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(ControlEpochContext_);
        ControlEpochContext_->CancelableContext->Cancel();
        ControlEpochContext_.Reset();
        LeaderLease_->Invalidate();
        LeaderRecovered_ = false;
        FollowerRecovered_ = false;

        SystemLockGuard_.Release();
    }

    TEpochContextPtr GetEpochContext(const TEpochId& epochId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto currentEpochId = ControlEpochContext_->EpochId;
        if (epochId != currentEpochId) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidEpoch,
                "Invalid epoch: expected %v, received %v",
                currentEpochId,
                epochId);
        }
        return ControlEpochContext_;
    }


    void OnLeaderSyncDeadlineReached(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        epochContext->LeaderSyncDeadlineReached = true;

        if (!epochContext->ActiveLeaderSyncPromise) {
            DoSyncWithLeader(epochContext);
        }
    }

    void DoSyncWithLeader(TEpochContextPtr epochContext)
    {
        LOG_DEBUG("Syncing with leader");

        epochContext->LeaderSyncDeadlineReached = false;

        YCHECK(!epochContext->ActiveLeaderSyncPromise);
        swap(epochContext->ActiveLeaderSyncPromise, epochContext->PendingLeaderSyncPromise);

        auto channel = CellManager_->GetPeerChannel(epochContext->LeaderId);
        YCHECK(channel);

        THydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

        auto req = proxy.SyncWithLeader();
        ToProto(req->mutable_epoch_id(), epochContext->EpochId);

        req->Invoke().Subscribe(
            BIND(
                &TDistributedHydraManager::OnSyncWithLeaderResponse,
                MakeStrong(this),
                epochContext)
            .Via(epochContext->EpochUserAutomatonInvoker));
    }

    void OnSyncWithLeaderResponse(
        TEpochContextPtr epochContext,
        const THydraServiceProxy::TErrorOrRspSyncWithLeaderPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!rspOrError.IsOK()) {
            if (Restart(epochContext)) {
                LOG_ERROR(rspOrError, "Failed to sync with leader");
            }
            return;
        }

        const auto& rsp = rspOrError.Value();

        YCHECK(!epochContext->ActiveLeaderSyncVersion);
        epochContext->ActiveLeaderSyncVersion = TVersion::FromRevision(rsp->committed_revision());

        LOG_DEBUG("Received sync response from leader (CommittedVersion: %s)",
            epochContext->ActiveLeaderSyncVersion);

        CheckForPendingLeaderSync(epochContext);
    }

    void CheckForPendingLeaderSync(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!epochContext->ActiveLeaderSyncPromise || !epochContext->ActiveLeaderSyncVersion)
            return;

        auto neededCommittedVersion = *epochContext->ActiveLeaderSyncVersion;
        auto actualCommittedVersion = DecoratedAutomaton_->GetAutomatonVersion();
        if (neededCommittedVersion > actualCommittedVersion)
            return;

        LOG_DEBUG("Leader synced successfully (NeededCommittedVersion: %v, ActualCommittedVersion: %v)",
            neededCommittedVersion,
            actualCommittedVersion);

        epochContext->ActiveLeaderSyncPromise.Set();
        epochContext->ActiveLeaderSyncPromise.Reset();
        epochContext->ActiveLeaderSyncVersion.Reset();

        if (epochContext->LeaderSyncDeadlineReached) {
            DoSyncWithLeader(epochContext);
        }
    }


    void CommitMutationsAtFollower(TEpochContextPtr epochContext, TVersion committedVersion)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->CommitMutations(committedVersion);
        CheckForPendingLeaderSync(std::move(epochContext));
    }


    // THydraServiceBase overrides.
    virtual IHydraManagerPtr GetHydraManager() override
    {
        return this;
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

////////////////////////////////////////////////////////////////////////////////

IHydraManagerPtr CreateDistributedHydraManager(
    TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    IAutomatonPtr automaton,
    IServerPtr rpcServer,
    TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options)
{
    YCHECK(config);
    YCHECK(controlInvoker);
    YCHECK(automatonInvoker);
    YCHECK(automaton);
    YCHECK(rpcServer);
    YCHECK(cellManager);
    YCHECK(changelogStore);
    YCHECK(snapshotStore);

    return New<TDistributedHydraManager>(
        config,
        controlInvoker,
        automatonInvoker,
        automaton,
        rpcServer,
        cellManager,
        changelogStore,
        snapshotStore,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
