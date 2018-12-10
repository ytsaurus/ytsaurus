#include "distributed_hydra_manager.h"
#include "private.h"
#include "automaton.h"
#include "changelog.h"
#include "checkpointer.h"
#include "config.h"
#include "decorated_automaton.h"
#include "hydra_manager.h"
#include "hydra_service.h"
#include "lease_tracker.h"
#include "mutation_committer.h"
#include "mutation_context.h"
#include "recovery.h"
#include "snapshot.h"
#include "snapshot_discovery.h"

#include <yt/server/election/election_manager.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/fluent.h>

#include <atomic>

namespace NYT::NHydra {

using namespace NElection;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto PostponeBackoffTime = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedHydraManager)

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

        virtual void OnStartLeading(NElection::TEpochContextPtr epochContext) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStartLeading,
                Owner_,
                epochContext));
        }

        virtual void OnStopLeading() override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStopLeading,
                Owner_));
        }

        virtual void OnStartFollowing(NElection::TEpochContextPtr epochContext) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStartFollowing,
                Owner_,
                epochContext));
        }

        virtual void OnStopFollowing() override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStopFollowing,
                Owner_));
        }

        virtual TPeerPriority GetPriority() override
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                THROW_ERROR_EXCEPTION("Election priority is not available");
            }
            return owner->GetElectionPriority();
        }

        virtual TString FormatPriority(TPeerPriority priority) override
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
        IElectionManagerPtr electionManager,
        TCellManagerPtr cellManager,
        IChangelogStoreFactoryPtr changelogStoreFactory,
        ISnapshotStorePtr snapshotStore,
        const TDistributedHydraManagerOptions& options)
        : THydraServiceBase(
            controlInvoker,
            THydraServiceProxy::GetDescriptor(),
            NLogging::TLogger(HydraLogger)
                .AddTag("CellId: %v", cellManager->GetCellId()),
            cellManager->GetCellId())
        , Config_(config)
        , RpcServer_(rpcServer)
        , ElectionManager_(electionManager)
        , CellManager_(cellManager)
        , ControlInvoker_(controlInvoker)
        , CancelableControlInvoker_(CancelableContext_->CreateInvoker(ControlInvoker_))
        , AutomatonInvoker_(automatonInvoker)
        , ChangelogStoreFactory_(changelogStoreFactory)
        , SnapshotStore_(snapshotStore)
        , Options_(options)
        , ElectionCallbacks_(New<TElectionCallbacks>(this))
        , Profiler(HydraProfiler.AddTags(Options_.ProfilingTagIds))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
        VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);

        DecoratedAutomaton_ = New<TDecoratedAutomaton>(
            Config_,
            Options_,
            CellManager_,
            automaton,
            AutomatonInvoker_,
            ControlInvoker_,
            SnapshotStore_,
            Profiler);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AcceptMutations));
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

        DecoratedAutomaton_->Initialize();

        RpcServer_->RegisterService(this);

        LOG_INFO("Hydra instance initialized (SelfAddress: %v, SelfId: %v)",
            CellManager_->GetSelfConfig(),
            CellManager_->GetSelfPeerId());

        ControlState_ = EPeerState::Elections;

        IncrementRestartCounter("Initialization");

        Participate();
    }

    virtual TFuture<void> Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped) {
            return VoidFuture;
        }

        LOG_INFO("Hydra instance is finalizing");

        CancelableContext_->Cancel();

        ElectionManager_->Abandon();

        if (ControlState_ != EPeerState::None) {
            RpcServer_->UnregisterService(this);
        }

        if (ControlEpochContext_) {
            StopEpoch();
        }

        ControlState_ = EPeerState::Stopped;

        LeaderLease_->Invalidate();
        LeaderRecovered_ = false;
        FollowerRecovered_ = false;

        return BIND(&TDistributedHydraManager::DoFinalize, MakeStrong(this))
            .AsyncVia(AutomatonInvoker_)
            .Run();
    }

    virtual IElectionCallbacksPtr GetElectionCallbacks() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ElectionCallbacks_;
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

    virtual TVersion GetAutomatonVersion() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return DecoratedAutomaton_->GetAutomatonVersion();
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

    virtual TFuture<int> BuildSnapshot(bool setReadOnly) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = AutomatonEpochContext_;

        if (!epochContext || !IsActiveLeader()) {
            return MakeFuture<int>(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active leader"));
        }

        if (!epochContext->Checkpointer->CanBuildSnapshot()) {
            return MakeFuture<int>(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot build a snapshot at the moment"));
        }

        SetReadOnly(setReadOnly);

        return BuildSnapshotAndWatch(epochContext, setReadOnly).Apply(
            BIND([] (const TRemoteSnapshotParams& params) {
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
                    .Item("voting").Value(CellManager_->GetSelfConfig().Voting)
                    .Item("committed_version").Value(ToString(DecoratedAutomaton_->GetCommittedVersion()))
                    .Item("automaton_version").Value(ToString(DecoratedAutomaton_->GetAutomatonVersion()))
                    .Item("logged_version").Value(ToString(DecoratedAutomaton_->GetLoggedVersion()))
                    .Item("active_leader").Value(IsActiveLeader())
                    .Item("active_follower").Value(IsActiveFollower())
                    .Item("read_only").Value(GetReadOnly())
                    .Item("warming_up").Value(Options_.ResponseKeeper ? Options_.ResponseKeeper->IsWarmingUp() : false)
                .EndMap();
        });
    }

    virtual TFuture<void> SyncWithUpstream() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(!HasMutationContext());

        auto epochContext = AutomatonEpochContext_;
        if (!epochContext || !IsActiveLeader() && !IsActiveFollower()) {
            return MakeFuture(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active peer"));
        }

        if (GetAutomatonState() == EPeerState::Leading && UpstreamSync_.Empty()) {
            return VoidFuture;
        }

        if (!epochContext->PendingUpstreamSyncPromise) {
            epochContext->PendingUpstreamSyncPromise = NewPromise<void>();
            TDelayedExecutor::Submit(
                BIND(&TDistributedHydraManager::OnUpsteamSyncDeadlineReached, MakeStrong(this), epochContext)
                    .Via(epochContext->EpochUserAutomatonInvoker),
                Config_->UpstreamSyncDelay);
        }

        return epochContext->PendingUpstreamSyncPromise;
    }

    virtual TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (ReadOnly_) {
            return MakeFuture<TMutationResponse>(TError(
                NRpc::EErrorCode::Unavailable,
                "Read-only mode is active"));
        }

        auto epochContext = AutomatonEpochContext_;
        if (epochContext && epochContext->Restarting) {
            return MakeFuture<TMutationResponse>(TError(
                NRpc::EErrorCode::Unavailable,
                "Peer is restarting"));
        }

        auto state = GetAutomatonState();
        switch (state) {
            case EPeerState::Leading:
                if (!LeaderRecovered_) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader has not yet recovered"));
                }

                if (!LeaderLease_->IsValid()) {
                    auto error = TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader lease is no longer valid");
                    Restart(epochContext, error);
                    return MakeFuture<TMutationResponse>(error);
                }

                return epochContext->LeaderCommitter->Commit(std::move(request));

            case EPeerState::Following:
                if (!FollowerRecovered_) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Follower has not yet recovered"));
                }

                if (!request.AllowLeaderForwarding) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader mutation forwarding is not allowed"));
                }

                return epochContext->FollowerCommitter->Forward(std::move(request));

            default:
                return MakeFuture<TMutationResponse>(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Peer is in %Qlv state",
                    state));
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
    DEFINE_SIGNAL(TFuture<void>(), UpstreamSync);

private:
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();

    const TDistributedHydraManagerConfigPtr Config_;
    const IServerPtr RpcServer_;
    const IElectionManagerPtr ElectionManager_;
    const TCellManagerPtr CellManager_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr CancelableControlInvoker_;
    const IInvokerPtr AutomatonInvoker_;
    const IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    const ISnapshotStorePtr SnapshotStore_;
    const TDistributedHydraManagerOptions Options_;

    const IElectionCallbacksPtr ElectionCallbacks_;

    const NProfiling::TProfiler Profiler;

    std::atomic<bool> ReadOnly_ = {false};
    const TLeaderLeasePtr LeaderLease_ = New<TLeaderLease>();
    std::atomic<bool> LeaderRecovered_ = {false};
    std::atomic<bool> FollowerRecovered_ = {false};
    EPeerState ControlState_ = EPeerState::None;
    TSystemLockGuard SystemLockGuard_;

    IChangelogStorePtr ChangelogStore_;
    std::optional<TVersion> ReachableVersion_;

    TDecoratedAutomatonPtr DecoratedAutomaton_;

    TEpochContextPtr ControlEpochContext_;
    TEpochContextPtr AutomatonEpochContext_;

    THashMap<TString, NProfiling::TMonotonicCounter> RestartCounters_;

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

    DECLARE_RPC_SERVICE_METHOD(NProto, AcceptMutations)
    {
        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto startVersion = TVersion::FromRevision(request->start_revision());
        auto committedVersion = TVersion::FromRevision(request->committed_revision());
        auto mutationCount = request->Attachments().size();

        context->SetRequestInfo("StartVersion: %v, CommittedVersion: %v, EpochId: %v, MutationCount: %v",
            startVersion,
            committedVersion,
            epochId,
            mutationCount);

        bool again;
        do {
            again = false;

            // AcceptMutations and RotateChangelog handling must start in Control Thread
            // since during recovery Automaton Thread may be busy for prolonged periods of time
            // and we must still be able to capture and postpone the relevant mutations.
            //
            // Additionally, it is vital for AcceptMutations, BuildSnapshot, and RotateChangelog handlers
            // to follow the same thread transition pattern (start in ControlThread, then switch to
            // Automaton Thread) to ensure consistent callbacks ordering.
            //
            // E.g. BuildSnapshot and RotateChangelog calls rely on the fact than all mutations
            // that were previously sent via AcceptMutations are accepted (and the logged version is
            // propagated appropriately).
            VERIFY_THREAD_AFFINITY(ControlThread);

            if (ControlState_ != EPeerState::Following && ControlState_ != EPeerState::FollowerRecovery) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
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
                        auto asyncResult = epochContext->FollowerCommitter->AcceptMutations(
                            startVersion,
                            request->Attachments());
                        WaitFor(asyncResult)
                            .ThrowOnError();
                        response->set_logged(Options_.WriteChangelogsAtFollowers);
                    } catch (const std::exception& ex) {
                        auto error = TError("Error logging mutations")
                            << ex;
                        Restart(epochContext, error);
                        THROW_ERROR error;
                    }
                    break;
                }

                case EPeerState::FollowerRecovery: {
                    try {
                        CheckForInitialPing(startVersion);
                        auto followerRecovery = epochContext->FollowerRecovery;
                        if (followerRecovery) {
                            if (!followerRecovery->PostponeMutations(startVersion, request->Attachments())) {
                                BackoffPostpone();
                                again = true;
                                continue;
                            }
                            followerRecovery->SetCommittedVersion(committedVersion);
                        }
                        response->set_logged(false);
                    } catch (const std::exception& ex) {
                        auto error = TError("Error postponing mutations during recovery")
                            << ex;
                        Restart(epochContext, error);
                        THROW_ERROR error;
                    }
                    break;
                }

                default:
                    Y_UNREACHABLE();
            }
        } while (again);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingFollower)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto pingVersion = TVersion::FromRevision(request->ping_revision());
        auto committedVersion = TVersion::FromRevision(request->committed_revision());

        context->SetRequestInfo("PingVersion: %v, CommittedVersion: %v, EpochId: %v",
            pingVersion,
            committedVersion,
            epochId);

        if (ControlState_ != EPeerState::Following && ControlState_ != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot handle follower ping in %Qlv state",
                ControlState_);
        }

        auto epochContext = GetEpochContext(epochId);

        switch (ControlState_) {
            case EPeerState::Following:
                epochContext->EpochUserAutomatonInvoker->Invoke(
                    BIND(&TDecoratedAutomaton::CommitMutations, DecoratedAutomaton_, committedVersion, true));
                break;

            case EPeerState::FollowerRecovery: {
                CheckForInitialPing(pingVersion);
                auto followerRecovery = epochContext->FollowerRecovery;
                if (followerRecovery) {
                    followerRecovery->SetCommittedVersion(committedVersion);
                }
                break;
            }

            default:
                Y_UNREACHABLE();
        }

        response->set_state(static_cast<int>(ControlState_));

        // Reply with OK in any case.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, BuildSnapshot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        Y_UNUSED(response);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());
        bool setReadOnly = request->set_read_only();

        context->SetRequestInfo("EpochId: %v, Version: %v, SetReadOnly: %v",
            epochId,
            version,
            setReadOnly);

        if (ControlState_ != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot build snapshot in %Qlv state",
                ControlState_);
        }

        if (!Options_.WriteSnapshotsAtFollowers) {
            THROW_ERROR_EXCEPTION("Cannot build snapshot at follower");
        }

        auto epochContext = GetEpochContext(epochId);

        SwitchTo(epochContext->EpochUserAutomatonInvoker);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (DecoratedAutomaton_->GetLoggedVersion() != version) {
            auto error = TError(
                NHydra::EErrorCode::InvalidVersion,
                "Invalid logged version")
                << TErrorAttribute("expected_version", ToString(version))
                << TErrorAttribute("actual_version", ToString(DecoratedAutomaton_->GetLoggedVersion()));
            Restart(epochContext, error);
            context->Reply(error);
            return;
        }

        SetReadOnly(setReadOnly);

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

        int snapshotId = WaitFor(BuildSnapshot(setReadOnly))
            .ValueOrThrow();

        context->SetResponseInfo("SnapshotId: %v",
            snapshotId);

        response->set_snapshot_id(snapshotId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RotateChangelog)
    {
        Y_UNUSED(response);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %v, Version: %v",
            epochId,
            version);

        bool again;
        do {
            again = false;

            // See AcceptMutations.
            VERIFY_THREAD_AFFINITY(ControlThread);

            if (ControlState_ != EPeerState::Following && ControlState_  != EPeerState::FollowerRecovery) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
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
                                NRpc::EErrorCode::Unavailable,
                                "Changelog is already being rotated");
                        }

                        followerCommitter->SuspendLogging();

                        WaitFor(DecoratedAutomaton_->RotateChangelog())
                            .ThrowOnError();

                        followerCommitter->ResumeLogging();
                    } catch (const std::exception& ex) {
                        auto error = TError("Error rotating changelog")
                            << ex;
                        Restart(epochContext, error);
                        THROW_ERROR error;
                    }

                    break;
                }

                case EPeerState::FollowerRecovery: {
                    auto followerRecovery = epochContext->FollowerRecovery;
                    if (!followerRecovery) {
                        // NB: No restart.
                        THROW_ERROR_EXCEPTION(
                            NRpc::EErrorCode::Unavailable,
                            "Initial ping is not received yet");
                    }

                    try {
                        if (!followerRecovery->PostponeChangelogRotation(version)) {
                            BackoffPostpone();
                            again = true;
                            continue;
                        }
                    } catch (const std::exception& ex) {
                        auto error = TError("Error postponing changelog rotation during recovery")
                            << ex;
                        Restart(epochContext, error);
                        THROW_ERROR error;
                    }

                    break;
                }

                default:
                    Y_UNREACHABLE();
            }
        } while (again);

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
                NRpc::EErrorCode::Unavailable,
                "Not an active leader");
        }

        // Validate epoch id.
        GetEpochContext(epochId);

        auto version = DecoratedAutomaton_->GetCommittedVersion();

        context->SetResponseInfo("CommittedVersion: %v",
            version);

        response->set_committed_revision(version.ToRevision());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitMutation)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMutationRequest mutationRequest;
        mutationRequest.Type = request->type();
        if (request->has_mutation_id()) {
            mutationRequest.MutationId = FromProto<TMutationId>(request->mutation_id());
            mutationRequest.Retry = request->retry();
        }
        mutationRequest.Data = request->Attachments()[0];

        context->SetRequestInfo("MutationType: %v, MutationId: %v, Retry: %v",
            mutationRequest.Type,
            mutationRequest.MutationId,
            mutationRequest.Retry);

        CommitMutation(std::move(mutationRequest))
            .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                if (!result.IsOK()) {
                    context->Reply(result);
                    return;
                }

                const auto& mutationResponse = result.Value();
                response->Attachments() = mutationResponse.Data.ToVector();
                context->Reply();
            }));
    }


    i64 GetElectionPriority()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!ReachableVersion_) {
            THROW_ERROR_EXCEPTION("Election priority is not available");
        }

        auto version = ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::Following
            ? DecoratedAutomaton_->GetAutomatonVersion()
            : *ReachableVersion_;

        return version.ToRevision();
    }


    void Participate()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        CancelableControlInvoker_->Invoke(
            BIND(&TDistributedHydraManager::DoParticipate, MakeStrong(this)));
    }

    void IncrementRestartCounter(const TString& message)
    {
        auto it = RestartCounters_.find(message);
        if (it == RestartCounters_.end()) {
            auto tagIds = Options_.ProfilingTagIds;
            tagIds.push_back(NProfiling::TProfileManager::Get()->RegisterTag("reason", message));
            auto counter = NProfiling::TMonotonicCounter("/restart_count", tagIds);
            it = RestartCounters_.insert(std::make_pair(message, counter)).first;
        }

        Profiler.Increment(it->second);
    }

    void Restart(TEpochContextPtr epochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        bool expected = false;
        if (!epochContext->Restarting.compare_exchange_strong(expected, true)) {
            return;
        }

        LOG_WARNING(error, "Restarting Hydra instance");

        IncrementRestartCounter(error.GetMessage());

        CancelableControlInvoker_->Invoke(BIND(
            &TDistributedHydraManager::DoRestart,
            MakeWeak(this),
            epochContext));
    }


    void DoRestart(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ElectionManager_->Abandon();
    }

    void DoParticipate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Initializing persistent stores");

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

                auto asyncChangelogStore = ChangelogStoreFactory_->Lock();
                ChangelogStore_ = WaitFor(asyncChangelogStore)
                    .ValueOrThrow();

                auto changelogVersion = ChangelogStore_->GetReachableVersion();
                LOG_INFO("The latest changelog version is %v", changelogVersion);

                ReachableVersion_ = changelogVersion.SegmentId < maxSnapshotId
                    ? TVersion(maxSnapshotId, 0)
                    : changelogVersion;

                break;
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Error initializing persistent stores, backing off and retrying");
                TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            }
        }

        LOG_INFO("Reachable version is %v", ReachableVersion_);

        ElectionManager_->Participate();
    }

    void DoFinalize()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: Epoch invokers are already canceled so we don't expect any more callbacks to
        // go through the automaton invoker.

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

        LOG_INFO("Hydra instance finalized");
    }


    IChangelogPtr OpenChangelogOrThrow(int id)
    {
        if (!ChangelogStore_) {
            THROW_ERROR_EXCEPTION("Changelog store is not currently available");
        }
        return WaitFor(ChangelogStore_->OpenChangelog(id))
            .ValueOrThrow();
    }


    void OnCheckpointNeeded(const TWeakPtr<TEpochContext>& epochContext_, bool snapshotIsMandatory)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = epochContext_.Lock();
        if (!epochContext || !IsActiveLeader()) {
            return;
        }

        const auto& checkpointer = epochContext->Checkpointer;
        if (checkpointer->CanBuildSnapshot()) {
            BuildSnapshotAndWatch(epochContext, false);
        } else if (checkpointer->CanRotateChangelogs() && !snapshotIsMandatory) {
            LOG_WARNING("Cannot build a snapshot, just rotating changlogs");
            RotateChangelogAndWatch(epochContext);
        }
    }

    void OnCommitFailed(const TWeakPtr<TEpochContext>& epochContext_, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = epochContext_.Lock();
        if (!epochContext) {
            return;
        }

        auto wrappedError = TError("Error committing mutation")
            << error;
        Restart(epochContext, wrappedError);
    }

    void OnLeaderLeaseLost(const TWeakPtr<TEpochContext>& epochContext_, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto epochContext = epochContext_.Lock();
        if (!epochContext) {
            return;
        }

        auto wrappedError = TError("Leader lease is lost")
            << error;
        Restart(epochContext, wrappedError);
    }


    void RotateChangelogAndWatch(TEpochContextPtr epochContext)
    {
        auto changelogResult = epochContext->Checkpointer->RotateChangelog();
        WatchChangelogRotation(epochContext, changelogResult);
    }

    TFuture<TRemoteSnapshotParams> BuildSnapshotAndWatch(TEpochContextPtr epochContext, bool setReadOnly)
    {
        TFuture<void> changelogResult;
        TFuture<TRemoteSnapshotParams> snapshotResult;
        std::tie(changelogResult, snapshotResult) = epochContext->Checkpointer->BuildSnapshot(setReadOnly);
        WatchChangelogRotation(epochContext, changelogResult);
        return snapshotResult;
    }

    void WatchChangelogRotation(TEpochContextPtr epochContext, TFuture<void> result)
    {
        result.Subscribe(BIND(
            &TDistributedHydraManager::OnChangelogRotated,
            MakeWeak(this),
            MakeWeak(epochContext)));
    }

    void OnChangelogRotated(const TWeakPtr<TEpochContext>& epochContext_, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto epochContext = epochContext_.Lock();
        if (!epochContext) {
            return;
        }

        if (!error.IsOK()) {
            auto wrappedError = TError("Distributed changelog rotation failed")
                << error;
            Restart(epochContext, wrappedError);
            return;
        }

        LOG_INFO("Distributed changelog rotation succeeded");
    }


    void OnElectionStartLeading(NElection::TEpochContextPtr electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting leader recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::LeaderRecovery;

        auto epochContext = StartEpoch(electionEpochContext);

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
            Options_,
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
            Options_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext->LeaderCommitter,
            SnapshotStore_,
            epochContext.Get());

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_ = epochContext;
        DecoratedAutomaton_->OnStartLeading(epochContext);
        StartLeading_.Fire();
        epochContext->LeaseTracker->Start();

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
                Options_,
                CellManager_,
                DecoratedAutomaton_,
                ChangelogStore_,
                SnapshotStore_,
                Options_.ResponseKeeper,
                epochContext.Get());

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            auto asyncRecoveryResult = epochContext->LeaderRecovery->Run();
            WaitFor(asyncRecoveryResult)
                .ThrowOnError();

            DecoratedAutomaton_->OnLeaderRecoveryComplete();
            LeaderRecoveryComplete_.Fire();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            YCHECK(ControlState_ == EPeerState::LeaderRecovery);
            ControlState_ = EPeerState::Leading;

            LOG_INFO("Leader recovery completed");

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

            epochContext->HeartbeatMutationCommitExecutor->Start();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            SystemLockGuard_.Release();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Leader recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            Restart(epochContext, ex);
        }
    }

    void OnElectionStopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Stopped leading");

        // Save for later to respect the thread affinity.
        auto leaderCommitter = ControlEpochContext_->LeaderCommitter;

        StopEpoch();

        YCHECK(ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery);
        ControlState_ = EPeerState::Elections;

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_.Reset();
        leaderCommitter->Stop();
        DecoratedAutomaton_->OnStopLeading();
        StopLeading_.Fire();

        Participate();
    }


    void OnElectionStartFollowing(NElection::TEpochContextPtr electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting follower recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::FollowerRecovery;

        auto epochContext = StartEpoch(electionEpochContext);

        epochContext->FollowerCommitter = New<TFollowerCommitter>(
            Config_,
            Options_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext.Get());

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_ = epochContext;
        DecoratedAutomaton_->OnStartFollowing(epochContext);
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

            LOG_INFO("Follower recovery completed");

            DecoratedAutomaton_->OnFollowerRecoveryComplete();
            FollowerRecoveryComplete_.Fire();

            FollowerRecovered_ = true;
            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            SystemLockGuard_.Release();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Follower recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            Restart(epochContext, ex);
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

        SystemLockGuard_.Release();
    }

    void CheckForInitialPing(TVersion pingVersion)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(ControlState_ == EPeerState::FollowerRecovery);

        auto epochContext = ControlEpochContext_;

        // Check if initial ping is already received.
        if (epochContext->FollowerRecovery) {
            return;
        }

        // Check if the logged version at leader is lower than our reachable (logged) version.
        // This is a rare case but could happen at least in the following two scenarios:
        // 1) When a follower restarts rapid enough and appears
        // (for some limited time frame) ahead of the leader w.r.t. the current changelog.
        // 2) When the quorum gets broken during changelog rotation
        // and some follower joins the a newly established (and still recovering!) quorum
        // with an empty changelog that nobody else has.
        auto reachableVersion = epochContext->ReachableVersion;
        if (pingVersion < reachableVersion) {
            LOG_DEBUG("Received initial ping from leader with a stale version; ignored (LeaderVersion: %v, ReachableVersion: %v)",
                pingVersion,
                epochContext->ReachableVersion);
            return;
        }

        LOG_INFO("Received initial ping from leader (LeaderVersion: %v)",
            pingVersion);

        epochContext->FollowerRecovery = New<TFollowerRecovery>(
            Config_,
            Options_,
            CellManager_,
            DecoratedAutomaton_,
            ChangelogStore_,
            SnapshotStore_,
            Options_.ResponseKeeper,
            epochContext.Get(),
            pingVersion);

        epochContext->EpochControlInvoker->Invoke(
            BIND(&TDistributedHydraManager::RecoverFollower, MakeStrong(this)));
    }


    TEpochContextPtr StartEpoch(NElection::TEpochContextPtr electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = New<TEpochContext>();
        epochContext->ChangelogStore = ChangelogStore_;
        epochContext->ReachableVersion = *ReachableVersion_;
        epochContext->LeaderId = electionEpochContext->LeaderId;
        epochContext->EpochId = electionEpochContext->EpochId;
        epochContext->CancelableContext = electionEpochContext->CancelableContext;
        epochContext->EpochControlInvoker = epochContext->CancelableContext->CreateInvoker(CancelableControlInvoker_);
        epochContext->EpochSystemAutomatonInvoker = epochContext->CancelableContext->CreateInvoker(DecoratedAutomaton_->GetSystemInvoker());
        epochContext->EpochUserAutomatonInvoker = epochContext->CancelableContext->CreateInvoker(AutomatonInvoker_);
        epochContext->HeartbeatMutationCommitExecutor = New<TPeriodicExecutor>(
            epochContext->EpochUserAutomatonInvoker,
            BIND(&TDistributedHydraManager::OnHeartbeatMutationCommit, MakeWeak(this)),
            Config_->HeartbeatMutationPeriod);

        YCHECK(!ControlEpochContext_);
        ControlEpochContext_ = epochContext;

        SystemLockGuard_ = TSystemLockGuard::Acquire(DecoratedAutomaton_);

        return ControlEpochContext_;
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

        ChangelogStore_.Reset();
        ReachableVersion_.reset();
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


    void OnUpsteamSyncDeadlineReached(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        epochContext->UpstreamSyncDeadlineReached = true;

        if (!epochContext->ActiveUpstreamSyncPromise) {
            DoSyncWithUpstream(epochContext);
        }
    }

    void DoSyncWithUpstream(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        LOG_DEBUG("Synchronizing with upstream");

        epochContext->UpstreamSyncDeadlineReached = false;
        epochContext->UpstreamSyncStartTime = NProfiling::GetCpuInstant();

        YCHECK(!epochContext->ActiveUpstreamSyncPromise);
        swap(epochContext->ActiveUpstreamSyncPromise, epochContext->PendingUpstreamSyncPromise);

        std::vector<TFuture<void>> asyncResults;
        if (GetAutomatonState() == EPeerState::Following) {
            asyncResults.push_back(DoSyncWithLeader(epochContext));
        }
        for (const auto& callback : UpstreamSync_.ToVector()) {
            asyncResults.push_back(callback.Run());
        }

        CombineAll(asyncResults).Subscribe(
            BIND(&TDistributedHydraManager::OnUpstreamSyncReached, MakeStrong(this), MakeWeak(epochContext))
                .Via(epochContext->EpochUserAutomatonInvoker));
    }

    void OnUpstreamSyncReached(const TWeakPtr<TEpochContext>& epochContext_, const TErrorOr<std::vector<TError>>& resultsOrError)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = epochContext_.Lock();
        if (!epochContext) {
            return;
        }

        TError combinedError;
        if (resultsOrError.IsOK()) {
            for (const auto& error : resultsOrError.Value()) {
                if (!error.IsOK()) {
                    if (combinedError.IsOK()) {
                        combinedError = TError(
                            NRpc::EErrorCode::Unavailable,
                            "Error synchronizing with upstream");
                    }
                    combinedError.InnerErrors().push_back(error);
                }
            }
        } else {
            combinedError = resultsOrError;
        }

        if (combinedError.IsOK()) {
            LOG_DEBUG("Upstream synchronization complete");
            auto syncTime = NProfiling::CpuDurationToDuration(
                NProfiling::GetCpuInstant() -
                epochContext->UpstreamSyncStartTime);
            Profiler.Enqueue("/upstream_sync_time", syncTime.MilliSeconds(), NProfiling::EMetricType::Gauge);
        }

        epochContext->ActiveUpstreamSyncPromise.Set(combinedError);
        epochContext->ActiveUpstreamSyncPromise.Reset();

        if (epochContext->UpstreamSyncDeadlineReached) {
            DoSyncWithUpstream(epochContext);
        }
    }

    TFuture<void> DoSyncWithLeader(TEpochContextPtr epochContext)
    {
        LOG_DEBUG("Synchronizing with leader");

        YCHECK(!epochContext->LeaderSyncPromise);
        epochContext->LeaderSyncPromise = NewPromise<void>();

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

        return epochContext->LeaderSyncPromise;
    }

    void OnSyncWithLeaderResponse(
        TEpochContextPtr epochContext,
        const THydraServiceProxy::TErrorOrRspSyncWithLeaderPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!rspOrError.IsOK()) {
            epochContext->LeaderSyncPromise.Set(TError(
                NRpc::EErrorCode::Unavailable,
                "Failed to synchronize with leader")
                << rspOrError);
            epochContext->LeaderSyncPromise.Reset();
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto committedVersion = TVersion::FromRevision(rsp->committed_revision());

        LOG_DEBUG("Received synchronization response from leader (CommittedVersion: %v)",
            committedVersion);

        YCHECK(!epochContext->LeaderSyncVersion);
        epochContext->LeaderSyncVersion = committedVersion;
        DecoratedAutomaton_->CommitMutations(committedVersion, true);
        CheckForPendingLeaderSync(epochContext);
    }

    void CheckForPendingLeaderSync(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!epochContext->LeaderSyncPromise || !epochContext->LeaderSyncVersion)
            return;

        auto neededCommittedVersion = *epochContext->LeaderSyncVersion;
        auto actualCommittedVersion = DecoratedAutomaton_->GetAutomatonVersion();
        if (neededCommittedVersion > actualCommittedVersion)
            return;

        LOG_DEBUG("Leader synchronization complete (NeededCommittedVersion: %v, ActualCommittedVersion: %v)",
            neededCommittedVersion,
            actualCommittedVersion);

        epochContext->LeaderSyncPromise.Set();
        epochContext->LeaderSyncPromise.Reset();
        epochContext->LeaderSyncVersion.reset();
    }


    void CommitMutationsAtFollower(TEpochContextPtr epochContext, TVersion committedVersion)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->CommitMutations(committedVersion, true);
        CheckForPendingLeaderSync(std::move(epochContext));
    }


    void OnHeartbeatMutationCommit()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        LOG_DEBUG("Committing heartbeat mutation");

        // Fire-and-forget.
        CommitMutation(TMutationRequest());
    }


    bool GetReadOnly() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ReadOnly_;
    }

    void SetReadOnly(bool value)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!value) {
            return;
        }

        bool expected = false;
        if (ReadOnly_.compare_exchange_strong(expected, true)) {
            LOG_INFO("Read-only mode activated");
        }
    }


    void BackoffPostpone()
    {
        LOG_DEBUG("Cannot postpone more actions at the moment; backing off and retrying");
        TDelayedExecutor::WaitForDuration(PostponeBackoffTime);
        SwitchTo(ControlInvoker_);
    }


    // THydraServiceBase overrides.
    virtual IHydraManagerPtr GetHydraManager() override
    {
        return this;
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

DEFINE_REFCOUNTED_TYPE(TDistributedHydraManager)

////////////////////////////////////////////////////////////////////////////////

IHydraManagerPtr CreateDistributedHydraManager(
    TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    IAutomatonPtr automaton,
    IServerPtr rpcServer,
    IElectionManagerPtr electionManager,
    TCellManagerPtr cellManager,
    IChangelogStoreFactoryPtr changelogStoreFactory,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options)
{
    YCHECK(config);
    YCHECK(controlInvoker);
    YCHECK(automatonInvoker);
    YCHECK(automaton);
    YCHECK(rpcServer);
    YCHECK(electionManager);
    YCHECK(cellManager);
    YCHECK(changelogStoreFactory);
    YCHECK(snapshotStore);

    return New<TDistributedHydraManager>(
        config,
        controlInvoker,
        automatonInvoker,
        automaton,
        rpcServer,
        electionManager,
        cellManager,
        changelogStoreFactory,
        snapshotStore,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
