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

#include <yt/server/lib/election/election_manager.h>

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

        virtual void OnAlivePeerSetChanged(const TPeerIdSet& alivePeers) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionAlivePeerSetChanged,
                Owner_,
                alivePeers));
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Poke)
            .SetInvoker(DecoratedAutomaton_->GetDefaultGuardedUserInvoker()));
    }

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ != EPeerState::None) {
            return;
        }

        DecoratedAutomaton_->Initialize();

        RpcServer_->RegisterService(this);

        YT_LOG_INFO("Hydra instance initialized (SelfAddress: %v, SelfId: %v)",
            CellManager_->GetSelfConfig(),
            CellManager_->GetSelfPeerId());

        ControlState_ = EPeerState::Elections;

        ProfileRestart("Initialization");

        Participate();
    }

    virtual TFuture<void> Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped) {
            return VoidFuture;
        }

        YT_LOG_INFO("Hydra instance is finalizing");

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

        return DecoratedAutomaton_->GetState() == EPeerState::Leading && LeaderRecovered_ && LeaderLease_->IsValid();
    }

    virtual bool IsActiveFollower() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->GetState() == EPeerState::Following && FollowerRecovered_;
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

        return BuildSnapshotAndWatch(setReadOnly).Apply(
            BIND([] (const TRemoteSnapshotParams& params) {
                return params.SnapshotId;
            }));
    }

    void ValidateSnapshot(IAsyncZeroCopyInputStreamPtr reader) override
    {
        DecoratedAutomaton_->ValidateSnapshot(reader);
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

    virtual TPeerIdSet& AlivePeers() override
    {
        return AlivePeers_;
    }

    virtual TFuture<void> SyncWithUpstream() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto epochContext = DecoratedAutomaton_->GetEpochContext();
        if (!epochContext || !IsActive()) {
            return MakeFuture(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active peer"));
        }

        if (epochContext->LeaderId == CellManager_->GetSelfPeerId() && UpstreamSync_.Empty()) {
            // NB: Leader lease is already checked in IsActive.
            return VoidFuture;
        }

        return epochContext->UpstreamSyncBatcher->Run();
    }

    virtual TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: This is monotonic: once in read-only mode, cannot leave it.
        if (ReadOnly_) {
            return MakeFuture<TMutationResponse>(TError(
                NRpc::EErrorCode::Unavailable,
                "Read-only mode is active"));
        }

        auto state = GetAutomatonState();
        switch (state) {
            case EPeerState::Leading:
                if (!LeaderRecovered_) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader has not yet recovered"));
                }

                if (!LeaderLease_->IsValid() || AutomatonEpochContext_->LeaderLeaseExpired) {
                    auto error = TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader lease is no longer valid");
                    // Ensure monotonicity: once Hydra rejected a mutation, no more mutations are accepted.
                    AutomatonEpochContext_->LeaderLeaseExpired = true;
                    Restart(AutomatonEpochContext_, error);
                    return MakeFuture<TMutationResponse>(error);
                }

                return AutomatonEpochContext_->LeaderCommitter->Commit(std::move(request));

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

                return AutomatonEpochContext_->FollowerCommitter->Forward(std::move(request));

            default:
                return MakeFuture<TMutationResponse>(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Peer is in %Qlv state",
                    state));
        }
    }

    virtual TReign GetCurrentReign() override
    {
        return DecoratedAutomaton_->GetCurrentReign();
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

    DEFINE_SIGNAL(void (const TPeerIdSet&), AlivePeerSetChanged);

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
    NProfiling::TAggregateGauge UpstreamSyncTimeGauge_{"/upstream_sync_time"};

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

    TPeerIdSet AlivePeers_;

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

        YT_VERIFY(startRecordId >= 0);
        YT_VERIFY(recordCount >= 0);

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

            auto epochContext = GetControlEpochContext(epochId);

            switch (ControlState_) {
                case EPeerState::Following: {
                    SwitchTo(epochContext->EpochUserAutomatonInvoker);
                    VERIFY_THREAD_AFFINITY(AutomatonThread);

                    CommitMutationsAtFollower(committedVersion);

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
                    YT_ABORT();
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
        auto alivePeers = FromProto<TPeerIdSet>(request->alive_peers());
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

        auto epochContext = GetControlEpochContext(epochId);

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
                YT_ABORT();
        }

        if (alivePeers != AlivePeers_) {
            AlivePeers_ = std::move(alivePeers);
            AlivePeerSetChanged_.Fire(AlivePeers_);
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

        auto epochContext = GetControlEpochContext(epochId);

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

            auto epochContext = GetControlEpochContext(epochId);

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
                    YT_ABORT();
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
        GetControlEpochContext(epochId);

        auto version = DecoratedAutomaton_->GetCommittedVersion();

        context->SetResponseInfo("CommittedVersion: %v",
            version);

        response->set_committed_revision(version.ToRevision());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitMutation)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto mutationRequest = TMutationRequest(request->reign());
        mutationRequest.Type = request->type();
        if (request->has_mutation_id()) {
            mutationRequest.MutationId = FromProto<TMutationId>(request->mutation_id());
            mutationRequest.Retry = request->retry();
        }
        mutationRequest.Data = request->Attachments()[0];

        // COMPAT(savrus) Fix heartbeats from old participants.
        if (mutationRequest.Type != HeartbeatMutationType && !mutationRequest.Reign) {
            mutationRequest.Reign = GetCurrentReign();
        }

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

    DECLARE_RPC_SERVICE_METHOD(NProto, Poke)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        context->SetRequestInfo();

        CommitMutation(TMutationRequest(GetCurrentReign()))
            .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                context->Reply(result);
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

    void ProfileRestart(const TString& message)
    {
        auto tagIds = Options_.ProfilingTagIds;
        tagIds.push_back(NProfiling::TProfileManager::Get()->RegisterTag("reason", message));

        Profiler.Enqueue(
            "/restart_count",
            1,
            NProfiling::EMetricType::Gauge,
            tagIds);
    }

    void Restart(const TEpochContextPtr& epochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (epochContext->Restarting.test_and_set()) {
            return;
        }

        YT_LOG_DEBUG(error, "Requesting Hydra instance restart");

        CancelableControlInvoker_->Invoke(BIND(
            &TDistributedHydraManager::DoRestart,
            MakeWeak(this),
            epochContext,
            error));
    }

    void Restart(const TWeakPtr<TEpochContext>& weakEpochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto epochContext = weakEpochContext.Lock()) {
            Restart(epochContext, error);
        }
    }

    void DoRestart(const TEpochContextPtr& epochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlEpochContext_ != epochContext) {
            return;
        }

        YT_LOG_WARNING(error, "Restarting Hydra instance");

        ProfileRestart(error.GetMessage());

        ElectionManager_->Abandon();
    }

    void DoParticipate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Initializing persistent stores");

        while (true) {
            try {
                auto asyncMaxSnapshotId = SnapshotStore_->GetLatestSnapshotId();
                int maxSnapshotId = WaitFor(asyncMaxSnapshotId)
                    .ValueOrThrow();

                if (maxSnapshotId == InvalidSegmentId) {
                    YT_LOG_INFO("No snapshots found");
                    // Let's pretend we have snapshot 0.
                    maxSnapshotId = 0;
                } else {
                    YT_LOG_INFO("The latest snapshot is %v", maxSnapshotId);
                }

                ChangelogStore_ = WaitFor(ChangelogStoreFactory_->Lock())
                    .ValueOrThrow();

                auto changelogVersion = ChangelogStore_->GetReachableVersion();
                YT_LOG_INFO("The latest changelog version is %v", changelogVersion);

                ReachableVersion_ = changelogVersion.SegmentId < maxSnapshotId
                    ? TVersion(maxSnapshotId, 0)
                    : changelogVersion;

                break;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error initializing persistent stores, backing off and retrying");
                TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            }
        }

        YT_LOG_INFO("Reachable version is %v", ReachableVersion_);

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

        ResetAutomatonEpochContext();

        YT_LOG_INFO("Hydra instance finalized");
    }


    IChangelogPtr OpenChangelogOrThrow(int id)
    {
        if (!ChangelogStore_) {
            THROW_ERROR_EXCEPTION("Changelog store is not currently available");
        }
        return WaitFor(ChangelogStore_->OpenChangelog(id))
            .ValueOrThrow();
    }


    void OnCheckpointNeeded(bool snapshotIsMandatory)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto checkpointer = AutomatonEpochContext_->Checkpointer;
        if (checkpointer->CanBuildSnapshot()) {
            BuildSnapshotAndWatch(false);
        } else if (checkpointer->CanRotateChangelogs() && !snapshotIsMandatory) {
            YT_LOG_WARNING("Cannot build a snapshot, just rotating changelogs");
            RotateChangelogAndWatch();
        }
    }

    void OnCommitFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto wrappedError = TError("Error committing mutation")
            << error;
        Restart(AutomatonEpochContext_, wrappedError);
    }

    void OnLoggingFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto wrappedError = TError("Error logging mutations")
            << error;
        Restart(AutomatonEpochContext_, wrappedError);
    }

    void OnLeaderLeaseLost(const TWeakPtr<TEpochContext>& weakEpochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto wrappedError = TError("Leader lease is lost")
            << error;
        Restart(weakEpochContext, wrappedError);
    }


    void RotateChangelogAndWatch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto changelogResult = AutomatonEpochContext_->Checkpointer->RotateChangelog();
        WatchChangelogRotation(changelogResult);
    }

    TFuture<TRemoteSnapshotParams> BuildSnapshotAndWatch(bool setReadOnly)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TFuture<void> changelogResult;
        TFuture<TRemoteSnapshotParams> snapshotResult;
        std::tie(changelogResult, snapshotResult) = AutomatonEpochContext_->Checkpointer->BuildSnapshot(setReadOnly);
        WatchChangelogRotation(changelogResult);
        return snapshotResult;
    }

    void WatchChangelogRotation(TFuture<void> result)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        result.Subscribe(BIND(
            &TDistributedHydraManager::OnChangelogRotated,
            MakeWeak(this),
            MakeWeak(AutomatonEpochContext_)));
    }

    void OnChangelogRotated(const TWeakPtr<TEpochContext>& weakEpochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!error.IsOK()) {
            auto wrappedError = TError("Distributed changelog rotation failed")
                << error;
            Restart(weakEpochContext, wrappedError);
            return;
        }

        YT_LOG_INFO("Distributed changelog rotation succeeded");
    }


    void OnElectionStartLeading(const NElection::TEpochContextPtr& electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting leader recovery");

        YT_VERIFY(ControlState_ == EPeerState::Elections);
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
            BIND(&TDistributedHydraManager::OnCheckpointNeeded, MakeWeak(this)));
        epochContext->LeaderCommitter->SubscribeCommitFailed(
            BIND(&TDistributedHydraManager::OnCommitFailed, MakeWeak(this)));
        epochContext->LeaderCommitter->SubscribeLoggingFailed(
            BIND(&TDistributedHydraManager::OnLoggingFailed, MakeWeak(this)));

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

        YT_VERIFY(!AutomatonEpochContext_);
        AutomatonEpochContext_ = epochContext;

        DecoratedAutomaton_->OnStartLeading(epochContext);

        StartLeading_.Fire();

        epochContext->LeaseTracker->SetAlivePeers(GetAllPeers());
        epochContext->LeaseTracker->Start();

        SwitchTo(epochContext->EpochControlInvoker);
        VERIFY_THREAD_AFFINITY(ControlThread);

        RecoverLeader();
    }

    TPeerIdSet GetAllPeers()
    {
        TPeerIdSet result;
        result.reserve(CellManager_->GetTotalPeerCount());
        for (TPeerId id = 0; id < CellManager_->GetTotalPeerCount(); ++id) {
            result.insert(id);
        }
        return result;
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

            YT_VERIFY(ControlState_ == EPeerState::LeaderRecovery);
            ControlState_ = EPeerState::Leading;

            YT_LOG_INFO("Leader recovery completed");

            YT_LOG_INFO("Waiting for leader lease");

            WaitFor(epochContext->LeaseTracker->GetLeaseAcquired())
                .ThrowOnError();

            YT_LOG_INFO("Leader lease acquired");

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            WaitFor(epochContext->Checkpointer->RotateChangelog())
                .ThrowOnError();

            YT_LOG_INFO("Initial changelog rotated");

            ApplyFinalRecoveryAction(true);

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
            YT_LOG_WARNING(ex, "Leader recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            Restart(epochContext, ex);
        }
    }

    void OnElectionAlivePeerSetChanged(const TPeerIdSet& alivePeers)
    {
        AlivePeers_ = alivePeers;
        // Send the change to the followers.
        ControlEpochContext_->LeaseTracker->SetAlivePeers(alivePeers);
        // Fire the event here, on the leader.
        AlivePeerSetChanged_.Fire(alivePeers);
    }

    void OnElectionStopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Stopped leading");

        // Save for later to respect the thread affinity.
        auto leaderCommitter = ControlEpochContext_->LeaderCommitter;

        StopEpoch();

        YT_VERIFY(ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery);
        ControlState_ = EPeerState::Elections;

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ResetAutomatonEpochContext();

        leaderCommitter->Stop();

        DecoratedAutomaton_->OnStopLeading();

        StopLeading_.Fire();

        Participate();
    }


    void OnElectionStartFollowing(const NElection::TEpochContextPtr& electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting follower recovery");

        YT_VERIFY(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::FollowerRecovery;

        auto epochContext = StartEpoch(electionEpochContext);

        epochContext->FollowerCommitter = New<TFollowerCommitter>(
            Config_,
            Options_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext.Get());
        epochContext->FollowerCommitter->SubscribeLoggingFailed(
            BIND(&TDistributedHydraManager::OnLoggingFailed, MakeWeak(this)));

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(!AutomatonEpochContext_);
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

            YT_VERIFY(ControlState_ == EPeerState::FollowerRecovery);
            ControlState_ = EPeerState::Following;

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            YT_LOG_INFO("Follower recovery completed");

            DecoratedAutomaton_->OnFollowerRecoveryComplete();
            FollowerRecoveryComplete_.Fire();

            ApplyFinalRecoveryAction(false);

            FollowerRecovered_ = true;
            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            SystemLockGuard_.Release();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Follower recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            Restart(epochContext, ex);
        }
    }

    void OnElectionStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Stopped following");

        // Save for later to respect the thread affinity.
        auto followerCommitter = ControlEpochContext_->FollowerCommitter;

        StopEpoch();

        YT_VERIFY(ControlState_ == EPeerState::Following || ControlState_ == EPeerState::FollowerRecovery);
        ControlState_ = EPeerState::Elections;

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ResetAutomatonEpochContext();

        followerCommitter->Stop();

        DecoratedAutomaton_->OnStopFollowing();

        StopFollowing_.Fire();

        Participate();

        SystemLockGuard_.Release();
    }

    void ApplyFinalRecoveryAction(bool isLeader)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto finalAction = DecoratedAutomaton_->GetFinalRecoveryAction();
        YT_LOG_INFO("Applying final recovery action (FinalRecoveryAction: %v)",
            finalAction);

        switch (finalAction) {
            case EFinalRecoveryAction::None:
                break;

            case EFinalRecoveryAction::BuildSnapshotAndRestart:
                SetReadOnly(true);
                if (isLeader || Options_.WriteSnapshotsAtFollowers) {
                    DecoratedAutomaton_->RotateAutomatonVersionAfterRecovery();
                    WaitFor(DecoratedAutomaton_->BuildSnapshot())
                        .ThrowOnError();
                    YT_LOG_INFO("Compatibility snapshot saved, stopping Hydra instance");
                }
                SwitchTo(ControlEpochContext_->EpochControlInvoker);
                WaitFor(Finalize())
                    .ThrowOnError();
                // Unreachable because Finalize stops epoch executor.
                YT_ABORT();

            default:
                YT_ABORT();
        }
    }

    void CheckForInitialPing(TVersion pingVersion)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(ControlState_ == EPeerState::FollowerRecovery);

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
            YT_LOG_DEBUG("Received initial ping from leader with a stale version; ignored (LeaderVersion: %v, ReachableVersion: %v)",
                pingVersion,
                epochContext->ReachableVersion);
            return;
        }

        YT_LOG_INFO("Received initial ping from leader (LeaderVersion: %v)",
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


    TEpochContextPtr StartEpoch(const NElection::TEpochContextPtr& electionEpochContext)
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
        epochContext->UpstreamSyncBatcher = New<TAsyncBatcher<void>>(
            BIND(&TDistributedHydraManager::DoSyncWithUpstream, MakeWeak(this), MakeWeak(epochContext)),
            Config_->UpstreamSyncDelay);

        YT_VERIFY(!ControlEpochContext_);
        ControlEpochContext_ = epochContext;

        SystemLockGuard_ = TSystemLockGuard::Acquire(DecoratedAutomaton_);

        return ControlEpochContext_;
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResetControlEpochContext();

        LeaderLease_->Invalidate();

        LeaderRecovered_ = false;
        FollowerRecovered_ = false;

        SystemLockGuard_.Release();

        if (ChangelogStore_) {
            ChangelogStore_->Abort();
            ChangelogStore_.Reset();
        }

        ReachableVersion_.reset();
    }

    TEpochContextPtr GetControlEpochContext(TEpochId epochId)
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

    void ResetControlEpochContext()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!ControlEpochContext_) {
            return;
        }

        ControlEpochContext_->CancelableContext->Cancel();

        ControlEpochContext_.Reset();
    }

    void ResetAutomatonEpochContext()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!AutomatonEpochContext_) {
            return;
        }

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
        AutomatonEpochContext_->UpstreamSyncBatcher->Cancel(error);
        if (AutomatonEpochContext_->LeaderSyncPromise) {
            AutomatonEpochContext_->LeaderSyncPromise.TrySet(error);
        }

        AutomatonEpochContext_.Reset();
    }


    static TFuture<void> DoSyncWithUpstream(
        const TWeakPtr<TDistributedHydraManager>& weakThis,
        const TWeakPtr<TEpochContext>& weakEpochContext)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto this_ = weakThis.Lock();
        auto epochContext = weakEpochContext.Lock();
        if (!this_ || !epochContext) {
            return MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));
        }

        const auto& Logger = this_->Logger;
        YT_LOG_DEBUG("Synchronizing with upstream");

        epochContext->UpstreamSyncTimer.Restart();

        return BIND(&TDistributedHydraManager::DoSyncWithUpstreamCore, this_, epochContext)
            .AsyncVia(epochContext->EpochUserAutomatonInvoker)
            .Run();
    }

    TFuture<void> DoSyncWithUpstreamCore(const TEpochContextPtr& epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<TFuture<void>> asyncResults;
        if (GetAutomatonState() == EPeerState::Following) {
            asyncResults.push_back(DoSyncWithLeader());
        }
        for (const auto& callback : UpstreamSync_.ToVector()) {
            asyncResults.push_back(callback.Run());
        }

        // NB: Many subscribers are typically waiting for the upstream sync to complete.
        // Make sure the promise is set in a large thread pool.
        return CombineAll(asyncResults).Apply(
            BIND(&TDistributedHydraManager::OnUpstreamSyncReached, MakeStrong(this), epochContext)
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    void OnUpstreamSyncReached(
        const TEpochContextPtr& epochContext,
        const TErrorOr<std::vector<TError>>& resultsOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& results = resultsOrError.ValueOrThrow();
        TError combinedError;
        for (const auto& error : results) {
            if (!error.IsOK()) {
                if (combinedError.IsOK()) {
                    combinedError = TError(
                        NRpc::EErrorCode::Unavailable,
                        "Error synchronizing with upstream");
                }
                combinedError.InnerErrors().push_back(error);
            }
        }
        combinedError.ThrowOnError();

        YT_LOG_DEBUG("Upstream synchronization complete");
        Profiler.Update(UpstreamSyncTimeGauge_, epochContext->UpstreamSyncTimer.GetElapsedValue());
    }

    TFuture<void> DoSyncWithLeader()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_DEBUG("Synchronizing with leader");

        auto epochContext = AutomatonEpochContext_;

        YT_VERIFY(!epochContext->LeaderSyncPromise);
        epochContext->LeaderSyncPromise = NewPromise<void>();

        auto channel = CellManager_->GetPeerChannel(epochContext->LeaderId);
        YT_VERIFY(channel);

        THydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

        auto req = proxy.SyncWithLeader();
        ToProto(req->mutable_epoch_id(), epochContext->EpochId);

        req->Invoke().Subscribe(
            BIND(&TDistributedHydraManager::OnSyncWithLeaderResponse, MakeStrong(this))
                .Via(epochContext->EpochUserAutomatonInvoker));

        return epochContext->LeaderSyncPromise;
    }

    void OnSyncWithLeaderResponse(const THydraServiceProxy::TErrorOrRspSyncWithLeaderPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = AutomatonEpochContext_;

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

        YT_LOG_DEBUG("Received synchronization response from leader (CommittedVersion: %v)",
            committedVersion);

        YT_VERIFY(!epochContext->LeaderSyncVersion);
        epochContext->LeaderSyncVersion = committedVersion;
        DecoratedAutomaton_->CommitMutations(committedVersion, true);
        CheckForPendingLeaderSync();
    }

    void CheckForPendingLeaderSync()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = AutomatonEpochContext_;

        if (!epochContext->LeaderSyncPromise || !epochContext->LeaderSyncVersion) {
            return;
        }

        auto neededCommittedVersion = *epochContext->LeaderSyncVersion;
        auto actualCommittedVersion = DecoratedAutomaton_->GetAutomatonVersion();
        if (neededCommittedVersion > actualCommittedVersion) {
            return;
        }

        YT_LOG_DEBUG("Leader synchronization complete (NeededCommittedVersion: %v, ActualCommittedVersion: %v)",
            neededCommittedVersion,
            actualCommittedVersion);

        epochContext->LeaderSyncPromise.Set();
        epochContext->LeaderSyncPromise.Reset();
        epochContext->LeaderSyncVersion.reset();
    }


    void CommitMutationsAtFollower(TVersion committedVersion)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->CommitMutations(committedVersion, true);
        CheckForPendingLeaderSync();
    }


    void OnHeartbeatMutationCommit()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (GetReadOnly()) {
            return;
        }

        YT_LOG_DEBUG("Committing heartbeat mutation");

        CommitMutation(TMutationRequest(GetCurrentReign()))
            .WithTimeout(Config_->HeartbeatMutationTimeout)
            .Subscribe(BIND([=, this_ = MakeStrong(this), weakEpochContext = MakeWeak(AutomatonEpochContext_)] (const TErrorOr<TMutationResponse>& result){
                if (result.IsOK()) {
                    YT_LOG_DEBUG("Heartbeat mutation commit succeeded");
                } else if (!GetReadOnly()) {
                    Restart(
                        weakEpochContext,
                        TError("Heartbeat mutation commit failed") << result);
                }
            }));
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
            YT_LOG_INFO("Read-only mode activated");
        }
    }


    void BackoffPostpone()
    {
        YT_LOG_DEBUG("Cannot postpone more actions at the moment; backing off and retrying");
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
    YT_VERIFY(config);
    YT_VERIFY(controlInvoker);
    YT_VERIFY(automatonInvoker);
    YT_VERIFY(automaton);
    YT_VERIFY(rpcServer);
    YT_VERIFY(electionManager);
    YT_VERIFY(cellManager);
    YT_VERIFY(changelogStoreFactory);
    YT_VERIFY(snapshotStore);

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
