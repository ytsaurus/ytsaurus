#include "distributed_hydra_manager.h"
#include "private.h"
#include "changelog_acquisition.h"
#include "decorated_automaton.h"
#include "lease_tracker.h"
#include "mutation_committer.h"
#include "recovery.h"
#include "changelog_discovery.h"

#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/hydra_service.h>
#include <yt/yt/server/lib/hydra_common/mutation_context.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>
#include <yt/yt/server/lib/hydra_common/snapshot_discovery.h>
#include <yt/yt/server/lib/hydra_common/state_hash_checker.h>
#include <yt/yt/server/lib/hydra_common/private.h>
#include <yt/yt/server/lib/hydra_common/serialize.h>

#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/config.h>
#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <util/generic/cast.h>

#include <atomic>

namespace NYT::NHydra2 {

using namespace NElection;
using namespace NLogging;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NHydra;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TEpochIdInjectingInvoker
    : public TInvokerWrapper
{
public:
    TEpochIdInjectingInvoker(IInvokerPtr underlying, TEpochId epochId)
        : TInvokerWrapper(std::move(underlying))
        , EpochId_(epochId)
    { }

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND([callback = std::move(callback), epochId = EpochId_] {
            *CurrentEpochId = epochId;
            callback();
        }));
    }

private:
    const TEpochId EpochId_;
};

DECLARE_REFCOUNTED_CLASS(TDistributedHydraManager)

DEFINE_ENUM(EGraceDelayStatus,
    (None)
    (GraceDelayDisabled)
    (GraceDelayExecuted)
    (PreviousLeaseAbandoned)
);

class TDistributedHydraManager
    : public IDistributedHydraManager
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

        void OnStartLeading(NElection::TEpochContextPtr epochContext) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStartLeading,
                Owner_,
                epochContext));
        }

        void OnStopLeading(const TError& error) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStopLeading,
                Owner_,
                error));
        }

        void OnStartFollowing(NElection::TEpochContextPtr epochContext) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStartFollowing,
                Owner_,
                epochContext));
        }

        void OnStopFollowing(const TError& error) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStopFollowing,
                Owner_,
                error));
        }

        void OnStopVoting(const TError& error) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnElectionStopVoting,
                Owner_,
                error));
        }

        TPeerPriority GetPriority() override
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                THROW_ERROR_EXCEPTION("Election priority is not available");
            }
            return owner->GetElectionPriority();
        }

        TString FormatPriority(TPeerPriority priority) override
        {
            return Format("{Term: %v, SequenceNumber: %v, Boost: %v}",
                priority.first,
                priority.second / 2,
                priority.second % 2 == 1);
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
        TCellId cellId,
        IChangelogStoreFactoryPtr changelogStoreFactory,
        ISnapshotStorePtr snapshotStore,
        const TDistributedHydraManagerOptions& options,
        const TDistributedHydraManagerDynamicOptions& dynamicOptions)
        : Config_(std::move(config))
        , RpcServer_(std::move(rpcServer))
        , ElectionManager_(std::move(electionManager))
        , ControlInvoker_(std::move(controlInvoker))
        , CancelableControlInvoker_(CancelableContext_->CreateInvoker(ControlInvoker_))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , ChangelogStoreFactory_(std::move(changelogStoreFactory))
        , SnapshotStore_(std::move(snapshotStore))
        , Options_(options)
        , StateHashChecker_(New<TStateHashChecker>(Config_->MaxStateHashCheckerEntryCount, HydraLogger))
        , DynamicOptions_(dynamicOptions)
        , ElectionCallbacks_(New<TElectionCallbacks>(this))
        , Profiler_(HydraProfiler.WithTag("cell_id", ToString(cellId)))
        , Logger(HydraLogger.WithTag("CellId: %v", cellId))
        , LeaderSyncTimer_(Profiler_.Timer("/leader_sync_time"))
        , DecoratedAutomaton_(New<TDecoratedAutomaton>(
            Config_,
            Options_,
            automaton,
            AutomatonInvoker_,
            ControlInvoker_,
            SnapshotStore_,
            StateHashChecker_,
            Logger,
            Profiler_))
        , HydraService_(New<THydraService>(
            this,
            ControlInvoker_,
            cellId))
        , InternalHydraService_(New<TInternalHydraService>(
            this,
            ControlInvoker_,
            cellId))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
        VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ != EPeerState::None) {
            return;
        }

        DecoratedAutomaton_->Initialize();

        RpcServer_->RegisterService(HydraService_);
        RpcServer_->RegisterService(InternalHydraService_);

        YT_LOG_INFO("Hydra instance initialized");

        ControlState_ = EPeerState::Elections;

        Participate();
    }

    TFuture<void> Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped) {
            return VoidFuture;
        }

        YT_LOG_INFO("Hydra instance is finalizing");

        auto error = TError("Hydra instance is finalizing");

        CancelableContext_->Cancel(error);

        ElectionManager_->Abandon(error);

        if (ControlState_ != EPeerState::None) {
            RpcServer_->UnregisterService(HydraService_);
            RpcServer_->UnregisterService(InternalHydraService_);
        }

        StopEpoch();

        ControlState_ = EPeerState::Stopped;

        LeaderRecovered_ = false;
        FollowerRecovered_ = false;

        return BIND(&TDistributedHydraManager::DoFinalize, MakeStrong(this))
            .AsyncVia(AutomatonInvoker_)
            .Run();
    }

    IElectionCallbacksPtr GetElectionCallbacks() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ElectionCallbacks_;
    }

    EPeerState GetControlState() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ControlState_;
    }

    EPeerState GetAutomatonState() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->GetState();
    }

    TVersion GetAutomatonVersion() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->GetAutomatonVersion();
    }

    IInvokerPtr CreateGuardedAutomatonInvoker(IInvokerPtr underlyingInvoker) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->CreateGuardedUserInvoker(underlyingInvoker);
    }

    bool IsActiveLeader() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->GetState() == EPeerState::Leading && LeaderRecovered_ && LeaderLease_->IsValid();
    }

    bool IsActiveFollower() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->GetState() == EPeerState::Following && FollowerRecovered_;
    }

    bool IsMutationLoggingEnabled() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return !IsRecovery() || Config_->ForceMutationLogging;
    }

    TCancelableContextPtr GetControlCancelableContext() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ControlEpochContext_ ? ControlEpochContext_->CancelableContext : nullptr;
    }

    TCancelableContextPtr GetAutomatonCancelableContext() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_ ? AutomatonEpochContext_->CancelableContext : nullptr;
    }

    TFuture<int> BuildSnapshot(bool setReadOnly, bool waitForSnapshotCompletion) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;
        if (!IsActiveLeader() || !epochContext || !epochContext->LeaderCommitter) {
            return MakeFuture<int>(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active leader"));
        }
        const auto& leaderCommitter = epochContext->LeaderCommitter;

        auto loggedVersion = leaderCommitter->GetLoggedVersion();
        // TODO: make sure this still makes sense.
        if (GetReadOnly() && loggedVersion.RecordId == 0) {
            auto lastSnapshotId = DecoratedAutomaton_->GetLastSuccessfulSnapshotId();
            if (loggedVersion.SegmentId == lastSnapshotId) {
                return MakeFuture<int>(TError(
                    NHydra2::EErrorCode::ReadOnlySnapshotBuilt,
                    "The requested read-only snapshot is already built")
                    << TErrorAttribute("snapshot_id", lastSnapshotId));
            }
            return MakeFuture<int>(TError(
                NHydra2::EErrorCode::ReadOnlySnapshotBuildFailed,
                "Cannot build a snapshot in read-only mode"));
        }

        if (!leaderCommitter->CanBuildSnapshot()) {
            return MakeFuture<int>(TError(
                NRpc::EErrorCode::Unavailable,
                "Snapshot is already being built"));
        }

        SetReadOnly(setReadOnly);

        return leaderCommitter->BuildSnapshot(waitForSnapshotCompletion);
    }

    void ValidateSnapshot(IAsyncZeroCopyInputStreamPtr reader) override
    {
        DecoratedAutomaton_->ValidateSnapshot(reader);
    }

    TYsonProducer GetMonitoringProducer() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND([=, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
            VERIFY_THREAD_AFFINITY_ANY();

            BuildYsonFluently(consumer)
                .BeginMap()
                    // TODO(aleksandra-zh): add real committed/logged info.
                    .Item("committed_version").Value(ToString(DecoratedAutomaton_->GetAutomatonVersion()))

                    .Item("state").Value(DecoratedAutomaton_->GetState())
                    .Item("automaton_version").Value(ToString(DecoratedAutomaton_->GetAutomatonVersion()))
                    .Item("automaton_random_seed").Value(DecoratedAutomaton_->GetRandomSeed())
                    .Item("automaton_sequence_number").Value(DecoratedAutomaton_->GetSequenceNumber())
                    .Item("automaton_state_hash").Value(DecoratedAutomaton_->GetStateHash())

                    .Item("active").Value(IsActive())
                    .Item("active_leader").Value(IsActiveLeader())
                    .Item("active_follower").Value(IsActiveFollower())
                    .Item("read_only").Value(GetReadOnly())
                    .Item("warming_up").Value(Options_.ResponseKeeper ? Options_.ResponseKeeper->IsWarmingUp() : false)
                    .Item("grace_delay_status").Value(GraceDelayStatus_.load())
                    .Item("building_snapshot").Value(DecoratedAutomaton_->IsBuildingSnapshotNow())
                    .Item("last_snapshot_id").Value(DecoratedAutomaton_->GetLastSuccessfulSnapshotId())
                .EndMap();
        });
    }

    TPeerIdSet GetAlivePeerIds() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_ ? AutomatonEpochContext_->AlivePeerIds.Load() : TPeerIdSet();
    }

    TFuture<void> SyncWithLeader() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto epochContext = DecoratedAutomaton_->GetEpochContext();
        if (!epochContext || !IsActive()) {
            return MakeFuture(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active peer"));
        }

        if (epochContext->LeaderId == epochContext->CellManager->GetSelfPeerId()) {
            // NB: Leader lease is already checked in IsActive.
            return VoidFuture;
        }

        return epochContext->LeaderSyncBatcher->Run();
    }

    TEpochId GetCurrentEpochId()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return *CurrentEpochId;
    }

    TFuture<TMutationResponse> EnqueueMutation(TMutationRequest&& request)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto keptResponse = DecoratedAutomaton_->TryBeginKeptRequest(request);
        if (keptResponse) {
            return keptResponse;
        }

        auto promise = NewPromise<TMutationResponse>();
        auto future = promise.ToFuture();
        if (!request.EpochId) {
            request.EpochId = GetCurrentEpochId();
        }

        YT_LOG_DEBUG("Enqueue mutation");
        PreliminaryMutationQueue_.Enqueue({
            .Request = request,
            .Promise = std::move(promise)
        });
        return future;
    }

    TFuture<TMutationResponse> Forward(TMutationRequest&& request)
    {
        // TODO: Change affinity to any.
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;

        const auto& cellManager = epochContext->CellManager;
        auto channel = cellManager->GetPeerChannel(epochContext->LeaderId);
        YT_VERIFY(channel);

        TInternalHydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->CommitForwardingRpcTimeout);

        auto req = proxy.CommitMutation();
        req->set_type(request.Type);
        req->set_reign(request.Reign);
        if (request.MutationId) {
            ToProto(req->mutable_mutation_id(), request.MutationId);
            req->set_retry(request.Retry);
        }
        req->Attachments().push_back(request.Data);

        return req->Invoke().Apply(BIND([] (const TInternalHydraServiceProxy::TErrorOrRspCommitMutationPtr& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error forwarding mutation to leader");
            const auto& rsp = rspOrError.Value();
            return TMutationResponse{
                EMutationResponseOrigin::LeaderForwarding,
                TSharedRefArray(rsp->Attachments(), TSharedRefArray::TMoveParts{})
            };
        }));
    }

    TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) override
    {
        auto state = GetAutomatonState();
        switch (state) {
            case EPeerState::Leading:
                return EnqueueMutation(std::move(request));

            case EPeerState::Following:
                if (!request.AllowLeaderForwarding) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader mutation forwarding is not allowed"));
                }

                YT_VERIFY(ControlEpochContext_);
                return BIND(&TDistributedHydraManager::Forward, MakeStrong(this))
                    .AsyncVia(ControlEpochContext_->EpochControlInvoker)
                    .Run(std::move(request));

            default:
                return MakeFuture<TMutationResponse>(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Peer is in %Qlv state",
                    state));
        }
    }

    TReign GetCurrentReign() override
    {
        return DecoratedAutomaton_->GetCurrentReign();
    }

    bool GetReadOnly() const override
    {
        // VERIFY_THREAD_AFFINITY(ControlThread);

        return ReadOnly_;
    }

    TDistributedHydraManagerDynamicOptions GetDynamicOptions() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DynamicOptions_.Load();
    }

    void SetDynamicOptions(const TDistributedHydraManagerDynamicOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DynamicOptions_.Store(options);
    }

    DEFINE_SIGNAL_OVERRIDE(void(), StartLeading);
    DEFINE_SIGNAL_OVERRIDE(void(), AutomatonLeaderRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), ControlLeaderRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), LeaderActive);
    DEFINE_SIGNAL_OVERRIDE(void(), StopLeading);

    DEFINE_SIGNAL_OVERRIDE(void(), StartFollowing);
    DEFINE_SIGNAL_OVERRIDE(void(), AutomatonFollowerRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), ControlFollowerRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), StopFollowing);

    DEFINE_SIGNAL_OVERRIDE(TFuture<void>(), LeaderLeaseCheck);

private:
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();

    const TDistributedHydraManagerConfigPtr Config_;
    const IServerPtr RpcServer_;
    const IElectionManagerPtr ElectionManager_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr CancelableControlInvoker_;
    const IInvokerPtr AutomatonInvoker_;
    const IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    const ISnapshotStorePtr SnapshotStore_;
    const TDistributedHydraManagerOptions Options_;
    const TStateHashCheckerPtr StateHashChecker_;

    TAtomicObject<TDistributedHydraManagerDynamicOptions> DynamicOptions_;

    const IElectionCallbacksPtr ElectionCallbacks_;

    const NProfiling::TProfiler Profiler_;

    const TLogger Logger;

    THashMap<TString, NProfiling::TCounter> RestartCounter_;
    NProfiling::TEventTimer LeaderSyncTimer_;

    const TLeaderLeasePtr LeaderLease_ = New<TLeaderLease>();

    bool ReadOnly_ = false;

    int SnapshotId_ = -1;
    TFuture<TRemoteSnapshotParams> SnapshotFuture_;

    std::atomic<bool> LeaderRecovered_ = {false};
    std::atomic<bool> FollowerRecovered_ = {false};
    std::atomic<EGraceDelayStatus> GraceDelayStatus_ = EGraceDelayStatus::None;
    std::atomic<EPeerState> ControlState_ = EPeerState::None;

    TSystemLockGuard SystemLockGuard_;

    IChangelogStorePtr ChangelogStore_;
    std::optional<TElectionPriority> ElectionPriority_;
    bool EnablePriorityBoost_ = false;
    TPromise<void> ParticipationPromise_ = NewPromise<void>();

    TDecoratedAutomatonPtr DecoratedAutomaton_;

    TEpochContextPtr ControlEpochContext_;
    TEpochContextPtr AutomatonEpochContext_;

    TAtomicObject<TPeerIdSet> AlivePeerIds_;

    TMpscQueue<TMutationDraft> PreliminaryMutationQueue_;

    class TOwnedHydraServiceBase
        : public THydraServiceBase
    {
    protected:
        TOwnedHydraServiceBase(
            TDistributedHydraManagerPtr owner,
            IInvokerPtr invoker,
            const TServiceDescriptor& descriptor,
            TCellId cellId)
        : THydraServiceBase(
            invoker,
            descriptor,
            HydraLogger.WithTag("CellId: %v", cellId),
            cellId)
        , Owner_(owner)
    { }

    TDistributedHydraManagerPtr GetOwnerOrThrow()
    {
        auto owner = Owner_.Lock();
        if (!owner) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service is shutting down");
        }
        return owner;
    }

    // THydraServiceBase overrides.
    IHydraManagerPtr GetHydraManager() override
    {
        return GetOwnerOrThrow();
    }

    private:
        const TWeakPtr<TDistributedHydraManager> Owner_;
    };

    class THydraService
        : public TOwnedHydraServiceBase
    {
    public:
        THydraService(
            TDistributedHydraManagerPtr distributedHydraManager,
            IInvokerPtr controlInvoker,
            TCellId cellId)
        : TOwnedHydraServiceBase(
            distributedHydraManager,
            controlInvoker,
            THydraServiceProxy::GetDescriptor(),
            cellId)
        {
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceBuildSnapshot));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceSyncWithLeader));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(Poke));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(PrepareLeaderSwitch));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceRestart));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPeerState));
        }

    private:
        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, ForceBuildSnapshot)
        {
            bool setReadOnly = request->set_read_only();
            bool waitForSnapshotCompletion = request->wait_for_snapshot_completion();
            context->SetRequestInfo("SetReadOnly: %v, WaitForSnapshotCompletion: %v",
                setReadOnly,
                waitForSnapshotCompletion);

            auto owner = GetOwnerOrThrow();
            int snapshotId = WaitFor(owner->BuildSnapshot(setReadOnly, waitForSnapshotCompletion))
                .ValueOrThrow();

            context->SetResponseInfo("SnapshotId: %v",
                snapshotId);

            response->set_snapshot_id(snapshotId);

            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, ForceSyncWithLeader)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();
            context->ReplyFrom(owner->SyncWithLeader());
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, PrepareLeaderSwitch)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();
            owner->PrepareLeaderSwitch();

            TMutationRequest mutationRequest{
                .Reign = owner->GetCurrentReign(),
                .Type = HeartbeatMutationType,
                .Data = TSharedMutableRef()
            };

            owner->CommitMutation(std::move(mutationRequest))
                .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                    context->Reply(TError(result));
                }));
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, Poke)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();
            owner->CommitMutation(TMutationRequest{.Reign = owner->GetCurrentReign()})
                .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                    context->Reply(result);
                }));
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, ForceRestart)
        {
            auto reason = FromProto<TError>(request->reason());
            auto armPriorityBoost = request->arm_priority_boost();
            context->SetRequestInfo("Reason: %v, ArmPriorityBoost: %v",
                reason,
                armPriorityBoost);

            auto owner = GetOwnerOrThrow();
            owner->ForceRestart(reason, armPriorityBoost);

            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, GetPeerState)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();
            auto state = owner->GetControlState();

            context->SetResponseInfo("PeerState: %v", state);

            response->set_peer_state(ToUnderlying(state));
            context->Reply();
        }
    };
    const TIntrusivePtr<THydraService> HydraService_;

    class TInternalHydraService
        : public TOwnedHydraServiceBase
    {
    public:
        TInternalHydraService(
            TDistributedHydraManagerPtr distributedHydraManager,
            IInvokerPtr controlInvoker,
            TCellId cellId)
        : TOwnedHydraServiceBase(
            distributedHydraManager,
            controlInvoker,
            TInternalHydraServiceProxy::GetDescriptor(),
            cellId)
        {
            RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupChangelog));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(GetLatestChangelogId));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog)
                .SetCancelable(true));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(AcceptMutations));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(AcquireChangelog));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncWithLeader));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitMutation));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonLeaderLease));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ReportMutationsStateHashes));
        }

    private:
        DECLARE_RPC_SERVICE_METHOD(NProto, LookupChangelog)
        {
            int changelogId = request->changelog_id();
            context->SetRequestInfo("ChangelogId: %v", changelogId);

            auto owner = GetOwnerOrThrow();
            auto [recordCount, firstSequenceNumber] = owner->LookupChangelog(changelogId);

            response->set_record_count(recordCount);
            if (firstSequenceNumber) {
                response->set_first_sequence_number(*firstSequenceNumber);
            }

            context->SetResponseInfo("RecordCount: %v, FirstSequenceNumber: %v",
                recordCount,
                firstSequenceNumber);
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, GetLatestChangelogId)
        {
            auto owner = GetOwnerOrThrow();
            auto [changelogId, term] = owner->GetLatestChangelogId();

            response->set_changelog_id(changelogId);
            response->set_term(term);

            context->SetResponseInfo("ChangelogId: %v", changelogId);
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, ReadChangeLog)
        {
            int changelogId = request->changelog_id();
            int startRecordId = request->start_record_id();
            int recordCount = request->record_count();
            context->SetRequestInfo("ChangelogId: %v, StartRecordId: %v, RecordCount: %v",
                changelogId,
                startRecordId,
                recordCount);

            auto owner = GetOwnerOrThrow();
            auto recordsData = owner->ReadChangeLog(changelogId, startRecordId, recordCount);

            // Pack refs to minimize allocations.
            response->Attachments().push_back(PackRefs(recordsData));

            context->SetResponseInfo("RecordCount: %v", recordsData.size());
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, AcceptMutations)
        {
            auto owner = GetOwnerOrThrow();
            owner->AcceptMutations(request, response, context);
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, PingFollower)
        {
            auto epochId = FromProto<TEpochId>(request->epoch_id());
            auto term = request->has_term() ? std::make_optional(request->term()) : std::nullopt;
            auto alivePeerIds = FromProto<TPeerIdSet>(request->alive_peer_ids());
            context->SetRequestInfo("EpochId: %v, Term: %v, AlivePeerIds: %v",
                epochId,
                term,
                alivePeerIds);

            auto owner = GetOwnerOrThrow();
            auto state = owner->PingFollower(epochId, term, alivePeerIds);

            response->set_state(ToProto<int>(state));

            // Reply with OK in any case.
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, AcquireChangelog)
        {
            auto term = request->term();
            auto extractPriority = [&] () -> std::optional<TPeerPriority> {
                if (!request->has_priority()) {
                    return std::nullopt;
                }
                TPeerPriority priority;
                FromProto(&priority, request->priority());
                return priority;
            };
            auto priority = extractPriority();
            auto changelogId = request->changelog_id();

            context->SetRequestInfo("Term: %v, Priority: %v, ChangelogId: %v",
                term,
                priority,
                changelogId);

            auto owner = GetOwnerOrThrow();
            owner->AcquireChangelog(term, priority, changelogId);

            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, SyncWithLeader)
        {
            auto epochId = FromProto<TEpochId>(request->epoch_id());
            auto term = request->term();
            context->SetRequestInfo("EpochId: %v, Term: %v",
                epochId,
                term);

            auto owner = GetOwnerOrThrow();
            auto state = owner->SyncWithLeader(epochId, term);

            context->SetResponseInfo("CommittedState: %v",
                state);

            response->set_committed_sequence_number(state.SequenceNumber);
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, CommitMutation)
        {
            TMutationRequest mutationRequest;
            mutationRequest.Reign = request->reign();
            mutationRequest.Type = request->type();
            if (request->has_mutation_id()) {
                mutationRequest.MutationId = FromProto<TMutationId>(request->mutation_id());
                mutationRequest.Retry = request->retry();
            }
            mutationRequest.Data = request->Attachments()[0];

            auto owner = GetOwnerOrThrow();

            // COMPAT(savrus) Fix heartbeats from old participants.
            if (mutationRequest.Type != HeartbeatMutationType && !mutationRequest.Reign) {
                mutationRequest.Reign = owner->GetCurrentReign();
            }

            context->SetRequestInfo("MutationType: %v, MutationId: %v, Retry: %v",
                mutationRequest.Type,
                mutationRequest.MutationId,
                mutationRequest.Retry);

            owner->CommitMutation(std::move(mutationRequest))
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

        DECLARE_RPC_SERVICE_METHOD(NProto, AbandonLeaderLease)
        {
            auto peerId = request->peer_id();
            context->SetRequestInfo("PeerId: %v",
                peerId);

            auto owner = GetOwnerOrThrow();
            auto abandoned = owner->AbandonLeaderLease(peerId);

            response->set_abandoned(abandoned);
            context->SetResponseInfo("Abandoned: %v",
                abandoned);
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, ReportMutationsStateHashes)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();

            // TODO: remove it from here.
            if (owner->Config_->EnableStateHashChecker) {
                for (const auto& mutationInfo : request->mutations_info()) {
                    auto sequenceNumber = mutationInfo.sequence_number();
                    auto stateHash = mutationInfo.state_hash();
                    owner->StateHashChecker_->Report(sequenceNumber, stateHash);
                }
            }

            context->Reply();
        }
    };
    const TIntrusivePtr<TInternalHydraService> InternalHydraService_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    std::pair<int, std::optional<i64>> LookupChangelog(int changelogId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto changelog = OpenChangelogOrThrow(changelogId);
        auto recordCount = changelog->GetRecordCount();
        if (recordCount == 0) {
            return {recordCount, std::nullopt};
        }

        // TODO(aleksandra-zh): extract.
        auto asyncRecordsData = changelog->Read(
            0,
            1,
            std::numeric_limits<i64>::max());
        auto recordsData = WaitFor(asyncRecordsData)
            .ValueOrThrow();

        if (recordsData.empty()) {
            THROW_ERROR_EXCEPTION("Read zero records in changelog %v", changelogId);
        }

        TMutationHeader header;
        TSharedRef requestData;
        DeserializeMutationRecord(recordsData[0], &header, &requestData);

        return {recordCount, header.sequence_number()};
    }

    std::pair<int, int> GetLatestChangelogId()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!ChangelogStore_) {
            THROW_ERROR_EXCEPTION("Changelog store is not yet initialized");
        }

        auto changelogId = WaitFor(ChangelogStore_->GetLatestChangelogId())
            .ValueOrThrow();
        if (changelogId == InvalidSegmentId) {
            return {InvalidSegmentId, InvalidTerm};
        }
        auto changelog = OpenChangelogOrThrow(changelogId);
        auto meta = changelog->GetMeta();

        // We can ingore snaphots terms here: if there is a snapshot built in term X,
        // there is at least quorum peers with changelog with term X
        // (at least one of them was created during initial changelog aqcuisition as a persistent vote).
        return {changelogId, meta.term()};
    }

    std::vector<TSharedRef> ReadChangeLog(int changelogId, i64 startRecordId, i64 recordCount)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(startRecordId >= 0);
        YT_VERIFY(recordCount >= 0);

        auto changelog = OpenChangelogOrThrow(changelogId);

        auto asyncRecordsData = changelog->Read(
            startRecordId,
            recordCount,
            Config_->MaxChangelogBytesPerRequest);
        return WaitFor(asyncRecordsData)
            .ValueOrThrow();
    }

    void AcceptMutations(
        TTypedServiceRequest<NProto::TReqAcceptMutations>* request,
        TTypedServiceResponse<NProto::TRspAcceptMutations>* response,
        const TIntrusivePtr<TTypedServiceContext<NProto::TReqAcceptMutations, NProto::TRspAcceptMutations>>& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto startSequenceNumber = request->start_sequence_number();
        auto committedSequenceNumber = request->committed_sequence_number();
        auto committedSegmentId = request->committed_segment_id();
        auto term = request->term();

        auto epochContext = GetControlEpochContext(epochId);
        // TODO(aleksandra-zh): I hate that.
        SwitchTo(epochContext->EpochControlInvoker);

        VERIFY_THREAD_AFFINITY(ControlThread);

        auto mutationCount = request->Attachments().size();
        context->SetRequestInfo("StartSequenceNumber: %v, CommittedSequenceNumber: %v, CommittedSegmentId: %v, EpochId: %v, MutationCount: %v",
            startSequenceNumber,
            committedSequenceNumber,
            committedSegmentId,
            epochId,
            mutationCount);

        auto controlState = GetControlState();
        if (controlState != EPeerState::Following && controlState != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot accept mutations in %Qlv state",
                controlState);
        }

        if (term < epochContext->Term) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot accept mutations from term %v in term %v",
                term,
                epochContext->Term);
        }

        if (epochContext->AcquiringChangelog) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot accept mutations while acquiring changelog");
        }

        if (!CheckForInitialPing({committedSegmentId, committedSequenceNumber}, term)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot accept mutations before initial ping with OK version is recieved");
        }

        // We can not verify term before CheckForInitialPing because term may change on initial ping.
        // However, we should verify our term is not greater then leader's before CheckForInitialPing to
        // prevent term changing to a lower value.
        if (term > epochContext->Term) {
            YT_LOG_INFO("Received accept mutations with a greater term (CurrentTerm: %v, NewTerm: %v)",
                epochContext->Term,
                term);
            auto error = TError("Received accept mutations with a greater term")
                << TErrorAttribute("self_term", epochContext->Term)
                << TErrorAttribute("leader_term", term);
            ScheduleRestart(epochContext, error);
            // TODO(aleksandra-zh): Maybe replace restart with:
            // epochContext->Term = term;
            THROW_ERROR(error);
        }

        // This should be done before processing mutations, otherwise we might apply a mutation
        // with a greater sequence number.
        if (controlState == EPeerState::Following && request->has_snapshot_request()) {
            const auto& snapshotRequest = request->snapshot_request();
            auto snapshotId = snapshotRequest.snapshot_id();
            if (snapshotId < SnapshotId_) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Received a snapshot request with a snapshot id %v while last snasphot id %v is greater",
                    snapshotId,
                    SnapshotId_);
            } else if (snapshotId == SnapshotId_) {
                YT_LOG_DEBUG("Received a redunant snasphot request, ignoring (SnapshotId: %v)",
                    SnapshotId_);
            } else {
                SnapshotId_ = snapshotRequest.snapshot_id();
                YT_LOG_INFO("Received a new snasphot request (SnapshotId: %v)",
                    SnapshotId_);
                SnapshotFuture_ = BIND(&TDecoratedAutomaton::BuildSnapshot, DecoratedAutomaton_)
                    .AsyncVia(epochContext->EpochUserAutomatonInvoker)
                    .Run(snapshotRequest.snapshot_id(), snapshotRequest.sequence_number());
            }

            if (SnapshotFuture_ && SnapshotFuture_.IsSet()) {
                auto* snapshotResponse = response->mutable_snapshot_response();
                const auto& valueOrError = SnapshotFuture_.Get();
                // TODO (aleksandra-zh): error is actually useful.
                snapshotResponse->set_ok(valueOrError.IsOK());
                if (valueOrError.IsOK()) {
                    auto value = valueOrError.Value();

                    snapshotResponse->set_snapshot_id(value.SnapshotId);
                    snapshotResponse->set_checksum(value.Checksum);
                }
            }
        }

        epochContext->FollowerCommitter->AcceptMutations(
            startSequenceNumber,
            request->Attachments());

        if (controlState == EPeerState::Following) {
            epochContext->FollowerCommitter->LogMutations();
            CommitMutationsAtFollower(committedSequenceNumber);
        }

        // we must make sure LoggedSequenceNumber in committer is initialized
        auto loggedSequenceNumber = epochContext->FollowerCommitter->GetLoggedSequenceNumber();
        auto expectedSequenceNumber = epochContext->FollowerCommitter->GetExpectedSequenceNumber();
        response->set_logged_sequence_number(loggedSequenceNumber);
        response->set_expected_sequence_number(expectedSequenceNumber);

        context->SetResponseInfo("LoggedSequenceNumber: %v, ExpectedSequenceNumber: %v",
            loggedSequenceNumber,
            expectedSequenceNumber);
        context->Reply();
    }

    // epochId seems redundant
    EPeerState PingFollower(TEpochId epochId, std::optional<int> term, TPeerIdSet alivePeerIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controlState = GetControlState();
        if (controlState != EPeerState::Following && controlState != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot handle follower ping in %Qlv state",
                controlState);
        }

        auto epochContext = GetControlEpochContext(epochId);
        if (term && epochContext->Term != term) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Term mismatch: expected %v, got %v",
                epochContext->Term,
                term);
        }

        epochContext->AlivePeerIds.Store(alivePeerIds);

        return controlState;
    }

    void AcquireChangelog(int term, std::optional<TPeerPriority> priority, int changelogId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto elections = priority.has_value();

        auto epochContext = ControlEpochContext_;
        auto changelogStore = ChangelogStore_;

        auto controlState = GetControlState();

        // TODO(aleksandra-zh): can we actually be following?
        if (controlState != EPeerState::FollowerRecovery && controlState != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v in %Qlv state",
                changelogId,
                controlState);
        }

        if (epochContext->Term > term || (elections && epochContext->Term == term)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v with term %v in term %v",
                changelogId,
                term,
                epochContext->Term);
        }

        auto selfPriority = GetElectionPriority();
        if (elections && selfPriority > priority) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v because self priority %v is greater than leader priority %v",
                changelogId,
                selfPriority,
                priority);
        }

        auto currentChangelogId = WaitFor(changelogStore->GetLatestChangelogId())
            .ValueOrThrow();

        if (currentChangelogId >= changelogId) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v because changelog %v exists",
                changelogId,
                currentChangelogId);
        }

        if (epochContext->AcquiringChangelog) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Changelog is already being acquired");
        }

        if (elections) {
            epochContext->Term = term;
            epochContext->AcquiringChangelog = true;
        }

        auto acquiringChangelogGuard = Finally([&] {
            epochContext->AcquiringChangelog = false;
        });

        if (elections) {
            WaitFor(epochContext->FollowerCommitter->GetLastLoggedMutationFuture())
                .ThrowOnError();
        }

        NHydra::NProto::TChangelogMeta meta;
        meta.set_term(term);

        for (int i = currentChangelogId + 1; i <= changelogId; ++i) {
            auto changelog = WaitFor(changelogStore->CreateChangelog(i, meta))
                .ValueOrThrow();
            epochContext->FollowerCommitter->RegisterNextChangelog(i, changelog);
        }

        YT_LOG_INFO("Changelog acquired (ChangelogId: %v, Priority: %v, Term: %v)",
            changelogId,
            priority,
            term);
    }

    // I'm sure there are more places where I can use it.
    void ValidateTerm(const TEpochContextPtr& epochContext, int term)
    {
        if (term > epochContext->Term) {
            auto error = TError("Leader's term %v is less than follower's term %v",
                epochContext->Term,
                term);
            ScheduleRestart(epochContext, error);
        }

        if (term < epochContext->Term) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Current term %v is greater than follower's term %v",
                epochContext->Term,
                term);
        }
    }

    TReachableState SyncWithLeader(TEpochId epochId, int term)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!IsActiveLeader()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Not an active leader");
        }

        // Validate epoch id.
        auto epochContext = GetControlEpochContext(epochId);

        ValidateTerm(epochContext, term);

        return epochContext->LeaderCommitter->GetCommittedState();
    }

    void PrepareLeaderSwitch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto state = GetAutomatonState();
        if (state != EPeerState::Leading) {
            THROW_ERROR_EXCEPTION("Peer is not leading");
        }

        auto epochContext = ControlEpochContext_;
        if (epochContext->LeaderSwitchStarted) {
            THROW_ERROR_EXCEPTION("Leader is already being switched");
        }

        YT_LOG_INFO("Preparing leader switch (Timeout: %v)",
            Config_->LeaderSwitchTimeout);

        TDelayedExecutor::Submit(
            BIND([=, this_ = MakeWeak(this)] {
                ScheduleRestart(
                    epochContext,
                    TError("Leader switch did not complete within timeout"));
            }),
            Config_->LeaderSwitchTimeout);

        epochContext->LeaderSwitchStarted = true;
    }

    bool AbandonLeaderLease(int peerId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        bool abandoned = LeaderLease_->TryAbandon();
        if (abandoned) {
            YT_LOG_INFO("Leader lease abandonded (RequestingPeerId: %v)",
                peerId);
        }
        return abandoned;
    }

    void ForceRestart(const TError& reason, bool armPriorityBoost)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;
        if (!epochContext) {
            THROW_ERROR_EXCEPTION(
                "Peer is in %Qlv state",
                GetControlState());
        }

        if (armPriorityBoost) {
            SetPriorityBoost(true);
        }

        ScheduleRestart(epochContext, reason);

        if (armPriorityBoost) {
            YT_LOG_DEBUG("Waiting for participation");
            WaitFor(ParticipationPromise_.ToFuture())
                .ThrowOnError();
        }
    }

    void SetPriorityBoost(bool value)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (EnablePriorityBoost_ == value) {
            return;
        }

        if (value) {
            YT_LOG_INFO("Priority boost armed");
        } else {
            YT_LOG_INFO("Priority boost disarmed");
        }

        EnablePriorityBoost_ = value;
    }

    TPeerPriority GetElectionPriority()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // order?
        if (!ElectionPriority_) {
            THROW_ERROR_EXCEPTION("Election priority is not available");
        }

        auto controlState = GetControlState();
        auto getElectionPriority = [&] {
            if (controlState == EPeerState::Leading || controlState == EPeerState::Following) {
                YT_VERIFY(ControlEpochContext_);
                // it looks possible that last mutation's term might be < ControlEpochContext_->Term
                // but for now lets pretend it is not
                return TElectionPriority(ControlEpochContext_->Term, ControlEpochContext_->Term, DecoratedAutomaton_->GetReachableState());
            } else {
                return *ElectionPriority_;
            }
        };
        auto priority = getElectionPriority();
        return {priority.LastMutationTerm, priority.ReachableState.SequenceNumber * 2 + (EnablePriorityBoost_ ? 1 : 0)};
    }

    void Participate()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        CancelableControlInvoker_->Invoke(
            BIND(&TDistributedHydraManager::DoParticipate, MakeStrong(this)));
    }

    void ProfileRestart(const TString& reason)
    {
        auto it = RestartCounter_.find(reason);
        if (it == RestartCounter_.end()) {
            it = RestartCounter_.emplace(reason, Profiler_.WithTag("reason", reason).Counter("/restart_count")).first;
        }
        it->second.Increment();
    }

    void ProfileRestart(const TError& error)
    {
        ProfileRestart(error.GetMessage());
    }

    void ScheduleRestart(const TEpochContextPtr& epochContext, const TError& error)
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

    void ScheduleRestart(const TWeakPtr<TEpochContext>& weakEpochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto epochContext = weakEpochContext.Lock()) {
            ScheduleRestart(epochContext, error);
        }
    }

    void DoRestart(const TEpochContextPtr& epochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlEpochContext_ != epochContext) {
            return;
        }

        YT_LOG_WARNING(error, "Restarting Hydra instance");
        ElectionManager_->Abandon(error);
    }

    TElectionPriority GetSnapshotElectionPriority()
    {
        auto asyncMaxSnapshotId = SnapshotStore_->GetLatestSnapshotId();
        int maxSnapshotId = WaitFor(asyncMaxSnapshotId)
            .ValueOrThrow();

        if (maxSnapshotId == InvalidSegmentId) {
            YT_LOG_INFO("No snapshots found");
            // Let's pretend we have snapshot 0.
            return {0, 0, 0, 0};
        }

        auto reader = SnapshotStore_->CreateReader(maxSnapshotId);
        WaitFor(reader->Open())
            .ThrowOnError();

        auto meta = reader->GetParams().Meta;
        if (!meta.has_term()) {
            // COMPAT(aleksandra-zh): Looks safe.
            YT_LOG_INFO("No term in snapshot meta (SnapshotId: %v, SequenceNumber: %v)",
                maxSnapshotId,
                meta.sequence_number());
            return {0, 0, maxSnapshotId, meta.sequence_number()};
        }

        // TODO: think about this.
        TElectionPriority electionPriority(meta.term(), meta.last_mutation_term(), meta.last_segment_id(), meta.sequence_number());
        YT_LOG_INFO(
            "The latest snapshot election priority is available (SnapshotId: %v, ElectionPriority: %v)",
            maxSnapshotId,
            electionPriority);
        return electionPriority;
    }

    TElectionPriority GetChangelogElectionPriority()
    {
        ChangelogStore_ = WaitFor(ChangelogStoreFactory_->Lock())
            .ValueOrThrow();

        auto optionalElectionPriority = ChangelogStore_->GetElectionPriority();
        if (optionalElectionPriority) {
            YT_LOG_INFO("Changelog election priority is available (ElectionPriority: %v)",
                *optionalElectionPriority);
            return *optionalElectionPriority;
        } else {
            // meh
            auto state = DecoratedAutomaton_->GetReachableState();
            YT_VERIFY(ControlEpochContext_);
            YT_LOG_INFO("Election priority is not available, using automaton state instead (AutomatonState: %v)",
                state);
            return {DecoratedAutomaton_->GetLastMutationTerm(), ControlEpochContext_->Term, state};
        }
    }

    void DoParticipate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Initializing persistent stores");

        auto backoffTime = Config_->MinPersistentStoreInitializationBackoffTime;

        while (true) {
            try {
                auto snapshotElectionPriority = GetSnapshotElectionPriority();
                auto changelogElectionPriority = GetChangelogElectionPriority();
                ElectionPriority_ = std::max(snapshotElectionPriority, changelogElectionPriority);
                ElectionPriority_->Term = std::max(snapshotElectionPriority.Term, changelogElectionPriority.Term);
                break;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error initializing persistent stores, backing off and retrying");
                TDelayedExecutor::WaitForDuration(backoffTime);
                backoffTime = std::min(
                    backoffTime * Config_->PersistentStoreInitializationBackoffTimeMultiplier,
                    Config_->MaxPersistentStoreInitializationBackoffTime);
            }
        }

        ParticipationPromise_.Set();
        ParticipationPromise_ = NewPromise<void>();

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

        if (AutomatonEpochContext_) {
            AutomatonEpochContext_.Reset();
        }

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

    void OnLoggingFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto wrappedError = TError("Error logging mutations")
            << error;
        ScheduleRestart(ControlEpochContext_, wrappedError);
    }

    void OnLeaderLeaseLost(const TWeakPtr<TEpochContext>& weakEpochContext, const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto wrappedError = TError("Leader lease is lost")
            << error;
        ScheduleRestart(weakEpochContext, wrappedError);
    }


    void OnUpdateAlivePeers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery) {
            ControlEpochContext_->AlivePeerIds.Store(ElectionManager_->GetAlivePeerIds());
        }
    }

    int AcquireChangelog()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // verify state

        auto selfChangelogId = WaitFor(ChangelogStore_->GetLatestChangelogId())
            .ValueOrThrow();

        YT_VERIFY(ElectionPriority_);

        auto asyncResult = ComputeQuorumLatestChangelogId(
            Config_,
            ControlEpochContext_->CellManager,
            selfChangelogId,
            ElectionPriority_->Term);
        auto [changelogId, term] = WaitFor(asyncResult)
            .ValueOrThrow();

        auto newChangelogId = changelogId + 1;
        auto newTerm = term + 1;

        ControlEpochContext_->Term = newTerm;

        // TODO: What kind of barrier should I have here (if any)?
        auto priority = GetElectionPriority();
        YT_LOG_INFO("Acquiring changelog (ChangelogId: %v, Priority: %v, Term: %v)",
            newChangelogId,
            priority,
            newTerm);
        WaitFor(RunChangelogAcquisition(Config_, ControlEpochContext_, newChangelogId, priority))
            .ThrowOnError();
        YT_LOG_INFO("Changelog acquired");

        return newChangelogId;
    }

    void OnElectionStartLeading(const NElection::TEpochContextPtr& electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting leader recovery (SelfAddress: %v, SelfId: %v)",
            electionEpochContext->CellManager->GetSelfConfig(),
            electionEpochContext->CellManager->GetSelfPeerId());

        YT_VERIFY(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::LeaderRecovery;

        auto epochContext = StartEpoch(electionEpochContext);
        auto automatonError = WaitFor(BIND(&TDistributedHydraManager::OnElectionStartLeadingAutomaton, MakeWeak(this))
            .AsyncVia(DecoratedAutomaton_->GetSystemInvoker())
            .Run(epochContext));
        if (!automatonError.IsOK()) {
            return;
        }

        epochContext->EpochControlInvoker->Invoke(
            BIND(&TDistributedHydraManager::RecoverLeader, MakeStrong(this)));
    }

    void OnElectionStartLeadingAutomaton(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(!AutomatonEpochContext_);
        AutomatonEpochContext_ = epochContext;

        DecoratedAutomaton_->OnStartLeading(epochContext);

        StartLeading_.Fire();
    }

    void RecoverLeader()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;

        try {
            epochContext->LeaseTracker = New<TLeaseTracker>(
                Config_,
                epochContext.Get(),
                LeaderLease_,
                LeaderLeaseCheck_.ToVector(),
                Logger);
            epochContext->LeaseTracker->SubscribeLeaseLost(
                BIND(&TDistributedHydraManager::OnLeaderLeaseLost, MakeWeak(this), MakeWeak(epochContext)));

            YT_LOG_INFO("Waiting for followers to enter recovery state");
            WaitFor(epochContext->LeaseTracker->GetNextQuorumFuture())
                .ThrowOnError();
            YT_LOG_INFO("Followers are in recovery");

            // TODO(aleksandra-zh): fix this when there are no changelogs.
            auto changelogId = AcquireChangelog();
            auto changelog = OpenChangelogOrThrow(changelogId);

            epochContext->LeaderCommitter = New<TLeaderCommitter>(
                Config_,
                Options_,
                DecoratedAutomaton_,
                LeaderLease_,
                &PreliminaryMutationQueue_,
                changelog,
                epochContext->ReachableState,
                epochContext.Get(),
                Logger,
                Profiler_);
            epochContext->LeaderCommitter->SubscribeLoggingFailed(
                BIND(&TDistributedHydraManager::OnLoggingFailed, MakeWeak(this)));

            epochContext->Recovery = New<TRecovery>(
                Config_,
                Options_,
                GetDynamicOptions(),
                DecoratedAutomaton_,
                ChangelogStore_,
                SnapshotStore_,
                Options_.ResponseKeeper,
                epochContext.Get(),
                epochContext->ReachableState,
                true,
                Logger);
            WaitFor(epochContext->Recovery->Run(epochContext->Term))
                .ThrowOnError();

            if (Config_->DisableLeaderLeaseGraceDelay) {
                YT_LOG_WARNING("Leader lease grace delay disabled; cluster can only be used for testing purposes");
                GraceDelayStatus_ = EGraceDelayStatus::GraceDelayDisabled;
            } else if (TryAbandonExistingLease(epochContext)) {
                YT_LOG_INFO("Previous leader lease was abandoned; ignoring leader lease grace delay");
                GraceDelayStatus_ = EGraceDelayStatus::PreviousLeaseAbandoned;
            } else {
                YT_LOG_INFO("Waiting for previous leader lease to expire (Delay: %v)",
                    Config_->LeaderLeaseGraceDelay);
                TDelayedExecutor::WaitForDuration(Config_->LeaderLeaseGraceDelay);
                GraceDelayStatus_ = EGraceDelayStatus::GraceDelayExecuted;
            }

            YT_LOG_INFO("Acquiring leader lease");
            epochContext->LeaseTracker->EnableTracking();
            WaitFor(epochContext->LeaseTracker->GetNextQuorumFuture())
                .ThrowOnError();
            YT_LOG_INFO("Leader lease acquired");

            YT_VERIFY(ControlState_ == EPeerState::LeaderRecovery);
            ControlState_ = EPeerState::Leading;

            ControlLeaderRecoveryComplete_.Fire();

            YT_LOG_INFO("Leader recovery completed");
            SystemLockGuard_.Release();

            LeaderRecovered_ = true;
            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }

            WaitFor(BIND(&TDistributedHydraManager::OnLeaderRecoveryCompleteAutomaton, MakeWeak(this))
                .AsyncVia(epochContext->EpochSystemAutomatonInvoker)
                .Run())
                .ThrowOnError();

            epochContext->LeaderCommitter->Start();

            epochContext->HeartbeatMutationCommitExecutor->Start();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Leader recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            ScheduleRestart(epochContext, ex);
        }
    }

    void OnLeaderRecoveryCompleteAutomaton()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->OnLeaderRecoveryComplete();
        AutomatonLeaderRecoveryComplete_.Fire();

        LeaderActive_.Fire();
    }

    bool TryAbandonExistingLease(const TEpochContextPtr& epochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!GetDynamicOptions().AbandonLeaderLeaseDuringRecovery) {
            return false;
        }

        YT_LOG_INFO("Trying to abandon existing leader lease");

        std::vector<TFuture<TInternalHydraServiceProxy::TRspAbandonLeaderLeasePtr>> futures;
        const auto& cellManager = epochContext->CellManager;
        for (int peerId = 0; peerId < cellManager->GetTotalPeerCount(); ++peerId) {
            auto peerChannel = cellManager->GetPeerChannel(peerId);
            if (!peerChannel) {
                continue;
            }

            YT_LOG_INFO("Requesting peer to abandon existing leader lease (PeerId: %v)",
                peerId);

            TInternalHydraServiceProxy proxy(std::move(peerChannel));
            auto req = proxy.AbandonLeaderLease();
            req->SetTimeout(Config_->AbandonLeaderLeaseRequestTimeout);
            req->set_peer_id(cellManager->GetSelfPeerId());
            futures.push_back(req->Invoke());
        }

        auto rspsOrError = WaitFor(AllSet(futures));
        if (!rspsOrError.IsOK()) {
            YT_LOG_INFO(rspsOrError, "Failed to abandon existing leader lease");
            return false;
        }

        const auto& rsps = rspsOrError.Value();
        for (int peerId = 0; peerId < std::ssize(rsps); ++peerId) {
            const auto& rspOrError = rsps[peerId];
            if (!rspOrError.IsOK()) {
                YT_LOG_INFO(rspOrError, "Failed to abandon peer leader lease (PeerId: %v)",
                    peerId);
                continue;
            }

            const auto& rsp = rspOrError.Value();
            if (rsp->abandoned()) {
                YT_LOG_INFO("Previous leader lease abandoned by peer (PeerId: %v)",
                    peerId);
                return true;
            }

            YT_LOG_INFO("Peer did not have leader lease (PeerId: %v)",
                peerId);
        }

        return false;
    }

    void OnElectionAlivePeerSetChanged(const TPeerIdSet& peerIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ControlEpochContext_->AlivePeerIds.Store(peerIds);
    }

    void OnElectionStopLeading(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO(error, "Stopped leading");

        ProfileRestart(error);

        auto epochContext = ControlEpochContext_;

        // Save for later to respect the thread affinity.
        auto leaderCommitter = ControlEpochContext_->LeaderCommitter;

        StopEpoch();

        YT_VERIFY(ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery);
        ControlState_ = EPeerState::Elections;

        if (leaderCommitter) {
            leaderCommitter->Stop();
        }

        auto automatonError = WaitFor(BIND(&TDistributedHydraManager::OnElectionStopLeadingAutomaton, MakeWeak(this))
            .AsyncVia(DecoratedAutomaton_->GetSystemInvoker())
            .Run());
        if (!automatonError.IsOK()) {
            return;
        }

        Participate();
    }

    void OnElectionStopLeadingAutomaton()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (AutomatonEpochContext_) {
            AutomatonEpochContext_.Reset();
        }
        DecoratedAutomaton_->OnStopLeading();

        StopLeading_.Fire();
    }

    void OnElectionStartFollowing(const NElection::TEpochContextPtr& electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting follower recovery (SelfAddress: %v, SelfId: %v)",
            electionEpochContext->CellManager->GetSelfConfig(),
            electionEpochContext->CellManager->GetSelfPeerId());

        SetPriorityBoost(false);

        YT_VERIFY(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::FollowerRecovery;

        auto epochContext = StartEpoch(electionEpochContext);

        epochContext->FollowerCommitter = New<TFollowerCommitter>(
            Config_,
            Options_,
            DecoratedAutomaton_,
            epochContext.Get(),
            Logger,
            Profiler_);
        epochContext->FollowerCommitter->SubscribeLoggingFailed(
            BIND(&TDistributedHydraManager::OnLoggingFailed, MakeWeak(this)));

        auto automatonError = WaitFor(BIND(&TDistributedHydraManager::OnElectionsStartFollowingAutomaton, MakeWeak(this))
            .AsyncVia(DecoratedAutomaton_->GetSystemInvoker())
            .Run(epochContext));
        if (!automatonError.IsOK()) {
            return;
        }
    }

    void OnElectionsStartFollowingAutomaton(TEpochContextPtr epochContext)
    {
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
            WaitFor(epochContext->Recovery->Run(epochContext->Term))
                .ThrowOnError();

            YT_VERIFY(ControlState_ == EPeerState::FollowerRecovery);
            ControlState_ = EPeerState::Following;
            ControlFollowerRecoveryComplete_.Fire();

            YT_LOG_INFO("Follower recovery completed");
            SystemLockGuard_.Release();

            WaitFor(BIND(&TDistributedHydraManager::OnFollowerRecoveryCompleteAutomaton, MakeWeak(this))
                .AsyncVia(epochContext->EpochSystemAutomatonInvoker)
                .Run())
                .ThrowOnError();

            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }

            FollowerRecovered_ = true;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Follower recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->RestartBackoffTime);
            ScheduleRestart(epochContext, ex);
        }
    }

    void OnFollowerRecoveryCompleteAutomaton()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->OnFollowerRecoveryComplete();
        AutomatonFollowerRecoveryComplete_.Fire();
    }

    void OnElectionStopFollowing(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO(error, "Stopped following");

        ProfileRestart(error);

        auto epochContext = ControlEpochContext_;

        // Save for later to respect the thread affinity.
        auto followerCommitter = epochContext->FollowerCommitter;
        followerCommitter->Stop();

        StopEpoch();

        YT_VERIFY(ControlState_ == EPeerState::Following || ControlState_ == EPeerState::FollowerRecovery);
        ControlState_ = EPeerState::Elections;

        auto automatonError = WaitFor(BIND(&TDistributedHydraManager::OnElectionStopFollowingAutomaton, MakeWeak(this))
            .AsyncVia(DecoratedAutomaton_->GetSystemInvoker())
            .Run());
        if (!automatonError.IsOK()) {
            return;
        }

        Participate();
    }

    void OnElectionStopFollowingAutomaton()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (AutomatonEpochContext_) {
            AutomatonEpochContext_.Reset();
        }
        DecoratedAutomaton_->OnStopFollowing();

        StopFollowing_.Fire();
    }

    void OnElectionStopVoting(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO(error, "Stopped voting");

        StopEpoch();

        YT_VERIFY(ControlState_ == EPeerState::Elections);

        Participate();
    }

    bool CheckForInitialPing(TReachableState committedState, int term)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ != EPeerState::FollowerRecovery) {
            YT_VERIFY(ControlState_ == EPeerState::Following);
            return true;
        }

        auto epochContext = ControlEpochContext_;
        // Check if initial ping is already received.
        if (epochContext->Recovery) {
            return true;
        }

        YT_VERIFY(ControlState_ == EPeerState::FollowerRecovery);

        auto reachableState = epochContext->ReachableState;
        // This is weird now.
        if (committedState < reachableState) {
            YT_LOG_DEBUG("Received initial ping from leader with a stale version; ignored (LeaderState: %v, ReachableState: %v)",
                committedState,
                epochContext->ReachableState);
            return false;
        }

        YT_LOG_INFO("Received initial ping from leader (CommittedState: %v, Term: %v)",
            committedState,
            term);

        // If we join an active quorum, we wont get an AcquireChangelog request from leader,
        // so we wont be able to promote our term there, so we have to do it here.
        // Looks safe.
        epochContext->Term = term;

        epochContext->Recovery = New<TRecovery>(
            Config_,
            Options_,
            GetDynamicOptions(),
            DecoratedAutomaton_,
            ChangelogStore_,
            SnapshotStore_,
            Options_.ResponseKeeper,
            epochContext.Get(),
            committedState,
            false,
            Logger);
        epochContext->FollowerCommitter->SetSequenceNumber(committedState.SequenceNumber);

        // Make sure we dont try to build the same snapshot twice.
        // (It does not always help, LoadSnapshot in decorated automaton explains why).
        SnapshotId_ = committedState.SegmentId;

        epochContext->EpochControlInvoker->Invoke(
            BIND(&TDistributedHydraManager::RecoverFollower, MakeStrong(this)));

        return true;
    }

    IInvokerPtr CreateEpochInvoker(const TEpochContextPtr& epochContext, const IInvokerPtr& underlying)
    {
        auto invoker = epochContext->CancelableContext->CreateInvoker(underlying);
        return New<TEpochIdInjectingInvoker>(invoker, epochContext->EpochId);
    }

    TEpochContextPtr StartEpoch(const NElection::TEpochContextPtr& electionEpochContext)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = New<TEpochContext>();
        epochContext->CellManager = electionEpochContext->CellManager;
        epochContext->ChangelogStore = ChangelogStore_;
        epochContext->ReachableState = ElectionPriority_->ReachableState;
        epochContext->Term = ElectionPriority_->Term;
        epochContext->LeaderId = electionEpochContext->LeaderId;
        epochContext->EpochId = electionEpochContext->EpochId;
        epochContext->CancelableContext = electionEpochContext->CancelableContext;
        epochContext->EpochControlInvoker = CreateEpochInvoker(epochContext, CancelableControlInvoker_);
        epochContext->EpochSystemAutomatonInvoker = CreateEpochInvoker(epochContext, DecoratedAutomaton_->GetSystemInvoker());
        epochContext->EpochUserAutomatonInvoker = CreateEpochInvoker(epochContext, AutomatonInvoker_);
        epochContext->HeartbeatMutationCommitExecutor = New<TPeriodicExecutor>(
            epochContext->EpochControlInvoker,
            BIND(&TDistributedHydraManager::OnHeartbeatMutationCommit, MakeWeak(this)),
            Config_->HeartbeatMutationPeriod);
        if (epochContext->LeaderId == epochContext->CellManager->GetSelfPeerId()) {
            epochContext->AlivePeersUpdateExecutor = New<TPeriodicExecutor>(
                epochContext->EpochControlInvoker,
                BIND(&TDistributedHydraManager::OnUpdateAlivePeers, MakeWeak(this)),
                TDuration::Seconds(5));
            epochContext->AlivePeersUpdateExecutor->Start();
        }
        epochContext->LeaderSyncBatcher = New<TAsyncBatcher<void>>(
            BIND_DONT_CAPTURE_TRACE_CONTEXT(&TDistributedHydraManager::DoSyncWithLeader, MakeWeak(this), MakeWeak(epochContext)),
            Config_->LeaderSyncDelay);

        YT_VERIFY(!ControlEpochContext_);
        ControlEpochContext_ = epochContext;

        SystemLockGuard_ = TSystemLockGuard::Acquire(DecoratedAutomaton_);

        SetPriorityBoost(false);

        return ControlEpochContext_;
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResetControlEpochContext();

        LeaderRecovered_ = false;
        FollowerRecovered_ = false;
        GraceDelayStatus_ = EGraceDelayStatus::None;

        SystemLockGuard_.Release();

        if (ChangelogStore_) {
            ChangelogStore_->Abort();
            ChangelogStore_.Reset();
        }

        ElectionPriority_.reset();
    }

    TEpochContextPtr GetControlEpochContext(TEpochId epochId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!ControlEpochContext_) {
            THROW_ERROR_EXCEPTION(
                NHydra2::EErrorCode::InvalidEpoch,
                "No control epoch context");
        }

        auto currentEpochId = ControlEpochContext_->EpochId;
        if (epochId != currentEpochId) {
            THROW_ERROR_EXCEPTION(
                NHydra2::EErrorCode::InvalidEpoch,
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

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
        ControlEpochContext_->CancelableContext->Cancel(error);

        ControlEpochContext_->LeaderSyncBatcher->Cancel(error);
        if (ControlEpochContext_->LeaderSyncPromise) {
            TrySetLeaderSyncPromise(ControlEpochContext_, error);
        }

        ControlEpochContext_.Reset();
    }

    static TFuture<void> DoSyncWithLeader(
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
        YT_LOG_DEBUG("Synchronizing with leader");

        return BIND(&TDistributedHydraManager::DoSyncWithLeaderCore, this_)
            .AsyncViaGuarded(
                epochContext->EpochControlInvoker,
                TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"))
            .Run();
    }

    TFuture<void> DoSyncWithLeaderCore()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (GetAutomatonState() == EPeerState::Leading) {
            return VoidFuture;
        }

        YT_LOG_DEBUG("Synchronizing with leader");

        auto epochContext = ControlEpochContext_;

        YT_VERIFY(!epochContext->LeaderSyncPromise);
        epochContext->LeaderSyncPromise = NewPromise<void>();
        epochContext->LeaderSyncTimer.Restart();

        auto channel = epochContext->CellManager->GetPeerChannel(epochContext->LeaderId);
        YT_VERIFY(channel);

        TInternalHydraServiceProxy proxy(std::move(channel));
        proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

        auto req = proxy.SyncWithLeader();
        ToProto(req->mutable_epoch_id(), epochContext->EpochId);
        req->set_term(epochContext->Term);

        req->Invoke().Subscribe(
            BIND(&TDistributedHydraManager::OnSyncWithLeaderResponse, MakeStrong(this))
                .Via(epochContext->EpochControlInvoker));

        return epochContext->LeaderSyncPromise;
    }

    void OnSyncWithLeaderResponse(const TInternalHydraServiceProxy::TErrorOrRspSyncWithLeaderPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;

        if (!rspOrError.IsOK()) {
            TrySetLeaderSyncPromise(epochContext, rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto committedSequenceNumber = rsp->committed_sequence_number();

        YT_LOG_DEBUG("Received synchronization response from leader (CommittedSequenceNumber: %v)",
            committedSequenceNumber);

        YT_VERIFY(!epochContext->LeaderSyncSequenceNumber);
        epochContext->LeaderSyncSequenceNumber = committedSequenceNumber;
        CommitMutationsAtFollower(committedSequenceNumber);
    }

    void CheckForPendingLeaderSync()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochContext = ControlEpochContext_;

        if (!epochContext->LeaderSyncPromise || !epochContext->LeaderSyncSequenceNumber) {
            return;
        }

        auto neededCommittedSequenceNumber = *epochContext->LeaderSyncSequenceNumber;
        auto automatonSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
        if (neededCommittedSequenceNumber > automatonSequenceNumber) {
            return;
        }

        YT_LOG_DEBUG("Leader synchronization complete (NeededCommittedSequenceNumber: %v, AutomatonSequenceNumber: %v)",
            neededCommittedSequenceNumber,
            automatonSequenceNumber);

        TrySetLeaderSyncPromise(epochContext);
        epochContext->LeaderSyncSequenceNumber.reset();
    }

    void TrySetLeaderSyncPromise(const TEpochContextPtr& epochContext, TError error = {})
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TError wrappedError;
        if (error.IsOK()) {
            LeaderSyncTimer_.Record(epochContext->LeaderSyncTimer.GetElapsedTime());
        } else {
            wrappedError = TError(NRpc::EErrorCode::Unavailable, "Error synchronizing with leader")
                << std::move(error);
        }

        // NB: Many subscribers are typically waiting for the leader sync to complete.
        // Make sure the promise is set in a suitably large thread pool.
        NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
            BIND([promise = std::move(epochContext->LeaderSyncPromise), wrappedError = std::move(wrappedError)] {
                promise.TrySet(std::move(wrappedError));
            }));
    }

    void CommitMutationsAtFollower(i64 committedSequenceNumber)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto lastSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();

        ControlEpochContext_->FollowerCommitter->CommitMutations(committedSequenceNumber);
        CheckForPendingLeaderSync();

        auto currentSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();

        if (Config_->EnableStateHashChecker) {
            ReportMutationStateHashesToLeader(lastSequenceNumber, currentSequenceNumber);
        }
    }

    void ReportMutationStateHashesToLeader(i64 startSequenceNumber, i64 endSequenceNumber)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto rate = Config_->StateHashCheckerMutationVerificationSamplingRate;

        // First sequence number divisible by rate greater than startSequenceNumber, since
        // startSequenceNumber was already reported.
        startSequenceNumber += rate;
        startSequenceNumber -= startSequenceNumber % rate;

        endSequenceNumber -= endSequenceNumber % rate;

        if (startSequenceNumber > endSequenceNumber) {
            return;
        }

        auto epochContext = AutomatonEpochContext_;

        auto channel = epochContext->CellManager->GetPeerChannel(AutomatonEpochContext_->LeaderId);
        YT_VERIFY(channel);

        TInternalHydraServiceProxy proxy(std::move(channel));
        auto request = proxy.ReportMutationsStateHashes();

        std::vector<i64> sequenceNumbers;
        for (auto sequenceNumber = startSequenceNumber; sequenceNumber <= endSequenceNumber; sequenceNumber += rate) {
            sequenceNumbers.push_back(sequenceNumber);
        }

        for (auto [sequenceNumber, stateHash] : StateHashChecker_->GetStateHashes(std::move(sequenceNumbers))) {
            auto mutationInfo = request->add_mutations_info();
            mutationInfo->set_sequence_number(sequenceNumber);
            mutationInfo->set_state_hash(stateHash);
        }

        if (request->mutations_info().empty()) {
            return;
        }

        request->Invoke().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TInternalHydraServiceProxy::TErrorOrRspReportMutationsStateHashesPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Mutations state hashes reported (StartSequenceNumber: %v, EndSequenceNumber: %v)",
                    startSequenceNumber,
                    endSequenceNumber);
            } else {
                YT_LOG_DEBUG(rspOrError, "Error reporting mutations state hashes (StartSequenceNumber: %v, EndSequenceNumber: %v)",
                    startSequenceNumber,
                    endSequenceNumber);
            }
        }));
    }

    void OnHeartbeatMutationCommit()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (GetReadOnly()) {
            return;
        }

        YT_LOG_DEBUG("Committing heartbeat mutation");

        CommitMutation(TMutationRequest{.Reign = GetCurrentReign()})
            .WithTimeout(Config_->HeartbeatMutationTimeout)
            .Subscribe(BIND([=, this_ = MakeStrong(this), weakEpochContext = MakeWeak(ControlEpochContext_)] (const TErrorOr<TMutationResponse>& result){
                if (result.IsOK()) {
                    YT_LOG_DEBUG("Heartbeat mutation commit succeeded");
                    return;
                }

                if (GetReadOnly()) {
                    return;
                }

                auto epochContext = weakEpochContext.Lock();
                if (!epochContext) {
                    return;
                }

                if (epochContext->LeaderSwitchStarted) {
                    return;
                }

                ScheduleRestart(
                    epochContext,
                    TError("Heartbeat mutation commit failed") << result);
            }));
    }


    void SetReadOnly(bool value)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!value) {
            return;
        }

        YT_VERIFY(IsActiveLeader());

        if (!ReadOnly_) {
            ReadOnly_ = true;
            YT_LOG_INFO("Read-only mode activated");

            ControlEpochContext_->LeaderCommitter->SetReadOnly();
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedHydraManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedHydraManagerPtr CreateDistributedHydraManager(
    TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    IAutomatonPtr automaton,
    IServerPtr rpcServer,
    IElectionManagerPtr electionManager,
    TCellId cellId,
    IChangelogStoreFactoryPtr changelogStoreFactory,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options,
    const TDistributedHydraManagerDynamicOptions& dynamicOptions)
{
    YT_VERIFY(config);
    YT_VERIFY(controlInvoker);
    YT_VERIFY(automatonInvoker);
    YT_VERIFY(automaton);
    YT_VERIFY(rpcServer);
    YT_VERIFY(electionManager);
    YT_VERIFY(changelogStoreFactory);
    YT_VERIFY(snapshotStore);

    return New<TDistributedHydraManager>(
        config,
        controlInvoker,
        automatonInvoker,
        automaton,
        rpcServer,
        electionManager,
        cellId,
        changelogStoreFactory,
        snapshotStore,
        options,
        dynamicOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
