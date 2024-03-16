#include "distributed_hydra_manager.h"

#include "changelog.h"
#include "changelog_acquisition.h"
#include "changelog_discovery.h"
#include "config.h"
#include "decorated_automaton.h"
#include "helpers.h"
#include "hydra_manager.h"
#include "hydra_service.h"
#include "hydra_service_proxy.h"
#include "lease_tracker.h"
#include "mutation.h"
#include "mutation_committer.h"
#include "mutation_context.h"
#include "private.h"
#include "recovery.h"
#include "serialize.h"
#include "snapshot.h"
#include "snapshot_discovery.h"
#include "state_hash_checker.h"

#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/config.h>
#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/election/election_service_proxy.h>
#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <util/generic/cast.h>

#include <atomic>

namespace NYT::NHydra {

using namespace NElection;
using namespace NLogging;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
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
            TCurrentEpochIdGuard guard(epochId);
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

        void OnDiscombobulate(i64 leaderSequenceNumber) override
        {
            CancelableControlInvoker_->Invoke(BIND(
                &TDistributedHydraManager::OnDiscombobulate,
                Owner_,
                leaderSequenceNumber));
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
        IAuthenticatorPtr authenticator,
        const TDistributedHydraManagerOptions& options,
        const TDistributedHydraManagerDynamicOptions& dynamicOptions)
        : StaticConfig_(config)
        , Config_(New<TConfigWrapper>(config))
        , RpcServer_(std::move(rpcServer))
        , ElectionManager_(std::move(electionManager))
        , ControlInvoker_(std::move(controlInvoker))
        , CancelableControlInvoker_(CancelableContext_->CreateInvoker(ControlInvoker_))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , ChangelogStoreFactory_(std::move(changelogStoreFactory))
        , SnapshotStore_(std::move(snapshotStore))
        , Options_(options)
        , StateHashChecker_(New<TStateHashChecker>(Config_->Get()->MaxStateHashCheckerEntryCount, HydraLogger))
        , DynamicOptions_(dynamicOptions)
        , ElectionCallbacks_(New<TElectionCallbacks>(this))
        , Profiler_(HydraProfiler.WithTag("cell_id", ToString(cellId)).WithSparse())
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
            cellId,
            authenticator))
        , InternalHydraService_(New<TInternalHydraService>(
            this,
            ControlInvoker_,
            cellId,
            authenticator))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
        VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);

        Profiler_.AddFuncGauge("/leading", MakeStrong(this), [this] {
            return IsLeader();
        });
        Profiler_.AddFuncGauge("/active", MakeStrong(this), [this] {
            return IsActive();
        });
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
        return ElectionManager_->Abandon(error)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] {
                CancelableContext_->Cancel(error);

                if (ControlState_ != EPeerState::None) {
                    RpcServer_->UnregisterService(HydraService_);
                    RpcServer_->UnregisterService(InternalHydraService_);
                }

                StopEpoch();

                ControlState_ = EPeerState::Stopped;

                LeaderRecovered_ = false;
                FollowerRecovered_ = false;
            }).AsyncVia(ControlInvoker_))
            .Apply(BIND(&TDistributedHydraManager::DoFinalize, MakeStrong(this))
                .AsyncVia(AutomatonInvoker_));
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

    TEpochId GetAutomatonEpochId() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_ ? AutomatonEpochContext_->EpochId : TEpochId();
    }

    int GetAutomatonTerm() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_ ? AutomatonEpochContext_->Term : InvalidTerm;
    }

    TFuture<void> Reconfigure(TDynamicDistributedHydraManagerConfigPtr dynamicConfig) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto epochContext = AtomicEpochContext_.Acquire()) {
            return BIND(&TDistributedHydraManager::ReconfigureControl, MakeStrong(this), std::move(dynamicConfig))
                .AsyncVia(epochContext->EpochControlInvoker)
                .Run();
        }
        return VoidFuture;
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

        if (GetReadOnly()) {
            if (setReadOnly) {
                auto result = leaderCommitter->GetLastSnapshotFuture(waitForSnapshotCompletion, setReadOnly);
                if (result) {
                    return *result;
                }
            }

            auto loggedVersion = leaderCommitter->GetNextLoggedVersion();
            auto lastSnapshotId = DecoratedAutomaton_->GetLastSuccessfulSnapshotId();
            if (loggedVersion.RecordId == 0 && loggedVersion.SegmentId == lastSnapshotId) {
                return MakeFuture<int>(TError(
                    NHydra::EErrorCode::ReadOnlySnapshotBuilt,
                    "The requested read-only snapshot was already built and its result was already lost")
                    << TErrorAttribute("snapshot_id", lastSnapshotId));
            }
            return MakeFuture<int>(TError(
                NHydra::EErrorCode::ReadOnlySnapshotBuildFailed,
                "Cannot build a snapshot in read-only mode"));
        }

        if (!leaderCommitter->CanBuildSnapshot()) {
            return MakeFuture<int>(TError(
                NRpc::EErrorCode::Unavailable,
                "Snapshot is already being built"));
        }

        if (setReadOnly) {
            WaitFor(CommitMutation(MakeSystemMutationRequest(EnterReadOnlyMutationType)))
                .ThrowOnError();
        }

        return leaderCommitter->BuildSnapshot(waitForSnapshotCompletion, setReadOnly);
    }

    TYsonProducer GetMonitoringProducer() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND([=, this, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
            VERIFY_THREAD_AFFINITY_ANY();
            auto epochContext = AtomicEpochContext_.Acquire();

            BuildYsonFluently(consumer)
                .BeginMap()
                    .DoIf(static_cast<bool>(epochContext), [&] (TFluentMap fluent) {
                        auto selfId = epochContext->CellManager->GetSelfPeerId();
                        fluent
                            .Item("term").Value(epochContext->Term)
                            .Item("epoch_id").Value(epochContext->EpochId)
                            .Item("leader_id").Value(epochContext->LeaderId)
                            .Item("self_id").Value(selfId)
                            .Item("voting").Value(epochContext->CellManager->GetPeerConfig(selfId)->Voting);
                            // TODO(aleksandra-zh): add stuff.
                    })
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
                    .Item("last_snapshot_read_only").Value(DecoratedAutomaton_->GetLastSuccessfulSnapshotReadOnly())
                    .Item("discombobulated").Value(IsDiscombobulated())
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

        if (epochContext->LeaderId == epochContext->CellManager->GetSelfPeerId() ||
            epochContext->Discombobulated)
        {
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

    TFuture<TMutationResponse> Forward(TMutationRequest&& request)
    {
        // TODO(aleksandra-zh): Change affinity to any.
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!ControlEpochContext_) {
            return MakeFuture<TMutationResponse>(TError(
                NRpc::EErrorCode::Unavailable,
                "Peer is not active"));
        }

        auto epochContext = ControlEpochContext_;

        const auto& cellManager = epochContext->CellManager;
        auto channel = cellManager->GetPeerChannel(epochContext->LeaderId);
        YT_VERIFY(channel);

        TInternalHydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->Get()->CommitForwardingRpcTimeout);

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

    TMutationRequest MakeSystemMutationRequest(const TString& mutationType)
    {
        return {
            .Reign = GetCurrentReign(),
            .Type = mutationType
        };
    }

    TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto state = GetAutomatonState();
        switch (state) {
            case EPeerState::Leading: {
                if (!LeaderRecovered_) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader has not recovered yet"));
                }

                return EnqueueMutation(std::move(request));
            }

            case EPeerState::Following:
                if (!request.AllowLeaderForwarding) {
                    return MakeFuture<TMutationResponse>(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Leader mutation forwarding is not allowed"));
                }

                return BIND(&TDistributedHydraManager::Forward, MakeStrong(this))
                    .AsyncVia(ControlInvoker_)
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
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto epochContext = AtomicEpochContext_.Acquire()) {
            return epochContext->ReadOnly;
        }
        return false;
    }

    bool IsDiscombobulated() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto epochContext = AtomicEpochContext_.Acquire()) {
            return epochContext->Discombobulated;
        }
        return false;
    }

    i64 GetSequenceNumber() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedAutomaton_->GetSequenceNumber();
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

    const TDistributedHydraManagerConfigPtr StaticConfig_;
    const TConfigWrapperPtr Config_;
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
    const TMutationDraftQueuePtr MutationDraftQueue_ = New<TMutationDraftQueue>();

    int SnapshotId_ = -1;
    TFuture<TRemoteSnapshotParams> SnapshotFuture_;

    std::atomic<bool> LeaderRecovered_ = false;
    std::atomic<bool> FollowerRecovered_ = false;
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
    TAtomicIntrusivePtr<TEpochContext> AtomicEpochContext_;

    TAtomicObject<TPeerIdSet> AlivePeerIds_;

    class TOwnedHydraServiceBase
        : public THydraServiceBase
    {
    protected:
        TOwnedHydraServiceBase(
            TDistributedHydraManagerPtr owner,
            IInvokerPtr invoker,
            const TServiceDescriptor& descriptor,
            TCellId cellId,
            IAuthenticatorPtr authenticator)
            : THydraServiceBase(
                owner,
                invoker,
                descriptor,
                HydraLogger.WithTag("CellId: %v", cellId),
                cellId,
                CreateHydraManagerUpstreamSynchronizer(owner),
                std::move(authenticator))
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
            TCellId cellId,
            IAuthenticatorPtr authenticator)
        : TOwnedHydraServiceBase(
            distributedHydraManager,
            controlInvoker,
            THydraServiceProxy::GetDescriptor(),
            cellId,
            std::move(authenticator))
        {
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceBuildSnapshot));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceSyncWithLeader));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(Poke));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(PrepareLeaderSwitch));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ForceRestart));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPeerState));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ResetStateHash));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ExitReadOnly));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(DiscombobulateNonvotingPeers));
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

            context->Reply();
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

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, ResetStateHash)
        {
            context->SetRequestInfo("NewStateHash: %x", request->new_state_hash());
            context->SetResponseInfo();

            auto owner = GetOwnerOrThrow();

            auto mutation = CreateMutation(owner, *request);
            YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, ExitReadOnly)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();
            if (!owner->IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }

            owner->CommitMutation(owner->MakeSystemMutationRequest(ExitReadOnlyMutationType))
                .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                    if (!result.IsOK()) {
                        context->Reply(result);
                        return;
                    }

                    if (owner->GetReadOnly()) {
                        owner->ScheduleRestart(owner->ControlEpochContext_, TError("Exited read only mode"));
                    }
                    context->Reply();
                }).Via(owner->ControlInvoker_));
        }

        DECLARE_RPC_SERVICE_METHOD(NHydra::NProto, DiscombobulateNonvotingPeers)
        {
            context->SetRequestInfo();

            auto owner = GetOwnerOrThrow();
            owner->DiscombobulateNonvotingPeers();

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
            TCellId cellId,
            NRpc::IAuthenticatorPtr authenticator)
        : TOwnedHydraServiceBase(
            distributedHydraManager,
            controlInvoker,
            TInternalHydraServiceProxy::GetDescriptor(),
            cellId,
            std::move(authenticator))
        {
            RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot)
                .SetStreamingEnabled(true)
                .SetCancelable(true));
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
        DECLARE_RPC_SERVICE_METHOD(NProto, LookupSnapshot)
        {
            auto maxSnapshotId = request->max_snapshot_id();
            auto exactId = request->exact_id();
            context->SetRequestInfo("MaxSnapshotId: %v, ExactId: %v",
                maxSnapshotId,
                exactId);

            auto owner = GetOwnerOrThrow();

            auto snapshotId = maxSnapshotId;
            if (!exactId) {
                snapshotId = WaitFor(owner->SnapshotStore_->GetLatestSnapshotId(maxSnapshotId))
                    .ValueOrThrow();
            }

            response->set_snapshot_id(snapshotId);

            if (snapshotId != InvalidSegmentId) {
                auto reader = owner->SnapshotStore_->CreateReader(snapshotId);
                WaitFor(reader->Open())
                    .ThrowOnError();

                auto params = reader->GetParams();
                response->set_compressed_length(params.CompressedLength);
                response->set_uncompressed_length(params.UncompressedLength);
                response->set_checksum(params.Checksum);
                *response->mutable_meta() = params.Meta;
            }

            context->SetResponseInfo("SnapshotId: %v", snapshotId);
            context->Reply();
        }

        DECLARE_RPC_SERVICE_METHOD(NProto, ReadSnapshot)
        {
            auto snapshotId = request->snapshot_id();
            context->SetRequestInfo("SnapshotId: %v, ResponseCodec: %v",
                snapshotId,
                context->GetResponseCodec());

            auto owner = GetOwnerOrThrow();
            auto reader = owner->SnapshotStore_->CreateReader(snapshotId);

            WaitFor(reader->Open())
                .ThrowOnError();

            HandleInputStreamingRequest(context, reader);
        }

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
            context->SetRequestInfo();

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
            auto sequenceNumber = owner->SyncWithLeader(epochId, term);

            context->SetResponseInfo("SyncSequenceNumber: %v",
                sequenceNumber);

            response->set_sync_sequence_number(sequenceNumber);
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
            auto peerId = request->peer_id();
            context->SetRequestInfo("PeerId: %v",
                peerId);

            auto owner = GetOwnerOrThrow();

            // TODO(aleksandra-zh): remove it from here.
            if (owner->Config_->Get()->EnableStateHashChecker) {
                for (const auto& mutationInfo : request->mutations_info()) {
                    auto sequenceNumber = mutationInfo.sequence_number();
                    auto stateHash = mutationInfo.state_hash();
                    owner->StateHashChecker_->Report(sequenceNumber, stateHash, peerId);
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

        auto changelogId = ChangelogStore_->GetLatestChangelogIdOrThrow();
        if (changelogId == InvalidSegmentId) {
            return {InvalidSegmentId, InvalidTerm};
        }

        auto term = ChangelogStore_->GetTermOrThrow();
        return {changelogId, term};
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
            Config_->Get()->MaxChangelogBytesPerRequest);
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

        auto mutationCount = request->Attachments().size();
        context->SetRequestInfo("StartSequenceNumber: %v, CommittedSequenceNumber: %v, CommittedSegmentId: %v, EpochId: %v, MutationCount: %v",
            startSequenceNumber,
            committedSequenceNumber,
            committedSegmentId,
            epochId,
            mutationCount);

        auto epochContext = GetControlEpochContext(epochId);
        // TODO(aleksandra-zh): I hate that.
        SwitchTo(epochContext->EpochControlInvoker);

        VERIFY_THREAD_AFFINITY(ControlThread);

        auto isPersistenceEnabled = IsPersistenceEnabled(epochContext->CellManager, Options_);
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
                "Cannot accept mutations before initial ping with OK version is received");
        }

        // We can not verify term before CheckForInitialPing because term may change on initial ping.
        // However, we should verify our term is not greater than leader's before CheckForInitialPing to
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
        if (request->has_snapshot_request()) {
            const auto& snapshotRequest = request->snapshot_request();
            auto snapshotId = snapshotRequest.snapshot_id();

            if (snapshotId < SnapshotId_) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Received a snapshot request with a snapshot id %v while last snapshot id %v is greater",
                    snapshotId,
                    SnapshotId_);
            }

            if (controlState == EPeerState::Following) {
                if (snapshotId == SnapshotId_) {
                    YT_LOG_DEBUG("Received a redundant snapshot request, ignoring (SnapshotId: %v)",
                        SnapshotId_);
                } else {
                    auto readOnly = snapshotRequest.read_only();
                    auto snapshotSequenceNumber = snapshotRequest.sequence_number();
                    SetReadOnly(readOnly);

                    SnapshotId_ = snapshotId;
                    YT_LOG_INFO("Received a new snapshot request (SnapshotId: %v, SequenceNumber: %v, ReadOnly: %v)",
                        SnapshotId_,
                        snapshotSequenceNumber,
                        readOnly);
                    if (isPersistenceEnabled) {
                        SnapshotFuture_ = BIND(&TDecoratedAutomaton::BuildSnapshot, DecoratedAutomaton_)
                            .AsyncVia(epochContext->EpochUserAutomatonInvoker)
                            .Run(SnapshotId_, snapshotSequenceNumber, readOnly);
                    } else {
                        SnapshotFuture_ = MakeFuture<TRemoteSnapshotParams>(TError("Followers cannot build snapshot"));
                    }
                }
            } else {
                YT_VERIFY(controlState == EPeerState::FollowerRecovery);
                YT_VERIFY(SnapshotId_ <= snapshotId);

                if (SnapshotId_ < snapshotId) {
                    SnapshotId_ = snapshotId;
                    SnapshotFuture_ = MakeFuture<TRemoteSnapshotParams>(TError("Cannot build snapshot during recovery"));
                }
            }

            if (SnapshotFuture_ && SnapshotFuture_.IsSet()) {
                auto* snapshotResponse = response->mutable_snapshot_response();
                const auto& snapshotParamsOrError = SnapshotFuture_.Get();
                snapshotResponse->set_snapshot_id(SnapshotId_);
                if (snapshotParamsOrError.IsOK()) {
                    const auto& snapshotParams = snapshotParamsOrError.Value();
                    YT_VERIFY(SnapshotId_ == snapshotParams.SnapshotId);
                    snapshotResponse->set_checksum(snapshotParams.Checksum);
                } else {
                    ToProto(snapshotResponse->mutable_error(), snapshotParamsOrError);
                }
            }
        }

        auto mutationsAccepted = epochContext->FollowerCommitter->AcceptMutations(
            startSequenceNumber,
            request->Attachments());

        if (controlState == EPeerState::Following || epochContext->CatchingUp) {
            epochContext->FollowerCommitter->LogMutations();
            CommitMutationsAtFollower(committedSequenceNumber);
        }

        // LoggedSequenceNumber in committer should already be initialized.
        auto loggedSequenceNumber = epochContext->FollowerCommitter->GetLoggedSequenceNumber();
        auto expectedSequenceNumber = epochContext->FollowerCommitter->GetExpectedSequenceNumber();

        response->set_logged_sequence_number(loggedSequenceNumber);
        response->set_expected_sequence_number(expectedSequenceNumber);
        response->set_mutations_accepted(mutationsAccepted);

        context->SetResponseInfo("LoggedSequenceNumber: %v, ExpectedSequenceNumber: %v, MutationsAccepted: %v",
            loggedSequenceNumber,
            expectedSequenceNumber,
            mutationsAccepted);
        context->Reply();
    }

    TFuture<TMutationResponse> ForceCommitMutation(TMutationRequest&& request)
    {
        auto state = GetAutomatonState();
        if (state != EPeerState::Leading) {
            return MakeFuture<TMutationResponse>(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot commit mutation in %Qlv state",
                state));
        }

        return EnqueueMutation(std::move(request));
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

        auto randomSeed = RandomNumber<ui64>();
        YT_LOG_DEBUG("Enqueue mutation (RandomSeed: %x, MutationType: %v, MutationId: %v, EpochId: %v)",
            randomSeed,
            request.Type,
            request.MutationId,
            request.EpochId);

        MutationDraftQueue_->Enqueue({
            .Request = request,
            .Promise = std::move(promise),
            .RandomSeed = randomSeed
        });

        if (Config_->Get()->MinimizeCommitLatency) {
            if (auto epochContext = AtomicEpochContext_.Acquire()) {
                epochContext->EpochControlInvoker->Invoke(BIND([=, this, this_ = MakeStrong(this)] {
                    VERIFY_THREAD_AFFINITY(ControlThread);
                    if (ControlState_ == EPeerState::Leading) {
                        epochContext->LeaderCommitter->SerializeMutations();
                    }
                }));
            }
        }

        return future;
    }

    // TODO(aleksandra-zh): epochId seems redundant.
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

        if (term && controlState != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot handle follower ping with term in %Qlv state",
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

        if (controlState != EPeerState::FollowerRecovery && controlState != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v in %Qlv state",
                changelogId,
                controlState);
        }

        if (controlState == EPeerState::Following && elections) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v because peer is already following",
                changelogId);
        }

        if (epochContext->Recovery && elections) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v after recovery started",
                changelogId);
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

        auto currentChangelogId = changelogStore->GetLatestChangelogIdOrThrow();
        if (currentChangelogId >= changelogId) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v because changelog %v exists",
                changelogId,
                currentChangelogId);
        }

        auto currentSnapshotId = WaitFor(SnapshotStore_->GetLatestSnapshotId())
            .ValueOrThrow();
        if (currentSnapshotId >= changelogId) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot acquire changelog %v because snapshot %v exists",
                changelogId,
                currentSnapshotId);
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

        auto startChangelogId = std::max(currentChangelogId + 1, changelogId - Config_->Get()->MaxChangelogsToCreateDuringAcquisition);
        for (int id = startChangelogId; id <= changelogId; ++id) {
            auto changelog = WaitFor(changelogStore->CreateChangelog(id, /*meta*/ {}, {.CreateWriterEagerly = true}))
                .ValueOrThrow();
            epochContext->FollowerCommitter->RegisterNextChangelog(id, changelog);
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

    i64 SyncWithLeader(TEpochId epochId, int term)
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

        YT_VERIFY(epochContext->LeaderCommitter);
        auto lastOffloadedSequenceNumber = epochContext->LeaderCommitter->GetLastOffloadedSequenceNumber();
        // We still do not want followers to apply mutations before leader.
        WaitFor(epochContext->LeaderCommitter->GetLastOffloadedMutationsFuture())
            .ThrowOnError();

        return lastOffloadedSequenceNumber;
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

        auto leaderSwitchTimeout = Config_->Get()->LeaderSwitchTimeout;
        YT_LOG_INFO("Preparing leader switch (Timeout: %v)",
            leaderSwitchTimeout);

        TDelayedExecutor::Submit(
            BIND([=, this, this_ = MakeWeak(this)] {
                ScheduleRestart(
                    epochContext,
                    TError("Leader switch did not complete within timeout"));
            }),
            leaderSwitchTimeout);

        epochContext->LeaderSwitchStarted = true;
        WaitFor(epochContext->LeaderCommitter->GetLastMutationFuture())
            .ThrowOnError();
    }

    bool AbandonLeaderLease(int peerId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        bool abandoned = LeaderLease_->TryAbandon();
        if (abandoned) {
            YT_LOG_INFO("Leader lease abandoned (RequestingPeerId: %v)",
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

        // TODO(aleksandra-zh): order?
        if (!ElectionPriority_) {
            THROW_ERROR_EXCEPTION("Election priority is not available");
        }

        auto controlState = GetControlState();
        auto getElectionPriority = [&] () -> std::pair<int, i64> {
            switch (controlState) {
                case EPeerState::Leading:
                    YT_VERIFY(ControlEpochContext_);
                    return {ControlEpochContext_->Term, ControlEpochContext_->LeaderCommitter->GetLoggedSequenceNumber()};

                case EPeerState::Following:
                    YT_VERIFY(ControlEpochContext_);
                    return {ControlEpochContext_->Term, ControlEpochContext_->FollowerCommitter->GetLoggedSequenceNumber()};

                default:
                    return {ElectionPriority_->LastMutationTerm, ElectionPriority_->ReachableState.SequenceNumber};
            }
        };
        auto priority = getElectionPriority();
        return {priority.first, priority.second * 2 + (EnablePriorityBoost_ ? 1 : 0)};
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
        WaitFor(ElectionManager_->Abandon(error))
            .ThrowOnError();
    }

    TElectionPriority GetSnapshotElectionPriority()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto asyncMaxSnapshotId = SnapshotStore_->GetLatestSnapshotId();
        int maxSnapshotId = WaitFor(asyncMaxSnapshotId)
            .ValueOrThrow();

        if (maxSnapshotId == InvalidSegmentId) {
            YT_LOG_INFO("No snapshots found");
            // Let's pretend we have snapshot 0.
            return {0, 0, 0};
        }

        auto reader = SnapshotStore_->CreateReader(maxSnapshotId);
        WaitFor(reader->Open())
            .ThrowOnError();

        auto meta = reader->GetParams().Meta;
        if (!meta.has_last_mutation_term()) {
            // COMPAT(aleksandra-zh): Looks safe.
            YT_LOG_INFO("No term in snapshot meta (SnapshotId: %v, SequenceNumber: %v)",
                maxSnapshotId,
                meta.sequence_number());
            return {0, maxSnapshotId, meta.sequence_number()};
        }

        TElectionPriority electionPriority(meta.last_mutation_term(), meta.last_segment_id(), meta.sequence_number());
        YT_LOG_INFO(
            "The latest snapshot election priority is available (SnapshotId: %v, ElectionPriority: %v)",
            maxSnapshotId,
            electionPriority);
        return electionPriority;
    }

    TElectionPriority GetChangelogElectionPriority()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ChangelogStore_ = WaitFor(ChangelogStoreFactory_->Lock())
            .ValueOrThrow();

        auto optionalElectionPriority = ChangelogStore_->GetElectionPriority();
        if (optionalElectionPriority) {
            YT_LOG_INFO("Changelog election priority is available (ElectionPriority: %v)",
                *optionalElectionPriority);
            return *optionalElectionPriority;
        } else {
            // TODO(aleksandra-zh): our priority is logged state.
            auto state = DecoratedAutomaton_->GetReachableState();
            TElectionPriority priority(DecoratedAutomaton_->GetLastMutationTerm(), state);
            YT_LOG_INFO("Election priority is not available, using automaton state instead (AutomatonElectionPriority: %v)",
                priority);
            return priority;
        }
    }

    void DoParticipate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Initializing persistent stores");

        auto backoffTime = Config_->Get()->MinPersistentStoreInitializationBackoffTime;

        while (true) {
            try {
                auto snapshotElectionPriority = GetSnapshotElectionPriority();
                auto changelogElectionPriority = GetChangelogElectionPriority();
                if (snapshotElectionPriority.LastMutationTerm != 0 &&
                    changelogElectionPriority.LastMutationTerm == 0 &&
                    changelogElectionPriority.ReachableState > snapshotElectionPriority.ReachableState)
                {
                    YT_LOG_ALERT("Nonzero term snapshot is followed by a zero term changelog (SnapshotPriority: %v, ChangelogPriority: %v)",
                        snapshotElectionPriority,
                        changelogElectionPriority);
                    YT_ABORT();
                }
                ElectionPriority_ = std::max(snapshotElectionPriority, changelogElectionPriority);
                break;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error initializing persistent stores, backing off and retrying");
                TDelayedExecutor::WaitForDuration(backoffTime);
                backoffTime = std::min(
                    backoffTime * Config_->Get()->PersistentStoreInitializationBackoffTimeMultiplier,
                    Config_->Get()->MaxPersistentStoreInitializationBackoffTime);
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

    void ReconfigureControl(TDynamicDistributedHydraManagerConfigPtr dynamicConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto newConfig = StaticConfig_->ApplyDynamic(dynamicConfig);
        Config_->Set(newConfig);

        StateHashChecker_->ReconfigureLimit(newConfig->MaxStateHashCheckerEntryCount);

        if (!ControlEpochContext_) {
            return;
        }

        if (ControlEpochContext_->LeaderCommitter) {
            ControlEpochContext_->LeaderCommitter->Reconfigure();
        }

        if (ControlEpochContext_->HeartbeatMutationCommitExecutor) {
            ControlEpochContext_->HeartbeatMutationCommitExecutor->SetPeriod(Config_->Get()->HeartbeatMutationPeriod);
        }
    }

    IChangelogPtr OpenChangelogOrThrow(int id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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

    IChangelogPtr AcquireChangelog()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // TODO(aleksandra-zh): verify state.

        int selfChangelogId = ChangelogStore_->GetLatestChangelogIdOrThrow();

        YT_VERIFY(ElectionPriority_);

        auto asyncResult = ComputeQuorumLatestChangelogId(
            Config_->Get(),
            ControlEpochContext_->CellManager,
            selfChangelogId,
            ChangelogStore_->GetTermOrThrow());
        auto [changelogId, changelogTerm] = WaitFor(asyncResult)
            .ValueOrThrow();

        auto [snapshotId, snapshotTerm] = ComputeQuorumLatestSnapshotId();

        auto newChangelogId = std::max(snapshotId, changelogId) + 1;
        auto newTerm = std::max(snapshotTerm, changelogTerm) + 1;

        WaitFor(ChangelogStore_->SetTerm(newTerm))
            .ThrowOnError();
        ControlEpochContext_->Term = newTerm;

        // TODO(aleksandra-zh): What kind of barrier should I have here (if any)?
        auto priority = GetElectionPriority();
        YT_LOG_INFO("Acquiring changelog (ChangelogId: %v, Priority: %v, Term: %v)",
            newChangelogId,
            priority,
            newTerm);
        auto newChangelog = WaitFor(RunChangelogAcquisition(Config_->Get(), ControlEpochContext_, newChangelogId, priority, Logger))
            .ValueOrThrow();
        YT_LOG_INFO("Changelog acquired (ChangelogId: %v)",
            newChangelogId);

        return newChangelog;
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
                Config_->Get(),
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
            auto newChangelog = AcquireChangelog();

            epochContext->LeaderCommitter = New<TLeaderCommitter>(
                Config_,
                Options_,
                DecoratedAutomaton_,
                LeaderLease_,
                MutationDraftQueue_,
                newChangelog,
                epochContext->ReachableState,
                epochContext.Get(),
                Logger,
                Profiler_);

            epochContext->LeaderCommitter->SubscribeLoggingFailed(
                BIND(&TDistributedHydraManager::OnLoggingFailed, MakeWeak(this)));

            epochContext->Recovery = New<TRecovery>(
                Config_->Get(),
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
            WaitFor(epochContext->Recovery->Run())
                .ThrowOnError();

            MaybeSetReadOnlyOnRecovery();

            if (Config_->Get()->DisableLeaderLeaseGraceDelay) {
                YT_LOG_WARNING("Leader lease grace delay disabled; cluster can only be used for testing purposes");
                GraceDelayStatus_ = EGraceDelayStatus::GraceDelayDisabled;
            } else if (TryAbandonExistingLease(epochContext)) {
                YT_LOG_INFO("Previous leader lease was abandoned; ignoring leader lease grace delay");
                GraceDelayStatus_ = EGraceDelayStatus::PreviousLeaseAbandoned;
            } else {
                YT_LOG_INFO("Waiting for previous leader lease to expire (Delay: %v)",
                    Config_->Get()->LeaderLeaseGraceDelay);
                TDelayedExecutor::WaitForDuration(Config_->Get()->LeaderLeaseGraceDelay);
                GraceDelayStatus_ = EGraceDelayStatus::GraceDelayExecuted;
            }

            YT_LOG_INFO("Waiting for followers to recover");
            epochContext->LeaseTracker->EnableSendingTerm();
            WaitFor(epochContext->LeaseTracker->GetNextQuorumFuture())
                .ThrowOnError();
            YT_LOG_INFO("Followers recovered");
            YT_LOG_INFO("Acquiring leader lease");
            epochContext->LeaseTracker->EnableTracking();
            WaitFor(epochContext->LeaseTracker->GetNextQuorumFuture())
                .ThrowOnError();
            YT_LOG_INFO("Leader lease acquired");

            YT_VERIFY(ControlState_ == EPeerState::LeaderRecovery);
            ControlState_ = EPeerState::Leading;

            const auto& leaderCommitter = epochContext->LeaderCommitter;
            leaderCommitter->Start();

            WaitFor(BIND(&TDistributedHydraManager::OnLeaderRecoveryCompleteAutomaton, MakeWeak(this))
                .AsyncVia(epochContext->EpochSystemAutomatonInvoker)
                .Run())
                .ThrowOnError();

            ControlLeaderRecoveryComplete_.Fire();

            YT_LOG_INFO("Committing initial heartbeat mutation");
            WaitFor(ForceCommitMutation(MakeSystemMutationRequest(HeartbeatMutationType)))
                .ThrowOnError();
            YT_LOG_INFO("Initial heartbeat mutation committed");

            if (Options_.ResponseKeeper) {
                Options_.ResponseKeeper->Start();
            }

            WaitFor(BIND(&TDistributedHydraManager::OnLeaderActiveAutomaton, MakeWeak(this))
                .AsyncVia(epochContext->EpochSystemAutomatonInvoker)
                .Run())
                .ThrowOnError();

            epochContext->HeartbeatMutationCommitExecutor->Start();

            auto mutationCount = DecoratedAutomaton_->GetMutationCountSinceLastSnapshot();
            auto mutationSize = DecoratedAutomaton_->GetMutationSizeSinceLastSnapshot();
            auto lastSnapshotId = DecoratedAutomaton_->GetLastSuccessfulSnapshotId();
            auto tailChangelogCount = epochContext->ReachableState.SegmentId - lastSnapshotId;
            if (tailChangelogCount >= Config_->Get()->MaxChangelogsForRecovery ||
                mutationCount >= Config_->Get()->MaxChangelogMutationCountForRecovery ||
                mutationSize >= Config_->Get()->MaxTotalChangelogSizeForRecovery)
            {
                YT_LOG_INFO("Tail changelogs limits violated, force building snapshot "
                    "(TailChangelogCount: %v, MutationCount: %v, TotalChangelogSize: %v)",
                    tailChangelogCount,
                    mutationCount,
                    mutationSize);
                // If committer cannot build snapshot, then snapshot is already being built and it is ok.
                if (leaderCommitter->CanBuildSnapshot()) {
                    YT_UNUSED_FUTURE(leaderCommitter->BuildSnapshot(
                        /*waitForSnapshotCompletion*/ false,
                        /*setReadOnly*/ false));
                }
            } else {
                YT_LOG_INFO("Tail changelogs limits are OK "
                    "(TailChangelogCount: %v, MutationCount: %v, TotalChangelogSize: %v)",
                    tailChangelogCount,
                    mutationCount,
                    mutationSize);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Leader recovery failed, backing off");
            TDelayedExecutor::WaitForDuration(Config_->Get()->RestartBackoffTime);
            ScheduleRestart(epochContext, ex);
        }
    }

    void OnLeaderRecoveryCompleteAutomaton()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_INFO("Leader recovery completed");

        DecoratedAutomaton_->OnLeaderRecoveryComplete();

        AutomatonLeaderRecoveryComplete_.Fire();
    }

    void OnLeaderActiveAutomaton()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_INFO("Leader active");

        LeaderRecovered_ = true;

        SystemLockGuard_.Release();

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
            req->SetTimeout(Config_->Get()->AbandonLeaderLeaseRequestTimeout);
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

        if (auto leaseTracker = epochContext->LeaseTracker) {
            leaseTracker->Finalize();
        }

        // Save for later to respect the thread affinity.
        auto leaderCommitter = epochContext->LeaderCommitter;

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
            WaitFor(epochContext->Recovery->Run())
                .ThrowOnError();

            YT_LOG_INFO("Follower is catching up");

            epochContext->CatchingUp = true;

            auto followerCommitter = epochContext->FollowerCommitter;
            YT_VERIFY(followerCommitter);

            followerCommitter->CatchUp();

            // NB: Do not discard CatchingUp.

            YT_LOG_INFO("Follower caught up");

            MaybeSetReadOnlyOnRecovery();

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
            TDelayedExecutor::WaitForDuration(Config_->Get()->RestartBackoffTime);
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

    void OnDiscombobulate(i64 leaderSequenceNumber)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Entering discomobulated state");

        WaitFor(SyncWithLeader())
            .ThrowOnError();

        auto sequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
        if (sequenceNumber < leaderSequenceNumber) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Couldn`t sync to required sequence number (expected: %v, actual: %v)",
                leaderSequenceNumber,
                sequenceNumber);
        }

        YT_VERIFY(ControlState_ == EPeerState::Following && ControlEpochContext_);
        ControlEpochContext_->Discombobulated = true;
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

        YT_LOG_INFO("Received initial ping from leader (CommittedState: %v, Term: %v)",
            committedState,
            term);

        epochContext->Recovery = New<TRecovery>(
            Config_->Get(),
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

        auto isPersistenceEnabled = IsPersistenceEnabled(epochContext->CellManager, Options_);
        // If we join an active quorum, we wont get an AcquireChangelog request from leader,
        // so we wont be able to promote our term there, so we have to do it here.
        // Looks safe.
        if (isPersistenceEnabled) {
            WaitFor(ChangelogStore_->SetTerm(term))
                .ThrowOnError();
        }
        epochContext->Term = term;

        // Make sure we dont try to build the same snapshot twice.
        // (It does not always help, LoadSnapshot in decorated automaton explains why).
        SnapshotId_ = committedState.SegmentId;
        SnapshotFuture_ = MakeFuture<TRemoteSnapshotParams>(TError("Not building snapshot as it could have been downloaded during recovery"));

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

        auto selfPeerId = electionEpochContext->CellManager->GetSelfPeerId();
        auto voting = electionEpochContext->CellManager->GetPeerConfig(selfPeerId)->Voting;
        auto isPersistenceEnabled = IsPersistenceEnabled(electionEpochContext->CellManager, Options_);

        auto epochContext = New<TEpochContext>();
        epochContext->CellManager = electionEpochContext->CellManager;
        epochContext->ChangelogStore = ChangelogStore_;
        epochContext->ReachableState = ElectionPriority_->ReachableState;
        epochContext->Term = isPersistenceEnabled ? ChangelogStore_->GetTermOrThrow() : InvalidTerm;
        epochContext->LeaderId = electionEpochContext->LeaderId;
        epochContext->EpochId = electionEpochContext->EpochId;
        epochContext->CancelableContext = electionEpochContext->CancelableContext;
        epochContext->EpochControlInvoker = CreateEpochInvoker(epochContext, CancelableControlInvoker_);
        epochContext->EpochSystemAutomatonInvoker = CreateEpochInvoker(epochContext, DecoratedAutomaton_->GetSystemInvoker());
        epochContext->EpochUserAutomatonInvoker = CreateEpochInvoker(epochContext, AutomatonInvoker_);
        epochContext->HeartbeatMutationCommitExecutor = New<TPeriodicExecutor>(
            epochContext->EpochControlInvoker,
            BIND(&TDistributedHydraManager::OnHeartbeatMutationCommit, MakeWeak(this)),
            Config_->Get()->HeartbeatMutationPeriod);
        if (epochContext->LeaderId == selfPeerId) {
            YT_VERIFY(voting);
            epochContext->AlivePeersUpdateExecutor = New<TPeriodicExecutor>(
                epochContext->EpochControlInvoker,
                BIND(&TDistributedHydraManager::OnUpdateAlivePeers, MakeWeak(this)),
                TDuration::Seconds(5));
            epochContext->AlivePeersUpdateExecutor->Start();
        }
        epochContext->LeaderSyncBatcher = New<TAsyncBatcher<void>>(
            BIND_NO_PROPAGATE(&TDistributedHydraManager::DoSyncWithLeader, MakeWeak(this), MakeWeak(epochContext)),
            Config_->Get()->LeaderSyncDelay);
        epochContext->Discombobulated = electionEpochContext->Discombobulated;

        YT_VERIFY(!ControlEpochContext_);
        ControlEpochContext_ = epochContext;
        AtomicEpochContext_.Store(epochContext);

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
                NHydra::EErrorCode::InvalidEpoch,
                "No control epoch context");
        }

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

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
        ControlEpochContext_->CancelableContext->Cancel(error);

        ControlEpochContext_->LeaderSyncBatcher->Cancel(error);
        if (ControlEpochContext_->LeaderSyncPromise) {
            TrySetLeaderSyncPromise(ControlEpochContext_, error);
        }

        ControlEpochContext_.Reset();
        AtomicEpochContext_.Reset();
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
        proxy.SetDefaultTimeout(Config_->Get()->ControlRpcTimeout);

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
        auto sequenceNumber = rsp->sync_sequence_number();


        YT_LOG_DEBUG("Received synchronization response from leader (SyncSequenceNumber: %v)",
            sequenceNumber);

        YT_VERIFY(!epochContext->LeaderSyncSequenceNumber);
        epochContext->LeaderSyncSequenceNumber = sequenceNumber;
        CommitMutationsAtFollower(sequenceNumber);
        CheckForPendingLeaderSync();
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

        if (auto future = ControlEpochContext_->FollowerCommitter->CommitMutations(committedSequenceNumber)) {
            future.Subscribe(BIND(&TDistributedHydraManager::OnMutationsCommittedAtFollower, MakeStrong(this))
                .Via(ControlEpochContext_->EpochControlInvoker));
        }
    }

    void OnMutationsCommittedAtFollower(const TErrorOr<TFollowerCommitter::TCommitMutationsResult>& resultOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!resultOrError.IsOK()) {
            return;
        }
        const auto& result = resultOrError.Value();

        CheckForPendingLeaderSync();

        if (Config_->Get()->EnableStateHashChecker) {
            ReportMutationStateHashesToLeader(result);
        }
    }

    void ReportMutationStateHashesToLeader(const TFollowerCommitter::TCommitMutationsResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto rate = Config_->Get()->StateHashCheckerMutationVerificationSamplingRate;
        auto startSequenceNumber = (result.FirstSequenceNumber + rate - 1) / rate * rate;
        auto endSequenceNumber = result.LastSequenceNumber / rate * rate;
        if (startSequenceNumber > endSequenceNumber) {
            return;
        }

        auto epochContext = ControlEpochContext_;

        auto channel = epochContext->CellManager->GetPeerChannel(epochContext->LeaderId);
        YT_VERIFY(channel);

        TInternalHydraServiceProxy proxy(std::move(channel));
        auto request = proxy.ReportMutationsStateHashes();
        request->set_peer_id(epochContext->CellManager->GetSelfPeerId());

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

        request->Invoke().Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TInternalHydraServiceProxy::TErrorOrRspReportMutationsStateHashesPtr& rspOrError) {
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

        CommitMutation(MakeSystemMutationRequest(HeartbeatMutationType))
            .WithTimeout(Config_->Get()->HeartbeatMutationTimeout)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this), weakEpochContext = MakeWeak(ControlEpochContext_)] (const TErrorOr<TMutationResponse>& result) {
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

        if (ControlEpochContext_->ReadOnly == value) {
            return;
        }

        YT_VERIFY(value ||
            ControlState_ == EPeerState::LeaderRecovery ||
            ControlState_ == EPeerState::FollowerRecovery);

        ControlEpochContext_->ReadOnly = value;

        YT_LOG_INFO("Read-only mode %v", value ? "enabled" : "disabled");
    }

    void MaybeSetReadOnlyOnRecovery()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(ControlState_ == EPeerState::LeaderRecovery || ControlState_ == EPeerState::FollowerRecovery);

        SetReadOnly(DecoratedAutomaton_->GetReadOnly());
    }

    void DiscombobulateNonvotingPeers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Discombobulating nonvoting peers");

        auto epochContext = ControlEpochContext_;
        if (!IsActiveLeader() || !epochContext || !epochContext->LeaderCommitter) {
            THROW_ERROR_EXCEPTION(TError(
                NRpc::EErrorCode::Unavailable,
                "Not an active leader"));
        }

        if (!GetReadOnly()) {
            THROW_ERROR_EXCEPTION(TError(
                NRpc::EErrorCode::Unavailable,
                "Must be in read-only mode to discombobulate"));
        }

        std::vector<TFuture<TElectionServiceProxy::TRspDiscombobulatePtr>> futures;
        const auto& cellManager = epochContext->CellManager;
        for (int peerId = 0; peerId < cellManager->GetTotalPeerCount(); ++peerId) {
            if (cellManager->GetPeerConfig(peerId)->Voting) {
                continue;
            }

            auto peerChannel = cellManager->GetPeerChannel(peerId);
            if (!peerChannel) {
                continue;
            }

            YT_LOG_INFO("Discombobulating observer (PeerId: %v)",
                peerId);

            TElectionServiceProxy proxy(std::move(peerChannel));
            auto req = proxy.Discombobulate();
            req->set_sequence_number(DecoratedAutomaton_->GetSequenceNumber());
            futures.push_back(req->Invoke());
        }

        AllSucceeded(std::move(futures))
            .Get()
            .ThrowOnError();
    }

    std::pair<int, int> ComputeQuorumLatestSnapshotId()
    {
        auto getTerm = [] (const TSnapshotMeta& meta) {
            return meta.has_last_mutation_term() ? meta.last_mutation_term() : 0;
        };
        auto snapshotId = WaitFor(SnapshotStore_->GetLatestSnapshotId())
            .ValueOrThrow();
        auto term = 0;
        if (snapshotId != InvalidSegmentId) {
            auto reader = SnapshotStore_->CreateReader(snapshotId);
            term = getTerm(reader->GetParams().Meta);
        }
        auto snapshotParams = WaitFor(
            DiscoverLatestSnapshot(Config_->Get(), ControlEpochContext_->CellManager))
            .ValueOrThrow();
        snapshotId = std::max(snapshotId, snapshotParams.SnapshotId);
        term = std::max(term, getTerm(snapshotParams.Meta));
        return {snapshotId, term};
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
    IAuthenticatorPtr authenticator,
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
        std::move(authenticator),
        options,
        dynamicOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
