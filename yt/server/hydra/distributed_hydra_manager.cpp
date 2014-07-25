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
#include "follower_tracker.h"
#include "mutation_context.h"
#include "mutation_committer.h"
#include "checkpointer.h"
#include "snapshot_discovery.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/scheduler.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/ytree/fluent.h>

#include <core/rpc/response_keeper.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>
#include <core/profiling/profiling_manager.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

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
    : public TServiceBase
    , public IHydraManager
{
public:
    class TElectionCallbacks
        : public IElectionCallbacks
    {
    public:
        explicit TElectionCallbacks(TDistributedHydraManagerPtr owner)
            : Owner_(owner)
        { }

        virtual void OnStartLeading() override
        {
            Owner_->ControlInvoker_->Invoke(
                BIND(&TDistributedHydraManager::OnElectionStartLeading, Owner_));
        }

        virtual void OnStopLeading() override
        {
            Owner_->ControlInvoker_->Invoke(
                BIND(&TDistributedHydraManager::OnElectionStopLeading, Owner_));
        }

        virtual void OnStartFollowing() override
        {
            Owner_->ControlInvoker_->Invoke(
                BIND(&TDistributedHydraManager::OnElectionStartFollowing, Owner_));
        }

        virtual void OnStopFollowing() override
        {
            Owner_->ControlInvoker_->Invoke(
                BIND(&TDistributedHydraManager::OnElectionStopFollowing, Owner_));
        }

        virtual TPeerPriority GetPriority() override
        {
            return Owner_->GetElectionPriority();
        }

        virtual Stroka FormatPriority(TPeerPriority priority) override
        {
            auto version = TVersion::FromRevision(priority);
            return ToString(version);
        }

    private:
        TDistributedHydraManagerPtr Owner_;

    };

    TDistributedHydraManager(
        TDistributedHydraManagerConfigPtr config,
        IInvokerPtr controlInvoker,
        IInvokerPtr automatonInvoker,
        IAutomatonPtr automaton,
        IServerPtr rpcServer,
        TCellManagerPtr cellManager,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore)
        : TServiceBase(
            controlInvoker,
            NRpc::TServiceId(THydraServiceProxy::GetServiceName(), cellManager->GetCellGuid()),
            HydraLogger)
        , Config_(config)
        , RpcServer_(rpcServer)
        , CellManager_(cellManager)
        , ControlInvoker_(controlInvoker)
        , AutomatonInvoker_(automatonInvoker)
        , ChangelogStore_(changelogStore)
        , SnapshotStore_(snapshotStore)
        , ReadOnly_(false)
        , ControlState_(EPeerState::None)
        , Profiler(HydraProfiler)
    {
        VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
        VERIFY_INVOKER_AFFINITY(automatonInvoker, AutomatonThread);
        VERIFY_INVOKER_AFFINITY(GetHydraIOInvoker(), IOThread);

        Logger.AddTag("CellGuid: %v", CellManager_->GetCellGuid());

        auto tagId = NProfiling::TProfilingManager::Get()->RegisterTag("cell_guid", CellManager_->GetCellGuid());
        Profiler.TagIds().push_back(tagId);

        DecoratedAutomaton_ = New<TDecoratedAutomaton>(
            Config_,
            CellManager_,
            automaton,
            AutomatonInvoker_,
            ControlInvoker_,
            SnapshotStore_,
            ChangelogStore_,
            Profiler);

        ElectionManager_ = New<TElectionManager>(
            Config_->Elections,
            CellManager_,
            controlInvoker,
            New<TElectionCallbacks>(this),
            rpcServer);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LogMutations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RotateChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));

        CellManager_->SubscribePeerReconfigured(
            BIND(&TDistributedHydraManager::OnPeerReconfigured, Unretained(this))
                .Via(ControlInvoker_));
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ != EPeerState::None)
            return;

        DecoratedAutomaton_->GetSystemInvoker()->Invoke(BIND(
            &TDecoratedAutomaton::Clear,
            DecoratedAutomaton_));

        RpcServer_->RegisterService(this);

        LOG_INFO("Hydra instance started (SelfAddress: %v, SelfId: %v)",
            CellManager_->GetSelfAddress(),
            CellManager_->GetSelfId());

        ControlState_ = EPeerState::Elections;

        Participate();
    }

    virtual void Stop() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped)
            return;

        if (ControlState_ != EPeerState::None) {
            RpcServer_->UnregisterService(this);
        }

        ElectionManager_.Reset();

        if (ControlEpochContext_) {
            StopEpoch();
        }

        ControlState_ = EPeerState::Stopped;

        AutomatonInvoker_->Invoke(BIND(&TDistributedHydraManager::DoStop, MakeStrong(this)));

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

        auto epochContext = ControlEpochContext_;
        return epochContext ? epochContext->IsActiveLeader : false;
    }

    virtual NElection::TEpochContextPtr GetEpochContext() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ControlEpochContext_;
    }

    virtual NElection::TEpochContextPtr GetAutomatonEpochContext() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return AutomatonEpochContext_;
    }

    virtual bool IsMutating() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return GetMutationContext() != nullptr;
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
                NHydra::EErrorCode::NoLeader,
                "Not a leader");
        }

        ReadOnly_ = value;
    }

    virtual TFuture<TErrorOr<int>> BuildSnapshotDistributed() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto epochContext = ControlEpochContext_;

        if (!epochContext || GetAutomatonState() != EPeerState::Leading) {
            return MakeFuture<TErrorOr<int>>(TError(
                NHydra::EErrorCode::InvalidState,
                "Not an active leader"));
        }

        if (!epochContext->IsActiveLeader) {
            return MakeFuture<TErrorOr<int>>(TError(
                NHydra::EErrorCode::NoQuorum,
                "No active quorum"));
        }

        return epochContext
            ->Checkpointer
            ->BuildSnapshot()
            .Apply(BIND([] (TErrorOr<TRemoteSnapshotParams> errorOrParams) -> TErrorOr<int> {
                if (!errorOrParams.IsOK()) {
                    return TError(errorOrParams);
                }
                const auto& params = errorOrParams.Value();
                return params.SnapshotId;
            }));
    }

    virtual TYsonProducer GetMonitoringProducer() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto this_ = MakeStrong(this);
        return BIND([this, this_] (IYsonConsumer* consumer) {
            auto tracker = GetFollowerTrackerAsync();
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("state").Value(ControlState_)
                    .Item("committed_version").Value(ToString(DecoratedAutomaton_->GetAutomatonVersion()))
                    .Item("logged_version").Value(ToString(DecoratedAutomaton_->GetLoggedVersion()))
                    .Item("elections").Do(ElectionManager_->GetMonitoringProducer())
                    .DoIf(tracker, [=] (TFluentMap fluent) {
                        fluent
                            .Item("has_active_quorum").Value(IsActiveLeader())
                            .Item("active_followers").DoListFor(
                                0,
                                CellManager_->GetPeerCount(),
                                [=] (TFluentList fluent, TPeerId id) {
                                    if (tracker->IsFollowerActive(id)) {
                                        fluent.Item().Value(id);
                                    }
                                });
                    })
                .EndMap();
        });
    }

    virtual TFuture<TErrorOr<TMutationResponse>> CommitMutation(const TMutationRequest& request) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(!DecoratedAutomaton_->GetMutationContext());

        if (GetAutomatonState() != EPeerState::Leading) {
            return MakeFuture(TErrorOr<TMutationResponse>(TError(
                NHydra::EErrorCode::NoLeader,
                "Not a leader")));
        }

        if (ReadOnly_) {
            return MakeFuture(TErrorOr<TMutationResponse>(TError(
                NHydra::EErrorCode::ReadOnly,
                "Read-only mode is active")));
        }

        auto epochContext = ControlEpochContext_;
        if (!epochContext || !epochContext->IsActiveLeader) {
            return MakeFuture(TErrorOr<TMutationResponse>(TError(
                NHydra::EErrorCode::NoQuorum,
                "Not an active leader")));
        }

        if (request.Id != NullMutationId) {
            LOG_DEBUG("Returning kept response (MutationId: %v)", request.Id);
            auto keptResponse = DecoratedAutomaton_->FindKeptResponse(request.Id);
            if (keptResponse) {
                return MakeFuture(TErrorOr<TMutationResponse>(*keptResponse));
            }
        }

        return epochContext->LeaderCommitter->Commit(request);
    }

    virtual void RegisterKeptResponse(
        const TMutationId& mutationId,
        const TMutationResponse& response) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->RegisterKeptResponse(
            mutationId,
            response);
    }

    virtual TNullable<TMutationResponse> FindKeptResponse(const TMutationId& mutationId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return DecoratedAutomaton_->FindKeptResponse(mutationId);
    }

    virtual TMutationContext* GetMutationContext() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return DecoratedAutomaton_->GetMutationContext();
    }


    DEFINE_SIGNAL(void(), StartLeading);
    DEFINE_SIGNAL(void(), LeaderRecoveryComplete);
    DEFINE_SIGNAL(void(), LeaderActive);
    DEFINE_SIGNAL(void(), StopLeading);

    DEFINE_SIGNAL(void(), StartFollowing);
    DEFINE_SIGNAL(void(), FollowerRecoveryComplete);
    DEFINE_SIGNAL(void(), StopFollowing);

private:
    TDistributedHydraManagerConfigPtr Config_;
    NRpc::IServerPtr RpcServer_;
    TCellManagerPtr CellManager_;
    IInvokerPtr ControlInvoker_;
    IInvokerPtr AutomatonInvoker_;
    IChangelogStorePtr ChangelogStore_;
    ISnapshotStorePtr SnapshotStore_;
    std::atomic<bool> ReadOnly_;
    EPeerState ControlState_;

    TVersion ReachableVersion_;

    // NB: Cyclic references: this -> ElectionManager -> Callbacks -> this
    TElectionManagerPtr ElectionManager_;
    TDecoratedAutomatonPtr DecoratedAutomaton_;

    TEpochContextPtr ControlEpochContext_;
    TEpochContextPtr AutomatonEpochContext_;

    NProfiling::TProfiler Profiler;


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
        UNUSED(response);

        int changelogId = request->changelog_id();
        int startRecordId = request->start_record_id();
        int recordCount = request->record_count();

        context->SetRequestInfo("ChangelogId: %v, StartRecordId: %v, RecordCount: %v",
            changelogId,
            startRecordId,
            recordCount);

        YCHECK(startRecordId >= 0);
        YCHECK(recordCount >= 0);

        SwitchTo(GetHydraIOInvoker());
        VERIFY_THREAD_AFFINITY(IOThread);

        auto changelog = OpenChangelogOrThrow(changelogId);

        std::vector<TSharedRef> recordData;
        auto recordsData = changelog->Read(
            startRecordId,
            recordCount,
            Config_->MaxChangelogReadSize);

        // Pack refs to minimize allocations.
        context->Response().Attachments().push_back(PackRefs(recordsData));

        context->SetResponseInfo("RecordCount: %v", recordsData.size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, LogMutations)
    {
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
                "Cannot apply mutations while not following");
        }

        ValidateEpoch(epochId);

        switch (ControlState_) {
            case EPeerState::Following: {
                auto this_ = MakeStrong(this);
                ControlEpochContext_->FollowerCommitter->LogMutations(startVersion, request->Attachments())
                    .Subscribe(BIND([this, this_, context, response] (TError error) {
                        response->set_logged(error.IsOK());

                        if (error.GetCode() == NHydra::EErrorCode::OutOfOrderMutations) {
                            Restart();
                        }

                        context->Reply(error);
                    }));
                break;
            }

            case EPeerState::FollowerRecovery: {
                if (ControlEpochContext_->FollowerRecovery) {
                    auto error = ControlEpochContext_->FollowerRecovery->PostponeMutations(startVersion, request->Attachments());
                    if (!error.IsOK()) {
                        LOG_WARNING(error, "Error postponing mutations, restarting");
                        Restart();
                    }

                    response->set_logged(false);

                    context->Reply();
                } else {
                    context->Reply(TError(
                        NHydra::EErrorCode::InvalidState,
                        "Ping is not received yet"));
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
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
                "Cannot process follower ping while not following");
        }

        ValidateEpoch(epochId);
        auto* epochContext = ControlEpochContext_.Get();

        switch (ControlState_) {
            case EPeerState::Following:
                epochContext->EpochUserAutomatonInvoker->Invoke(
                    BIND(&TDecoratedAutomaton::CommitMutations, DecoratedAutomaton_, committedVersion));
                break;

            case EPeerState::FollowerRecovery:
                if (!epochContext->FollowerRecovery) {
                    LOG_INFO("Received sync ping from leader (Version: %v, EpochId: %v)",
                        loggedVersion,
                        epochId);

                    epochContext->FollowerRecovery = New<TFollowerRecovery>(
                        Config_,
                        CellManager_,
                        DecoratedAutomaton_,
                        ChangelogStore_,
                        SnapshotStore_,
                        epochContext,
                        loggedVersion);

                    epochContext->EpochControlInvoker->Invoke(
                        BIND(&TDistributedHydraManager::RecoverFollower, MakeStrong(this)));
                }
                break;

            default:
                YUNREACHABLE();
        }

        response->set_state(ControlState_);

        // Reply with OK in any case.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, BuildSnapshot)
    {
        UNUSED(response);
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %v, Version: %v",
            epochId,
            version);

        if (ControlState_ != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot build snapshot while not following");
        }

        ValidateEpoch(epochId);
        auto epochContext = ControlEpochContext_;

        SwitchTo(epochContext->EpochUserAutomatonInvoker);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (DecoratedAutomaton_->GetLoggedVersion() != version) {
            Restart();
            context->Reply(TError(
                NHydra::EErrorCode::InvalidVersion,
                "Invalid logged version: expected %v, received %v",
                DecoratedAutomaton_->GetLoggedVersion(),
                version));
            return;
        }

        auto asyncResult = DecoratedAutomaton_->BuildSnapshot();
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result);

        response->set_checksum(result.Value().Checksum);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RotateChangelog)
    {
        UNUSED(response);
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %v, Version: %v",
            epochId,
            version);

        if (ControlState_ != EPeerState::Following && ControlState_  != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot rotate changelog while not following");
        }

        ValidateEpoch(epochId);
        auto epochContext = ControlEpochContext_;

        switch (ControlState_) {
            case EPeerState::Following: {
                SwitchTo(epochContext->EpochUserAutomatonInvoker);
                VERIFY_THREAD_AFFINITY(AutomatonThread);

                if (DecoratedAutomaton_->GetLoggedVersion() != version) {
                    Restart();
                    context->Reply(TError(
                        NHydra::EErrorCode::InvalidVersion,
                        "Invalid logged version: expected %v, received %v",
                        DecoratedAutomaton_->GetLoggedVersion(),
                        version));
                    return;
                }

                WaitFor(DecoratedAutomaton_->RotateChangelog(epochContext));

                context->Reply();
                break;
            }

            case EPeerState::FollowerRecovery:
                if (epochContext->FollowerRecovery) {
                    auto error = epochContext->FollowerRecovery->PostponeChangelogRotation(version);
                    if (!error.IsOK()) {
                        LOG_ERROR(error);
                        Restart();
                    }

                    context->Reply();
                } else {
                    context->Reply(TError(
                        NHydra::EErrorCode::InvalidState,
                        "Sync ping is not received yet"));
                }
                break;

            default:
                YUNREACHABLE();
        }
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

        ControlInvoker_->Invoke(BIND(&TDistributedHydraManager::DoParticipate, MakeStrong(this)));
    }

    void Restart()
    {
        ElectionManager_->Stop();
    }


    void DoStop()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_.Reset();
    }

    void DoParticipate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto checkState = [&] () {
            if (ControlState_ == EPeerState::Stopped) {
                throw TFiberCanceledException();
            }
            YCHECK(ControlState_ = EPeerState::Elections);
        };

        LOG_INFO("Computing reachable version");

        while (true) {
            try {
                checkState();

                auto maxSnapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId());
                THROW_ERROR_EXCEPTION_IF_FAILED(maxSnapshotIdOrError);
                int maxSnapshotId = maxSnapshotIdOrError.Value();

                if (maxSnapshotId == NonexistingSegmentId) {
                    LOG_INFO("No snapshots found");
                    // Let's pretend we have snapshot 0.
                    maxSnapshotId = 0;
                } else {
                    LOG_INFO("The latest snapshot is %v", maxSnapshotId);
                }

                auto maxChangelogIdOrError = WaitFor(ChangelogStore_->GetLatestChangelogId(maxSnapshotId));
                THROW_ERROR_EXCEPTION_IF_FAILED(maxChangelogIdOrError);
                int maxChangelogId = maxChangelogIdOrError.Value();

                if (maxChangelogId == NonexistingSegmentId) {
                    LOG_INFO("No changelogs found");
                    ReachableVersion_ = TVersion(maxSnapshotId, 0);
                } else {
                    LOG_INFO("The latest changelog is %v", maxChangelogId);
                    auto changelog = OpenChangelogOrThrow(maxChangelogId);
                    ReachableVersion_ = TVersion(maxChangelogId, changelog->GetRecordCount());
                }
                break;
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Error computing reachable version, backing off and retrying");
                WaitFor(MakeDelayed(Config_->BackoffTime));
            }
        }

        checkState();

        LOG_INFO("Reachable version is %v", ReachableVersion_);
        DecoratedAutomaton_->SetLoggedVersion(ReachableVersion_);
        ElectionManager_->Start();
    }


    IChangelogPtr OpenChangelogOrThrow(int id)
    {
        auto changelogOrError = WaitFor(ChangelogStore_->OpenChangelog(id));
        THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
        return changelogOrError.Value();
    }


    void OnCheckpointNeeded(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(GetAutomatonState() == EPeerState::Leading);
        YCHECK(epochContext->IsActiveLeader);

        auto changelogRotation = epochContext->Checkpointer;
        TAsyncError result;
        if (changelogRotation->CanBuildSnapshot()) {
            result = changelogRotation->BuildSnapshot().Apply(BIND([] (TErrorOr<TRemoteSnapshotParams> result) {
                return TError(result);
            }));
        } else if (changelogRotation->CanRotateChangelogs()) {
            LOG_WARNING("Snapshot is still being built, just rotating changlogs");
            result = changelogRotation->RotateChangelog();
        } else {
            LOG_WARNING("Cannot neither build a snapshot nor rotate changelogs");
        }

        result.Subscribe(BIND(&TDistributedHydraManager::OnCheckpointResult, MakeStrong(this))
            .Via(epochContext->EpochControlInvoker));
    }

    void OnCheckpointResult(TError error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!error.IsOK()) {
            LOG_ERROR(error);
            Restart();
        }
    }


    void OnCommitFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->CancelPendingLeaderMutations(error);

        LOG_ERROR(error, "Error committing mutation, restarting");
        Restart();
    }


    TFuture<TErrorOr<TRemoteSnapshotParams>> DoBuildSnapshotDistributed(TEpochContextPtr epochContext) 
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return epochContext->Checkpointer->BuildSnapshot();
    }


    void OnElectionStartLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped)
            return;

        LOG_INFO("Starting leader recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::LeaderRecovery;

        StartEpoch();
        auto* epochContext = ControlEpochContext_.Get();

        epochContext->FollowerTracker = New<TFollowerTracker>(
            Config_->FollowerTracker,
            CellManager_,
            DecoratedAutomaton_,
            epochContext);

        epochContext->LeaderCommitter = New<TLeaderCommitter>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            ChangelogStore_,
            epochContext,
            Profiler);
        epochContext->LeaderCommitter->SubscribeCheckpointNeeded(
            BIND(&TDistributedHydraManager::OnCheckpointNeeded, MakeWeak(this), epochContext));
        epochContext->LeaderCommitter->SubscribeCommitFailed(
            BIND(&TDistributedHydraManager::OnCommitFailed, MakeWeak(this)));

        epochContext->Checkpointer = New<TCheckpointer>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext->LeaderCommitter,
            SnapshotStore_,
            epochContext);

        epochContext->FollowerTracker->Start();

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
        try {
            VERIFY_THREAD_AFFINITY(ControlThread);

            auto* epochContext = ControlEpochContext_.Get();
            epochContext->LeaderRecovery = New<TLeaderRecovery>(
                Config_,
                CellManager_,
                DecoratedAutomaton_,
                ChangelogStore_,
                SnapshotStore_,
                epochContext);

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            auto version = DecoratedAutomaton_->GetLoggedVersion();
            auto asyncRecoveryResult = epochContext->LeaderRecovery->Run(version);
            auto recoveryResult = WaitFor(asyncRecoveryResult);
            VERIFY_THREAD_AFFINITY(AutomatonThread);
            THROW_ERROR_EXCEPTION_IF_FAILED(recoveryResult);

            DecoratedAutomaton_->OnLeaderRecoveryComplete();
            LeaderRecoveryComplete_.Fire();

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            YCHECK(ControlState_ == EPeerState::LeaderRecovery);
            ControlState_ = EPeerState::Leading;

            LOG_INFO("Leader recovery complete");

            WaitFor(epochContext->FollowerTracker->GetActiveQuorum());
            VERIFY_THREAD_AFFINITY(ControlThread);

            LOG_INFO("Active quorum established");

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            // Let's be neat and omit changelog rotation for the very first run.
            if (DecoratedAutomaton_->GetLoggedVersion() != TVersion()) {
                auto rotateResult = WaitFor(epochContext->Checkpointer->RotateChangelog());
                VERIFY_THREAD_AFFINITY(AutomatonThread);
                THROW_ERROR_EXCEPTION_IF_FAILED(rotateResult);
            }

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            epochContext->IsActiveLeader = true;

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            LOG_INFO("Leader active");

            LeaderActive_.Fire();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Leader recovery failed, backing off and restarting");
            WaitFor(MakeDelayed(Config_->BackoffTime));
            Restart();
        }
    }

    void OnElectionStopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped)
            return;

        LOG_INFO("Stopped leading");

        StopEpoch();

        YCHECK(ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery);
        ControlState_ = EPeerState::Elections;
        
        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_.Reset();
        StopLeading_.Fire();
        DecoratedAutomaton_->OnStopLeading();

        Participate();
    }


    void OnElectionStartFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped)
            return;

        LOG_INFO("Starting follower recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::FollowerRecovery;

        StartEpoch();
        auto* epochContext = ControlEpochContext_.Get();

        epochContext->FollowerCommitter = New<TFollowerCommitter>(
            CellManager_,
            DecoratedAutomaton_,
            epochContext,
            Profiler);

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_ = epochContext;
        DecoratedAutomaton_->OnStartFollowing();
        StartFollowing_.Fire();
    }

    void RecoverFollower()
    {
        try {
            VERIFY_THREAD_AFFINITY(ControlThread);

            auto epochContext = ControlEpochContext_;

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            auto asyncRecoveryResult = epochContext->FollowerRecovery->Run();
            auto recoveryResult = WaitFor(asyncRecoveryResult);
            VERIFY_THREAD_AFFINITY(AutomatonThread);
            THROW_ERROR_EXCEPTION_IF_FAILED(recoveryResult);

            SwitchTo(epochContext->EpochControlInvoker);
            VERIFY_THREAD_AFFINITY(ControlThread);

            YCHECK(ControlState_ == EPeerState::FollowerRecovery);
            ControlState_ = EPeerState::Following;

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);
        
            LOG_INFO("Follower recovery complete");

            DecoratedAutomaton_->OnFollowerRecoveryComplete();
            FollowerRecoveryComplete_.Fire();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Follower recovery failed, backing off and restarting");
            WaitFor(MakeDelayed(Config_->BackoffTime));
            Restart();
        }
    }

    void OnElectionStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ControlState_ == EPeerState::Stopped)
            return;

        LOG_INFO("Stopped following");

        StopEpoch();

        YCHECK(ControlState_ == EPeerState::Following || ControlState_ == EPeerState::FollowerRecovery);
        ControlState_ = EPeerState::Elections;

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AutomatonEpochContext_.Reset();
        StopFollowing_.Fire();
        DecoratedAutomaton_->OnStopFollowing();

        Participate();
    }


    void StartEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto electionEpochContext = ElectionManager_->GetEpochContext();

        YCHECK(!ControlEpochContext_);
        ControlEpochContext_ = New<TEpochContext>();
        ControlEpochContext_->LeaderId = electionEpochContext->LeaderId;
        ControlEpochContext_->EpochId = electionEpochContext->EpochId;
        ControlEpochContext_->StartTime = electionEpochContext->StartTime;
        ControlEpochContext_->CancelableContext = electionEpochContext->CancelableContext;
        ControlEpochContext_->EpochControlInvoker = ControlEpochContext_->CancelableContext->CreateInvoker(ControlInvoker_);
        ControlEpochContext_->EpochSystemAutomatonInvoker = ControlEpochContext_->CancelableContext->CreateInvoker(DecoratedAutomaton_->GetSystemInvoker());
        ControlEpochContext_->EpochUserAutomatonInvoker = ControlEpochContext_->CancelableContext->CreateInvoker(AutomatonInvoker_);
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(ControlEpochContext_);
        ControlEpochContext_->IsActiveLeader = false;
        ControlEpochContext_->CancelableContext->Cancel();
        ControlEpochContext_.Reset();
    }

    void ValidateEpoch(const TEpochId& epochId)
    {
        auto currentEpochId = ControlEpochContext_->EpochId;
        if (epochId != currentEpochId) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidEpoch,
                "Invalid epoch: expected %v, received %v",
                currentEpochId,
                epochId);
        }
    }


    TFollowerTrackerPtr GetFollowerTrackerAsync() const
    {
        auto epochContext = ControlEpochContext_;
        return epochContext ? epochContext->FollowerTracker : nullptr;
    }


    void OnPeerReconfigured(TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if ((ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery) &&
            peerId != CellManager_->GetSelfId())
        {
            ControlEpochContext_->FollowerTracker->ResetFollower(peerId);
        }
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(IOThread);

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
    ISnapshotStorePtr snapshotStore)
{
    YCHECK(config);
    YCHECK(controlInvoker);
    YCHECK(automatonInvoker);
    YCHECK(automaton);
    YCHECK(rpcServer);

    return New<TDistributedHydraManager>(
        config,
        controlInvoker,
        automatonInvoker,
        automaton,
        rpcServer,
        cellManager,
        changelogStore,
        snapshotStore);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
