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
#include "response_keeper.h"
#include "mutation_committer.h"
#include "changelog_rotation.h"
#include "snapshot_discovery.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/fiber.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/ytree/fluent.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

#include <server/election/election_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

struct TEpochContext;
typedef TIntrusivePtr<TEpochContext> TEpochContextPtr;

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public NElection::TEpochContext
{
    TEpochContext()
        : IsActiveLeader(false)
    { }

    IInvokerPtr EpochSystemAutomatonInvoker;
    IInvokerPtr EpochUserAutomatonInvoker;
    IInvokerPtr EpochControlInvoker;
    TChangelogRotationPtr ChangelogRotation;
    TLeaderRecoveryPtr LeaderRecovery;
    TFollowerRecoveryPtr FollowerRecovery;
    TLeaderCommitterPtr LeaderCommitter;
    TFollowerCommitterPtr FollowerCommitter;
    TFollowerTrackerPtr FollowerTracker;
    bool IsActiveLeader;
};

////////////////////////////////////////////////////////////////////////////////

class TDistributedHydraManager
    : public TServiceBase
    , public IHydraManager
{
public:
    class TElectionCallbacks
        : public IElectionCallbacks
    {
    public:
        explicit TElectionCallbacks(TDistributedHydraManager* owner)
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
            return ToString(TVersion::FromRevision(priority));
        }

    private:
        TDistributedHydraManager* Owner_;

    };

    TDistributedHydraManager(
        TDistributedHydraManagerConfigPtr config,
        IInvokerPtr controlInvoker,
        IInvokerPtr automatonInvoker,
        IAutomatonPtr automaton,
        IRpcServerPtr rpcServer,
        TCellManagerPtr cellManager,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore)
        : TServiceBase(
            controlInvoker,
            NRpc::TServiceId(THydraServiceProxy::GetServiceName(), cellManager->GetCellGuid()),
            HydraLogger.GetCategory())
        , Config_(config)
        , RpcServer(rpcServer)
        , CellManager_(cellManager)
        , ControlInvoker_(controlInvoker)
        , AutomatonInvoker_(automatonInvoker)
        , ChangelogStore_(changelogStore)
        , SnapshotStore_(snapshotStore)
        , ReadOnly_(false)
        , ControlState_(EPeerState::Stopped)
        , Logger(HydraLogger)
    {
        VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
        VERIFY_INVOKER_AFFINITY(automatonInvoker, AutomatonThread);
        VERIFY_INVOKER_AFFINITY(HydraIOQueue->GetInvoker(), IOThread);

        Logger.AddTag(Sprintf("CellGuid: %s",
            ~ToString(CellManager_->GetCellGuid())));

        DecoratedAutomaton_ = New<TDecoratedAutomaton>(
            Config_,
            CellManager_,
            automaton,
            AutomatonInvoker_,
            ControlInvoker_,
            SnapshotStore_,
            ChangelogStore_);

        ElectionManager_ = New<TElectionManager>(
            Config_->Elections,
            CellManager_,
            controlInvoker,
            New<TElectionCallbacks>(this),
            rpcServer);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LogMutations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshotLocal));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RotateChangelog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQuorum));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshotDistributed)
            .SetInvoker(DecoratedAutomaton_->CreateGuardedUserInvoker(AutomatonInvoker_)));

        CellManager_->SubscribePeerReconfigured(
            BIND(&TThis::OnPeerReconfigured, Unretained(this))
                .Via(ControlInvoker_));
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Hydra instance is starting");

        ControlInvoker_->Invoke(BIND(&TThis::DoStart, MakeStrong(this)));
    }

    virtual void Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Hydra instance is stopping");

        ControlInvoker_->Invoke(BIND(&TThis::DoStop, MakeStrong(this)));
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

        auto epochContext = EpochContext_;
        return epochContext ? epochContext->IsActiveLeader : false;
    }

    virtual NElection::TEpochContextPtr GetEpochContext() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return EpochContext_;
    }

    virtual bool IsMutating() override
    {
        return GetMutationContext() != nullptr;
    }

    virtual bool GetReadOnly() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return AtomicGet(ReadOnly_);
    }

    virtual void SetReadOnly(bool value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        AtomicSet(ReadOnly_, value);
    }

    virtual TYsonProducer GetMonitoringProducer() override
    {
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

    DEFINE_SIGNAL(void(), StartLeading);
    DEFINE_SIGNAL(void(), LeaderRecoveryComplete);
    DEFINE_SIGNAL(void(), LeaderActive);
    DEFINE_SIGNAL(void(), StopLeading);

    DEFINE_SIGNAL(void(), StartFollowing);
    DEFINE_SIGNAL(void(), FollowerRecoveryComplete);
    DEFINE_SIGNAL(void(), StopFollowing);

    virtual TFuture< TErrorOr<TMutationResponse> > CommitMutation(const TMutationRequest& request) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(!DecoratedAutomaton_->GetMutationContext());

        if (GetAutomatonState() != EPeerState::Leading) {
            return MakeFuture(TErrorOr<TMutationResponse>(TError(
                NHydra::EErrorCode::NoLeader,
                "Not a leader")));
        }

        if (AtomicGet(ReadOnly_)) {
            return MakeFuture(TErrorOr<TMutationResponse>(TError(
                NHydra::EErrorCode::ReadOnly,
                "Read-only mode is active")));
        }

        auto epochContext = EpochContext_;
        if (!epochContext || !epochContext->IsActiveLeader) {
            return MakeFuture(TErrorOr<TMutationResponse>(TError(
                NHydra::EErrorCode::NoQuorum,
                "Not an active leader")));
        }

        if (request.Id != NullMutationId) {
            auto response = FindKeptResponse(request.Id);
            if (response) {
                return MakeFuture(TErrorOr<TMutationResponse>(*response));
            }
        }

        return epochContext->LeaderCommitter->Commit(request)
            .Apply(BIND(&TThis::OnMutationCommitted, MakeStrong(this)));
    }

    virtual TNullable<TMutationResponse> FindKeptResponse(const TMutationId& mutationId) override
    {
        TSharedRef responseData;
        if (!DecoratedAutomaton_->FindKeptResponse(mutationId, &responseData))
            return Null;

        LOG_DEBUG("Kept response returned (MutationId: %s)", ~ToString(mutationId));
        
        TMutationResponse response;
        response.Data = std::move(responseData);
        return response;
    }

    virtual TMutationContext* GetMutationContext() override
    {
        return DecoratedAutomaton_->GetMutationContext();
    }

 private:
    typedef TDistributedHydraManager TThis;

    TDistributedHydraManagerConfigPtr Config_;
    NRpc::IRpcServerPtr RpcServer;
    TCellManagerPtr CellManager_;
    IInvokerPtr ControlInvoker_;
    IInvokerPtr AutomatonInvoker_;
    IChangelogStorePtr ChangelogStore_;
    ISnapshotStorePtr SnapshotStore_;
    TAtomic ReadOnly_;
    EPeerState ControlState_;

    NElection::TElectionManagerPtr ElectionManager_;
    TDecoratedAutomatonPtr DecoratedAutomaton_;

    TEpochContextPtr EpochContext_;

    NLog::TTaggedLogger Logger;


    DECLARE_RPC_SERVICE_METHOD(NProto, LookupSnapshot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int maxSnapshotId = request->max_snapshot_id();
        bool exactId = request->exact_id();

        context->SetRequestInfo("MaxSnapshotId: %d, ExactId: %s",
            maxSnapshotId,
            ~FormatBool(exactId));

        int snapshotId;
        if (exactId) {
            snapshotId = maxSnapshotId;
        } else {
            snapshotId = SnapshotStore_->GetLatestSnapshotId(maxSnapshotId);
            if (snapshotId == NonexistingSegmentId) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::NoSuchSnapshot,
                    "No appropriate snapshots in store");
            }
        } 

        auto params = SnapshotStore_->GetSnapshotParamsOrThrow(snapshotId);
        response->set_snapshot_id(snapshotId);
        response->set_length(params.CompressedLength);
        response->set_checksum(params.Checksum);

        context->SetResponseInfo("SnapshotId: %d, Length: %" PRId64 ", Checksum: " PRIx64,
            snapshotId,
            params.CompressedLength,
            params.Checksum);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReadSnapshot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        UNUSED(response);

        int snapshotId = request->snapshot_id();
        i64 offset = request->offset();
        i64 length = request->length();

        context->SetRequestInfo("SnapshotId: %d, Offset: %" PRId64 ", Length: %" PRId64,
            snapshotId,
            offset,
            length);

        YCHECK(offset >= 0);
        YCHECK(length >= 0);

        SwitchTo(HydraIOQueue->GetInvoker());
        VERIFY_THREAD_AFFINITY(IOThread);

        auto reader = SnapshotStore_->CreateRawReaderOrThrow(snapshotId, offset);

        struct TSnapshotBlockTag { };
        auto buffer = TSharedRef::Allocate<TSnapshotBlockTag>(length, false);
        size_t bytesRead = reader->GetStream()->Read(buffer.Begin(), length);
        auto data = buffer.Slice(TRef(buffer.Begin(), bytesRead));
        context->Response().Attachments().push_back(data);

        context->SetResponseInfo("BytesRead: %" PRISZT, bytesRead);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, LookupChangelog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int changelogId = request->changelog_id();

        context->SetRequestInfo("ChangelogId: %d",
            changelogId);

        auto changelog = ChangelogStore_->OpenChangelogOrThrow(changelogId);
        int recordCount = changelog->GetRecordCount();
        response->set_record_count(recordCount);

        context->SetResponseInfo("RecordCount: %d", recordCount);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReadChangeLog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        UNUSED(response);

        int changelogId = request->changelog_id();
        int startRecordId = request->start_record_id();
        int recordCount = request->record_count();

        context->SetRequestInfo("ChangelogId: %d, StartRecordId: %d, RecordCount: %d",
            changelogId,
            startRecordId,
            recordCount);

        YCHECK(startRecordId >= 0);
        YCHECK(recordCount >= 0);

        SwitchTo(HydraIOQueue->GetInvoker());
        VERIFY_THREAD_AFFINITY(IOThread);

        auto changelog = ChangelogStore_->OpenChangelogOrThrow(changelogId);

        std::vector<TSharedRef> recordData;
        auto recordsData = changelog->Read(
            startRecordId,
            recordCount,
            Config_->MaxChangelogReadSize);

        // Pack refs to minimize allocations.
        context->Response().Attachments().push_back(PackRefs(recordsData));

        context->SetResponseInfo("RecordCount: %d", recordsData.size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, LogMutations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto startVersion = TVersion::FromRevision(request->start_revision());
        auto committedVersion = TVersion::FromRevision(request->committed_revision());
        int mutationCount = static_cast<int>(request->Attachments().size());

        context->SetRequestInfo("StartVersion: %s, CommittedVersion: %s, EpochId: %s, MutationCount: %d",
            ~ToString(startVersion),
            ~ToString(committedVersion),
            ~ToString(epochId),
            mutationCount);

        if (ControlState_ != EPeerState::Following && ControlState_ != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::InvalidState,
                "Cannot apply mutations while not following");
        }

        ValidateEpoch(epochId);

        switch (ControlState_) {
            case EPeerState::Following: {
                auto this_ = MakeStrong(this);
                EpochContext_->FollowerCommitter->LogMutations(startVersion, request->Attachments())
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
                if (EpochContext_->FollowerRecovery) {
                    auto error = EpochContext_->FollowerRecovery->PostponeMutations(startVersion, request->Attachments());
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

        context->SetRequestInfo("LoggedVersion: %s, CommittedVersion: %s, EpochId: %s",
            ~ToString(loggedVersion),
            ~ToString(committedVersion),
            ~ToString(epochId));

        if (ControlState_ != EPeerState::Following && ControlState_ != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot process follower ping while not following");
        }

        ValidateEpoch(epochId);
        auto epochContext = EpochContext_;

        switch (ControlState_) {
            case EPeerState::Following:
                epochContext->EpochSystemAutomatonInvoker->Invoke(
                    BIND(&TDecoratedAutomaton::CommitMutations, DecoratedAutomaton_, committedVersion));
                break;

            case EPeerState::FollowerRecovery:
                if (!epochContext->FollowerRecovery) {
                    LOG_INFO("Received sync ping from leader (Version: %s, Epoch: %s)",
                        ~ToString(committedVersion),
                        ~ToString(epochId));

                    epochContext->FollowerRecovery = New<TFollowerRecovery>(
                        Config_,
                        CellManager_,
                        DecoratedAutomaton_,
                        ChangelogStore_,
                        SnapshotStore_,
                        epochContext->EpochId,
                        epochContext->LeaderId,
                        epochContext->EpochSystemAutomatonInvoker);

                    epochContext->EpochControlInvoker->Invoke(
                        BIND(&TDistributedHydraManager::RecoverFollower, MakeStrong(this), loggedVersion));
                }
                break;

            default:
                YUNREACHABLE();
        }

        response->set_state(ControlState_);

        // Reply with OK in any case.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, BuildSnapshotLocal)
    {
        UNUSED(response);
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %s, Version: %s",
            ~ToString(epochId),
            ~ToString(version));

        if (ControlState_ != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::InvalidState,
                "Cannot build snapshot while not following");
        }

        ValidateEpoch(epochId);
        auto epochContext = EpochContext_;

        SwitchTo(epochContext->EpochUserAutomatonInvoker);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (DecoratedAutomaton_->GetLoggedVersion() != version) {
            Restart();
            context->Reply(TError(
                NHydra::EErrorCode::InvalidVersion,
                "Invalid logged version: expected %s, received %s",
                ~ToString(DecoratedAutomaton_->GetLoggedVersion()),
                ~ToString(version)));
            return;
        }

        auto asyncResult = DecoratedAutomaton_->BuildSnapshot();
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result);

        response->set_checksum(result.GetValue().Checksum);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RotateChangelog)
    {
        UNUSED(response);
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto version = TVersion::FromRevision(request->revision());

        context->SetRequestInfo("EpochId: %s, Version: %s",
            ~ToString(epochId),
            ~ToString(version));

        if (ControlState_ != EPeerState::Following && ControlState_  != EPeerState::FollowerRecovery) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::InvalidState,
                "Cannot rotate changelog while not following");
        }

        ValidateEpoch(epochId);
        auto epochContext = EpochContext_;

        switch (ControlState_) {
            case EPeerState::Following: {
                SwitchTo(epochContext->EpochUserAutomatonInvoker);
                VERIFY_THREAD_AFFINITY(AutomatonThread);

                if (DecoratedAutomaton_->GetLoggedVersion() != version) {
                    Restart();
                    context->Reply(TError(
                        NHydra::EErrorCode::InvalidVersion,
                        "Invalid logged version: expected %s, received %s",
                        ~ToString(DecoratedAutomaton_->GetLoggedVersion()),
                        ~ToString(version)));
                    return;
                }

                auto asyncResult = DecoratedAutomaton_->RotateChangelog();
                WaitFor(asyncResult);

                context->Reply();
                break;
            }

            case EPeerState::FollowerRecovery:
                if (epochContext->FollowerRecovery) {
                    LOG_DEBUG("AdvanceSegment: postponing snapshot creation");

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

    DECLARE_RPC_SERVICE_METHOD(NProto, GetQuorum)
    {
        UNUSED(request);
        VERIFY_THREAD_AFFINITY(ControlThread);

        context->SetRequestInfo("");

        if (GetControlState() != EPeerState::Leading) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidState,
                "Cannot answer quorum queries while not leading");
        }

        response->set_leader_address(CellManager_->GetSelfAddress());
        for (auto id = 0; id < CellManager_->GetPeerCount(); ++id) {
            if (EpochContext_->FollowerTracker->IsFollowerActive(id)) {
                response->add_follower_addresses(CellManager_->GetPeerAddress(id));
            }
        }

        ToProto(response->mutable_epoch_id(), EpochContext_->EpochId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, BuildSnapshotDistributed)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        bool setReadOnly = request->set_read_only();

        context->SetRequestInfo("SetReadOnly: %s",
            ~FormatBool(setReadOnly));

        auto epochContext = EpochContext_;

        if (!epochContext || GetAutomatonState() != EPeerState::Leading) {
            THROW_ERROR_EXCEPTION(NHydra::EErrorCode::InvalidState, "Not a leader");
        }

        if (!epochContext->IsActiveLeader) {
            THROW_ERROR_EXCEPTION(NHydra::EErrorCode::NoQuorum, "No active quorum");
        }

        auto asyncResult = DoBuildSnapshotDistributed(epochContext);

        if (setReadOnly) {
            SetReadOnly(true);
        }

        auto result = WaitFor(asyncResult, ControlInvoker_);
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!result.IsOK()) {
            context->Reply(result);
            return;
        }

        const auto& info = result.GetValue();
        response->set_snapshot_id(info.SnapshotId);

        context->Reply();
    }


    i64 GetElectionPriority()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto version =
            ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::Following
            ? DecoratedAutomaton_->GetAutomatonVersion()
            : DecoratedAutomaton_->GetLoggedVersion();

        return version.ToRevision();
    }


    void Restart()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ElectionManager_->Restart();
    }


    void DoStart()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(ControlState_ == EPeerState::Stopped);
        ControlState_ = EPeerState::Elections;

        LOG_INFO("Computing reachable version");

        SwitchTo(HydraIOQueue->GetInvoker());
        VERIFY_THREAD_AFFINITY(IOThread);

        int maxSnapshotId = SnapshotStore_->GetLatestSnapshotId();
        if (maxSnapshotId == NonexistingSegmentId) {
            LOG_INFO("No snapshots found");
            // Let's pretend we have snapshot 0.
            maxSnapshotId = 0;
        } else {
            LOG_INFO("The latest snapshot is %d", maxSnapshotId);
        }

        TVersion version;
        int maxChangelogId = ChangelogStore_->GetLatestChangelogId(maxSnapshotId);
        if (maxChangelogId == NonexistingSegmentId) {
            LOG_INFO("No changelogs found");
            version = TVersion(maxSnapshotId, 0);
        } else {
            LOG_INFO("The latest changelog is %d", maxChangelogId);
            auto changelog = ChangelogStore_->OpenChangelogOrThrow(maxChangelogId);
            version = TVersion(maxChangelogId, changelog->GetRecordCount());
        }

        LOG_INFO("Reachable version is %s", ~ToString(version));

        SwitchTo(ControlInvoker_);
        VERIFY_THREAD_AFFINITY(ControlThread);
	
        DecoratedAutomaton_->SetLoggedVersion(version);
        DecoratedAutomaton_->GetSystemInvoker()->Invoke(BIND(
            &TDecoratedAutomaton::Clear,
            DecoratedAutomaton_));

        ElectionManager_->Start();

        RpcServer->RegisterService(this);

        LOG_INFO("Hydra instance started (SelfAddress: %s, SelfId: %d)",
            ~CellManager_->GetSelfAddress(),
            CellManager_->GetSelfId());
    }

    void DoStop()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: This will raise the needed callbacks
        // (OnElectionStopLeading or OnElectionStopFollowing).
        ElectionManager_->Stop();

        RpcServer->UnregisterService(this);

        LOG_INFO("Hydra instance stopped");
    }


    TErrorOr<TMutationResponse> OnMutationCommitted(TErrorOr<TMutationResponse> result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!result.IsOK()) {
            LOG_ERROR(result, "Error committing mutation, restarting");
            Restart();
        }

        return result;
    }

    void OnChangelogLimitReached(TEpochContextPtr epochContext)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoBuildSnapshotDistributed(epochContext);
    }

    TFuture<TErrorOr<TSnapshotInfo>> DoBuildSnapshotDistributed(TEpochContextPtr epochContext) 
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(GetAutomatonState() == EPeerState::Leading);
        YCHECK(epochContext->IsActiveLeader);

        return epochContext->ChangelogRotation->BuildSnapshot();
    }


    void OnElectionStartLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting leader recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::LeaderRecovery;

        StartEpoch();
        auto epochContext = EpochContext_;

        epochContext->FollowerTracker = New<TFollowerTracker>(
            Config_->FollowerTracker,
            CellManager_,
            DecoratedAutomaton_,
            epochContext->EpochId,
            epochContext->EpochControlInvoker);

        epochContext->LeaderCommitter = New<TLeaderCommitter>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            ChangelogStore_,
            epochContext->FollowerTracker,
            epochContext->EpochId,
            epochContext->EpochControlInvoker,
            epochContext->EpochUserAutomatonInvoker);
        epochContext->LeaderCommitter->SubscribeChangelogLimitReached(
            BIND(&TThis::OnChangelogLimitReached, MakeWeak(this), epochContext));

        epochContext->ChangelogRotation = New<TChangelogRotation>(
            Config_,
            CellManager_,
            DecoratedAutomaton_,
            epochContext->LeaderCommitter,
            SnapshotStore_,
            epochContext->EpochId,
            epochContext->EpochControlInvoker,
            epochContext->EpochSystemAutomatonInvoker);

        epochContext->FollowerTracker->Start();

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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

            auto epochContext = EpochContext_;
            epochContext->LeaderRecovery = New<TLeaderRecovery>(
                Config_,
                CellManager_,
                DecoratedAutomaton_,
                ChangelogStore_,
                SnapshotStore_,
                epochContext->EpochId,
                epochContext->EpochSystemAutomatonInvoker);

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
                auto asyncRotateResult = epochContext->ChangelogRotation->RotateChangelog();
                auto rotateResult = WaitFor(asyncRotateResult);
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
            LOG_WARNING(ex, "Leader recovery failed, restarting");
            Restart();
        }
    }

    void OnElectionStopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Stopped leading");

        StopEpoch();

        YCHECK(ControlState_ == EPeerState::Leading|| ControlState_ == EPeerState::LeaderRecovery);
        ControlState_ = EPeerState::Elections;

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        StopLeading_.Fire();
        DecoratedAutomaton_->OnStopLeading();
    }


    void OnElectionStartFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting follower recovery");

        YCHECK(ControlState_ == EPeerState::Elections);
        ControlState_ = EPeerState::FollowerRecovery;

        StartEpoch();
        auto epochContext = EpochContext_;

        epochContext->FollowerCommitter = New<TFollowerCommitter>(
            CellManager_,
            DecoratedAutomaton_,
            epochContext->EpochControlInvoker,
            epochContext->EpochUserAutomatonInvoker);

        SwitchTo(DecoratedAutomaton_->GetSystemInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DecoratedAutomaton_->OnStartFollowing();
        StartFollowing_.Fire();
    }

    void RecoverFollower(TVersion syncVersion)
    {
        try {
            VERIFY_THREAD_AFFINITY(ControlThread);

            auto epochContext = EpochContext_;

            SwitchTo(epochContext->EpochSystemAutomatonInvoker);
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            auto asyncRecoveryResult = epochContext->FollowerRecovery->Run(syncVersion);
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
            LOG_WARNING(ex, "Follower recovery failed, restarting");
            Restart();
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

        StopFollowing_.Fire();
        DecoratedAutomaton_->OnStopFollowing();
    }


    void StartEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto electionEpochContext = ElectionManager_->GetEpochContext();

        YCHECK(!EpochContext_);
        EpochContext_ = New<TEpochContext>();
        EpochContext_->LeaderId = electionEpochContext->LeaderId;
        EpochContext_->EpochId = electionEpochContext->EpochId;
        EpochContext_->StartTime = electionEpochContext->StartTime;
        EpochContext_->CancelableContext = electionEpochContext->CancelableContext;
        EpochContext_->EpochControlInvoker = EpochContext_->CancelableContext->CreateInvoker(ControlInvoker_);
        EpochContext_->EpochSystemAutomatonInvoker = EpochContext_->CancelableContext->CreateInvoker(DecoratedAutomaton_->GetSystemInvoker());
        EpochContext_->EpochUserAutomatonInvoker = EpochContext_->CancelableContext->CreateInvoker(AutomatonInvoker_);
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(EpochContext_);
        EpochContext_->IsActiveLeader = false;
        EpochContext_->CancelableContext->Cancel();
        EpochContext_.Reset();
    }

    void ValidateEpoch(const TEpochId& epochId)
    {
        auto currentEpochId = EpochContext_->EpochId;
        if (epochId != currentEpochId) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidEpoch,
                "Invalid epoch: expected %s, received %s",
                ~ToString(currentEpochId),
                ~ToString(epochId));
        }
    }


    TFollowerTrackerPtr GetFollowerTrackerAsync() const
    {
        auto epochContext = EpochContext_;
        return epochContext ? epochContext->FollowerTracker : nullptr;
    }


    void OnPeerReconfigured(TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if ((ControlState_ == EPeerState::Leading || ControlState_ == EPeerState::LeaderRecovery) &&
            peerId != CellManager_->GetSelfId())
        {
            EpochContext_->FollowerTracker->ResetFollower(peerId);
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
    IRpcServerPtr rpcServer,
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
