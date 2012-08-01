#include "stdafx.h"
#include "persistent_state_manager.h"
#include "private.h"
#include "config.h"
#include "change_log.h"
#include "change_log_cache.h"
#include "meta_state_manager_proxy.h"
#include "snapshot.h"
#include "snapshot_builder.h"
#include "recovery.h"
#include "mutation_committer.h"
#include "quorum_tracker.h"
#include "follower_pinger.h"
#include "meta_state.h"
#include "snapshot_store.h"
#include "decorated_meta_state.h"
#include "mutation_context.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/election/cell_manager.h>
#include <ytlib/election/election_manager.h>

#include <ytlib/rpc/service.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/meta_state/meta_state_manager.pb.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;
using namespace NRpc;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

class TPersistentStateManager;
typedef TIntrusivePtr<TPersistentStateManager> TPersistentStateManagerPtr;

class TPersistentStateManager
    : public TServiceBase
    , public IMetaStateManager
{
public:
    class TElectionCallbacks
        : public IElectionCallbacks
    {
    public:
        TElectionCallbacks(TPersistentStateManagerPtr owner)
            : Owner(owner)
        { }

    private:
        TPersistentStateManagerPtr Owner;

        void OnStartLeading(const TEpoch& epoch)
        {
            Owner->OnElectionStartLeading(epoch);
        }

        void OnStopLeading()
        {
            Owner->OnElectionStopLeading();
        }

        void OnStartFollowing(TPeerId leaderId, const TEpoch& epoch)
        {
            Owner->OnElectionStartFollowing(leaderId, epoch);
        }

        void OnStopFollowing()
        {
            Owner->OnElectionStopFollowing();
        }

        TPeerPriority GetPriority()
        {
            return Owner->GetPriority();
        }

        Stroka FormatPriority(TPeerPriority priority)
        {
            return Owner->FormatPriority(priority);
        }
    };

    TPersistentStateManager(
        TPersistentStateManagerConfigPtr config,
        IInvokerPtr controlInvoker,
        IInvokerPtr stateInvoker,
        IMetaStatePtr metaState,
        NRpc::IServerPtr server)
        : TServiceBase(controlInvoker, TProxy::GetServiceName(), Logger.GetCategory())
        , ControlStatus(EPeerStatus::Stopped)
        , StateStatus(EPeerStatus::Stopped)
        , Config(config)
        , LeaderId(NElection::InvalidPeerId)
        , ControlInvoker(controlInvoker)
        , ReadOnly(false)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSnapshotInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChangeLogInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ApplyMutations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceSegment));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQuorum));

        ChangeLogCache = New<TChangeLogCache>(Config->ChangeLogs);

        SnapshotStore = New<TSnapshotStore>(Config->Snapshots);
        DecoratedState = New<TDecoratedMetaState>(
            Config,
            metaState,
            stateInvoker,
            controlInvoker,
            SnapshotStore,
            ChangeLogCache);

        IOQueue = New<TActionQueue>("MetaStateIO");

        VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
        VERIFY_INVOKER_AFFINITY(stateInvoker, StateThread);
        VERIFY_INVOKER_AFFINITY(IOQueue->GetInvoker(), IOThread);

        CellManager = New<TCellManager>(Config->Cell);

        LOG_INFO("SelfAddress: %s, PeerId: %d",
            ~CellManager->GetSelfAddress(),
            CellManager->GetSelfId());

        ElectionManager = New<TElectionManager>(
            ~Config->Election,
            ~CellManager,
            controlInvoker,
            ~New<TElectionCallbacks>(this));

        server->RegisterService(this);
        server->RegisterService(ElectionManager);
    }

    virtual void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(ControlStatus == EPeerStatus::Stopped);

        ChangeLogCache->Start();
        SnapshotStore->Start();
        DecoratedState->Start();

        ControlStatus = EPeerStatus::Elections;

        GetStateInvoker()->Invoke(BIND(
            &TDecoratedMetaState::Clear,
            DecoratedState));

        ElectionManager->Start();
    }

    virtual void Stop()
    {
        //TODO: implement this
        YUNIMPLEMENTED();
    }

    virtual EPeerStatus GetControlStatus() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ControlStatus;
    }

    virtual EPeerStatus GetStateStatus() const
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        return StateStatus;
    }

    virtual EPeerStatus GetStateStatusAsync() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return StateStatus;
    }

    virtual IInvokerPtr GetStateInvoker() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedState->GetStateInvoker();
    }

    virtual bool HasActiveQuorum() const
    {
        auto tracker = QuorumTracker;
        if (!tracker) {
            return false;
        }
        return tracker->HasActiveQuorum();
    }

    virtual TCancelableContextPtr GetEpochContext() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return EpochContext;
    }

    virtual void SetReadOnly(bool readOnly)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ReadOnly = readOnly;
    }

    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
    {
        auto tracker = QuorumTracker;

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("status").Scalar(FormatEnum(ControlStatus))
                .Item("version").Scalar(DecoratedState->GetVersionAsync().ToString())
                .Item("reachable_version").Scalar(DecoratedState->GetReachableVersionAsync().ToString())
                .Item("elections").Do(BIND(&TElectionManager::GetMonitoringInfo, ElectionManager))
                .DoIf(tracker, [=] (TFluentMap fluent) {
                    fluent
                        .Item("has_quorum").Scalar(tracker->HasActiveQuorum())
                        .Item("active_followers").DoListFor(
                            0,
                            CellManager->GetPeerCount(),
                            [=] (TFluentList fluent, TPeerId id) {
                                if (tracker->IsFollowerActive(id)) {
                                    fluent.Item().Scalar(id);
                                }
                            });
                })
            .EndMap();
    }

    DEFINE_SIGNAL(void(), StartLeading);
    DEFINE_SIGNAL(void(), LeaderRecoveryComplete);
    DEFINE_SIGNAL(void(), StopLeading);
    DEFINE_SIGNAL(void(), StartFollowing);
    DEFINE_SIGNAL(void(), FollowerRecoveryComplete);
    DEFINE_SIGNAL(void(), StopFollowing);

    virtual TFuture< TValueOrError<TMutationResponse> > CommitMutation(const TMutationRequest& request)
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        YCHECK(!DecoratedState->GetMutationContext());

        if (GetStateStatus() != EPeerStatus::Leading) {
            return MakeFuture(TValueOrError<TMutationResponse>(TError(
                ECommitCode::NoLeader,
                "Not a leader")));
        }

        if (ReadOnly) {
            return MakeFuture(TValueOrError<TMutationResponse>(TError(
                ECommitCode::ReadOnly,
                "State is read only")));
        }

        // FollowerTracker is modified concurrently from the ControlThread.
        auto followerTracker = QuorumTracker;
        if (!followerTracker || !followerTracker->HasActiveQuorum()) {
            return MakeFuture(TValueOrError<TMutationResponse>(TError(
                ECommitCode::NoQuorum,
                "No active quorum")));
        }

        return
            LeaderCommitter
            ->Commit(request)
            .Apply(BIND(&TThis::OnMutationCommitted, MakeStrong(this)));
    }

    virtual TMutationContext* GetMutationContext()
    {
        return DecoratedState->GetMutationContext();
    }

 private:
    typedef TPersistentStateManager TThis;
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    EPeerStatus ControlStatus;
    EPeerStatus StateStatus;
    TPersistentStateManagerConfigPtr Config;
    TPeerId LeaderId;
    TCellManagerPtr CellManager;
    IInvokerPtr ControlInvoker;
    bool ReadOnly;

    NElection::TElectionManager::TPtr ElectionManager;
    TChangeLogCachePtr ChangeLogCache;
    TSnapshotStorePtr SnapshotStore;
    TDecoratedMetaStatePtr DecoratedState;

    TActionQueuePtr IOQueue;

    TCancelableContextPtr EpochContext;

    IInvokerPtr EpochControlInvoker;
    IInvokerPtr EpochStateInvoker;

    TSnapshotBuilderPtr SnapshotBuilder;
    TLeaderRecoveryPtr LeaderRecovery;
    TFollowerRecoveryPtr FollowerRecovery;

    TLeaderCommitterPtr LeaderCommitter;
    TFollowerCommitterPtr FollowerCommitter;

    TQuorumTrackerPtr QuorumTracker;
    TFollowerPingerPtr FollowerPinger;

    // RPC methods
    DECLARE_RPC_SERVICE_METHOD(NProto, GetSnapshotInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 snapshotId = request->snapshot_id();

        context->SetRequestInfo("SnapshotId: %d", snapshotId);

        auto result = SnapshotStore->GetReader(snapshotId);
        if (!result.IsOK()) {
            // TODO: cannot use ythrow here
            throw TServiceException(result);
        }

        auto reader = result.Value();
        reader->Open();

        i64 length = reader->GetLength();
        auto checksum = reader->GetChecksum();
        int prevRecordCount = reader->GetPrevRecordCount();

        response->set_length(length);

        context->SetResponseInfo("Length: %" PRId64 ", PrevRecordCount: %d, Checksum: %" PRIx64,
            length,
            prevRecordCount,
            checksum);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReadSnapshot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        UNUSED(response);

        i32 snapshotId = request->snapshot_id();
        i64 offset = request->offset();
        i32 length = request->length();

        context->SetRequestInfo("SnapshotId: %d, Offset: %" PRId64 ", Length: %d",
            snapshotId,
            offset,
            length);

        YCHECK(offset >= 0);
        YCHECK(length >= 0);

        auto fileName = SnapshotStore->GetSnapshotFileName(snapshotId);
        if (!isexist(~fileName)) {
            context->Reply(TError(
                EErrorCode::NoSuchSnapshot,
                Sprintf("No such snapshot %d", snapshotId)));
            return;
        }

        TSharedPtr<TFile, TAtomicCounter> snapshotFile;
        try {
            snapshotFile = new TFile(fileName, OpenExisting | RdOnly);
        }
        catch (const std::exception& ex) {
            LOG_FATAL("IO error while opening snapshot %d\n%s",
                snapshotId,
                ex.what());
        }

        IOQueue->GetInvoker()->Invoke(context->Wrap(BIND([=] () {
            VERIFY_THREAD_AFFINITY(IOThread);

            TBlob data(length);
            i32 bytesRead;
            try {
                snapshotFile->Seek(offset, sSet);
                bytesRead = snapshotFile->Read(&*data.begin(), length);
            } catch (const std::exception& ex) {
                LOG_FATAL("IO error while reading snapshot %d\n%s",
                    snapshotId,
                    ex.what());
            }

            data.erase(data.begin() + bytesRead, data.end());
            context->Response().Attachments().push_back(TSharedRef(MoveRV(data)));

            context->SetResponseInfo("BytesRead: %d", bytesRead);

            context->Reply();
        })));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetChangeLogInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 changeLogId = request->change_log_id();

        context->SetRequestInfo("ChangeLogId: %d",
            changeLogId);

        auto result = ChangeLogCache->Get(changeLogId);
        if (!result.IsOK()) {
            // TODO: cannot use ythrow here
            throw TServiceException(result);
        }

        auto changeLog = result.Value();
        i32 recordCount = changeLog->GetRecordCount();

        response->set_record_count(recordCount);

        context->SetResponseInfo("RecordCount: %d", recordCount);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReadChangeLog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        UNUSED(response);

        i32 changeLogId = request->change_log_id();
        i32 startRecordId = request->start_record_id();
        i32 recordCount = request->record_count();

        context->SetRequestInfo("ChangeLogId: %d, StartRecordId: %d, RecordCount: %d",
            changeLogId,
            startRecordId,
            recordCount);

        YCHECK(startRecordId >= 0);
        YCHECK(recordCount >= 0);

        auto result = ChangeLogCache->Get(changeLogId);
        if (!result.IsOK()) {
            // TODO: cannot use ythrow here
            throw TServiceException(result);
        }

        IOQueue->GetInvoker()->Invoke(context->Wrap(BIND(
            &TThis::DoReadChangeLog,
            MakeStrong(this),
            result.Value(),
            startRecordId,
            recordCount)));
    }

    void DoReadChangeLog(
        TCachedAsyncChangeLogPtr changeLog,
        i32 startRecordId,
        i32 recordCount,
        TCtxReadChangeLogPtr context)
    {
        VERIFY_THREAD_AFFINITY(IOThread);

        std::vector<TSharedRef> recordData;
        try {
            changeLog->Read(startRecordId, recordCount, &recordData);
        } catch (const std::exception& ex) {
            LOG_FATAL("IO error while reading changelog %d\n%s",
                changeLog->GetId(),
                ex.what());
        }

        // Pack refs to minimize allocations.
        context->Response().Attachments().push_back(PackRefs(recordData));

        context->SetResponseInfo("RecordCount: %d", recordData.size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ApplyMutations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epoch = TEpoch::FromProto(request->epoch());
        i32 segmentId = request->segment_id();
        i32 recordCount = request->record_count();
        TMetaVersion version(segmentId, recordCount);

        context->SetRequestInfo("Epoch: %s, Version: %s",
            ~epoch.ToString(),
            ~version.ToString());

        if (GetControlStatus() != EPeerStatus::Following && GetControlStatus() != EPeerStatus::FollowerRecovery) {
            ythrow TServiceException(EErrorCode::InvalidStatus) <<
                Sprintf("Cannot apply changes while in %s", ~GetControlStatus().ToString());
        }

        CheckEpoch(epoch);

        int changeCount = request->Attachments().size();
        switch (GetControlStatus()) {
            case EPeerStatus::Following: {
                LOG_DEBUG("ApplyChange: applying changes (Version: %s, ChangeCount: %d)",
                    ~version.ToString(),
                    changeCount);

                FollowerCommitter
                    ->Commit(version, request->Attachments())
                    .Subscribe(BIND(&TThis::OnFollowerCommitted, MakeStrong(this), context));
                break;
            }

            case EPeerStatus::FollowerRecovery: {
                if (FollowerRecovery) {
                    LOG_DEBUG("ApplyChange: keeping postponed changes (Version: %s, ChangeCount: %d)",
                        ~version.ToString(),
                        changeCount);

                    auto error = FollowerRecovery->PostponeMutations(version, request->Attachments());
                    if (!error.IsOK()) {
                        LOG_WARNING("Error postponing mutations, restarting\n%s", ~error.ToString());
                        Restart();
                    }

                    response->set_committed(false);
                    context->Reply();
                } else {
                    LOG_DEBUG("ApplyChange: ignoring changes (Version: %s, ChangeCount: %d)",
                        ~version.ToString(),
                        changeCount);
                    context->Reply(
                        EErrorCode::InvalidStatus,
                        Sprintf("Ping is not received yet (Status: %s)", ~GetControlStatus().ToString()));
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    void OnFollowerCommitted(TCtxApplyMutationsPtr context, TError error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (error.IsOK()) {
            auto& response = context->Response();
            response.set_committed(true);
        } else {
            if (error.GetCode() == ECommitCode::OutOfOrderMutations) {
                Restart();
            }
        }

        context->Reply(error);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingFollower)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 segmentId = request->segment_id();
        i32 recordCount = request->record_count();
        auto version = TMetaVersion(segmentId, recordCount);
        auto epoch = TEpoch::FromProto(request->epoch());

        context->SetRequestInfo("Version: %s, Epoch: %s",
            ~version.ToString(),
            ~epoch.ToString());

        auto status = GetControlStatus();

        if (status != EPeerStatus::Following && status != EPeerStatus::FollowerRecovery) {
            ythrow TServiceException(EErrorCode::InvalidStatus) <<
                Sprintf("Cannot process follower ping while in %s", ~GetControlStatus().ToString());
        }

        CheckEpoch(epoch);

        switch (status) {
            case EPeerStatus::Following:
                // code here for two-phase commit
                // right now, ignoring ping
                break;

            case EPeerStatus::FollowerRecovery:
                if (!FollowerRecovery) {
                    LOG_INFO("Received sync ping from leader (Version: %s, Epoch: %s)",
                        ~version.ToString(),
                        ~epoch.ToString());

                    FollowerRecovery = New<TFollowerRecovery>(
                        Config,
                        CellManager,
                        DecoratedState,
                        ChangeLogCache,
                        SnapshotStore,
                        epoch,
                        LeaderId,
                        EpochControlInvoker,
                        EpochStateInvoker,
                        version);

                    FollowerRecovery->Run().Subscribe(
                        BIND(&TThis::OnControlFollowerRecoveryFinished, MakeStrong(this))
                        .Via(EpochControlInvoker));
                }
                break;

            default:
                YUNREACHABLE();
        }

        response->set_status(status);

        // Reply with OK in any case.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AdvanceSegment)
    {
        UNUSED(response);
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epoch = TEpoch::FromProto(request->epoch());
        i32 segmentId = request->segment_id();
        i32 recordCount = request->record_count();
        auto version = TMetaVersion(segmentId, recordCount);
        bool createSnapshot = request->create_snapshot();

        context->SetRequestInfo("Epoch: %s, Version: %s, CreateSnapshot: %s",
            ~epoch.ToString(),
            ~version.ToString(),
            ~ToString(createSnapshot));

        if (GetControlStatus() != EPeerStatus::Following && GetControlStatus() != EPeerStatus::FollowerRecovery) {
            ythrow TServiceException(EErrorCode::InvalidStatus) <<
                Sprintf("Cannot advance segment while in %s", ~GetControlStatus().ToString());
        }

        CheckEpoch(epoch);

        switch (GetControlStatus()) {
            case EPeerStatus::Following:
                if (createSnapshot) {
                    LOG_DEBUG("AdvanceSegment: starting snapshot creation");

                    BIND(&TSnapshotBuilder::CreateLocalSnapshot, SnapshotBuilder, version)
                        .AsyncVia(EpochStateInvoker)
                        .Run()
                        .Subscribe(BIND(
                            &TThis::OnCreateLocalSnapshot,
                            MakeStrong(this),
                            context));
                } else {
                    LOG_DEBUG("AdvanceSegment: advancing segment");

                    EpochStateInvoker->Invoke(context->Wrap(BIND(
                        &TThis::DoStateAdvanceSegment,
                        MakeStrong(this),
                        version)));
                }
                break;

            case EPeerStatus::FollowerRecovery: {
                if (FollowerRecovery) {
                    LOG_DEBUG("AdvanceSegment: postponing snapshot creation");

                    auto error = FollowerRecovery->PostponeSegmentAdvance(version);
                    if (!error.IsOK()) {
                        LOG_ERROR("%s", ~error.ToString());
                        Restart();
                    }

                    if (createSnapshot) {
                        context->Reply(
                            EErrorCode::InvalidStatus,
                            "Unable to create a snapshot during recovery");
                    } else {
                        context->Reply();
                    }
                } else {
                    context->Reply(
                        EErrorCode::InvalidStatus,
                        Sprintf("Ping is not received yet (Status: %s)",
                            ~GetControlStatus().ToString()));
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    void DoStateAdvanceSegment(TMetaVersion version, TCtxAdvanceSegmentPtr context)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        if (DecoratedState->GetVersion() != version) {
            Restart();
            ythrow TServiceException(EErrorCode::InvalidVersion) <<
                Sprintf("Invalid version, segment advancement canceled (Expected: %s, Received: %s)",
                ~version.ToString(),
                ~DecoratedState->GetVersion().ToString());
        }

        DecoratedState->RotateChangeLog();

        context->Reply();
    }

    void OnCreateLocalSnapshot(
        TCtxAdvanceSegmentPtr context,
        TSnapshotBuilder::TLocalResult result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto& response = context->Response();

        switch (result.ResultCode) {
            case TSnapshotBuilder::EResultCode::OK:
                response.set_checksum(result.Checksum);
                context->Reply();
                break;
            case TSnapshotBuilder::EResultCode::InvalidVersion:
                context->Reply(
                    TProxy::EErrorCode::InvalidVersion,
                    "Requested to create a snapshot for an invalid version");
                break;
            case TSnapshotBuilder::EResultCode::AlreadyInProgress:
                context->Reply(
                    TProxy::EErrorCode::SnapshotAlreadyInProgress,
                    "Snapshot creation is already in progress");
                break;
            case TSnapshotBuilder::EResultCode::ForkError:
                context->Reply(TError("Fork error"));
                break;
            case TSnapshotBuilder::EResultCode::TimeoutExceeded:
                context->Reply(TError("Snapshot creation timed out"));
                break;
            default:
                YUNREACHABLE();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, LookupSnapshot)
    {
        i32 maxSnapshotId = request->max_snapshot_id();
        context->SetRequestInfo("MaxSnapshotId: %d", maxSnapshotId);

        i32 snapshotId = SnapshotStore->LookupLatestSnapshot(maxSnapshotId);

        response->set_snapshot_id(snapshotId);
        context->SetResponseInfo("SnapshotId: %d", snapshotId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetQuorum)
    {
        UNUSED(request);
        VERIFY_THREAD_AFFINITY(ControlThread);

        context->SetRequestInfo("");

        if (GetControlStatus() != EPeerStatus::Leading) {
            ythrow TServiceException(EErrorCode::InvalidStatus) <<
                Sprintf("Cannot answer quorum queries while in %s", ~GetControlStatus().ToString());
        }

        auto tracker = QuorumTracker;
        response->set_leader_address(CellManager->GetSelfAddress());
        for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
            if (tracker->IsFollowerActive(id)) {
                response->add_follower_addresses(CellManager->GetPeerAddress(id));
            }
        }

        *response->mutable_epoch() = DecoratedState->GetEpoch().ToProto();

        context->Reply();
    }

    // End of RPC methods


    void Restart()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // To prevent multiple restarts.
        auto epochContext = EpochContext;
        if (epochContext) {
            epochContext->Cancel();
        }

        ElectionManager->Restart();
    }

    TValueOrError<TMutationResponse> OnMutationCommitted(TValueOrError<TMutationResponse> result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!result.IsOK()) {
            LOG_ERROR("Error committing mutation, restarting\n%s", ~result.ToString());
            Restart();
        }

        return result;
    }


    void OnElectionStartLeading(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting leader recovery");

        ControlStatus = EPeerStatus::LeaderRecovery;
        LeaderId = CellManager->GetSelfId();

        StartEpoch(epoch);

        YCHECK(!QuorumTracker);
        QuorumTracker = New<TQuorumTracker>(
            Config->FollowerTracker,
            CellManager,
            EpochControlInvoker);

        YCHECK(!LeaderCommitter);
        LeaderCommitter = New<TLeaderCommitter>(
            Config->LeaderCommitter,
            CellManager,
            DecoratedState,
            ChangeLogCache,
            QuorumTracker,
            epoch,
            EpochControlInvoker,
            EpochStateInvoker);
        LeaderCommitter->SubscribeMutationApplied(BIND(&TThis::OnMutationApplied, MakeWeak(this)));

        // During recovery the leader is reporting its reachable version to followers.
        auto version = DecoratedState->GetReachableVersionAsync();
        DecoratedState->SetPingVersion(version);

        YCHECK(!FollowerPinger);
        FollowerPinger = New<TFollowerPinger>(
            Config->FollowerPinger,
            CellManager,
            DecoratedState,
            QuorumTracker,
            epoch,
            EpochControlInvoker);
        FollowerPinger->Start();

        EpochStateInvoker->Invoke(BIND(
            &TThis::DoStateStartLeading,
            MakeStrong(this)));
    }

    void DoStateStartLeading()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YCHECK(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::LeaderRecovery;

        StartLeading_.Fire();

        YCHECK(!LeaderRecovery);
        LeaderRecovery = New<TLeaderRecovery>(
            ~Config,
            ~CellManager,
            ~DecoratedState,
            ~ChangeLogCache,
            ~SnapshotStore,
            DecoratedState->GetEpoch(),
            ~EpochControlInvoker,
            ~EpochStateInvoker);

        BIND(&TLeaderRecovery::Run, LeaderRecovery)
            .AsyncVia(EpochControlInvoker)
            .Run()
            .Subscribe(
                BIND(&TThis::OnStateLeaderRecoveryFinished, MakeStrong(this))
                .Via(EpochStateInvoker));
    }

    void OnStateLeaderRecoveryFinished(TError error)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YCHECK(LeaderRecovery);
        LeaderRecovery.Reset();

        if (!error.IsOK()) {
            LOG_WARNING("Leader recovery failed, restarting\n%s", ~error.ToString());
            Restart();
            return;
        }

        YCHECK(!SnapshotBuilder);
        SnapshotBuilder = New<TSnapshotBuilder>(
            Config->SnapshotBuilder,
            CellManager,
            DecoratedState,
            SnapshotStore,
            DecoratedState->GetEpoch(),
            EpochControlInvoker,
            EpochStateInvoker);

        // Switch to a new changelog unless the current one is empty.
        // This enables changelog truncation for those followers that are down and have uncommitted changes.
        auto version = DecoratedState->GetVersion();
        if (version.RecordCount > 0) {
            LOG_INFO("Switching to a new changelog %d for the new epoch", version.SegmentId + 1);
            SnapshotBuilder->RotateChangeLog();
        }

        LeaderRecoveryComplete_.Fire();

        YCHECK(StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Leading;

        EpochControlInvoker->Invoke(BIND(
            &TThis::DoControlLeaderRecoveryFinished,
            MakeStrong(this)));

    }

    void DoControlLeaderRecoveryFinished()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        QuorumTracker->Start(CellManager->GetSelfId());

        YCHECK(ControlStatus == EPeerStatus::LeaderRecovery);
        ControlStatus = EPeerStatus::Leading;

        LOG_INFO("Leader recovery complete");
    }


    void OnElectionStopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Stopped leading");

        GetStateInvoker()->Invoke(BIND(
            &TThis::DoStateStopLeading,
            MakeStrong(this)));

        ControlStatus = EPeerStatus::Elections;

        StopEpoch();

        if (LeaderCommitter) {
            LeaderCommitter->Stop();
            LeaderCommitter.Reset();
        }

        if (FollowerPinger) {
            FollowerPinger->Stop();
            FollowerPinger.Reset();
        }

        if (QuorumTracker) {
            QuorumTracker->Stop();
            QuorumTracker.Reset();
        }

        if (LeaderRecovery) {
            LeaderRecovery.Reset();
        }

        if (SnapshotBuilder) {
            GetStateInvoker()->Invoke(BIND(
                &TSnapshotBuilder::WaitUntilFinished,
                SnapshotBuilder));
            SnapshotBuilder.Reset();
        }
    }

    void DoStateStopLeading()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YCHECK(StateStatus == EPeerStatus::Leading || StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopLeading_.Fire();
    }


    void OnElectionStartFollowing(TPeerId leaderId, const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting follower recovery");

        ControlStatus = EPeerStatus::FollowerRecovery;
        LeaderId = leaderId;

        StartEpoch(epoch);

        EpochStateInvoker->Invoke(BIND(
            &TThis::DoStateStartFollowing,
            MakeStrong(this)));
    }

    void DoStateStartFollowing()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YCHECK(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::FollowerRecovery;

        StartFollowing_.Fire();
    }

    void OnControlFollowerRecoveryFinished(TError error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(FollowerRecovery);
        FollowerRecovery.Reset();

        if (!error.IsOK()) {
            LOG_WARNING("Follower recovery failed, restarting\n%s",
                ~error.ToString());
            Restart();
            return;
        }

        EpochStateInvoker->Invoke(BIND(
            &TThis::DoStateFollowerRecoveryComplete,
            MakeStrong(this)));

        YCHECK(!FollowerCommitter);
        FollowerCommitter = New<TFollowerCommitter>(
            DecoratedState,
            EpochControlInvoker,
            EpochStateInvoker);

        YCHECK(!SnapshotBuilder);
        SnapshotBuilder = New<TSnapshotBuilder>(
            Config->SnapshotBuilder,
            CellManager,
            DecoratedState,
            SnapshotStore,
            DecoratedState->GetEpoch(),
            EpochControlInvoker,
            EpochStateInvoker);

        ControlStatus = EPeerStatus::Following;

        LOG_INFO("Follower recovery complete");
    }

    void DoStateFollowerRecoveryComplete()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YCHECK(StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Following;

        FollowerRecoveryComplete_.Fire();
    }



    void OnElectionStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Stopped following");

        GetStateInvoker()->Invoke(BIND(
            &TThis::DoStateStopFollowing,
            MakeStrong(this)));

        ControlStatus = EPeerStatus::Elections;

        StopEpoch();

        if (FollowerRecovery) {
            FollowerRecovery.Reset();
        }

        if (FollowerCommitter) {
            FollowerCommitter.Reset();
        }

        if (SnapshotBuilder) {
            GetStateInvoker()->Invoke(BIND(
                &TSnapshotBuilder::WaitUntilFinished,
                SnapshotBuilder));
            SnapshotBuilder.Reset();
        }
    }

    void DoStateStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YCHECK(StateStatus == EPeerStatus::Following || StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopFollowing_.Fire();
    }


    void StartEpoch(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(!EpochContext);
        EpochContext = New<TCancelableContext>();
        EpochControlInvoker = EpochContext->CreateInvoker(ControlInvoker);
        EpochStateInvoker = EpochContext->CreateInvoker(GetStateInvoker());

        DecoratedState->SetEpoch(epoch);
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LeaderId = NElection::InvalidPeerId;
        DecoratedState->SetEpoch(TEpoch());

        YCHECK(EpochContext);
        EpochContext->Cancel();
        EpochContext.Reset();
        EpochControlInvoker.Reset();
        EpochStateInvoker.Reset();
    }

    void CheckEpoch(const TEpoch& epoch) const
    {
        auto currentEpoch = DecoratedState->GetEpoch();
        if (epoch != currentEpoch) {
            ythrow TServiceException(EErrorCode::InvalidEpoch) <<
                Sprintf("Invalid epoch: expected %s, received %s",
                    ~currentEpoch.ToString(),
                    ~epoch.ToString());
        }
    }


    void OnMutationApplied()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        YVERIFY(StateStatus == EPeerStatus::Leading);

        auto version = DecoratedState->GetVersion();
        auto period = Config->MaxChangesBetweenSnapshots;
        if (period &&
            version.RecordCount > 0 &&
            version.RecordCount % period.Get() == 0)
        {
            LeaderCommitter->Flush(true);
            SnapshotBuilder->CreateDistributedSnapshot();
        }
    }


    TPeerPriority GetPriority()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto version = DecoratedState->GetReachableVersionAsync();
        return ((TPeerPriority) version.SegmentId << 32) | version.RecordCount;
    }

    static Stroka FormatPriority(TPeerPriority priority)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        i32 segmentId = (priority >> 32);
        i32 recordCount = priority & ((1ll << 32) - 1);
        return Sprintf("(%d, %d)", segmentId, recordCount);
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
    DECLARE_THREAD_AFFINITY_SLOT(IOThread);
};

////////////////////////////////////////////////////////////////////////////////

IMetaStateManagerPtr CreatePersistentStateManager(
    TPersistentStateManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr stateInvoker,
    IMetaStatePtr metaState,
    NRpc::IServerPtr server)
{
    YCHECK(controlInvoker);
    YCHECK(metaState);
    YCHECK(server);

    return New<TPersistentStateManager>(
        config,
        controlInvoker,
        stateInvoker,
        metaState,
        server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
