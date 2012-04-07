#include "stdafx.h"
#include "persistent_state_manager.h"
#include "common.h"
#include "config.h"
#include "change_log.h"
#include "change_log_cache.h"
#include "meta_state_manager_proxy.h"
#include "snapshot.h"
#include "snapshot_builder.h"
#include "recovery.h"
#include "change_committer.h"
#include "follower_tracker.h"
#include "follower_pinger.h"
#include "meta_state.h"
#include "snapshot_store.h"
#include "decorated_meta_state.h"

#include <ytlib/election/cell_manager.h>
#include <ytlib/election/election_manager.h>
#include <ytlib/rpc/service.h>
#include <ytlib/actions/bind.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/ytree/fluent.h>

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
    typedef TPersistentStateManagerConfig TConfig;

    class TElectionCallbacks
        : public IElectionCallbacks
    {
    public:
        TElectionCallbacks(TPersistentStateManager* owner)
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
        TConfig* config,
        IInvoker* controlInvoker,
        IInvoker* stateInvoker,
        IMetaState* metaState,
        NRpc::IServer* server)
        : TServiceBase(controlInvoker, TProxy::GetServiceName(), Logger.GetCategory())
        , ControlStatus(EPeerStatus::Stopped)
        , StateStatus(EPeerStatus::Stopped)
        , Config(config)
        , LeaderId(NElection::InvalidPeerId)
        , ControlInvoker(controlInvoker)
        , ReadOnly(false)
        , InCommit(false)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSnapshotInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChangeLogInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ApplyChanges));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceSegment));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));

        ChangeLogCache = New<TChangeLogCache>(Config->LogPath, Config->ChangeLogCache);

        SnapshotStore = New<TSnapshotStore>(Config->SnapshotPath);

        DecoratedState = New<TDecoratedMetaState>(
            metaState,
            stateInvoker,
            controlInvoker,
            SnapshotStore,
            ChangeLogCache);

        IOQueue = New<TActionQueue>("MetaStateIO");

        VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
        VERIFY_INVOKER_AFFINITY(stateInvoker, StateThread);
        VERIFY_INVOKER_AFFINITY(IOQueue->GetInvoker(), IOThread);

        CellManager = New<TCellManager>(~Config->Cell);

        LOG_INFO("Self peer address is %s and peer id is %d",
            ~CellManager->GetSelfAddress(),
            CellManager->GetSelfId());

        ElectionManager = New<TElectionManager>(
            ~Config->Election,
            ~CellManager,
            controlInvoker,
            ~New<TElectionCallbacks>(this));

        server->RegisterService(this);
        server->RegisterService(~ElectionManager);
    }

    virtual void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(ControlStatus == EPeerStatus::Stopped);

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

    virtual IInvoker::TPtr GetStateInvoker() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DecoratedState->GetStateInvoker();
    }

    virtual bool HasActiveQuorum() const
    {
        auto tracker = FollowerTracker;
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
        auto tracker = FollowerTracker;

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Scalar(FormatEnum(ControlStatus))
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

    virtual TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        TClosure changeAction)
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        YASSERT(!InCommit);

        if (GetStateStatus() != EPeerStatus::Leading) {
            return New<TAsyncCommitResult>(ECommitResult::InvalidStatus);
        }

        if (ReadOnly) {
            return New<TAsyncCommitResult>(ECommitResult::ReadOnly);
        }

        // FollowerTracker is modified concurrently from the ControlThread.
        auto followerTracker = FollowerTracker;
        if (!followerTracker || !followerTracker->HasActiveQuorum()) {
            return New<TAsyncCommitResult>(ECommitResult::NotCommitted);
        }

        InCommit = true;

        auto actualChangeAction =
            changeAction.IsNull()
            ? BIND(&IMetaState::ApplyChange, DecoratedState->GetState(), changeData)
            : changeAction;

        auto result =
            LeaderCommitter
            ->Commit(actualChangeAction, changeData)
            ->Apply(BIND(&TThis::OnChangeCommitted, MakeStrong(this)));

        InCommit = false;

        return result;
    }

    virtual bool IsInCommit() const
    {
        return InCommit;
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
    IInvoker::TPtr ControlInvoker;
    bool ReadOnly;
    bool InCommit;

    NElection::TElectionManager::TPtr ElectionManager;
    TChangeLogCachePtr ChangeLogCache;
    TSnapshotStorePtr SnapshotStore;
    TDecoratedMetaStatePtr DecoratedState;

    TActionQueue::TPtr IOQueue;

    TEpoch Epoch;
    TCancelableContextPtr EpochContext;

    IInvoker::TPtr EpochControlInvoker;
    IInvoker::TPtr EpochStateInvoker;

    TSnapshotBuilderPtr SnapshotBuilder;
    TLeaderRecoveryPtr LeaderRecovery;
    TFollowerRecoveryPtr FollowerRecovery;

    TLeaderCommitterPtr LeaderCommitter;
    TFollowerCommitterPtr FollowerCommitter;

    TFollowerTrackerPtr FollowerTracker;
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

        YASSERT(offset >= 0);
        YASSERT(length >= 0);

        auto fileName = SnapshotStore->GetSnapshotFileName(snapshotId);
        if (!isexist(~fileName)) {
            context->Reply(TError(
                EErrorCode::NoSuchSnapshot,
                Sprintf("No such snapshot %d", snapshotId)));
            return;
        }

        TSharedPtr<TFile> snapshotFile;
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
                bytesRead = snapshotFile->Read(data.begin(), length);
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

        YASSERT(startRecordId >= 0);
        YASSERT(recordCount >= 0);
    
        auto result = ChangeLogCache->Get(changeLogId);
        if (!result.IsOK()) {
            // TODO: cannot use ythrow here
            throw TServiceException(result);
        }

        auto changeLog = result.Value();
        IOQueue->GetInvoker()->Invoke(context->Wrap(BIND([=] () {
            VERIFY_THREAD_AFFINITY(IOThread);

            yvector<TSharedRef> recordData;
            try {
                changeLog->Read(startRecordId, recordCount, &recordData);
            } catch (const std::exception& ex) {
                LOG_FATAL("IO error while reading changelog (ChangeLogId: %d)\n%s",
                    changeLogId,
                    ex.what());
            }

            context->Response().set_records_read(recordData.ysize());
            context->Response().Attachments().insert(
                context->Response().Attachments().end(),
                recordData.begin(),
                recordData.end());

            context->SetResponseInfo("RecordCount: %d", recordData.ysize());
            context->Reply();
        })));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ApplyChanges)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TEpoch epoch = TEpoch::FromProto(request->epoch());
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

        if (epoch != Epoch) {
            ythrow TServiceException(EErrorCode::InvalidEpoch) <<
                Sprintf("Invalid epoch: expected %s, received %s",
                    ~Epoch.ToString(),
                    ~epoch.ToString());
        }

        int changeCount = request->Attachments().size();
        switch (GetControlStatus()) {
            case EPeerStatus::Following: {
                LOG_DEBUG("ApplyChange: applying changes (Version: %s, ChangeCount: %d)",
                    ~version.ToString(),
                    changeCount);

                YASSERT(FollowerCommitter);

                FollowerCommitter
                    ->Commit(version, request->Attachments())
                    ->Subscribe(BIND(&TThis::OnFollowerCommitted, MakeStrong(this), context));
                break;
            }

            case EPeerStatus::FollowerRecovery: {
                if (FollowerRecovery) {
                    LOG_DEBUG("ApplyChange: keeping postponed changes (Version: %s, ChangeCount: %d)",
                        ~version.ToString(),
                        changeCount);

                    auto result = FollowerRecovery->PostponeChanges(version, request->Attachments());
                    if (result != TRecovery::EResult::OK) {
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

    void OnFollowerCommitted(TCtxApplyChanges::TPtr context, TLeaderCommitter::EResult result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto& response = context->Response();

        switch (result) {
        case TCommitter::EResult::Committed:
            response.set_committed(true);
            context->Reply();
            break;

        case TCommitter::EResult::LateChanges:
            context->Reply(
                TProxy::EErrorCode::InvalidVersion,
                "Changes are late");
            break;

        case TCommitter::EResult::OutOfOrderChanges:
            context->Reply(
                TProxy::EErrorCode::InvalidVersion,
                "Changes are out of order");
            Restart();
            break;

        default:
            YUNREACHABLE();
        }
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

        if (epoch != Epoch) {
            ythrow TServiceException(EErrorCode::InvalidEpoch) <<
                Sprintf("Invalid epoch: expected %s, received %s",
                    ~Epoch.ToString(),
                    ~epoch.ToString());
        }

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
                        ~Config,
                        ~CellManager,
                        ~DecoratedState,
                        ~ChangeLogCache,
                        ~SnapshotStore,
                        Epoch,
                        LeaderId,
                        ~EpochControlInvoker,
                        ~EpochStateInvoker,
                        version);

                    FollowerRecovery->Run()->Subscribe(
                        BIND(&TThis::OnControlFollowerRecoveryFinished, MakeStrong(this))
                        .Via(~EpochControlInvoker));
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
        TMetaVersion version(segmentId, recordCount);
        bool createSnapshot = request->create_snapshot();

        context->SetRequestInfo("Epoch: %s, Version: %s, CreateSnapshot: %s",
            ~epoch.ToString(),
            ~version.ToString(),
            ~ToString(createSnapshot));

        if (GetControlStatus() != EPeerStatus::Following && GetControlStatus() != EPeerStatus::FollowerRecovery) {
            ythrow TServiceException(EErrorCode::InvalidStatus) <<
                Sprintf("Cannot advance segment while in %s", ~GetControlStatus().ToString());
        }

        if (epoch != Epoch) {
            ythrow TServiceException(EErrorCode::InvalidEpoch) <<
                Sprintf("Invalid epoch: expected: %s, received %s",
                    ~Epoch.ToString(),
                    ~epoch.ToString());
        }

        switch (GetControlStatus()) {
            case EPeerStatus::Following:
                if (createSnapshot) {
                    LOG_DEBUG("AdvanceSegment: starting snapshot creation");

                    BIND(&TSnapshotBuilder::CreateLocalSnapshot, SnapshotBuilder, version)
                        .AsyncVia(EpochStateInvoker)
                        .Run()
                        ->Subscribe(BIND(
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

                    auto result = FollowerRecovery->PostponeSegmentAdvance(version);
                    if (result != TRecovery::EResult::OK) {
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

    void DoStateAdvanceSegment(TMetaVersion version, TCtxAdvanceSegment::TPtr context)
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
        TCtxAdvanceSegment::TPtr context,
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

    ECommitResult OnChangeCommitted(TLeaderCommitter::EResult result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        switch (result) {
            case TLeaderCommitter::EResult::Committed:
                return ECommitResult::Committed;

            case TLeaderCommitter::EResult::MaybeCommitted:
                Restart();
                return ECommitResult::MaybeCommitted;

            default:
                YUNREACHABLE();
        }
    }


    void OnElectionStartLeading(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting leader recovery");

        ControlStatus = EPeerStatus::LeaderRecovery;
        LeaderId = CellManager->GetSelfId();

        StartEpoch(epoch);

        YASSERT(!FollowerTracker);
        FollowerTracker = New<TFollowerTracker>(
            ~Config->FollowerTracker,
            ~CellManager,
            ~EpochControlInvoker);

        YASSERT(!LeaderCommitter);
        LeaderCommitter = New<TLeaderCommitter>(
            ~Config->LeaderCommitter,
            ~CellManager,
            ~DecoratedState,
            ~ChangeLogCache,
            ~FollowerTracker,
            Epoch,
            ~EpochControlInvoker,
            ~EpochStateInvoker);
        LeaderCommitter->SubscribeChangeApplied(BIND(&TThis::OnChangeApplied, MakeWeak(this)));

        // During recovery the leader is reporting its reachable version to followers.
        auto version = DecoratedState->GetReachableVersionAsync();
        DecoratedState->SetPingVersion(version);

        YASSERT(!FollowerPinger);
        FollowerPinger = New<TFollowerPinger>(
            Config->FollowerPinger,
            CellManager,
            DecoratedState,
            FollowerTracker,
            Epoch,
            EpochControlInvoker);
        FollowerPinger->Start();

        EpochStateInvoker->Invoke(BIND(
            &TThis::DoStateStartLeading,
            MakeStrong(this)));
    }

    void DoStateStartLeading()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YASSERT(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::LeaderRecovery;

        StartLeading_.Fire();

        YASSERT(!LeaderRecovery);
        LeaderRecovery = New<TLeaderRecovery>(
            ~Config,
            ~CellManager,
            ~DecoratedState,
            ~ChangeLogCache,
            ~SnapshotStore,
            Epoch,
            ~EpochControlInvoker,
            ~EpochStateInvoker);

        BIND(&TLeaderRecovery::Run, LeaderRecovery)
            .AsyncVia(EpochControlInvoker)
            .Run()
            ->Subscribe(
                BIND(&TThis::OnStateLeaderRecoveryFinished, MakeStrong(this))
                .Via(~EpochStateInvoker));
    }

    void OnStateLeaderRecoveryFinished(TRecovery::EResult result)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YASSERT(LeaderRecovery);
        LeaderRecovery.Reset();

        if (result != TRecovery::EResult::OK) {
            LOG_WARNING("Leader recovery failed, restarting");
            Restart();
            return;
        }

        YASSERT(!SnapshotBuilder);
        SnapshotBuilder = New<TSnapshotBuilder>(
            ~Config->SnapshotBuilder,
            CellManager,
            DecoratedState,
            ChangeLogCache,
            SnapshotStore,
            Epoch,
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

        YASSERT(StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Leading;

        EpochControlInvoker->Invoke(BIND(
            &TThis::DoControlLeaderRecoveryFinished,
            MakeStrong(this)));

    }

    void DoControlLeaderRecoveryFinished()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FollowerTracker->Start();

        YASSERT(ControlStatus == EPeerStatus::LeaderRecovery);
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

        if (FollowerTracker) {
            FollowerTracker->Stop();
            FollowerTracker.Reset();
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

        YASSERT(StateStatus == EPeerStatus::Leading || StateStatus == EPeerStatus::LeaderRecovery);
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

        YASSERT(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::FollowerRecovery;

        StartFollowing_.Fire();
    }

    void OnControlFollowerRecoveryFinished(TRecovery::EResult result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YASSERT(FollowerRecovery);
        FollowerRecovery.Reset();

        if (result != TRecovery::EResult::OK) {
            LOG_INFO("Follower recovery failed, restarting");
            Restart();
            return;
        }

        EpochStateInvoker->Invoke(BIND(
            &TThis::DoStateFollowerRecoveryComplete,
            MakeStrong(this)));

        YASSERT(!FollowerCommitter);
        FollowerCommitter = New<TFollowerCommitter>(
            ~DecoratedState,
            ~EpochControlInvoker,
            ~EpochStateInvoker);

        YASSERT(!SnapshotBuilder);
        SnapshotBuilder = New<TSnapshotBuilder>(
            ~Config->SnapshotBuilder,
            CellManager,
            DecoratedState,
            ChangeLogCache,
            SnapshotStore,
            Epoch,
            EpochControlInvoker,
            EpochStateInvoker);

        ControlStatus = EPeerStatus::Following;

        LOG_INFO("Follower recovery complete");
    }

    void DoStateFollowerRecoveryComplete()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YASSERT(StateStatus == EPeerStatus::FollowerRecovery);
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

        YASSERT(StateStatus == EPeerStatus::Following || StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopFollowing_.Fire();
    }


    void StartEpoch(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YASSERT(!EpochContext);
        EpochContext = New<TCancelableContext>();
        EpochControlInvoker = EpochContext->CreateInvoker(ControlInvoker);
        EpochStateInvoker = EpochContext->CreateInvoker(GetStateInvoker());

        Epoch = epoch;
    }

    void StopEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LeaderId = NElection::InvalidPeerId;
        Epoch = TEpoch();

        YASSERT(EpochContext);
        EpochContext->Cancel();
        EpochContext.Reset();
        EpochControlInvoker.Reset();
        EpochStateInvoker.Reset();
    }


    void OnChangeApplied()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        YASSERT(StateStatus == EPeerStatus::Leading);

        auto version = DecoratedState->GetVersion();
        if (Config->MaxChangesBetweenSnapshots > 0 &&
            version.RecordCount > 0 &&
            version.RecordCount % Config->MaxChangesBetweenSnapshots == 0)
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
    TPersistentStateManagerConfig* config,
    IInvoker* controlInvoker,
    IInvoker* stateInvoker,
    IMetaState* metaState,
    NRpc::IServer* server)
{
    YASSERT(controlInvoker);
    YASSERT(metaState);
    YASSERT(server);

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
