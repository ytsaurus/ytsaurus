#include "meta_state_manager.h"
#include "follower_tracker.h"
#include "change_committer.h"
#include "leader_pinger.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"
#include "../misc/fs.h"
#include "../misc/guid.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TMetaStateManager::TMetaStateManager(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr serviceInvoker,
    IMetaState::TPtr metaState,
    NRpc::TServer::TPtr server)
    : TServiceBase(
        serviceInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , State(EState::Stopped)
    , Config(config)
    , LeaderId(InvalidPeerId)
    , CellManager(cellManager)
    , ServiceInvoker(serviceInvoker)
    , StateInvoker(metaState->GetInvoker())
{
    RegisterMethods();

    NFS::CleanTempFiles(config.LogLocation);
    ChangeLogCache = New<TChangeLogCache>(Config.LogLocation);

    NFS::CleanTempFiles(config.SnapshotLocation);
    SnapshotStore = New<TSnapshotStore>(Config.SnapshotLocation);

    MetaState = New<TDecoratedMetaState>(
        metaState,
        SnapshotStore,
        ChangeLogCache);

    // TODO: fill config
    ElectionManager = New<TElectionManager>(
        TElectionManager::TConfig(),
        CellManager,
        serviceInvoker,
        this,
        server);

    server->RegisterService(this);
}

TMetaStateManager::~TMetaStateManager()
{ }

void TMetaStateManager::RegisterMethods()
{
    RPC_REGISTER_METHOD(TMetaStateManager, ScheduleSync);
    RPC_REGISTER_METHOD(TMetaStateManager, Sync);
    RPC_REGISTER_METHOD(TMetaStateManager, GetSnapshotInfo);
    RPC_REGISTER_METHOD(TMetaStateManager, ReadSnapshot);
    RPC_REGISTER_METHOD(TMetaStateManager, GetChangeLogInfo);
    RPC_REGISTER_METHOD(TMetaStateManager, ReadChangeLog);
    RPC_REGISTER_METHOD(TMetaStateManager, ApplyChanges);
    RPC_REGISTER_METHOD(TMetaStateManager, CreateSnapshot);
    RPC_REGISTER_METHOD(TMetaStateManager, PingLeader);
}

void TMetaStateManager::Restart()
{
    // To prevent multiple restarts.
    ServiceEpochInvoker->Cancel();

    ElectionManager->Restart();
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChange(
    IAction::TPtr changeAction,
    TSharedRef changeData)
{
    if (State != EState::Leading) {
        return New<TCommitResult>(ECommitResult::InvalidState);
    }

    if (!FollowerTracker->HasActiveQuorum()) {
        return New<TCommitResult>(ECommitResult::NotCommitted);
    }

    return
        ChangeCommitter
        ->CommitLeader(changeAction, changeData)
        ->Apply(FromMethod(&TMetaStateManager::OnChangeCommit, TPtr(this)));
}

TMetaStateManager::ECommitResult TMetaStateManager::OnChangeCommit(
    TChangeCommitter::EResult result)
{
    switch (result) {
        case TChangeCommitter::EResult::Committed:
            return TMetaStateManager::ECommitResult::Committed;

        case TChangeCommitter::EResult::MaybeCommitted:
            Restart();
            return TMetaStateManager::ECommitResult::MaybeCommitted;

        default:
            YASSERT(false);
            return TMetaStateManager::ECommitResult::NotCommitted;
    }
}

void TMetaStateManager::Start()
{
    YASSERT(State == EState::Stopped);

    State = EState::Elections;

    MetaState->Clear();

    LOG_INFO("Meta state is reset to %s",
        ~MetaState->GetVersion().ToString());

    ElectionManager->Start();
}

void TMetaStateManager::StartEpoch(const TEpoch& epoch)
{
    YASSERT(~ServiceEpochInvoker == NULL);
    ServiceEpochInvoker = New<TCancelableInvoker>(ServiceInvoker);
    Epoch = epoch;

    ChangeCommitter = New<TChangeCommitter>(
        TChangeCommitter::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        ServiceInvoker,
        Epoch);

    SnapshotCreator = New<TSnapshotCreator>(
        TSnapshotCreator::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        ServiceInvoker);
}

void TMetaStateManager::StopEpoch()
{
    LeaderId = InvalidPeerId;
    Epoch = TEpoch();
    
    YASSERT(~ServiceEpochInvoker != NULL);
    ServiceEpochInvoker->Cancel();
    ServiceEpochInvoker.Drop();

    YASSERT(~ChangeCommitter != NULL);
    ChangeCommitter->Stop();
    ChangeCommitter.Drop();

    YASSERT(~SnapshotCreator != NULL);
    SnapshotCreator.Drop();
}

//////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ScheduleSync)
{
    UNUSED(response);

    TPeerId peerId = request->GetPeerId();

    context->SetRequestInfo("PeerId: %d", peerId);
   
    if (State != EState::Leading && State != EState::LeaderRecovery) {
        ythrow TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %d", (int) State);
    }

    context->Reply();

    StateInvoker->Invoke(FromMethod(
        &TMetaStateManager::SendSync,
        TPtr(this),
        peerId,
        Epoch));
}

void TMetaStateManager::SendSync(TPeerId peerId, TEpoch epoch)
{
    TMetaVersion version = MetaState->GetNextVersion();
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();

    THolder<TProxy> proxy(CellManager->GetMasterProxy<TProxy>(peerId));
    TProxy::TReqSync::TPtr request = proxy->Sync();
    request->SetSegmentId(version.SegmentId);
    request->SetRecordCount(version.RecordCount);
    request->SetEpoch(epoch.ToProto());
    request->SetMaxSnapshotId(maxSnapshotId);
    request->Invoke();

    LOG_DEBUG("Sync sent to peer %d (Version: %s, Epoch: %s, MaxSnapshotId: %d)",
        peerId,
        ~version.ToString(),
        ~epoch.ToString(),
        maxSnapshotId);
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, Sync)
{
    UNUSED(response);

    TMetaVersion version(
        request->GetSegmentId(),
        request->GetRecordCount());
    TEpoch epoch = TGuid::FromProto(request->GetEpoch());
    i32 maxSnapshotId = request->GetMaxSnapshotId();

    context->SetRequestInfo("Version: %s, Epoch: %s, MaxSnapshotId: %d",
        ~version.ToString(),
        ~epoch.ToString(),
        maxSnapshotId);

    context->Reply();

    if (~FollowerRecovery == NULL) {
        LOG_WARNING("Unexpected sync received");
        return;
    }

    FollowerRecovery->Sync(
        version,
        epoch,
        maxSnapshotId);
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, GetSnapshotInfo)
{
    i32 snapshotId = request->GetSnapshotId();

    context->SetRequestInfo("SnapshotId: %d",
        snapshotId);

    try {
        // TODO: extract method
        TSnapshotReader::TPtr reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            ythrow TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid snapshot id %d", snapshotId);
        }

        reader->Open();
        
        i64 length = reader->GetLength();
        TChecksum checksum = reader->GetChecksum();
        int prevRecordCount = reader->GetPrevRecordCount();

        response->SetLength(length);
        response->SetPrevRecordCount(prevRecordCount);
        response->SetChecksum(checksum);

        context->SetResponseInfo("Length: %" PRId64 ", PrevRecordCount: %d, Checksum: %" PRIx64,
            length,
            prevRecordCount,
            checksum);

        context->Reply();
    } catch (const yexception& ex) {
        // TODO: fail?
        ythrow TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error in snapshot %d: %s",
                snapshotId,
                ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ReadSnapshot)
{
    i32 snapshotId = request->GetSnapshotId();
    i64 offset = request->GetOffset();
    i32 length = request->GetLength();

    context->SetRequestInfo("SnapshotId: %d, Offset: %" PRId64 ", Length: %d",
        snapshotId,
        offset,
        length);

    YASSERT(offset >= 0);
    YASSERT(length >= 0);

    try {
        TSnapshotReader::TPtr reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            ythrow TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid snapshot id %d", snapshotId);
        }

        reader->Open(offset);

        TBlob data(length);
        i32 bytesRead = reader->GetStream().Read(data.begin(), length);
        data.erase(data.begin() + bytesRead, data.end());

        response->Attachments().push_back(TSharedRef(data));

        context->SetResponseInfo("BytesRead: %d",
            bytesRead);

        context->Reply();
    } catch (const yexception& ex) {
        // TODO: fail?
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("ReadSnapshot: IO error in snapshot %d: %s",
            snapshotId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, GetChangeLogInfo)
{
    i32 changeLogId = request->GetSegmentId();

    context->SetRequestInfo("ChangeLogId: %d",
        changeLogId);

    try {
        // TODO: extract method
        TCachedAsyncChangeLog::TPtr changeLog = ChangeLogCache->Get(changeLogId);
        if (~changeLog == NULL) {
            ythrow TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid changelog id %d", changeLogId);
        }

        i32 recordCount = changeLog->GetRecordCount();
        
        response->SetRecordCount(recordCount);
        
        context->SetResponseInfo("RecordCount: %d", recordCount);
        context->Reply();
    } catch (const yexception& ex) {
        // TODO: fail?
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("GetChangeLogInfo: IO error in changelog %d: %s",
            changeLogId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ReadChangeLog)
{
    i32 segmentId = request->GetSegmentId();
    i32 startRecordId = request->GetStartRecordId();
    i32 recordCount = request->GetRecordCount();
    
    context->SetRequestInfo("SegmentId: %d, StartRecordId: %d, RecordCount: %d",
        segmentId,
        startRecordId,
        recordCount);

    YASSERT(startRecordId >= 0);
    YASSERT(recordCount >= 0);
    
    try {
        // TODO: extract method
        TCachedAsyncChangeLog::TPtr changeLog = ChangeLogCache->Get(segmentId);
        if (~changeLog == NULL) {
            ythrow TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid changelog id %d", segmentId);
        }

        yvector<TSharedRef> recordData;
        changeLog->Read(startRecordId, recordCount, &recordData);

        response->SetRecordsRead(recordData.ysize());
        response->Attachments().insert(
            response->Attachments().end(),
            recordData.begin(),
            recordData.end());
        
        context->SetResponseInfo("RecordCount: %d", recordData.ysize());
        context->Reply();
    } catch (const yexception& ex) {
        // TODO: fail?
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("ReadChangeLog: IO error in changelog %d: %s",
            segmentId,
            ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ApplyChanges)
{
    TEpoch epoch = TGuid::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);

    context->SetRequestInfo("Epoch: %s, Version: %s",
        ~epoch.ToString(),
        ~version.ToString());

    if (State != EState::Following && State != EState::FollowerRecovery) {
        ythrow TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %s", ~State.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(TProxy::EErrorCode::InvalidEpoch) <<
            Sprintf("invalid epoch (expected: %s, received: %s)",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }
    
    int numChanges = request->Attachments().size();
    switch (State) {
        case EState::Following: {
            LOG_DEBUG("ApplyChange: applying %d changes", numChanges);

            for (int changeIndex = 0; changeIndex < numChanges; ++changeIndex) {
                YASSERT(State == EState::Following);
                TMetaVersion commitVersion(segmentId, recordCount + changeIndex);
                const TSharedRef& changeData = request->Attachments().at(changeIndex);
                TChangeCommitter::TResult::TPtr asyncResult =
                    ChangeCommitter->CommitFollower(commitVersion, changeData);

                // subscribe to last change
                if (changeIndex == numChanges - 1) {
                    asyncResult->Subscribe(FromMethod(
                            &TMetaStateManager::OnLocalCommit,
                            TPtr(this),
                            context)
                        ->Via(ServiceInvoker));
                }
            }
            break;
        }

        case EState::FollowerRecovery: {
            LOG_DEBUG("ApplyChange: keeping %d postponed change", numChanges);

            YASSERT(~FollowerRecovery != NULL);
            for (int changeIndex = 0; changeIndex < numChanges; ++changeIndex) {
                YASSERT(State == EState::FollowerRecovery);
                TMetaVersion commitVersion(segmentId, recordCount + changeIndex);
                const TSharedRef& changeData = request->Attachments().at(changeIndex);
                TRecovery::EResult result = FollowerRecovery
                    ->PostponeChange(commitVersion, changeData);
                if (result != TRecovery::EResult::OK) {
                    Restart();
                    break;
                }
            }

            response->SetCommitted(false);
            context->Reply();
            break;
        }

        default:
            YASSERT(false);
            break;
    }
}

void TMetaStateManager::OnLocalCommit(
    TChangeCommitter::EResult result,
    TCtxApplyChanges::TPtr context)
{
    TReqApplyChanges& request = context->Request();
    TRspApplyChanges& response = context->Response();

    TMetaVersion version(request.GetSegmentId(), request.GetRecordCount());

    switch (result) {
        case TChangeCommitter::EResult::Committed:
            response.SetCommitted(true);
            context->Reply();
            break;

        case TChangeCommitter::EResult::InvalidVersion:
            context->Reply(TProxy::EErrorCode::InvalidVersion);
            Restart();

            LOG_WARNING("ApplyChange: change %s is unexpected, restarting",
                ~version.ToString());
            break;

        default:
            YASSERT(false);
    }
}

//TODO: rename CreateSnapshot to AdvanceSegment
//TODO: reply whether snapshot was created
RPC_SERVICE_METHOD_IMPL(TMetaStateManager, CreateSnapshot)
{
    UNUSED(response);

    TEpoch epoch = TGuid::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);

    context->SetRequestInfo("Epoch: %s, Version: %s",
        ~epoch.ToString(),
        ~version.ToString());

    if (State != EState::Following && State != EState::FollowerRecovery) {
        ythrow TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %s", ~State.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(TProxy::EErrorCode::InvalidEpoch) <<
            Sprintf("invalid epoch: expected %s, received %s",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }

    switch (State) {
        case EState::Following:
            LOG_DEBUG("CreateSnapshot: creating snapshot");

            SnapshotCreator->CreateLocal(version)->Subscribe(FromMethod(
                &TMetaStateManager::OnCreateLocalSnapshot,
                TPtr(this),
                context));
            break;
            
        case EState::FollowerRecovery: {
            LOG_DEBUG("CreateSnapshot: keeping postponed segment advance");

            YASSERT(~FollowerRecovery != NULL);
            TRecovery::EResult result = FollowerRecovery->PostponeSegmentAdvance(version);
            if (result != TRecovery::EResult::OK) {
                Restart();
            }

            context->Reply(TProxy::EErrorCode::InvalidState);
            break;
        }

        default:
            YASSERT(false);
            break;
    }
}

void TMetaStateManager::OnCreateLocalSnapshot(
    TSnapshotCreator::TLocalResult result,
    TCtxCreateSnapshot::TPtr context)
{
    switch (result.ResultCode) {
        case TSnapshotCreator::EResultCode::OK:
            context->Response().SetChecksum(result.Checksum);
            context->Reply();
            break;
        case TSnapshotCreator::EResultCode::InvalidVersion:
            context->Reply(TProxy::EErrorCode::InvalidVersion);
            break;
        default:
            YASSERT(false);
            break;
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, PingLeader)
{
    UNUSED(response);

    TPeerId followerId = request->GetFollowerId();
    TEpoch followerEpoch = TGuid::FromProto(request->GetEpoch());
    EState followerState = static_cast<EState>(request->GetState());

    context->SetRequestInfo("Id: %d, Epoch: %s, State: %s",
        followerId,
        ~followerEpoch.ToString(),
        ~followerState.ToString());

    if (State != EState::Leading) {
        LOG_DEBUG("PingLeader: invalid state (State: %s)",
            ~State.ToString());
    } else if (followerEpoch != Epoch ) {
        LOG_DEBUG("PingLeader: invalid epoch (Epoch: %s)",
            ~Epoch.ToString());
    } else {
        FollowerTracker->ProcessPing(followerId, followerState);
    }

    // Reply with OK in any case.
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////
// IElectionCallbacks members

void TMetaStateManager::OnStartLeading(TEpoch epoch)
{
    LOG_INFO("Starting leader recovery");

    State = EState::LeaderRecovery;
    LeaderId = CellManager->GetSelfId();    
    StartEpoch(epoch);
    
    YASSERT(~LeaderRecovery == NULL);
    LeaderRecovery = new TLeaderRecovery(
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        LeaderId,
        ServiceInvoker);

    LeaderRecovery->Run()->Subscribe(
        FromMethod(&TMetaStateManager::OnLeaderRecovery, TPtr(this))
        ->Via(~ServiceEpochInvoker));
}

void TMetaStateManager::OnLeaderRecovery(TRecovery::EResult result)
{
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    if (result != TRecovery::EResult::OK) {
        LOG_WARNING("Leader recovery failed, restarting");
        Restart();
        return;
    }

    FollowerTracker = New<TFollowerTracker>(
        TFollowerTracker::TConfig(),
        CellManager,
        ServiceInvoker);

    ChangeCommitter->SetOnApplyChange(FromMethod(
        &TMetaStateManager::OnApplyChange,
        TPtr(this)));

    State = EState::Leading;

    LOG_INFO("Leader recovery complete");

    MetaState->OnStartLeading();
}

void TMetaStateManager::OnApplyChange()
{
    YASSERT(State == EState::Leading);
    TMetaVersion version = MetaState->GetVersion();
    if (version.RecordCount >= Config.MaxRecordCount) {
        ChangeCommitter->Flush();
        SnapshotCreator->CreateDistributed(version);
    }
}

void TMetaStateManager::OnStopLeading()
{
    LOG_INFO("Stopped leading");
    
    MetaState->OnStopLeading();
    
    State = EState::Elections;

    ChangeCommitter->SetOnApplyChange(NULL);

    StopEpoch();

    if (~LeaderRecovery != NULL) {
        LeaderRecovery->Stop();
        LeaderRecovery.Drop();
    }

    if (~FollowerTracker != NULL) {
        FollowerTracker->Stop();
        FollowerTracker.Drop();
    }
}

void TMetaStateManager::OnStartFollowing(TPeerId leaderId, TEpoch epoch)
{
    LOG_INFO("Starting follower state recovery");
    
    State = EState::FollowerRecovery;
    LeaderId = leaderId;
    StartEpoch(epoch);

    YASSERT(~FollowerRecovery == NULL);
    FollowerRecovery = New<TFollowerRecovery>(
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        LeaderId,
        ServiceInvoker);

    FollowerRecovery->Run()->Subscribe(
        FromMethod(&TMetaStateManager::OnFollowerRecovery, TPtr(this))
        ->Via(~ServiceEpochInvoker));
}

void TMetaStateManager::OnFollowerRecovery(TRecovery::EResult result)
{
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    if (result != TRecovery::EResult::OK) {
        LOG_INFO("Follower recovery failed, restarting");
        Restart();
        return;
    }

    LeaderPinger = New<TLeaderPinger>(
        TLeaderPinger::TConfig(),
        this,
        CellManager,
        LeaderId,
        Epoch,
        ServiceInvoker);

    State = EState::Following;

    LOG_INFO("Follower recovery complete");

    MetaState->OnStartFollowing();
}

void TMetaStateManager::OnStopFollowing()
{
    LOG_INFO("Stopped following");
    
    MetaState->OnStopFollowing();

    State = EState::Elections;
    
    StopEpoch();

    if (~FollowerRecovery != NULL) {
        FollowerRecovery->Stop();
        FollowerRecovery.Drop();
    }

    if (~LeaderPinger != NULL) {
        LeaderPinger->Stop();
        LeaderPinger.Drop();
    }
}

TPeerPriority TMetaStateManager::GetPriority()
{
    TMetaVersion version = MetaState->GetNextVersion();
    return ((TPeerPriority) version.SegmentId << 32) | version.RecordCount;
}

Stroka TMetaStateManager::FormatPriority(TPeerPriority priority)
{
    i32 segmentId = (priority >> 32);
    i32 recordCount = priority & ((1ll << 32) - 1);
    return Sprintf("(%d, %d)", segmentId, recordCount);
}

TMetaStateManager::EState TMetaStateManager::GetState() const
{
    return State;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
