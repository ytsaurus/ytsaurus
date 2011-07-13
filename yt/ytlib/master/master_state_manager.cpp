#include "master_state_manager.h"
#include "follower_state_tracker.h"
#include "change_committer.h"
#include "leader_pinger.h"

#include "../actions/action_util.h"
#include "../misc/string.h"
#include "../misc/serialize.h"
#include "../misc/fs.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterStateManager::TMasterStateManager(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr serviceInvoker,
    IMasterState::TPtr masterState,
    NRpc::TServer* server)
    : TServiceBase(TProxy::GetServiceName(), Logger.GetCategory())
    , State(EState::Stopped)
    , Config(config)
    , LeaderId(InvalidMasterId)
    , CellManager(cellManager)
    , ServiceInvoker(serviceInvoker)
    , WorkQueue(new TActionQueue())
{
    RegisterMethods();

    NFS::CleanTempFiles(config.LogLocation);
    ChangeLogCache = new TChangeLogCache(Config.LogLocation);

    NFS::CleanTempFiles(config.SnapshotLocation);
    SnapshotStore = new TSnapshotStore(Config.SnapshotLocation);

    MasterState = new TDecoratedMasterState(
        masterState,
        ~SnapshotStore, ChangeLogCache);

    // TODO: fill config
    ElectionManager = new TElectionManager(
        TElectionManager::TConfig(),
        CellManager,
        serviceInvoker,
        this,
        server);

    server->RegisterService(this);
}

TMasterStateManager::~TMasterStateManager()
{ }

void TMasterStateManager::RegisterMethods()
{
    RPC_REGISTER_METHOD(TMasterStateManager, ScheduleSync);
    RPC_REGISTER_METHOD(TMasterStateManager, Sync);
    RPC_REGISTER_METHOD(TMasterStateManager, GetSnapshotInfo);
    RPC_REGISTER_METHOD(TMasterStateManager, ReadSnapshot);
    RPC_REGISTER_METHOD(TMasterStateManager, GetChangeLogInfo);
    RPC_REGISTER_METHOD(TMasterStateManager, ReadChangeLog);
    RPC_REGISTER_METHOD(TMasterStateManager, ApplyChange);
    RPC_REGISTER_METHOD(TMasterStateManager, CreateSnapshot);
    RPC_REGISTER_METHOD(TMasterStateManager, PingLeader);
}

// TODO: cascading restart issue
void TMasterStateManager::Restart()
{
    MyEpoch = TGUID();
    ElectionManager->Restart();
}

TMasterStateManager::TCommitResult::TPtr TMasterStateManager::CommitChange(
    TSharedRef change)
{
    if (State != EState::Leading) {
        return new TCommitResult(ECommitResult::InvalidState);
    }

    if (!FollowerStateTracker->HasActiveQuorum()) {
        return new TCommitResult(ECommitResult::NotCommitted);
    }

    return
        ChangeCommitter
        ->CommitDistributed(change)
        ->Apply(FromMethod(&TMasterStateManager::OnChangeCommit, TPtr(this)));
}

TMasterStateManager::ECommitResult TMasterStateManager::OnChangeCommit(
    TChangeCommitter::EResult result)
{
    switch (result) {
        case TChangeCommitter::EResult::Committed:
            return TMasterStateManager::ECommitResult::Committed;

        case TChangeCommitter::EResult::MaybeCommitted:
            Restart();
            return TMasterStateManager::ECommitResult::MaybeCommitted;

        default:
            YASSERT(false);
            return TMasterStateManager::ECommitResult::NotCommitted;
    }
}

void TMasterStateManager::Start()
{
    YASSERT(State == EState::Stopped);

    State = EState::Elections;

    MasterState->Clear();

    LOG_INFO("Master state is reset to %s",
        ~MasterState->GetStateId().ToString());

    ElectionManager->Start();
}

void TMasterStateManager::StartEpoch(const TMasterEpoch& epoch)
{
    YASSERT(~EpochInvoker == NULL);
    EpochInvoker = ElectionManager->GetEpochInvoker();
    Epoch = epoch;

    CreateGuid(&MyEpoch);

    TChangeLogDownloader::TConfig changeLogDownloaderConfig;
    // TODO: fill config

    TSnapshotDownloader::TConfig snapshotDownloaderConfig;
    // TODO: fill config

    ChangeCommitter = new TChangeCommitter(
        CellManager,
        ~MasterState,
        ChangeLogCache,
        ServiceInvoker,
        WorkQueue,
        Epoch);

    SnapshotCreator = new TSnapshotCreator(
        TSnapshotCreator::TConfig(),
        CellManager,
        ~MasterState,
        ChangeLogCache,
        ~SnapshotStore,
        Epoch,
        ServiceInvoker,
        WorkQueue);
}

void TMasterStateManager::StopEpoch()
{
    YASSERT(~EpochInvoker != NULL);

    LeaderId = InvalidMasterId;
    EpochInvoker.Drop();
    Epoch = TMasterEpoch();
    MyEpoch = TMasterEpoch();
    ChangeCommitter.Drop();
    SnapshotCreator.Drop();
}

//////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ScheduleSync)
{
    UNUSED(response);

    TMasterId masterId = request->GetMasterId();

    context->SetRequestInfo("MasterId: %d", masterId);
   
    if (State != EState::Leading && State != EState::LeaderRecovery) {
        ythrow TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %d", (int) State);
    }

    context->Reply();

    WorkQueue->Invoke(FromMethod(
        &TMasterStateManager::SendSync,
        TPtr(this),
        masterId,
        Epoch));
}

void TMasterStateManager::SendSync(TMasterId masterId, TMasterEpoch epoch)
{
    TMasterStateId stateId = MasterState->GetAvailableStateId();
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();

    THolder<TProxy> proxy(CellManager->GetMasterProxy<TProxy>(masterId));
    TProxy::TReqSync::TPtr request = proxy->Sync();
    request->SetSegmentId(stateId.SegmentId);
    request->SetChangeCount(stateId.ChangeCount);
    request->SetEpoch(ProtoGuidFromGuid(epoch));
    request->SetMaxSnapshotId(maxSnapshotId);
    request->Invoke();

    LOG_DEBUG("Sync sent to master %d (StateId: %s, Epoch: %s, MaxSnapshotId: %d)",
        masterId,
        ~stateId.ToString(),
        ~StringFromGuid(epoch),
        maxSnapshotId);
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, Sync)
{
    UNUSED(response);

    TMasterStateId stateId(
        request->GetSegmentId(),
        request->GetChangeCount());
    TMasterEpoch epoch = GuidFromProtoGuid(request->GetEpoch());
    i32 maxSnapshotId = request->GetMaxSnapshotId();

    context->SetRequestInfo("StateId: %s, Epoch: %s, MaxSnapshotId: %d",
        ~stateId.ToString(),
        ~StringFromGuid(epoch),
        maxSnapshotId);

    context->Reply();

    if (~FollowerRecovery == NULL) {
        LOG_WARNING("Unexpected sync received");
        return;
    }

    FollowerRecovery->Sync(
        stateId,
        epoch,
        maxSnapshotId);
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, GetSnapshotInfo)
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

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ReadSnapshot)
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

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, GetChangeLogInfo)
{
    i32 changeLogId = request->GetSegmentId();

    context->SetRequestInfo("ChangeLogId: %d",
        changeLogId);

    try {
        // TODO: extract method
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(changeLogId);
        if (~cachedChangeLog == NULL) {
            ythrow TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid changelog id %d", changeLogId);
        }

        TChangeLog::TPtr changeLog = cachedChangeLog->GetChangeLog();

        i32 recordCount = changeLog->GetRecordCount();
        
        response->SetRecordCount(recordCount);
        
        context->SetResponseInfo("RecordCount: %d",
            recordCount);
        
        context->Reply();
    } catch (const yexception& ex) {
        // TODO: fail?
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("GetChangeLogInfo: IO error in changelog %d: %s",
            changeLogId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ReadChangeLog)
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
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            ythrow TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid changelog id %d", segmentId);
        }

        TAsyncChangeLog& changeLog = cachedChangeLog->GetWriter();

        yvector<TSharedRef> recordData;
        changeLog.Read(startRecordId, recordCount, &recordData);

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

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ApplyChange)
{
    TMasterEpoch epoch = GuidFromProtoGuid(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 changeCount = request->GetChangeCount();
    TMasterStateId stateId(segmentId, changeCount);

    context->SetRequestInfo("Epoch: %s, StateId: %s",
        ~StringFromGuid(epoch),
        ~stateId.ToString());

    if (State != EState::Following && State != EState::FollowerRecovery) {
        ythrow TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %s", ~State.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(TProxy::EErrorCode::InvalidEpoch) <<
            Sprintf("invalid epoch (expected: %s, received: %s)",
                ~StringFromGuid(Epoch),
                ~StringFromGuid(epoch));
    }
    
    YASSERT(request->Attachments().size() == 1);
    const TSharedRef& changeData = request->Attachments().at(0);

    switch (State) {
        case EState::Following:
            LOG_DEBUG("ApplyChange: applying change");

            ChangeCommitter->CommitLocal(stateId, changeData)->Subscribe(FromMethod(
                &TMasterStateManager::OnLocalCommit,
                TPtr(this),
                context,
                MyEpoch)->Via(ServiceInvoker));
            break;

        case EState::FollowerRecovery: {
            LOG_DEBUG("ApplyChange: keeping postponed change");
            
            YASSERT(~FollowerRecovery != NULL);
            TRecovery::EResult result = FollowerRecovery->PostponeChange(stateId, changeData);
            if (result != TRecovery::EResult::OK) {
                Restart();
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

void TMasterStateManager::OnLocalCommit(
    TChangeCommitter::EResult result,
    TCtxApplyChange::TPtr context,
    const TMasterEpoch& myEpoch)
{
    TReqApplyChange& request = context->Request();
    TRspApplyChange& response = context->Response();

    TMasterStateId stateId(request.GetSegmentId(), request.GetChangeCount());

    switch (result) {
        case TChangeCommitter::EResult::Committed:
            response.SetCommitted(true);
            context->Reply();
            break;

        case TChangeCommitter::EResult::InvalidStateId:
            context->Reply(TProxy::EErrorCode::InvalidStateId);

            if (myEpoch == MyEpoch) {
                Restart();
                LOG_WARNING("ApplyChange: change %s is unexpected, restarting",
                    ~stateId.ToString());
            } else {
                LOG_WARNING("ApplyChange: change %s is unexpected",
                    ~stateId.ToString());
            }

            break;

        default:
            YASSERT(false);
    }
}

//TODO: rename CreateSnapshot to AdvanceSegment
//TODO: reply whether snapshot was created
RPC_SERVICE_METHOD_IMPL(TMasterStateManager, CreateSnapshot)
{
    UNUSED(response);

    TMasterEpoch epoch = GuidFromProtoGuid(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 changeCount = request->GetChangeCount();
    TMasterStateId stateId(segmentId, changeCount);

    context->SetRequestInfo("Epoch: %s, StateId: %s",
        ~StringFromGuid(epoch),
        ~stateId.ToString());

    if (State != EState::Following && State != EState::FollowerRecovery) {
        ythrow TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %s", ~State.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(TProxy::EErrorCode::InvalidEpoch) <<
            Sprintf("invalid epoch: expected %s, received %s",
                ~StringFromGuid(Epoch),
                ~StringFromGuid(epoch));
    }

    switch (State) {
        case EState::Following:
            LOG_DEBUG("CreateSnapshot: creating snapshot");

            SnapshotCreator->CreateLocal(stateId)->Subscribe(FromMethod(
                &TMasterStateManager::OnCreateLocalSnapshot,
                TPtr(this),
                context));
            break;
            
        case EState::FollowerRecovery: {
            LOG_DEBUG("CreateSnapshot: keeping postponed segment advance");

            YASSERT(~FollowerRecovery != NULL);
            TRecovery::EResult result = FollowerRecovery->PostponeSegmentAdvance(stateId);
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

void TMasterStateManager::OnCreateLocalSnapshot(
    TSnapshotCreator::TLocalResult result,
    TCtxCreateSnapshot::TPtr context)
{
    switch (result.ResultCode) {
        case TSnapshotCreator::EResultCode::OK:
            context->Response().SetChecksum(result.Checksum);
            context->Reply();
            break;
        case TSnapshotCreator::EResultCode::InvalidStateId:
            context->Reply(TProxy::EErrorCode::InvalidStateId);
            break;
        default:
            YASSERT(false);
            break;
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, PingLeader)
{
    UNUSED(response);

    TMasterId followerId = request->GetFollowerId();
    TMasterEpoch followerEpoch = GuidFromProtoGuid(request->GetEpoch());
    EState followerState = static_cast<EState>(request->GetState());

    context->SetRequestInfo("Id: %d, Epoch: %s, State: %s",
        followerId,
        ~StringFromGuid(followerEpoch),
        ~followerState.ToString());

    if (State != EState::Leading) {
        LOG_DEBUG("PingLeader: invalid state (State: %s)",
            ~State.ToString());
    } else if (followerEpoch != Epoch ) {
        LOG_DEBUG("PingLeader: invalid epoch (Epoch: %s)",
            ~StringFromGuid(Epoch));
    } else {
        FollowerStateTracker->ProcessPing(followerId, followerState);
    }

    // Reply with OK in any case.
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////
// IElectionCallbacks members

void TMasterStateManager::StartLeading(TMasterEpoch epoch)
{
    LOG_INFO("Starting leader recovery");

    State = EState::LeaderRecovery;
    LeaderId = CellManager->GetSelfId();    
    StartEpoch(epoch);
    
    YASSERT(~LeaderRecovery == NULL);
    LeaderRecovery = new TLeaderRecovery(
        CellManager,
        ~MasterState,
        ChangeLogCache,
        ~SnapshotStore,
        Epoch,
        LeaderId,
        ServiceInvoker,
        EpochInvoker,
        WorkQueue);

    LeaderRecovery->Run()->Subscribe(
        FromMethod(&TMasterStateManager::OnLeaderRecovery, TPtr(this))
        ->Via(EpochInvoker)
        ->Via(ServiceInvoker));
}

void TMasterStateManager::OnLeaderRecovery(TRecovery::EResult result)
{
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    if (result != TRecovery::EResult::OK) {
        LOG_WARNING("Leader recovery failed, restarting");
        Restart();
        return;
    }

    State = EState::Leading;
    
    FollowerStateTracker = new TFollowerStateTracker(
        TFollowerStateTracker::TConfig(),
        CellManager,
        EpochInvoker,
        ServiceInvoker);

    ChangeCommitter->SetOnApplyChange(FromMethod(
        &TMasterStateManager::OnApplyChange,
        TPtr(this)));

    LOG_INFO("Leader recovery complete");
}

void TMasterStateManager::OnApplyChange()
{
    YASSERT(State == EState::Leading);
    TMasterStateId stateId = MasterState->GetStateId();
    if (stateId.ChangeCount >= Config.MaxChangeCount) {
        SnapshotCreator->CreateDistributed(stateId);
    }
}

void TMasterStateManager::StopLeading()
{
    LOG_INFO("Stopped leading");
    
    State = EState::Elections;
    
    ChangeCommitter->SetOnApplyChange(NULL);

    StopEpoch();

    LeaderRecovery.Drop();

    FollowerStateTracker.Drop();
}

void TMasterStateManager::StartFollowing(TMasterId leaderId, TMasterEpoch epoch)
{
    LOG_INFO("Starting follower state recovery");
    
    State = EState::FollowerRecovery;
    LeaderId = leaderId;
    StartEpoch(epoch);

    YASSERT(~FollowerRecovery == NULL);
    FollowerRecovery = new TFollowerRecovery(
        CellManager,
        ~MasterState,
        ChangeLogCache,
        ~SnapshotStore,
        Epoch,
        LeaderId,
        ServiceInvoker,
        EpochInvoker,
        WorkQueue);

    FollowerRecovery->Run()->Subscribe(
        FromMethod(&TMasterStateManager::OnFollowerRecovery, TPtr(this))
        ->Via(EpochInvoker)
        ->Via(ServiceInvoker));
}

void TMasterStateManager::OnFollowerRecovery(TRecovery::EResult result)
{
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    if (result != TRecovery::EResult::OK) {
        LOG_INFO("Follower recovery failed, restarting");
        Restart();
        return;
    }

    State = EState::Following;

    LeaderPinger = new TLeaderPinger(
        TLeaderPinger::TConfig(),
        this,
        CellManager,
        LeaderId,
        Epoch,
        EpochInvoker,
        ServiceInvoker);
}

void TMasterStateManager::StopFollowing()
{
    LOG_INFO("Stopped following");
    
    State = EState::Elections;
    
    StopEpoch();

    FollowerRecovery.Drop();

    if (~LeaderPinger != NULL) {
        LeaderPinger->Terminate();
        LeaderPinger.Drop();
    }
}

TMasterPriority TMasterStateManager::GetPriority()
{
    TMasterStateId stateId = MasterState->GetAvailableStateId();
    return ((TMasterPriority) stateId.SegmentId << 32) | stateId.ChangeCount;
}

Stroka TMasterStateManager::FormatPriority(TMasterPriority priority)
{
    i32 segmentId = (priority >> 32);
    i32 changeCount = priority & ((1ll << 32) - 1);
    return Sprintf("(%d, %d)", segmentId, changeCount);
}

TMasterStateManager::EState TMasterStateManager::GetState() const
{
    return State;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
