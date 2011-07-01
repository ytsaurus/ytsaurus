#include "master_state_manager.h"
#include "follower_state_tracker.h"
#include "leader_pinger.h"
#include "change_committer.h"

#include "../actions/action_util.h"
#include "../misc/string.h"
#include "../misc/serialize.h"
#include "../misc/fs.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MasterState");

////////////////////////////////////////////////////////////////////////////////

TMasterStateManager::TMasterStateManager(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr serviceInvoker,
    IMasterState::TPtr masterState,
    NRpc::TServer* server)
    : TServiceBase(TProxy::GetServiceName(), Logger.GetCategory())
    , State(S_Stopped)
    , Config(config)
    , LeaderId(InvalidMasterId)
    , CellManager(cellManager)
    , ServiceInvoker(serviceInvoker)
    , WorkQueue(new TActionQueue())
{
    RegisterMethods();

    CleanTempFiles(config.LogLocation);
    ChangeLogCache = new TChangeLogCache(Config.LogLocation);

    CleanTempFiles(config.SnapshotLocation);
    SnapshotStore.Reset(new TSnapshotStore(Config.SnapshotLocation));

    MasterState.Reset(new TDecoratedMasterState(masterState, ~SnapshotStore, ChangeLogCache)); // TODO: use initializer

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
    RPC_REGISTER_METHOD(TMasterStateManager, GetCurrentState);
    RPC_REGISTER_METHOD(TMasterStateManager, GetSnapshotInfo);
    RPC_REGISTER_METHOD(TMasterStateManager, ReadSnapshot);
    RPC_REGISTER_METHOD(TMasterStateManager, GetChangeLogInfo);
    RPC_REGISTER_METHOD(TMasterStateManager, ReadChangeLog);
    RPC_REGISTER_METHOD(TMasterStateManager, ApplyChange);
    RPC_REGISTER_METHOD(TMasterStateManager, CreateSnapshot);
    RPC_REGISTER_METHOD(TMasterStateManager, PingLeader);
}

void TMasterStateManager::Restart()
{
    ElectionManager->Restart();
}

namespace {

// TODO: turn into member
TMasterStateManager::ECommitResult OnChangeCommit(TChangeCommitter::EResult result)
{
    switch (result) {
        case TChangeCommitter::Committed:
            return TMasterStateManager::CR_Committed;

        case TChangeCommitter::MaybeCommitted:
            // TODO: restart!
            return TMasterStateManager::CR_MaybeCommitted;

        default:
            YASSERT(false);
            return TMasterStateManager::CR_NotCommitted;
    }
}

}

TMasterStateManager::TCommitResult::TPtr TMasterStateManager::CommitChange(
    TSharedRef change)
{
    if (State != S_Leading) {
        return new TCommitResult(CR_InvalidState);
    }

    if (!FollowerStateTracker->HasActiveQuorum()) {
        return new TCommitResult(CR_NotCommitted);
    }

    return
        ChangeCommitter
        ->CommitDistributed(change)
        ->Apply(FromMethod(OnChangeCommit));
}

void TMasterStateManager::Start()
{
    YASSERT(State == S_Stopped);

    State = S_Elections;

    MasterState->Clear();

    LOG_INFO("Master state is reset to %s",
        ~MasterState->GetStateId().ToString());

    ElectionManager->Start();
}

void TMasterStateManager::StartEpoch(TMasterEpoch epoch)
{
    YASSERT(~EpochInvoker == NULL);
    EpochInvoker = ElectionManager->GetEpochInvoker();
    Epoch = epoch;

    TChangeLogDownloader::TConfig changeLogDownloaderConfig;
    // TODO: fill config

    TSnapshotDownloader::TConfig snapshotDownloaderConfig;
    // TODO: fill config

    Recovery = new TMasterRecovery(
        snapshotDownloaderConfig,
        changeLogDownloaderConfig,
        CellManager,
        ~MasterState,
        ChangeLogCache,
        ~SnapshotStore,
        Epoch,
        LeaderId,
        ServiceInvoker,
        EpochInvoker,
        WorkQueue);

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
    Recovery.Drop();
    ChangeCommitter.Drop();
    SnapshotCreator.Drop();
}

//////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, GetCurrentState)
{
    UNUSED(request);
    UNUSED(response);
   
    if (State == S_Leading) {
        WorkQueue->Invoke(FromMethod(
            &TMasterStateManager::DoGetCurrentState,
            TPtr(this),
            context,
            Epoch));
    } else {
        context->Reply(TProxy::EErrorCode::InvalidState);

        LOG_DEBUG("GetSnapshotInfo: Invalid state %d",
                    (int) State);
    }
}

void TMasterStateManager::DoGetCurrentState(
    TCtxGetCurrentState::TPtr context,
    TMasterEpoch epoch)
{
    TMasterStateId stateId = MasterState->GetAvailableStateId();
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();

    context->Response().SetSegmentId(stateId.SegmentId);
    context->Response().SetChangeCount(stateId.ChangeCount);
    context->Response().SetEpoch(ProtoGuidFromGuid(epoch));
    context->Response().SetMaxSnapshotId(maxSnapshotId);
    context->Reply();

    LOG_DEBUG("GetCurrentState: (StateId: %s, Epoch: %s, MaxSnapshotId: %d)",
        ~stateId.ToString(),
        ~StringFromGuid(epoch),
        maxSnapshotId);
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, GetSnapshotInfo)
{
    i32 snapshotId = request->GetSnapshotId();
    try {
        TAutoPtr<TSnapshotReader> reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            LOG_DEBUG("GetSnapshotInfo: Invalid id %d", snapshotId);
            context->Reply(TProxy::EErrorCode::InvalidSegmentId);
            return;
        }

        reader->Open();
        i64 length = reader->GetLength();

        response->SetLength(length);
        response->SetPrevRecordCount(reader->GetPrevRecordCount());
        response->SetChecksum(reader->GetChecksum());
        context->Reply();

        LOG_DEBUG("GetSnapshotInfo: (Id: %d, Length: %" PRId64 ")",
            snapshotId,
            length);
    } catch (const yexception& ex) {
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_DEBUG("GetSnapshotInfo: IO error in snapshot %d: %s",
            snapshotId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ReadSnapshot)
{
    i32 snapshotId = request->GetSnapshotId();
    try {
        TAutoPtr<TSnapshotReader> reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            context->Reply(TProxy::EErrorCode::InvalidSegmentId);

            LOG_DEBUG("ReadSnapshot: Invalid id %d",
                snapshotId);
            return;
        }

        i64 offset = request->GetOffset();
        YASSERT(offset >= 0);

        i32 length = request->GetLength();
        YASSERT(length >= 0);

        reader->Open(offset);
        TBlob data(length);
        i32 bytesRead = reader->GetStream().Read(data.begin(), length);
        data.erase(data.begin() + bytesRead, data.end());

        response->Attachments().push_back(TSharedRef(data));

        context->Reply();

        LOG_DEBUG("ReadSnapshot: (Id: %d, Offset: %" PRId64 ", BytesRead: %d)",
            snapshotId,
            offset,
            bytesRead);
    } catch (const yexception& ex) {
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("ReadSnapshot: IO error in snapshot %d: %s",
            snapshotId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, GetChangeLogInfo)
{
    i32 segmentId = request->GetSegmentId();
    try {
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            LOG_DEBUG("GetChangeLogInfo: Invalid id %d",
                        segmentId);
            context->Reply(TProxy::EErrorCode::InvalidSegmentId);
            return;
        }

        TChangeLog::TPtr changeLog = cachedChangeLog->GetChangeLog();
        i32 recordCount = changeLog->GetRecordCount();
        response->SetRecordCount(recordCount);
        context->Reply();

        LOG_DEBUG("GetChangeLogInfo: Info reported (Id: %d, RecordCount: %d)",
                    segmentId, recordCount);
    } catch (const yexception& ex) {
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("GetChangeLogInfo: IO error in changelog %d: %s",
            segmentId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ReadChangeLog)
{
    i32 segmentId = request->GetSegmentId();
    try {
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            LOG_WARNING("ReadChangeLog: Invalid snapshot %d", segmentId);
            context->Reply(TProxy::EErrorCode::InvalidSegmentId);
            return;
        }

        TChangeLog::TPtr changeLog = cachedChangeLog->GetChangeLog();
        i32 startRecordCount = request->GetStartRecordId();
        YASSERT(startRecordCount >= 0);

        i32 recordCount = request->GetRecordCount();
        YASSERT(recordCount >= 0);

        TBlob dataHolder;
        yvector<TRef> recordData;
        changeLog->Read(startRecordCount, recordCount, &dataHolder, &recordData);

        response->SetRecordsRead(recordData.ysize());
        for (i32 i = 0; i < recordData.ysize(); ++i) {
            // TODO: drop this once Read returns TSharedRefs
            TBlob data = recordData[i].ToBlob();
            response->Attachments().push_back(TSharedRef(data));
        }
        
        context->Reply();

        LOG_DEBUG("ReadChangeLog: (Id: %d, RecordCount: %d)",
                    segmentId, recordData.ysize());
    } catch (const yexception& ex) {
        context->Reply(TProxy::EErrorCode::IOError);

        LOG_ERROR("ReadChangeLog: IO error in changelog %d: %s",
            segmentId, ex.what());
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, ApplyChange)
{
    if (State != S_Following && State != S_FollowerRecovery) {
        LOG_WARNING("ApplyChange: invalid state %d",
            State);
        context->Reply(TProxy::EErrorCode::InvalidState);
        return;
    }

    TMasterEpoch epoch = GuidFromProtoGuid(request->GetEpoch());
    if (epoch != Epoch) {
        LOG_WARNING("ApplyChange: invalid epoch (expected: %s, received: %s)",
            ~StringFromGuid(Epoch), ~StringFromGuid(epoch));
        context->Reply(TProxy::EErrorCode::InvalidEpoch);
        Restart();
        return;
    }

    i32 segmentId = request->GetSegmentId();
    i32 changeCount = request->GetChangeCount();
    TMasterStateId stateId(segmentId, changeCount);
    
    const TSharedRef& change = request->Attachments().at(0);

    switch (State) {
        case S_Following: {
            LOG_DEBUG("ApplyChange: applying change %s",
                ~stateId.ToString());

            ChangeCommitter->CommitLocal(stateId, change)->Subscribe(FromMethod(
                &TMasterStateManager::OnLocalCommit,
                TPtr(this),
                context));
            break;
        }

        case S_FollowerRecovery:
            LOG_DEBUG("ApplyChange: keeping postponed change %s",
                ~stateId.ToString());
            
            // TODO: code here

            response->SetCommitted(false);
            context->Reply();
            break;

        default:
            YASSERT(false);
    }
}

void TMasterStateManager::OnLocalCommit(
    TChangeCommitter::EResult result,
    TCtxApplyChange::TPtr context)
{
    TReqApplyChange& request = context->Request();
    TRspApplyChange& response = context->Response();

    TMasterStateId stateId(request.GetSegmentId(), request.GetChangeCount());

    switch (result) {
        case TChangeCommitter::Committed:
            response.SetCommitted(true);
            context->Reply();

            LOG_DEBUG("ApplyChange: Change %s is committed",
                ~stateId.ToString());
            break;

        case TChangeCommitter::InvalidStateId:
            context->Reply(TProxy::EErrorCode::InvalidStateId);
            Restart();

            LOG_WARNING("ApplyChange: Change %s is unexpected, restarting",
                ~stateId.ToString());
            break;

        default:
            YASSERT(false);
    }
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, CreateSnapshot)
{
    UNUSED(response);

    if (State != S_Following) {
        context->Reply(TProxy::EErrorCode::InvalidState);

        LOG_WARNING("CreateSnapshot: Invalid state %d",
            (int) State);
        return;
    }

    TMasterEpoch epoch = GuidFromProtoGuid(request->GetEpoch());
    if (epoch != Epoch) {
        context->Reply(TProxy::EErrorCode::InvalidEpoch);

        LOG_WARNING("CreateSnapshot: Invalid epoch: expected %s, received %s",
            ~StringFromGuid(Epoch), ~StringFromGuid(epoch));
        return;
    }

    i32 snapshotId = request->GetSnapshotId();
    i32 changeCount = request->GetChangeCount();
    TMasterStateId stateId(snapshotId, changeCount);

    SnapshotCreator->CreateLocal(stateId)->Subscribe(FromMethod(
        &TMasterStateManager::OnCreateLocalSnapshot,
        TPtr(this),
        context));
}

RPC_SERVICE_METHOD_IMPL(TMasterStateManager, PingLeader)
{
    UNUSED(response);
    
    TMasterId followerId = request->GetFollowerId();
    TMasterEpoch followerEpoch = GuidFromProtoGuid(request->GetEpoch());
    EState followerState = static_cast<EState>(request->GetState());

    if (State != S_Leading) {
        LOG_DEBUG("PingLeader: Invalid state (MyState: %d, FollowerId: %d, FollowerState: %d, FollowerEpoch: %s)",
            (int) State, followerId, (int) followerState, ~StringFromGuid(followerEpoch));
    } else if (followerEpoch != Epoch ) {
        LOG_DEBUG("PingLeader: Invalid epoch (FollowerId: %d, FollowerState: %d, FollowerEpoch: %s)",
            followerId, (int) followerState, ~StringFromGuid(followerEpoch));
    } else {
        LOG_DEBUG("PingLeader: (FollowerId: %d, State: %d)",
            followerId, static_cast<int>(followerState));
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

    State = S_LeaderRecovery;
    LeaderId = CellManager->GetSelfId();    
    StartEpoch(epoch);
    
    TMasterStateId stateId = MasterState->GetAvailableStateId();
    Recovery->RecoverLeader(stateId)->Subscribe(
        FromMethod(&TMasterStateManager::OnLeaderRecovery, TPtr(this))
        ->Via(EpochInvoker)
        ->Via(ServiceInvoker));
}

void TMasterStateManager::OnLeaderRecovery(TMasterRecovery::EResult result)
{
    YASSERT(result == TMasterRecovery::E_OK ||
            result == TMasterRecovery::E_Failed);

    if (result != TMasterRecovery::E_OK) {
        LOG_WARNING("Leader recovery failed, restarting");
        Restart();
        return;
    }

    State = S_Leading;
    
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

void TMasterStateManager::StopLeading()
{
    LOG_INFO("Stopped leading");
    
    State = S_Elections;
    
    ChangeCommitter->SetOnApplyChange(NULL);

    StopEpoch();

    FollowerStateTracker.Drop();
}

void TMasterStateManager::StartFollowing(TMasterId leaderId, TMasterEpoch epoch)
{
    LOG_INFO("Starting follower state recovery");
    
    State = S_FollowerRecovery;
    LeaderId = leaderId;
    StartEpoch(epoch);

    Recovery->RecoverFollower()->Subscribe(
        FromMethod(&TMasterStateManager::OnFollowerRecovery, TPtr(this))
        ->Via(EpochInvoker)
        ->Via(ServiceInvoker));
}

void TMasterStateManager::OnFollowerRecovery(TMasterRecovery::EResult result)
{
    YASSERT(result == TMasterRecovery::E_OK ||
            result == TMasterRecovery::E_Failed);

    if (result != TMasterRecovery::E_OK) {
        LOG_INFO("Follower recovery failed, restarting");
        Restart();
        return;
    }

    State = S_Following;

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
    
    State = S_Elections;
    
    StopEpoch();

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

////////////////////////////////////////////////////////////////////////////////

void TMasterStateManager::OnCreateLocalSnapshot(
    TSnapshotCreator::TLocalResult result,
    TCtxCreateSnapshot::TPtr context)
{
    switch (result.ResultCode) {
        case TSnapshotCreator::OK:
            context->Response().SetChecksum(result.Checksum);
            context->Reply();
            break;
        case TSnapshotCreator::InvalidStateId:
            context->Reply(TProxy::EErrorCode::InvalidStateId);
            break;
        default:
            YASSERT(false);
            break;
    }
}

void TMasterStateManager::OnApplyChange()
{
    YASSERT(State == S_Leading);
    TMasterStateId stateId = MasterState->GetStateId();
    if (stateId.ChangeCount >= Config.MaxChangeCount) {
        SnapshotCreator->CreateDistributed(stateId);
    }
}

TMasterStateManager::EState TMasterStateManager::GetState() const
{
    return State;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
