#include "meta_state_manager.h"
#include "follower_tracker.h"
#include "change_committer.h"
#include "leader_pinger.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"
#include "../misc/fs.h"
#include "../misc/guid.h"

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TMetaStateManager::TMetaStateManager(
    const TConfig& config,
    IInvoker::TPtr controlInvoker,
    IMetaState::TPtr metaState,
    NRpc::TServer::TPtr server)
    : TServiceBase(
        controlInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , State(EPeerState::Stopped)
    , Config(config)
    , LeaderId(NElection::InvalidPeerId)
    , ControlInvoker(controlInvoker)
{
    YVERIFY(~controlInvoker != NULL);
    YVERIFY(~metaState != NULL);
    YVERIFY(~server != NULL);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(metaState->GetInvoker(), StateThread);

    StateInvoker = metaState->GetInvoker();

    RegisterMethods();

    NFS::CleanTempFiles(config.LogLocation);
    ChangeLogCache = New<TChangeLogCache>(Config.LogLocation);

    NFS::CleanTempFiles(config.SnapshotLocation);
    SnapshotStore = New<TSnapshotStore>(Config.SnapshotLocation);

    MetaState = New<TDecoratedMetaState>(
        metaState,
        SnapshotStore,
        ChangeLogCache);

    CellManager = New<TCellManager>(Config.Cell);

    // TODO: fill config
    ElectionManager = New<NElection::TElectionManager>(
        NElection::TElectionManager::TConfig(),
        CellManager,
        controlInvoker,
        this,
        server);

    OnApplyChangeAction = FromMethod(
        &TMetaStateManager::OnApplyChange,
        TPtr(this));

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
    RPC_REGISTER_METHOD(TMetaStateManager, AdvanceSegment);
    RPC_REGISTER_METHOD(TMetaStateManager, PingLeader);
}

void TMetaStateManager::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // To prevent multiple restarts.
    ServiceEpochInvoker->Cancel();

    ElectionManager->Restart();
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeSync(const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return CommitChangeSync(
        FromMethod(
            &IMetaState::ApplyChange,
            MetaState->GetState(),
            changeData),
        changeData);
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeSync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (State != EPeerState::Leading) {
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

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeAsync(const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CommitChangeAsync(
        FromMethod(
            &IMetaState::ApplyChange,
            MetaState->GetState(),
            changeData),
        changeData);
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeAsync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        FromMethod(
            &TMetaStateManager::CommitChangeSync,
            TPtr(this),
            changeAction,
            changeData)
        ->AsyncVia(~StateInvoker)
        ->Do();
}

TMetaStateManager::ECommitResult TMetaStateManager::OnChangeCommit(
    TChangeCommitter::EResult result)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(State == EPeerState::Stopped);

    State = EPeerState::Elections;

    StateInvoker->Invoke(FromMethod(
        &TDecoratedMetaState::Clear,
        MetaState));

    ElectionManager->Start();
}

void TMetaStateManager::StartEpoch(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(~ServiceEpochInvoker == NULL);

    ServiceEpochInvoker = New<TCancelableInvoker>(ControlInvoker);
    Epoch = epoch;

    SnapshotCreator = New<TSnapshotCreator>(
        TSnapshotCreator::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        ControlInvoker);
}

void TMetaStateManager::StopEpoch()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LeaderId = NElection::InvalidPeerId;
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
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    auto peerId = request->GetPeerId();

    context->SetRequestInfo("PeerId: %d", peerId);
   
    if (State != EPeerState::Leading && State != EPeerState::LeaderRecovery) {
        ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidState) <<
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
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = MetaState->GetNextVersion();
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();

    auto proxy = CellManager->GetMasterProxy<TProxy>(peerId);
    auto request = proxy->Sync();
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    TMetaVersion version(
        request->GetSegmentId(),
        request->GetRecordCount());
    auto epoch = TEpoch::FromProto(request->GetEpoch());
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    i32 snapshotId = request->GetSnapshotId();

    context->SetRequestInfo("SnapshotId: %d",
        snapshotId);

    try {
        auto reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
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
    } catch (...) {
        // TODO: fail?
        ythrow NRpc::TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error while getting snapshot info (SnapshotId: %d, What: %s)",
                snapshotId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ReadSnapshot)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

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
        auto reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
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
    } catch (...) {
        // TODO: fail?
        ythrow NRpc::TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error while reading snapshot (SnapshotId: %d, What: %s)",
                snapshotId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, GetChangeLogInfo)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    i32 changeLogId = request->GetChangeLogId();

    context->SetRequestInfo("ChangeLogId: %d",
        changeLogId);

    try {
        auto changeLog = ChangeLogCache->Get(changeLogId);
        if (~changeLog == NULL) {
            ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid changelog id %d", changeLogId);
        }

        i32 recordCount = changeLog->GetRecordCount();
        
        response->SetRecordCount(recordCount);
        
        context->SetResponseInfo("RecordCount: %d", recordCount);
        context->Reply();
    } catch (...) {
        // TODO: fail?
        ythrow NRpc::TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error while getting changelog info (ChangeLogId: %d, What: %s)",
                changeLogId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ReadChangeLog)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    i32 changeLogId = request->GetChangeLogId();
    i32 startRecordId = request->GetStartRecordId();
    i32 recordCount = request->GetRecordCount();
    
    context->SetRequestInfo("ChangeLogId: %d, StartRecordId: %d, RecordCount: %d",
        changeLogId,
        startRecordId,
        recordCount);

    YASSERT(startRecordId >= 0);
    YASSERT(recordCount >= 0);
    
    try {
        auto changeLog = ChangeLogCache->Get(changeLogId);
        if (~changeLog == NULL) {
            ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidSegmentId) <<
                Sprintf("invalid changelog id %d", changeLogId);
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
    } catch (...) {
        // TODO: fail?
        ythrow NRpc::TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error while reading changelog (ChangeLogId: %d, What: %s)",
                changeLogId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, ApplyChanges)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TEpoch epoch = TEpoch::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);

    context->SetRequestInfo("Epoch: %s, Version: %s",
        ~epoch.ToString(),
        ~version.ToString());

    if (State != EPeerState::Following && State != EPeerState::FollowerRecovery) {
        ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %s", ~State.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidEpoch) <<
            Sprintf("invalid epoch (expected: %s, received: %s)",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }
    
    int changeCount = request->Attachments().size();
    switch (State) {
        case EPeerState::Following: {
            LOG_DEBUG("ApplyChange: applying %d changes", changeCount);

            YASSERT(~ChangeCommitter != NULL);
            for (int changeIndex = 0; changeIndex < changeCount; ++changeIndex) {
                YASSERT(State == EPeerState::Following);
                TMetaVersion commitVersion(segmentId, recordCount + changeIndex);
                const TSharedRef& changeData = request->Attachments().at(changeIndex);
                auto asyncResult = ChangeCommitter->CommitFollower(commitVersion, changeData);
                // Subscribe to the last change
                if (changeIndex == changeCount - 1) {
                    asyncResult->Subscribe(FromMethod(
                            &TMetaStateManager::OnLocalCommit,
                            TPtr(this),
                            context)
                        ->Via(ControlInvoker));
                }
            }
            break;
        }

        case EPeerState::FollowerRecovery: {
            LOG_DEBUG("ApplyChange: keeping %d postponed changes", changeCount);

            YASSERT(~FollowerRecovery != NULL);
            for (int changeIndex = 0; changeIndex < changeCount; ++changeIndex) {
                YASSERT(State == EPeerState::FollowerRecovery);
                TMetaVersion commitVersion(segmentId, recordCount + changeIndex);
                const TSharedRef& changeData = request->Attachments().at(changeIndex);
                auto result = FollowerRecovery->PostponeChange(commitVersion, changeData);
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& request = context->Request();
    auto& response = context->Response();

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

// TODO: reply whether snapshot was created
RPC_SERVICE_METHOD_IMPL(TMetaStateManager, AdvanceSegment)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    TEpoch epoch = TEpoch::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);

    context->SetRequestInfo("Epoch: %s, Version: %s",
        ~epoch.ToString(),
        ~version.ToString());

    if (State != EPeerState::Following && State != EPeerState::FollowerRecovery) {
        ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidState) <<
            Sprintf("invalid state %s", ~State.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow NRpc::TServiceException(TProxy::EErrorCode::InvalidEpoch) <<
            Sprintf("invalid epoch: expected %s, received %s",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }

    switch (State) {
        case EPeerState::Following:
            LOG_DEBUG("CreateSnapshot: creating snapshot");

            SnapshotCreator->CreateLocal(version)->Subscribe(FromMethod(
                &TMetaStateManager::OnCreateLocalSnapshot,
                TPtr(this),
                context));
            break;
            
        case EPeerState::FollowerRecovery: {
            LOG_DEBUG("CreateSnapshot: keeping postponed segment advance");

            YASSERT(~FollowerRecovery != NULL);
            auto result = FollowerRecovery->PostponeSegmentAdvance(version);
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
    TCtxAdvanceSegment::TPtr context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto& response = context->Response();

    switch (result.ResultCode) {
        case TSnapshotCreator::EResultCode::OK:
            response.SetChecksum(result.Checksum);
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto followerId = request->GetFollowerId();
    auto followerEpoch = TEpoch::FromProto(request->GetEpoch());
    auto followerState = static_cast<EPeerState>(request->GetState());

    context->SetRequestInfo("Id: %d, Epoch: %s, State: %s",
        followerId,
        ~followerEpoch.ToString(),
        ~followerState.ToString());

    if (State != EPeerState::Leading) {
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

void TMetaStateManager::OnStartLeading(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Starting leader recovery");

    State = EPeerState::LeaderRecovery;
    LeaderId = CellManager->GetSelfId();    
    StartEpoch(epoch);
    
    YASSERT(~LeaderRecovery == NULL);
    LeaderRecovery = new TLeaderRecovery(
        Config,
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        LeaderId,
        ControlInvoker);

    // TODO: get rid of this sync call
    auto version =
        FromMethod(&TMetaStateManager::GetNextVersion, TPtr(this))
        ->AsyncVia(~StateInvoker)
        ->Do()
        ->Get();

    LeaderRecovery->Run(version)->Subscribe(
        FromMethod(&TMetaStateManager::OnLeaderRecoveryComplete, TPtr(this))
        ->Via(~ServiceEpochInvoker));
}

void TMetaStateManager::OnLeaderRecoveryComplete(TRecovery::EResult result)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
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
        ControlInvoker);

    ChangeCommitter = New<TChangeCommitter>(
        TChangeCommitter::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        FollowerTracker,
        ControlInvoker,
        Epoch);

    ChangeCommitter->OnApplyChange().Subscribe(OnApplyChangeAction);

    State = EPeerState::Leading;

    StateInvoker->Invoke(FromMethod(
        &TDecoratedMetaState::OnStartLeading,
        MetaState));

    LOG_INFO("Leader recovery complete");
}

void TMetaStateManager::OnApplyChange()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(State == EPeerState::Leading);

    auto version = MetaState->GetVersion();
    if (version.RecordCount >= Config.MaxRecordCount) {
        ChangeCommitter->Flush();
        SnapshotCreator->CreateDistributed(version);
    }
}

void TMetaStateManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Stopped leading");
    
    StateInvoker->Invoke(FromMethod(
        &TDecoratedMetaState::OnStopLeading,
        MetaState));

    State = EPeerState::Elections;

    ChangeCommitter->OnApplyChange().Unsubscribe(OnApplyChangeAction);

    StopEpoch();

    if (~LeaderRecovery != NULL) {
        LeaderRecovery->Stop();
        LeaderRecovery.Drop();
    }

    if (~ChangeCommitter != NULL) {
        ChangeCommitter->Stop();
        ChangeCommitter.Drop();
    }

    if (~FollowerTracker != NULL) {
        FollowerTracker->Stop();
        FollowerTracker.Drop();
    }
}

void TMetaStateManager::OnStartFollowing(TPeerId leaderId, const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Starting follower state recovery");
    
    State = EPeerState::FollowerRecovery;
    LeaderId = leaderId;
    StartEpoch(epoch);

    YASSERT(~FollowerRecovery == NULL);
    FollowerRecovery = New<TFollowerRecovery>(
        Config,
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        LeaderId,
        ControlInvoker);

    FollowerRecovery->Run()->Subscribe(
        FromMethod(&TMetaStateManager::OnFollowerRecoveryComplete, TPtr(this))
        ->Via(~ServiceEpochInvoker));
}

void TMetaStateManager::OnFollowerRecoveryComplete(TRecovery::EResult result)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    if (result != TRecovery::EResult::OK) {
        LOG_INFO("Follower recovery failed, restarting");
        Restart();
        return;
    }

    ChangeCommitter = New<TChangeCommitter>(
        TChangeCommitter::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        TFollowerTracker::TPtr(NULL),
        ControlInvoker,
        Epoch);

    LeaderPinger = New<TLeaderPinger>(
        TLeaderPinger::TConfig(),
        this,
        CellManager,
        LeaderId,
        Epoch,
        ControlInvoker);

    State = EPeerState::Following;

    StateInvoker->Invoke(FromMethod(
        &TDecoratedMetaState::OnStartFollowing,
        MetaState));

    LOG_INFO("Follower recovery complete");
}

void TMetaStateManager::OnStopFollowing()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Stopped following");
    
    StateInvoker->Invoke(FromMethod(
        &TDecoratedMetaState::OnStopFollowing,
        MetaState));

    State = EPeerState::Elections;
    
    StopEpoch();

    if (~FollowerRecovery != NULL) {
        FollowerRecovery->Stop();
        FollowerRecovery.Drop();
    }

    if (~ChangeCommitter != NULL) {
        ChangeCommitter->Stop();
        ChangeCommitter.Drop();
    }

    if (~LeaderPinger != NULL) {
        LeaderPinger->Stop();
        LeaderPinger.Drop();
    }
}

TPeerPriority TMetaStateManager::GetPriority()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // TODO: get rid of this sync call
    auto version =
        FromMethod(&TMetaStateManager::GetNextVersion, TPtr(this))
        ->AsyncVia(~StateInvoker)
        ->Do()
        ->Get();

    return ((TPeerPriority) version.SegmentId << 32) | version.RecordCount;
}

TMetaVersion TMetaStateManager::GetNextVersion()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    return MetaState->GetNextVersion();
}

Stroka TMetaStateManager::FormatPriority(TPeerPriority priority)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i32 segmentId = (priority >> 32);
    i32 recordCount = priority & ((1ll << 32) - 1);
    return Sprintf("(%d, %d)", segmentId, recordCount);
}

EPeerState TMetaStateManager::GetState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return State;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
