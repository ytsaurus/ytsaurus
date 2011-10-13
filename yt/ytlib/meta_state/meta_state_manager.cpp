#include "meta_state_manager.h"
#include "follower_tracker.h"
#include "leader_pinger.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"
#include "../misc/fs.h"
#include "../misc/guid.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NMetaState {

using NElection::TElectionManager;

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
    , ControlStatus(EPeerStatus::Stopped)
    , StateStatus(EPeerStatus::Stopped)
    , Config(config)
    , LeaderId(NElection::InvalidPeerId)
    , ControlInvoker(controlInvoker)
{
    YASSERT(~controlInvoker != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~server != NULL);

    RegisterMethods();

    NFS::CleanTempFiles(config.LogLocation);
    ChangeLogCache = New<TChangeLogCache>(Config.LogLocation);

    NFS::CleanTempFiles(config.SnapshotLocation);
    SnapshotStore = New<TSnapshotStore>(Config.SnapshotLocation);

    MetaState = New<TDecoratedMetaState>(
        metaState,
        SnapshotStore,
        ChangeLogCache);

    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(GetStateInvoker(), StateThread);

    CellManager = New<TCellManager>(Config.Cell);

    // TODO: fill config
    ElectionManager = New<TElectionManager>(
        NElection::TElectionManager::TConfig(),
        CellManager,
        controlInvoker,
        this,
        server);

    server->RegisterService(this);
}

TMetaStateManager::~TMetaStateManager()
{ }

void TMetaStateManager::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Sync));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSnapshotInfo));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChangeLogInfo));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ApplyChanges));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceSegment));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingLeader));
}

void TMetaStateManager::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // To prevent multiple restarts.
    EpochControlInvoker->Cancel();

    ElectionManager->Restart();
}

EPeerStatus TMetaStateManager::GetControlStatus() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return ControlStatus;
}

EPeerStatus TMetaStateManager::GetStateStatus() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return StateStatus;
}

IInvoker::TPtr TMetaStateManager::GetStateInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MetaState->GetStateInvoker();
}

IInvoker::TPtr TMetaStateManager::GetEpochStateInvoker()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return ~EpochStateInvoker;
}

IInvoker::TPtr TMetaStateManager::GetSnapshotInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MetaState->GetSnapshotInvoker();
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeSync(
    const TSharedRef& changeData,
    ECommitMode mode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return CommitChangeSync(
        FromMethod(
            &IMetaState::ApplyChange,
            MetaState->GetState(),
            changeData),
        changeData,
        mode);
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeSync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData,
    ECommitMode mode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (ControlStatus != EPeerStatus::Leading) {
        return New<TCommitResult>(ECommitResult::InvalidStatus);
    }

    if (!FollowerTracker->HasActiveQuorum()) {
        return New<TCommitResult>(ECommitResult::NotCommitted);
    }

    return
        LeaderCommitter
        ->CommitLeader(changeAction, changeData, mode)
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
            changeData,
            ECommitMode::NeverFails)
        ->AsyncVia(GetStateInvoker())
        ->Do();
}

ECommitResult TMetaStateManager::OnChangeCommit(
    TLeaderCommitter::EResult result)
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
            return ECommitResult::NotCommitted;
    }
}

void TMetaStateManager::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(ControlStatus == EPeerStatus::Stopped);

    ControlStatus = EPeerStatus::Elections;

    GetStateInvoker()->Invoke(FromMethod(
        &TDecoratedMetaState::Clear,
        MetaState));

    ElectionManager->Start();
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, Sync)
{
    UNUSED(request);
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    if (ControlStatus != EPeerStatus::Leading && ControlStatus != EPeerStatus::LeaderRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid status (Status: %s)",
                ~ControlStatus.ToString());
    }

    GetStateInvoker()->Invoke(FromMethod(
        &TMetaStateManager::SendSync,
        TPtr(this),
        Epoch,
        context));
}

void TMetaStateManager::SendSync(const TEpoch& epoch, TCtxSync::TPtr context)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    auto version = MetaState->GetReachableVersion();
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();

    auto& response = context->Response();
    response.SetSegmentId(version.SegmentId);
    response.SetRecordCount(version.RecordCount);
    response.SetEpoch(epoch.ToProto());
    response.SetMaxSnapshotId(maxSnapshotId);

    context->SetResponseInfo("Version: %s, Epoch: %s, MaxSnapshotId: %d",
        ~version.ToString(),
        ~epoch.ToString(),
        maxSnapshotId);

    context->Reply();
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
            ythrow TServiceException(EErrorCode::InvalidSegmentId) <<
                Sprintf("Invalid snapshot id %d", snapshotId);
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
        ythrow TServiceException(EErrorCode::IOError) <<
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
            ythrow TServiceException(EErrorCode::InvalidSegmentId) <<
                Sprintf("Invalid snapshot id %d", snapshotId);
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
            ythrow TServiceException(EErrorCode::InvalidSegmentId) <<
                Sprintf("Invalid changelog id %d", changeLogId);
        }

        i32 recordCount = changeLog->GetRecordCount();
        
        response->SetRecordCount(recordCount);
        
        context->SetResponseInfo("RecordCount: %d", recordCount);
        context->Reply();
    } catch (...) {
        // TODO: fail?
        ythrow TServiceException(EErrorCode::IOError) <<
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
            ythrow TServiceException(EErrorCode::InvalidSegmentId) <<
                Sprintf("Invalid changelog id %d", changeLogId);
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
        ythrow TServiceException(EErrorCode::IOError) <<
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

    if (ControlStatus != EPeerStatus::Following && ControlStatus != EPeerStatus::FollowerRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid state %s", ~ControlStatus.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Invalid epoch (expected: %s, received: %s)",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }
    
    int changeCount = request->Attachments().size();
    switch (ControlStatus) {
        case EPeerStatus::Following: {
            LOG_DEBUG("ApplyChange: applying %d changes", changeCount);

            YASSERT(~FollowerCommitter != NULL);
            for (int changeIndex = 0; changeIndex < changeCount; ++changeIndex) {
                YASSERT(ControlStatus == EPeerStatus::Following);
                TMetaVersion commitVersion(segmentId, recordCount + changeIndex);
                const TSharedRef& changeData = request->Attachments().at(changeIndex);
                auto asyncResult = FollowerCommitter->CommitFollower(commitVersion, changeData);
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

        case EPeerStatus::FollowerRecovery: {
            LOG_DEBUG("ApplyChange: keeping %d postponed changes", changeCount);

            YASSERT(~FollowerRecovery != NULL);
            for (int changeIndex = 0; changeIndex < changeCount; ++changeIndex) {
                YASSERT(ControlStatus == EPeerStatus::FollowerRecovery);
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
            YUNREACHABLE();
            break;
    }
}

void TMetaStateManager::OnLocalCommit(
    TLeaderCommitter::EResult result,
    TCtxApplyChanges::TPtr context)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& request = context->Request();
    auto& response = context->Response();

    TMetaVersion version(request.GetSegmentId(), request.GetRecordCount());

    switch (result) {
        case TLeaderCommitter::EResult::Committed:
            response.SetCommitted(true);
            context->Reply();
            break;

        case TLeaderCommitter::EResult::InvalidVersion:
            context->Reply(TProxy::EErrorCode::InvalidVersion);
            Restart();

            LOG_WARNING("ApplyChange: change %s is unexpected, restarting",
                ~version.ToString());
            break;

        default:
            YUNREACHABLE();
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

    if (ControlStatus != EPeerStatus::Following && ControlStatus != EPeerStatus::FollowerRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid state %s", ~ControlStatus.ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Invalid epoch: expected %s, received %s",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }

    switch (ControlStatus) {
        case EPeerStatus::Following:
            LOG_DEBUG("CreateSnapshot: creating snapshot");

            SnapshotCreator->CreateLocal(version)->Subscribe(FromMethod(
                &TMetaStateManager::OnCreateLocalSnapshot,
                TPtr(this),
                context));
            break;
            
        case EPeerStatus::FollowerRecovery: {
            LOG_DEBUG("CreateSnapshot: keeping postponed segment advance");

            YASSERT(~FollowerRecovery != NULL);
            auto result = FollowerRecovery->PostponeSegmentAdvance(version);
            if (result != TRecovery::EResult::OK) {
                Restart();
            }

            context->Reply(TProxy::EErrorCode::InvalidStatus);
            break;
        }

        default:
            YUNREACHABLE();
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
            YUNREACHABLE();
            break;
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager, PingLeader)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto followerId = request->GetFollowerId();
    auto followerEpoch = TEpoch::FromProto(request->GetEpoch());
    auto followerStatus = static_cast<EPeerStatus>(request->GetStatus());

    context->SetRequestInfo("Id: %d, Epoch: %s, State: %s",
        followerId,
        ~followerEpoch.ToString(),
        ~followerStatus.ToString());

    if (ControlStatus != EPeerStatus::Leading) {
        LOG_DEBUG("PingLeader: invalid status (Status: %s)",
            ~ControlStatus.ToString());
    } else if (followerEpoch != Epoch ) {
        LOG_DEBUG("PingLeader: invalid epoch (Epoch: %s)",
            ~Epoch.ToString());
    } else {
        FollowerTracker->ProcessPing(followerId, followerStatus);
    }

    // Reply with OK in any case.
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////
// IElectionCallbacks members

void TMetaStateManager::StartControlEpoch(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YASSERT(~EpochControlInvoker == NULL);
    EpochControlInvoker = New<TCancelableInvoker>(ControlInvoker);

    Epoch = epoch;
}

void TMetaStateManager::StopControlEpoch()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LeaderId = NElection::InvalidPeerId;
    Epoch = TEpoch();
    
    YASSERT(~EpochControlInvoker != NULL);
    EpochControlInvoker->Cancel();
    EpochControlInvoker.Drop();
}

void TMetaStateManager::StartStateEpoch()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(~EpochStateInvoker == NULL);
    EpochStateInvoker = New<TCancelableInvoker>(GetStateInvoker());
}

void TMetaStateManager::StopStateEpoch()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(~EpochStateInvoker != NULL);
    EpochStateInvoker->Cancel();
    EpochStateInvoker.Drop();
}

void TMetaStateManager::OnStartLeading(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Starting leader recovery");

    ControlStatus = EPeerStatus::LeaderRecovery;
    LeaderId = CellManager->GetSelfId();    

    StartControlEpoch(epoch);
    
    GetStateInvoker()->Invoke(FromMethod(
        &TThis::DoStartLeading,
        TPtr(this)));

    YASSERT(~LeaderRecovery == NULL);
    LeaderRecovery = New<TLeaderRecovery>(
        Config,
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        LeaderId,
        ControlInvoker);

    LeaderRecovery->Run()->Subscribe(
        FromMethod(&TMetaStateManager::OnLeaderRecoveryComplete, TPtr(this))
        ->Via(~EpochControlInvoker));
}

void TMetaStateManager::DoStartLeading()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Stopped);
    StateStatus = EPeerStatus::LeaderRecovery;

    StartStateEpoch();

    OnStartLeading_.Fire();
}

void TMetaStateManager::OnLeaderRecoveryComplete(TRecovery::EResult result)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    YASSERT(~LeaderRecovery != NULL);
    LeaderRecovery->Stop();
    LeaderRecovery.Drop();

    if (result != TRecovery::EResult::OK) {
        LOG_WARNING("Leader recovery failed, restarting");
        Restart();
        return;
    }

    GetStateInvoker()->Invoke(FromMethod(
        &TThis::DoLeaderRecoveryComplete,
        TPtr(this)));

    YASSERT(~FollowerTracker == NULL);
    FollowerTracker = New<TFollowerTracker>(
        TFollowerTracker::TConfig(),
        CellManager,
        ControlInvoker);

    YASSERT(~LeaderCommitter == NULL);
    LeaderCommitter = New<TLeaderCommitter>(
        TLeaderCommitter::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        FollowerTracker,
        ControlInvoker,
        Epoch);
    LeaderCommitter->OnApplyChange().Subscribe(FromMethod(
        &TMetaStateManager::OnApplyChange,
        TPtr(this)));

    YASSERT(~SnapshotCreator == NULL);
    SnapshotCreator = New<TSnapshotCreator>(
        TSnapshotCreator::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        ControlInvoker);

    ControlStatus = EPeerStatus::Leading;

    LOG_INFO("Leader recovery complete");
}

void TMetaStateManager::DoLeaderRecoveryComplete()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::LeaderRecovery);
    StateStatus = EPeerStatus::Leading;

    OnRecoveryComplete_.Fire();
}

void TMetaStateManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Stopped leading");
    
    GetStateInvoker()->Invoke(FromMethod(
        &TThis::DoStopLeading,
        TPtr(this)));

    ControlStatus = EPeerStatus::Elections;

    StopControlEpoch();

    if (~LeaderRecovery != NULL) {
        LeaderRecovery->Stop();
        LeaderRecovery.Drop();
    }

    if (~LeaderCommitter != NULL) {
        LeaderCommitter->Stop();
        LeaderCommitter.Drop();
    }

    if (~FollowerTracker != NULL) {
        FollowerTracker->Stop();
        FollowerTracker.Drop();
    }

    if (~SnapshotCreator != NULL) {
        SnapshotCreator.Drop();
    }
}

void TMetaStateManager::DoStopLeading()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Leading || StateStatus == EPeerStatus::LeaderRecovery);
    StateStatus = EPeerStatus::Stopped;

    StopStateEpoch();

    OnStopLeading_.Fire();
}

void TMetaStateManager::OnApplyChange()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(ControlStatus == EPeerStatus::Leading);

    auto version = MetaState->GetVersion();
    if (Config.MaxChangesBetweenSnapshots >= 0 &&
        version.RecordCount >= Config.MaxChangesBetweenSnapshots)
    {
        LeaderCommitter->Flush();
        SnapshotCreator->CreateDistributed(version);
    }
}

void TMetaStateManager::OnStartFollowing(TPeerId leaderId, const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Starting follower state recovery");
    
    ControlStatus = EPeerStatus::FollowerRecovery;
    LeaderId = leaderId;

    StartControlEpoch(epoch);

    GetStateInvoker()->Invoke(FromMethod(
        &TThis::DoStartFollowing,
        TPtr(this)));

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
        ->Via(~EpochControlInvoker));
}

void TMetaStateManager::DoStartFollowing()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Stopped);
    StateStatus = EPeerStatus::FollowerRecovery;

    StartStateEpoch();

    OnStartFollowing_.Fire();
}

void TMetaStateManager::OnFollowerRecoveryComplete(TRecovery::EResult result)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(result == TRecovery::EResult::OK ||
            result == TRecovery::EResult::Failed);

    YASSERT(~FollowerRecovery != NULL);
    FollowerRecovery->Stop();
    FollowerRecovery.Drop();

    if (result != TRecovery::EResult::OK) {
        LOG_INFO("Follower recovery failed, restarting");
        Restart();
        return;
    }

    GetStateInvoker()->Invoke(FromMethod(
        &TThis::DoFollowerRecoveryComplete,
        TPtr(this)));

    YASSERT(~FollowerCommitter == NULL);
    FollowerCommitter = New<TFollowerCommitter>(
        MetaState,
        ControlInvoker);

    YASSERT(~LeaderPinger == NULL);
    LeaderPinger = New<TLeaderPinger>(
        TLeaderPinger::TConfig(),
        this,
        CellManager,
        LeaderId,
        Epoch,
        ControlInvoker);

    YASSERT(~SnapshotCreator == NULL);
    SnapshotCreator = New<TSnapshotCreator>(
        TSnapshotCreator::TConfig(),
        CellManager,
        MetaState,
        ChangeLogCache,
        SnapshotStore,
        Epoch,
        ControlInvoker);

    ControlStatus = EPeerStatus::Following;

    LOG_INFO("Follower recovery complete");
}

void TMetaStateManager::DoFollowerRecoveryComplete()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::FollowerRecovery);
    StateStatus = EPeerStatus::Following;

    OnRecoveryComplete_.Fire();
}

void TMetaStateManager::OnStopFollowing()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Stopped following");
    
    GetStateInvoker()->Invoke(FromMethod(
        &TThis::DoStopFollowing,
        TPtr(this)));

    ControlStatus = EPeerStatus::Elections;
    
    StopControlEpoch();

    if (~FollowerRecovery != NULL) {
        // This may happen if the recovery gets interrupted.
        FollowerRecovery->Stop();
        FollowerRecovery.Drop();
    }

    if (~FollowerCommitter != NULL) {
        FollowerCommitter->Stop();
        FollowerCommitter.Drop();
    }

    if (~LeaderPinger != NULL) {
        LeaderPinger->Stop();
        LeaderPinger.Drop();
    }

    if (~SnapshotCreator != NULL) {
        SnapshotCreator.Drop();
    }
}

void TMetaStateManager::DoStopFollowing()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Following || StateStatus == EPeerStatus::FollowerRecovery);
    StateStatus = EPeerStatus::Stopped;

    StopStateEpoch();

    OnStopFollowing_.Fire();
}

TPeerPriority TMetaStateManager::GetPriority()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto version = MetaState->GetReachableVersion();
    return ((TPeerPriority) version.SegmentId << 32) | version.RecordCount;
}

Stroka TMetaStateManager::FormatPriority(TPeerPriority priority)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i32 segmentId = (priority >> 32);
    i32 recordCount = priority & ((1ll << 32) - 1);
    return Sprintf("(%d, %d)", segmentId, recordCount);
}

void TMetaStateManager::GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
{
    auto current = NYTree::TFluentYsonBuilder::Create(consumer)
        .BeginMap()
            .Item("state").Scalar(ControlStatus.ToString())
            // TODO: fixme, thread affinity
            //.Item("version").Scalar(MetaState->GetVersion().ToString())
            .Item("reachable_version").Scalar(MetaState->GetReachableVersion().ToString())
            .Item("elections").Do(FromMethod(&TElectionManager::GetMonitoringInfo, ElectionManager));
    // TODO: refactor
    auto followerTracker = FollowerTracker;
    if (~followerTracker != NULL) {
        auto list = current
            .Item("followers_active").BeginList();
        for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
            list = list
                .Item().Scalar(followerTracker->IsFollowerActive(id));
        }
        current = list
            .EndList()
            .Item("has_quorum").Scalar(followerTracker->HasActiveQuorum());
    }
    current
        .EndMap();
}

TSignal& TMetaStateManager::OnStartLeading2()
{
    return OnStartLeading_;
}

TSignal& TMetaStateManager::OnStopLeading2()
{
    return OnStopLeading_;
}

TSignal& TMetaStateManager::OnStartFollowing2()
{
    return OnStartFollowing_;
}

TSignal& TMetaStateManager::OnStopFollowing2()
{
    return OnStopFollowing_;
}

TSignal& TMetaStateManager::OnRecoveryComplete2()
{
    return OnRecoveryComplete_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
