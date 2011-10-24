#include "stdafx.h"
#include "meta_state_manager.h"
#include "change_log.h"
#include "change_log_cache.h"
#include "meta_state_manager_rpc.h"
#include "snapshot.h"
#include "snapshot_creator.h"
#include "recovery.h"
#include "cell_manager.h"
#include "change_committer.h"
#include "follower_tracker.h"
#include "leader_pinger.h"

#include "../election/election_manager.h"
#include "../rpc/service.h"
#include "../actions/action_util.h"
#include "../actions/invoker.h"
#include "../misc/thread_affinity.h"
#include "../misc/serialize.h"
#include "../misc/fs.h"
#include "../misc/guid.h"
#include "../misc/property.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NMetaState {

using namespace NElection;
using namespace NRpc;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManager::TImpl
    : public TServiceBase
    , public IElectionCallbacks
{
public:
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        TMetaStateManager::TPtr owner,
        const TConfig& config,
        IInvoker::TPtr controlInvoker,
        IMetaState::TPtr metaState,
        TServer::TPtr server);

    void Start();

    EPeerStatus GetControlStatus() const;
    EPeerStatus GetStateStatus() const;

    IInvoker::TPtr GetStateInvoker();
    IInvoker::TPtr GetEpochStateInvoker();
    IInvoker::TPtr GetSnapshotInvoker();

    TCommitResult::TPtr CommitChangeSync(
        const TSharedRef& changeData,
        ECommitMode mode = ECommitMode::NeverFails);

    TCommitResult::TPtr CommitChangeSync(
        IAction::TPtr changeAction,
        const TSharedRef& changeData,
        ECommitMode mode = ECommitMode::NeverFails);

    TCommitResult::TPtr CommitChangeAsync(
        const TSharedRef& changeData);

    TCommitResult::TPtr CommitChangeAsync(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    void SetReadOnly(bool readOnly)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ReadOnly = readOnly;
    }

    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    // TODO: get rid of this stupid name clash with IElectionCallbacks
    DECLARE_BYREF_RW_PROPERTY(OnMyStartLeading, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyStopLeading, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyStartFollowing, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyStopFollowing, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnRecoveryComplete, TSignal);

private:
    typedef TImpl TThis;
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef TTypedServiceException<EErrorCode> TServiceException;

    TMetaStateManager::TPtr Owner;
    EPeerStatus ControlStatus;
    EPeerStatus StateStatus;
    TConfig Config;
    TPeerId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ControlInvoker;
    bool ReadOnly;

    NElection::TElectionManager::TPtr ElectionManager;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore::TPtr SnapshotStore;
    TDecoratedMetaState::TPtr MetaState;

    // Per epoch, service thread
    TEpoch Epoch;
    TCancelableInvoker::TPtr EpochControlInvoker;
    TCancelableInvoker::TPtr EpochStateInvoker;
    TSnapshotCreator::TPtr SnapshotCreator;
    TLeaderRecovery::TPtr LeaderRecovery;
    TFollowerRecovery::TPtr FollowerRecovery;

    TLeaderCommitter::TPtr LeaderCommitter;
    TFollowerCommitter::TPtr FollowerCommitter;

    TIntrusivePtr<TFollowerTracker> FollowerTracker;
    TIntrusivePtr<TLeaderPinger> LeaderPinger;

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, Sync);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetSnapshotInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadSnapshot);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadChangeLog);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ApplyChanges);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, AdvanceSegment);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, PingLeader);

    void RegisterMethods();
    void SendSync(const TEpoch& epoch, TCtxSync::TPtr context);

    void OnLeaderRecoveryComplete(TRecovery::EResult result);
    void OnFollowerRecoveryComplete(TRecovery::EResult result);

    void DoLeaderRecoveryComplete(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Leading;

        // Propagating AdvanceSegment
        auto version = MetaState->GetVersion();
        for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
            if (peerId == CellManager->GetSelfId()) continue;
            LOG_DEBUG("Requesting peer %d to advance segment",
                peerId);

            auto proxy = CellManager->GetMasterProxy<TProxy>(peerId);
            auto request = proxy->AdvanceSegment();
            request->SetSegmentId(version.SegmentId);
            request->SetRecordCount(version.RecordCount);
            request->SetEpoch(epoch.ToProto());
            request->SetCreateSnapshot(false);
            request->Invoke()->Subscribe(
                FromMethod(&TImpl::OnRemoteAdvanceSegment, TPtr(this), peerId, version));
        }
    
        OnRecoveryComplete_.Fire();
    }

    void OnRemoteAdvanceSegment(
        TProxy::TRspAdvanceSegment::TPtr response,
        TPeerId peerId,
        TMetaVersion version)
    {
        if (response->IsOK()) {
            LOG_DEBUG("Follower advanced segment successfully (follower: %d, version: %s)",
                peerId,
                ~version.ToString());
        } else {
            LOG_WARNING("Error advancing segment on follower (follower: %d, version: %s, error: %s)",
                peerId,
                ~version.ToString(),
                ~response->GetErrorCode().ToString());
        }
    }

    void DoAdvanceSegment(TCtxAdvanceSegment::TPtr context, TMetaVersion version)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        if (MetaState->GetVersion() != version) {
            Restart();
            throw TServiceException(EErrorCode::InvalidVersion) <<
                Sprintf("Invalid version, segment advancement canceled: expected %s, found %s",
                    ~version.ToString(),
                    ~MetaState->GetVersion().ToString());
        }

        MetaState->RotateChangeLog();

        context->Reply();
    }

    void OnLocalCommit(
        TLeaderCommitter::EResult result,
        TCtxApplyChanges::TPtr context);

    void Restart();

    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxAdvanceSegment::TPtr context);

    void OnApplyChange();

    ECommitResult OnChangeCommit(TLeaderCommitter::EResult result);

    // IElectionCallbacks implementation.
    virtual void OnStartLeading(const TEpoch& epoch);
    virtual void OnStopLeading();
    virtual void OnStartFollowing(TPeerId leaderId, const TEpoch& myEpoch);
    virtual void OnStopFollowing();
    virtual TPeerPriority GetPriority();
    virtual Stroka FormatPriority(TPeerPriority priority);


    void DoStartLeading();
    void DoStopLeading();

    void DoStartFollowing();
    void DoFollowerRecoveryComplete();
    void DoStopFollowing();

    void StartControlEpoch(const TEpoch& epoch);
    void StopControlEpoch();

    void StartStateEpoch();
    void StopStateEpoch();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

TMetaStateManager::TImpl::TImpl(
    TMetaStateManager::TPtr owner,
    const TConfig& config,
    IInvoker::TPtr controlInvoker,
    IMetaState::TPtr metaState,
    TServer::TPtr server)
    : TServiceBase(
        controlInvoker,
        TProxy::GetServiceName(),
        Logger.GetCategory())
    , Owner(owner)
    , ControlStatus(EPeerStatus::Stopped)
    , StateStatus(EPeerStatus::Stopped)
    , Config(config)
    , LeaderId(NElection::InvalidPeerId)
    , ControlInvoker(controlInvoker)
    , ReadOnly(false)
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

void TMetaStateManager::TImpl::RegisterMethods()
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

void TMetaStateManager::TImpl::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // To prevent multiple restarts.
    EpochControlInvoker->Cancel();

    ElectionManager->Restart();
}

EPeerStatus TMetaStateManager::TImpl::GetControlStatus() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return ControlStatus;
}

EPeerStatus TMetaStateManager::TImpl::GetStateStatus() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return StateStatus;
}

IInvoker::TPtr TMetaStateManager::TImpl::GetStateInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MetaState->GetStateInvoker();
}

IInvoker::TPtr TMetaStateManager::TImpl::GetEpochStateInvoker()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return ~EpochStateInvoker;
}

IInvoker::TPtr TMetaStateManager::TImpl::GetSnapshotInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MetaState->GetSnapshotInvoker();
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::TImpl::CommitChangeSync(
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
TMetaStateManager::TImpl::CommitChangeSync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData,
    ECommitMode mode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (GetStateStatus() != EPeerStatus::Leading) {
        return New<TCommitResult>(ECommitResult::InvalidStatus);
    }

    if (ReadOnly) {
        return New<TCommitResult>(ECommitResult::ReadOnly);
    }

    if (!FollowerTracker->HasActiveQuorum()) {
        return New<TCommitResult>(ECommitResult::NotCommitted);
    }

    return
        LeaderCommitter
        ->CommitLeader(changeAction, changeData, mode)
        ->Apply(FromMethod(&TThis::OnChangeCommit, TPtr(this)));
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::TImpl::CommitChangeAsync(const TSharedRef& changeData)
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
TMetaStateManager::TImpl::CommitChangeAsync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        FromMethod(
            &TImpl::CommitChangeSync,
            TPtr(this),
            changeAction,
            changeData,
            ECommitMode::NeverFails)
        ->AsyncVia(GetStateInvoker())
        ->Do();
}

ECommitResult TMetaStateManager::TImpl::OnChangeCommit(
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
    }
}

void TMetaStateManager::TImpl::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(ControlStatus == EPeerStatus::Stopped);

    ControlStatus = EPeerStatus::Elections;

    GetStateInvoker()->Invoke(FromMethod(
        &TDecoratedMetaState::Clear,
        MetaState));

    ElectionManager->Start();
}

void TMetaStateManager::TImpl::GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
{
    auto current = BuildYsonFluently(consumer)
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

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, Sync)
{
    UNUSED(request);
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    if (GetControlStatus() != EPeerStatus::Leading && GetControlStatus() != EPeerStatus::LeaderRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid status (Status: %s)",
                ~GetControlStatus().ToString());
    }

    GetStateInvoker()->Invoke(FromMethod(
        &TThis::SendSync,
        TPtr(this),
        Epoch,
        context));
}

void TMetaStateManager::TImpl::SendSync(const TEpoch& epoch, TCtxSync::TPtr context)
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

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, GetSnapshotInfo)
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

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, ReadSnapshot)
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
        ythrow TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error while reading snapshot (SnapshotId: %d, What: %s)",
                snapshotId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, GetChangeLogInfo)
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

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, ReadChangeLog)
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

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, ApplyChanges)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TEpoch epoch = TEpoch::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);

    context->SetRequestInfo("Epoch: %s, Version: %s",
        ~epoch.ToString(),
        ~version.ToString());

    if (GetControlStatus() != EPeerStatus::Following && GetControlStatus() != EPeerStatus::FollowerRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid state %s", ~GetControlStatus().ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Invalid epoch (expected: %s, received: %s)",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }
    
    int changeCount = request->Attachments().size();
    switch (GetControlStatus()) {
        case EPeerStatus::Following: {
            LOG_DEBUG("ApplyChange: applying %d changes", changeCount);

            YASSERT(~FollowerCommitter != NULL);
            for (int changeIndex = 0; changeIndex < changeCount; ++changeIndex) {
                YASSERT(GetControlStatus() == EPeerStatus::Following);
                TMetaVersion commitVersion(segmentId, recordCount + changeIndex);
                const TSharedRef& changeData = request->Attachments().at(changeIndex);
                auto asyncResult = FollowerCommitter->CommitFollower(commitVersion, changeData);
                // Subscribe to the last change
                if (changeIndex == changeCount - 1) {
                    asyncResult->Subscribe(FromMethod(
                            &TThis::OnLocalCommit,
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
                YASSERT(GetControlStatus() == EPeerStatus::FollowerRecovery);
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
    }
}

void TMetaStateManager::TImpl::OnLocalCommit(
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
RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, AdvanceSegment)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    TEpoch epoch = TEpoch::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);
    bool createSnapshot = request->GetCreateSnapshot();

    context->SetRequestInfo("Epoch: %s, Version: %s, CreateSnapshot: %s",
        ~epoch.ToString(),
        ~version.ToString(),
        createSnapshot ? "True" : "False");

    if (GetControlStatus() != EPeerStatus::Following && GetControlStatus() != EPeerStatus::FollowerRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid state %s", ~GetControlStatus().ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Invalid epoch: expected %s, received %s",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }

    switch (GetControlStatus()) {
        case EPeerStatus::Following:
            if (createSnapshot) {
                LOG_DEBUG("CreateSnapshot: creating snapshot");
    
                FromMethod(&TSnapshotCreator::CreateLocal, SnapshotCreator, version)
                    ->AsyncVia(GetStateInvoker())
                    ->Do()
                    ->Subscribe(FromMethod(
                        &TThis::OnCreateLocalSnapshot,
                        TPtr(this),
                        context));
            } else {
                LOG_DEBUG("Advancing segment");

                GetStateInvoker()->Invoke(
                    FromMethod(&TImpl::DoAdvanceSegment, TPtr(this), context, version));
            }
            break;
            
        case EPeerStatus::FollowerRecovery: {
            LOG_DEBUG("CreateSnapshot: keeping postponed segment advance");

            YASSERT(~FollowerRecovery != NULL);
            auto result = FollowerRecovery->PostponeSegmentAdvance(version);
            if (result != TRecovery::EResult::OK) {
                Restart();
            }

            if (createSnapshot) {
                context->Reply(EErrorCode::InvalidStatus);
            } else {
                context->Reply();
            }

            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TMetaStateManager::TImpl::OnCreateLocalSnapshot(
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
        case TSnapshotCreator::EResultCode::AlreadyInProgress:
            context->Reply(TProxy::EErrorCode::Busy);
            break;
        default:
            YUNREACHABLE();
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, PingLeader)
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

    if (GetControlStatus() != EPeerStatus::Leading) {
        LOG_DEBUG("PingLeader: invalid status (Status: %s)",
            ~GetControlStatus().ToString());
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

void TMetaStateManager::TImpl::StartControlEpoch(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YASSERT(~EpochControlInvoker == NULL);
    EpochControlInvoker = New<TCancelableInvoker>(ControlInvoker);

    Epoch = epoch;
}

void TMetaStateManager::TImpl::StopControlEpoch()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LeaderId = NElection::InvalidPeerId;
    Epoch = TEpoch();
    
    YASSERT(~EpochControlInvoker != NULL);
    EpochControlInvoker->Cancel();
    EpochControlInvoker.Drop();
}

void TMetaStateManager::TImpl::StartStateEpoch()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(~EpochStateInvoker == NULL);
    EpochStateInvoker = New<TCancelableInvoker>(GetStateInvoker());
}

void TMetaStateManager::TImpl::StopStateEpoch()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(~EpochStateInvoker != NULL);
    EpochStateInvoker->Cancel();
    EpochStateInvoker.Drop();
}

void TMetaStateManager::TImpl::OnStartLeading(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Starting leader recovery");

    GetControlStatus() = EPeerStatus::LeaderRecovery;
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
        FromMethod(&TThis::OnLeaderRecoveryComplete, TPtr(this))
        ->Via(~EpochControlInvoker));
}

void TMetaStateManager::TImpl::DoStartLeading()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Stopped);
    StateStatus = EPeerStatus::LeaderRecovery;

    StartStateEpoch();

    OnMyStartLeading_.Fire();
}

void TMetaStateManager::TImpl::OnLeaderRecoveryComplete(TRecovery::EResult result)
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
        TPtr(this),
        Epoch));

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
        &TThis::OnApplyChange,
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

void TMetaStateManager::TImpl::OnStopLeading()
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

void TMetaStateManager::TImpl::DoStopLeading()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Leading || StateStatus == EPeerStatus::LeaderRecovery);
    StateStatus = EPeerStatus::Stopped;

    StopStateEpoch();

    OnMyStopLeading_.Fire();
}

void TMetaStateManager::TImpl::OnApplyChange()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(StateStatus == EPeerStatus::Leading);

    if (Config.MaxChangesBetweenSnapshots > 0 &&
        MetaState->GetVersion().RecordCount % (Config.MaxChangesBetweenSnapshots + 1) == 0)
    {
        LeaderCommitter->Flush();
        SnapshotCreator->CreateDistributed();
    }
}

void TMetaStateManager::TImpl::OnStartFollowing(TPeerId leaderId, const TEpoch& epoch)
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
        FromMethod(&TThis::OnFollowerRecoveryComplete, TPtr(this))
        ->Via(~EpochControlInvoker));
}

void TMetaStateManager::TImpl::DoStartFollowing()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Stopped);
    StateStatus = EPeerStatus::FollowerRecovery;

    StartStateEpoch();

    OnMyStartFollowing_.Fire();
}

void TMetaStateManager::TImpl::OnFollowerRecoveryComplete(TRecovery::EResult result)
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
        Owner,
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

void TMetaStateManager::TImpl::DoFollowerRecoveryComplete()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::FollowerRecovery);
    StateStatus = EPeerStatus::Following;

    OnRecoveryComplete_.Fire();
}

void TMetaStateManager::TImpl::OnStopFollowing()
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

void TMetaStateManager::TImpl::DoStopFollowing()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    
    YASSERT(StateStatus == EPeerStatus::Following || StateStatus == EPeerStatus::FollowerRecovery);
    StateStatus = EPeerStatus::Stopped;

    StopStateEpoch();

    OnMyStopFollowing_.Fire();
}

TPeerPriority TMetaStateManager::TImpl::GetPriority()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto version = MetaState->GetReachableVersion();
    return ((TPeerPriority) version.SegmentId << 32) | version.RecordCount;
}

Stroka TMetaStateManager::TImpl::FormatPriority(TPeerPriority priority)
{
    VERIFY_THREAD_AFFINITY_ANY();

    i32 segmentId = (priority >> 32);
    i32 recordCount = priority & ((1ll << 32) - 1);
    return Sprintf("(%d, %d)", segmentId, recordCount);
}

////////////////////////////////////////////////////////////////////////////////

TMetaStateManager::TMetaStateManager(
    const TConfig& config,
    IInvoker::TPtr controlInvoker,
    IMetaState::TPtr metaState,
    TServer::TPtr server)
    : Impl(New<TImpl>(
        this,
        config,
        controlInvoker,
        metaState,
        server))
{ }

TMetaStateManager::~TMetaStateManager()
{ }

EPeerStatus TMetaStateManager::GetControlStatus() const
{
    return Impl->GetControlStatus();
}

EPeerStatus TMetaStateManager::GetStateStatus() const
{
    return Impl->GetStateStatus();
}

IInvoker::TPtr TMetaStateManager::GetStateInvoker()
{
    return Impl->GetStateInvoker();
}

IInvoker::TPtr TMetaStateManager::GetEpochStateInvoker()
{
    return Impl->GetEpochStateInvoker();
}

IInvoker::TPtr TMetaStateManager::GetSnapshotInvoker()
{
    return Impl->GetSnapshotInvoker();
}

TMetaStateManager::TCommitResult::TPtr
    TMetaStateManager::CommitChangeSync(
    const TSharedRef& changeData,
    ECommitMode mode)
{
    return Impl->CommitChangeSync(changeData, mode);
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeSync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData,
    ECommitMode mode)
{
    return Impl->CommitChangeSync(changeAction, changeData, mode);
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeAsync(const TSharedRef& changeData)
{
    return Impl->CommitChangeAsync(changeData);
}

TMetaStateManager::TCommitResult::TPtr
TMetaStateManager::CommitChangeAsync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    return Impl->CommitChangeAsync(changeAction, changeData);
}

void TMetaStateManager::SetReadOnly(bool readOnly)
{
    Impl->SetReadOnly(readOnly);
}

void TMetaStateManager::Start()
{
    Impl->Start();
}

void TMetaStateManager::GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
{
    return Impl->GetMonitoringInfo(consumer);
}

TSignal& TMetaStateManager::OnStartLeading()
{
    return Impl->OnMyStartLeading();
}

TSignal& TMetaStateManager::OnStopLeading()
{
    return Impl->OnMyStopLeading();
}

TSignal& TMetaStateManager::OnStartFollowing()
{
    return Impl->OnMyStartFollowing();
}

TSignal& TMetaStateManager::OnStopFollowing()
{
    return Impl->OnMyStopFollowing();
}

TSignal& TMetaStateManager::OnRecoveryComplete()
{
    return Impl->OnRecoveryComplete();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
