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
        TServer::TPtr server)
        : TServiceBase(controlInvoker, TProxy::GetServiceName(), Logger.GetCategory())
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

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Sync));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSnapshotInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChangeLogInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ApplyChanges));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceSegment));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingLeader));

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

        ReadQueue = New<TActionQueue>();

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
    void Start();

    EPeerStatus GetControlStatus() const;
    EPeerStatus GetStateStatus() const;

    IInvoker::TPtr GetStateInvoker();
    IInvoker::TPtr GetEpochStateInvoker();
    IInvoker::TPtr GetSnapshotInvoker();

    TAsyncCommitResult::TPtr CommitChangeSync(
        const TSharedRef& changeData,
        ECommitMode mode = ECommitMode::NeverFails);

    TAsyncCommitResult::TPtr CommitChangeSync(
        IAction::TPtr changeAction,
        const TSharedRef& changeData,
        ECommitMode mode = ECommitMode::NeverFails);

    void SetReadOnly(bool readOnly)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ReadOnly = readOnly;
    }

    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    // TODO: get rid of this stupid name clash with IElectionCallbacks
    DECLARE_BYREF_RW_PROPERTY(OnMyStartLeading, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyLeaderRecoveryComplete, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyStopLeading, TSignal);

    DECLARE_BYREF_RW_PROPERTY(OnMyStartFollowing, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyFollowerRecoveryComplete, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnMyStopFollowing, TSignal);

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

    TActionQueue::TPtr ReadQueue;

    // Per epoch, control (service) thread
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

    void DoReadSnapshot(
        TSnapshotReader::TPtr reader,
        i32 length,
        TCtxReadSnapshot::TPtr context) 
    {
        TBlob data(length);
        i32 bytesRead = reader->GetStream().Read(data.begin(), length);
        data.erase(data.begin() + bytesRead, data.end());

        context->Response().Attachments().push_back(TSharedRef(data));
        context->SetResponseInfo("BytesRead: %d", bytesRead);

        context->Reply();
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadChangeLog);

    void DoReadChangeLog(
        TCachedAsyncChangeLog::TPtr changeLog,
        i32 startRecordId,
        i32 recordCount,
        TCtxReadChangeLog::TPtr context) 
    {
        yvector<TSharedRef> recordData;
        changeLog->Read(startRecordId, recordCount, &recordData);

        context->Response().SetRecordsRead(recordData.ysize());
        context->Response().Attachments().insert(
            context->Response().Attachments().end(),
            recordData.begin(),
            recordData.end());

        context->SetResponseInfo("RecordCount: %d", recordData.ysize());
        context->Reply();
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ApplyChanges);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, AdvanceSegment);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, PingLeader);

    void SendSync(const TEpoch& epoch, TCtxSync::TPtr context);
    
    void OnLeaderRecoveryComplete(TRecovery::EResult result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YASSERT(result == TRecovery::EResult::OK ||
                result == TRecovery::EResult::Failed);

        YASSERT(~LeaderRecovery != NULL);
        LeaderRecovery->Stop();
        LeaderRecovery.Reset();

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

    void OnFollowerRecoveryComplete(TRecovery::EResult result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YASSERT(result == TRecovery::EResult::OK ||
                result == TRecovery::EResult::Failed);

        YASSERT(~FollowerRecovery != NULL);
        FollowerRecovery->Stop();
        FollowerRecovery.Reset();

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


    void DoLeaderRecoveryComplete(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Leading;

        // Switch to a new changelog unless the current one is empty.
        // This enables changelog truncation for those followers that are down and have uncommitted changes.
        auto version = MetaState->GetVersion();
        if (version.RecordCount > 0) {
            LOG_INFO("Switching to a new changelog for a new epoch (Version: %s)",
                ~version.ToString());

            for (TPeerId followerId = 0; followerId < CellManager->GetPeerCount(); ++followerId) {
                if (followerId == CellManager->GetSelfId()) continue;
                LOG_DEBUG("Requesting follower to advance segment (FollowerId: %d)",
                    followerId);

                auto proxy = CellManager->GetMasterProxy<TProxy>(followerId);
                auto request = proxy->AdvanceSegment();
                request->SetSegmentId(version.SegmentId);
                request->SetRecordCount(version.RecordCount);
                request->SetEpoch(epoch.ToProto());
                request->SetCreateSnapshot(false);
                request->Invoke()->Subscribe(FromMethod(
                    &TImpl::OnRemoteAdvanceSegment,
                    TPtr(this),
                    followerId,
                    version));
            }

            MetaState->RotateChangeLog();
        }
    
        OnMyLeaderRecoveryComplete_.Fire();
    }

    void OnRemoteAdvanceSegment(
        TProxy::TRspAdvanceSegment::TPtr response,
        TPeerId followerId,
        TMetaVersion version)
    {
        if (response->IsOK()) {
            LOG_DEBUG("Follower advanced segment successfully (FollowerId: %d, Version: %s)",
                followerId,
                ~version.ToString());
        } else {
            LOG_WARNING("Error advancing segment on follower (FollowerId: %d, Version: %s, Error: %s)",
                followerId,
                ~version.ToString(),
                ~response->GetError().ToString());
        }
    }

    void DoAdvanceSegment(TCtxAdvanceSegment::TPtr context, TMetaVersion version)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        if (MetaState->GetVersion() != version) {
            Restart();
            throw TServiceException(EErrorCode::InvalidVersion) <<
                Sprintf("Invalid version, segment advancement canceled (Expected: %s, Received: %s)",
                    ~version.ToString(),
                    ~MetaState->GetVersion().ToString());
        }

        MetaState->RotateChangeLog();

        context->Reply();
    }

    void OnFollowerCommit(
        TLeaderCommitter::EResult result,
        TCtxApplyChanges::TPtr context);

    void Restart();

    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxAdvanceSegment::TPtr context);

    void OnApplyChange();

    ECommitResult OnChangeCommit(TLeaderCommitter::EResult result);

    // IElectionCallbacks implementation.
    virtual void OnStartLeading(const TEpoch& epoch)
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

    virtual void OnStopLeading()
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
            LeaderRecovery.Reset();
        }

        if (~LeaderCommitter != NULL) {
            LeaderCommitter->Stop();
            LeaderCommitter.Reset();
        }

        if (~FollowerTracker != NULL) {
            FollowerTracker->Stop();
            FollowerTracker.Reset();
        }

        if (~SnapshotCreator != NULL) {
            GetStateInvoker()->Invoke(FromMethod(
                &TThis::WaitSnapshotCreation, TPtr(this), SnapshotCreator));
            SnapshotCreator.Reset();
        }
    }

    virtual void OnStartFollowing(TPeerId leaderId, const TEpoch& epoch)
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

    virtual void OnStopFollowing()
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
            FollowerRecovery.Reset();
        }

        if (~FollowerCommitter != NULL) {
            FollowerCommitter->Stop();
            FollowerCommitter.Reset();
        }

        if (~LeaderPinger != NULL) {
            LeaderPinger->Stop();
            LeaderPinger.Reset();
        }

        if (~SnapshotCreator != NULL) {
            GetStateInvoker()->Invoke(FromMethod(
                &TThis::WaitSnapshotCreation, TPtr(this), SnapshotCreator));
            SnapshotCreator.Reset();
        }
    }

    virtual TPeerPriority GetPriority();
    virtual Stroka FormatPriority(TPeerPriority priority);

    // Blocks state thread until snapshot creation is finished
    void WaitSnapshotCreation(TSnapshotCreator::TPtr snapshotCreator)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        snapshotCreator->GetLocalProgress()->Get();
    }

    void DoStartLeading()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::LeaderRecovery;

        StartStateEpoch();

        OnMyStartLeading_.Fire();
    }

    void DoStopLeading()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Leading || StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopStateEpoch();

        OnMyStopLeading_.Fire();
    }


    void DoStartFollowing()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::FollowerRecovery;

        StartStateEpoch();

        OnMyStartFollowing_.Fire();
    }

    void DoFollowerRecoveryComplete()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Following;

        OnMyFollowerRecoveryComplete_.Fire();
    }

    void DoStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Following || StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopStateEpoch();

        OnMyStopFollowing_.Fire();
    }


    void StartControlEpoch(const TEpoch& epoch);
    void StopControlEpoch();

    void StartStateEpoch();
    void StopStateEpoch();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

void TMetaStateManager::TImpl::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // To prevent multiple restarts.
    auto epochControlInvoker = EpochControlInvoker;
    if (~epochControlInvoker != NULL) {
        epochControlInvoker->Cancel();
    }

    auto epochStateInvoker = EpochStateInvoker;
    if (~epochStateInvoker != NULL) {
        epochStateInvoker->Cancel();
    }
    
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

TMetaStateManager::TAsyncCommitResult::TPtr
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

TMetaStateManager::TAsyncCommitResult::TPtr
TMetaStateManager::TImpl::CommitChangeSync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData,
    ECommitMode mode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (GetStateStatus() != EPeerStatus::Leading) {
        return New<TAsyncCommitResult>(ECommitResult::InvalidStatus);
    }

    if (ReadOnly) {
        return New<TAsyncCommitResult>(ECommitResult::ReadOnly);
    }

    // FollowerTracker is modified concurrently from the ControlThread.
    auto followerTracker = FollowerTracker;
    if (~followerTracker == NULL || !followerTracker->HasActiveQuorum()) {
        return New<TAsyncCommitResult>(ECommitResult::NotCommitted);
    }

    return
        LeaderCommitter
        ->Commit(changeAction, changeData, mode)
        ->Apply(FromMethod(&TThis::OnChangeCommit, TPtr(this)));
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
            Sprintf("Invalid status (Status: %s)", ~GetControlStatus().ToString());
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
                Sprintf("Invalid snapshot id (SnapshotId: %d)", snapshotId);
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
            Sprintf("IO error while getting snapshot info (SnapshotId: %d): %s",
                snapshotId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, ReadSnapshot)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UNUSED(response);

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
                Sprintf("Invalid snapshot (SnapshotId: %d)", snapshotId);
        }

        reader->Open(offset);

        ReadQueue->GetInvoker()->Invoke(
            FromMethod(&TImpl::DoReadSnapshot, TPtr(this), reader, length, context));
    } catch (...) {
        // TODO: fail?
        ythrow TServiceException(TProxy::EErrorCode::IOError) <<
            Sprintf("IO error while reading snapshot (SnapshotId: %d): %s",
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
                Sprintf("Invalid changelog id (ChangeLogId: %d)", changeLogId);
        }

        i32 recordCount = changeLog->GetRecordCount();
        
        response->SetRecordCount(recordCount);
        
        context->SetResponseInfo("RecordCount: %d", recordCount);
        context->Reply();
    } catch (...) {
        // TODO: fail?
        ythrow TServiceException(EErrorCode::IOError) <<
            Sprintf("IO error while getting changelog info (ChangeLogId: %d): %s",
                changeLogId,
                ~CurrentExceptionMessage());
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, ReadChangeLog)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UNUSED(response);

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
                Sprintf("Invalid changelog id (ChangeLogId: %d)", changeLogId);
        }

        ReadQueue->GetInvoker()->Invoke(
            FromMethod(
                &TImpl::DoReadChangeLog,
                TPtr(this),
                changeLog,
                startRecordId,
                recordCount,
                context));

    } catch (...) {
        // TODO: fail?
        ythrow TServiceException(EErrorCode::IOError) <<
            Sprintf("IO error while reading changelog (ChangeLogId: %d): %s",
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
            Sprintf("Invalid status (Status: %s)", ~GetControlStatus().ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Invalid epoch (Expected: %s, Received: %s)",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }
    
    int changeCount = request->Attachments().size();
    switch (GetControlStatus()) {
        case EPeerStatus::Following: {
            LOG_DEBUG("ApplyChange: applying changes (Version: %s, ChangeCount: %d)",
                ~version.ToString(),
                changeCount);

            YASSERT(~FollowerCommitter != NULL);

            FollowerCommitter
                ->Commit(version, request->Attachments())
                ->Subscribe(FromMethod(&TThis::OnFollowerCommit, TPtr(this), context));
            break;
        }

        case EPeerStatus::FollowerRecovery: {
            LOG_DEBUG("ApplyChange: keeping postponed changes (Version: %s, ChangeCount: %d)",
                ~version.ToString(),
                changeCount);

            YASSERT(~FollowerRecovery != NULL);

            auto result = FollowerRecovery->PostponeChanges(version, request->Attachments());
            if (result != TRecovery::EResult::OK) {
                Restart();
            }

            response->SetCommitted(false);
            context->Reply();
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TMetaStateManager::TImpl::OnFollowerCommit(
    TLeaderCommitter::EResult result,
    TCtxApplyChanges::TPtr context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto& response = context->Response();

    switch (result) {
        case TCommitterBase::EResult::Committed:
            response.SetCommitted(true);
            context->Reply();
            break;

        case TCommitterBase::EResult::LateChanges:
            context->Reply(TProxy::EErrorCode::InvalidVersion);
            break;

        case TCommitterBase::EResult::OutOfOrderChanges:
            context->Reply(TProxy::EErrorCode::InvalidVersion);
            Restart();
            break;

        default:
            YUNREACHABLE();
    }
}

RPC_SERVICE_METHOD_IMPL(TMetaStateManager::TImpl, AdvanceSegment)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epoch = TEpoch::FromProto(request->GetEpoch());
    i32 segmentId = request->GetSegmentId();
    i32 recordCount = request->GetRecordCount();
    TMetaVersion version(segmentId, recordCount);
    bool createSnapshot = request->GetCreateSnapshot();

    context->SetRequestInfo("Epoch: %s, Version: %s, CreateSnapshot: %s",
        ~epoch.ToString(),
        ~version.ToString(),
        ~ToString(createSnapshot));

    if (GetControlStatus() != EPeerStatus::Following && GetControlStatus() != EPeerStatus::FollowerRecovery) {
        ythrow TServiceException(EErrorCode::InvalidStatus) <<
            Sprintf("Invalid status (Status: %s)", ~GetControlStatus().ToString());
    }

    if (epoch != Epoch) {
        Restart();
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Invalid epoch (Expected: %s, Received: %s)",
                ~Epoch.ToString(),
                ~epoch.ToString());
    }

    switch (GetControlStatus()) {
        case EPeerStatus::Following:
            if (createSnapshot) {
                LOG_DEBUG("AdvanceSegment: starting snapshot creation (Version: %s)",
                    ~version.ToString());
    
                FromMethod(&TSnapshotCreator::CreateLocal, SnapshotCreator, version)
                    ->AsyncVia(GetStateInvoker())
                    ->Do()
                    ->Subscribe(FromMethod(
                        &TThis::OnCreateLocalSnapshot,
                        TPtr(this),
                        context));
            } else {
                LOG_DEBUG("AdvanceSegment: advancing segment (Version: %s)",
                    ~version.ToString());

                GetStateInvoker()->Invoke(context->Wrap(FromMethod(
                    &TImpl::DoAdvanceSegment,
                    TPtr(this),
                    version)));
            }
            break;
            
        case EPeerStatus::FollowerRecovery: {
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
    EpochControlInvoker.Reset();
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
    EpochStateInvoker.Reset();
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

TMetaStateManager::TAsyncCommitResult::TPtr
    TMetaStateManager::CommitChangeSync(
    const TSharedRef& changeData,
    ECommitMode mode)
{
    return Impl->CommitChangeSync(changeData, mode);
}

TMetaStateManager::TAsyncCommitResult::TPtr
TMetaStateManager::CommitChangeSync(
    IAction::TPtr changeAction,
    const TSharedRef& changeData,
    ECommitMode mode)
{
    return Impl->CommitChangeSync(changeAction, changeData, mode);
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

TSignal& TMetaStateManager::OnLeaderRecoveryComplete()
{
    return Impl->OnMyLeaderRecoveryComplete();
}

TSignal& TMetaStateManager::OnStopLeading()
{
    return Impl->OnMyStopLeading();
}

TSignal& TMetaStateManager::OnStartFollowing()
{
    return Impl->OnMyStartFollowing();
}

TSignal& TMetaStateManager::OnFollowerRecoveryComplete()
{
    return Impl->OnMyFollowerRecoveryComplete();
}

TSignal& TMetaStateManager::OnStopFollowing()
{
    return Impl->OnMyStopFollowing();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
