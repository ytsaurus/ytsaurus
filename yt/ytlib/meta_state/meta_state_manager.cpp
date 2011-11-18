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
#include "follower_pinger.h"

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
        IInvoker* controlInvoker,
        IMetaState* metaState,
        IServer* server)
        : TServiceBase(controlInvoker, TProxy::GetServiceName(), Logger.GetCategory())
        , Owner(owner)
        , ControlStatus(EPeerStatus::Stopped)
        , StateStatus(EPeerStatus::Stopped)
        , Config(config)
        , LeaderId(NElection::InvalidPeerId)
        , ControlInvoker(controlInvoker)
        , ReadOnly(false)
    {
        YASSERT(controlInvoker != NULL);
        YASSERT(metaState != NULL);
        YASSERT(server != NULL);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSnapshotInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChangeLogInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadChangeLog));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ApplyChanges));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceSegment));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));

        NFS::CleanTempFiles(config.LogLocation);
        ChangeLogCache = New<TChangeLogCache>(Config.LogLocation);

        NFS::CleanTempFiles(config.SnapshotLocation);
        SnapshotStore = New<TSnapshotStore>(Config.SnapshotLocation);

        MetaState = New<TDecoratedMetaState>(
            metaState,
            SnapshotStore,
            ChangeLogCache);

        ReadQueue = New<TActionQueue>();

        VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
        VERIFY_INVOKER_AFFINITY(GetStateInvoker(), StateThread);
        VERIFY_INVOKER_AFFINITY(ReadQueue->GetInvoker(), ReadThread);
        
        CellManager = New<TCellManager>(Config.Cell);

        // TODO: fill config
        ElectionManager = New<TElectionManager>(
            NElection::TElectionManager::TConfig(),
            ~CellManager,
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

    TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction);

    void SetReadOnly(bool readOnly)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ReadOnly = readOnly;
    }

    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    // TODO: get rid of this stupid name clash with IElectionCallbacks
    DECLARE_BYREF_RW_PROPERTY(TSignal, OnMyStartLeading);
    DECLARE_BYREF_RW_PROPERTY(TSignal, OnMyLeaderRecoveryComplete);
    DECLARE_BYREF_RW_PROPERTY(TSignal, OnMyStopLeading);

    DECLARE_BYREF_RW_PROPERTY(TSignal, OnMyStartFollowing);
    DECLARE_BYREF_RW_PROPERTY(TSignal, OnMyFollowerRecoveryComplete);
    DECLARE_BYREF_RW_PROPERTY(TSignal, OnMyStopFollowing);

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

    TFollowerTracker::TPtr FollowerTracker;
    TFollowerPinger::TPtr FollowerPinger;


    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetSnapshotInfo)
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
                Sprintf("IO error while getting snapshot info (SnapshotId: %d)\n%s",
                    snapshotId,
                    ~CurrentExceptionMessage());
        }
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadSnapshot)
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

        auto reader = SnapshotStore->GetReader(snapshotId);
        if (~reader == NULL) {
            ythrow TServiceException(EErrorCode::InvalidSegmentId) <<
                Sprintf("Invalid snapshot (SnapshotId: %d)", snapshotId);
        }

        ReadQueue->GetInvoker()->Invoke(
            context->Wrap(~FromMethod(
                &TImpl::DoReadSnapshot,
                TPtr(this),
                snapshotId,
                reader,
                offset,
                length)));
    }


    void DoReadSnapshot(
        TCtxReadSnapshot::TPtr context,
        i32 snapshotId,
        TSnapshotReader::TPtr reader,
        i64 offset,
        i32 length) 
    {
        VERIFY_THREAD_AFFINITY(ReadThread);

        try {
            reader->Open(offset);

            TBlob data(length);
            i32 bytesRead = reader->GetStream().Read(data.begin(), length);
            data.erase(data.begin() + bytesRead, data.end());

            context->Response().Attachments().push_back(TSharedRef(MoveRV(data)));
            context->SetResponseInfo("BytesRead: %d", bytesRead);

            context->Reply();
        } catch (...) {
            // TODO: fail?
            ythrow TServiceException(TProxy::EErrorCode::IOError) <<
                Sprintf("IO error while reading snapshot (SnapshotId: %d)\n%s",
                    snapshotId,
                    ~CurrentExceptionMessage());
        }
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetChangeLogInfo)
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
                Sprintf("IO error while getting changelog info (ChangeLogId: %d)\n%s",
                    changeLogId,
                    ~CurrentExceptionMessage());
        }
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadChangeLog)
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
    
        auto changeLog = ChangeLogCache->Get(changeLogId);
        if (~changeLog == NULL) {
            ythrow TServiceException(EErrorCode::InvalidSegmentId) <<
                Sprintf("Invalid changelog id (ChangeLogId: %d)", changeLogId);
        }

        ReadQueue->GetInvoker()->Invoke(~context->Wrap(~FromMethod(
            &TImpl::DoReadChangeLog,
            TPtr(this),
            changeLogId,
            changeLog,
            startRecordId,
            recordCount)));
    }


    void DoReadChangeLog(
        TCtxReadChangeLog::TPtr context,
        i32 changeLogId,
        TCachedAsyncChangeLog::TPtr changeLog,
        i32 startRecordId,
        i32 recordCount) 
    {
        VERIFY_THREAD_AFFINITY(ReadThread);

        try {
            yvector<TSharedRef> recordData;
            changeLog->Read(startRecordId, recordCount, &recordData);

            context->Response().SetRecordsRead(recordData.ysize());
            context->Response().Attachments().insert(
                context->Response().Attachments().end(),
                recordData.begin(),
                recordData.end());

            context->SetResponseInfo("RecordCount: %d", recordData.ysize());
            context->Reply();
        } catch (...) {
            // TODO: fail?
            ythrow TServiceException(EErrorCode::IOError) <<
                Sprintf("IO error while reading changelog (ChangeLogId: %d)\n%s",
                    changeLogId,
                    ~CurrentExceptionMessage());
        }
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ApplyChanges)
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
                if (~FollowerRecovery != NULL) {
                    LOG_DEBUG("ApplyChange: keeping postponed changes (Version: %s, ChangeCount: %d)",
                        ~version.ToString(),
                        changeCount);

                    auto result = FollowerRecovery->PostponeChanges(version, request->Attachments());
                    if (result != TRecovery::EResult::OK) {
                        Restart();
                    }

                    response->SetCommitted(false);
                    context->Reply();
                } else {
                    LOG_DEBUG("ApplyChange: ignoring changes (Version: %s, ChangeCount: %d)",
                        ~version.ToString(),
                        changeCount);
                    context->Reply(EErrorCode::InvalidStatus);
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, AdvanceSegment)
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

                    GetStateInvoker()->Invoke(context->Wrap(~FromMethod(
                        &TImpl::DoAdvanceSegment,
                        TPtr(this),
                        version)));
                }
                break;
            
            case EPeerStatus::FollowerRecovery: {
                // TODO: Logging
                if (~FollowerRecovery != NULL) {
                    auto result = FollowerRecovery->PostponeSegmentAdvance(version);
                    if (result != TRecovery::EResult::OK) {
                        Restart();
                    }

                    if (createSnapshot) {
                        context->Reply(EErrorCode::InvalidStatus);
                    } else {
                        context->Reply();
                    }
                } else {
                    context->Reply(EErrorCode::InvalidStatus);
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, PingFollower)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 segmentId = request->GetSegmentId();
        i32 recordCount = request->GetRecordCount();
        TMetaVersion version(segmentId, recordCount);
        auto epoch = TEpoch::FromProto(request->GetEpoch());
        i32 maxSnapshotId = request->GetMaxSnapshotId();
        
        context->SetRequestInfo("Version: %s,  Epoch: %s, MaxSnapshotId: %d",
            ~version.ToString(),
            ~epoch.ToString(),
            maxSnapshotId);

        auto status = GetControlStatus();

        if (status != EPeerStatus::Following && status != EPeerStatus::FollowerRecovery) {
            ythrow TServiceException(EErrorCode::InvalidStatus) <<
                Sprintf("Invalid status (Status: %s)",
                    ~GetControlStatus().ToString());
        }

        if (epoch != Epoch) {
            Restart();
            ythrow TServiceException(EErrorCode::InvalidEpoch) <<
                Sprintf("Invalid epoch (Expected: %s, Received: %s)",
                    ~Epoch.ToString(),
                    ~epoch.ToString());
        }

        switch (status) {
            case EPeerStatus::Following:
                // code here for two-phase commit
                // right now, ignoring ping
                break;

            case EPeerStatus::FollowerRecovery:
                if (~FollowerRecovery == NULL) {
                    FollowerRecovery = New<TFollowerRecovery>(
                        Config,
                        CellManager,
                        MetaState,
                        ChangeLogCache,
                        SnapshotStore,
                        Epoch,
                        LeaderId,
                        ControlInvoker,
                        version,
                        maxSnapshotId);

                    FollowerRecovery->Run()->Subscribe(
                        FromMethod(&TThis::OnFollowerRecoveryComplete, TPtr(this))
                        ->Via(~EpochControlInvoker));
                }
                break;
                
            default:
                YUNREACHABLE();
        }

        response->SetStatus(status);

        // Reply with OK in any case.
        context->Reply();
    }

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
            ythrow TServiceException(EErrorCode::InvalidVersion) <<
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

        YASSERT(~FollowerTracker == NULL);
        FollowerTracker = New<TFollowerTracker>(
            TFollowerTracker::TConfig(),
            CellManager,
            ControlInvoker);

        YASSERT(~FollowerPinger == NULL);
        FollowerPinger = New<TFollowerPinger>(
            TFollowerPinger::TConfig(), // Change to Config.LeaderPinger
            MetaState,
            CellManager,
            FollowerTracker,
            SnapshotStore,
            Epoch,
            ControlInvoker);

        
        YASSERT(~LeaderRecovery == NULL);
        LeaderRecovery = New<TLeaderRecovery>(
            Config,
            CellManager,
            MetaState,
            ChangeLogCache,
            SnapshotStore,
            Epoch,
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

        if (~FollowerPinger != NULL) {
            FollowerPinger->Stop();
            FollowerPinger.Reset();
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
    DECLARE_THREAD_AFFINITY_SLOT(ReadThread);
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
TMetaStateManager::TImpl::CommitChange(
    const TSharedRef& changeData,
    IAction* changeAction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (GetStateStatus() != EPeerStatus::Leading) {
        return New<TAsyncCommitResult>(ECommitResult::InvalidStatus);
    }

    if (ReadOnly) {
        return New<TAsyncCommitResult>(ECommitResult::ReadOnly);
    }

    IAction::TPtr changeAction_;
    if (changeAction == NULL) {
        changeAction_ = FromMethod(
            &IMetaState::ApplyChange,
            MetaState->GetState(),
            changeData);
    } else {
        changeAction_ = changeAction;
    }

    // FollowerTracker is modified concurrently from the ControlThread.
    auto followerTracker = FollowerTracker;
    if (~followerTracker == NULL || !followerTracker->HasActiveQuorum()) {
        return New<TAsyncCommitResult>(ECommitResult::NotCommitted);
    }

    return
        LeaderCommitter
        ->Commit(changeAction, changeData)
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
    IInvoker* controlInvoker,
    IMetaState* metaState,
    IServer* server)
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
TMetaStateManager::CommitChange(
    const TSharedRef& changeData,
    IAction* changeAction)
{
    return Impl->CommitChange(changeData, changeAction);
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
