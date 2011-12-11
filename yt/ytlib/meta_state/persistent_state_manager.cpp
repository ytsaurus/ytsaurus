#include "stdafx.h"
#include "persistent_state_manager.h"

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
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

class TPersistentStateManager
    : public TServiceBase
    , public IMetaStateManager
{
public:
    typedef TIntrusivePtr<TPersistentStateManager> TPtr;
    typedef TPersistentStateManagerConfig TConfig;

    class TElectionCallbacks
        : public IElectionCallbacks
    {
    public:
        TElectionCallbacks(TPersistentStateManager* owner)
            : Owner(owner)
        { }

    private:
        TPersistentStateManager::TPtr Owner;

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
        const TConfig& config,
        IInvoker* controlInvoker,
        IMetaState* metaState,
        IRpcServer* server)
        : TServiceBase(controlInvoker, TProxy::GetServiceName(), Logger.GetCategory())
        , ControlStatus(EPeerStatus::Stopped)
        , StateStatus(EPeerStatus::Stopped)
        , Config(config)
        , LeaderId(NElection::InvalidPeerId)
        , ControlInvoker(controlInvoker)
        , ReadOnly(false)
        , CommitInProgress(false)
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

        NFS::ForcePath(config.LogPath);
        NFS::CleanTempFiles(config.LogPath);
        ChangeLogCache = New<TChangeLogCache>(Config.LogPath);

        NFS::ForcePath(config.SnapshotPath);
        NFS::CleanTempFiles(config.SnapshotPath);
        SnapshotStore = New<TSnapshotStore>(Config.SnapshotPath);

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
            ~New<TElectionCallbacks>(this),
            server);

        server->RegisterService(this);
    }

    void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(ControlStatus == EPeerStatus::Stopped);

        ControlStatus = EPeerStatus::Elections;

        GetStateInvoker()->Invoke(FromMethod(
            &TDecoratedMetaState::Clear,
            MetaState));

        ElectionManager->Start();
    }

    void Stop()
    {
        //TODO: implement this
        YUNIMPLEMENTED();
    }

    EPeerStatus GetControlStatus() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ControlStatus;
    }

    EPeerStatus GetStateStatus() const
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        return StateStatus;
    }

    IInvoker::TPtr GetStateInvoker()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return MetaState->GetStateInvoker();
    }

    IInvoker::TPtr GetEpochStateInvoker()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        return ~EpochStateInvoker;
    }

    void SetReadOnly(bool readOnly)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ReadOnly = readOnly;
    }

    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
    {
        auto tracker = FollowerTracker;

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Scalar(ControlStatus.ToString())
                // TODO: fixme, thread affinity
                //.Item("version").Scalar(MetaState->GetVersion().ToString())
                .Item("reachable_version").Scalar(MetaState->GetReachableVersion().ToString())
                .Item("elections").Do(~FromMethod(&TElectionManager::GetMonitoringInfo, ElectionManager))
                .DoIf(~tracker != NULL, [=] (TFluentMap fluent)
                    {
                        fluent
                            .Item("has_quorum").Scalar(tracker->HasActiveQuorum())
                            .Item("followers_active").DoListFor(0, CellManager->GetPeerCount(),
                                [=] (TFluentList fluent, TPeerId id)
                                {
                                    fluent.Item().Scalar(tracker->IsFollowerActive(id));
                                });
                    })
            .EndMap();
    }

    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStartLeading);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnLeaderRecoveryComplete);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStopLeading);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStartFollowing);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnFollowerRecoveryComplete);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStopFollowing);

    TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction)
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        YASSERT(!CommitInProgress);

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

        CommitInProgress = true;

        auto changeAction_ =
            changeAction == NULL
            ? FromMethod(&IMetaState::ApplyChange, MetaState->GetState(), changeData)
            : changeAction;

        auto result =
            LeaderCommitter
            ->Commit(changeAction, changeData)
            ->Apply(FromMethod(&TThis::OnChangeCommit, TPtr(this)));

        CommitInProgress = false;

        return result;
    }

 private:
    typedef TPersistentStateManager TThis;
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    EPeerStatus ControlStatus;
    EPeerStatus StateStatus;
    TConfig Config;
    TPeerId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ControlInvoker;
    bool ReadOnly;
    bool CommitInProgress;

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

    // RPC methods.
    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, GetSnapshotInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 snapshotId = request->snapshotid();

        context->SetRequestInfo("SnapshotId: %d",
            snapshotId);

        try {
            auto result = SnapshotStore->GetReader(snapshotId);
            if (!result.IsOK()) {
                // TODO: cannot use ythrow here
                throw TServiceException(result);
            }

            auto reader = result.Value();
            reader->Open();
        
            i64 length = reader->GetLength();
            TChecksum checksum = reader->GetChecksum();
            int prevRecordCount = reader->GetPrevRecordCount();

            response->set_length(length);

            context->SetResponseInfo("Length: %" PRId64 ", PrevRecordCount: %d, Checksum: %" PRIx64,
                length,
                prevRecordCount,
                checksum);

            context->Reply();
        } catch (...) {
            LOG_FATAL("IO error while getting snapshot info (SnapshotId: %d)\n%s",
                snapshotId,
                ~CurrentExceptionMessage());
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, ReadSnapshot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        UNUSED(response);

        i32 snapshotId = request->snapshotid();
        i64 offset = request->offset();
        i32 length = request->length();

        context->SetRequestInfo("SnapshotId: %d, Offset: %" PRId64 ", Length: %d",
            snapshotId,
            offset,
            length);

        YASSERT(offset >= 0);
        YASSERT(length >= 0);

        auto result = SnapshotStore->GetRawReader(snapshotId);
        if (!result.IsOK()) {
            // TODO: cannot use ythrow here
            throw TServiceException(result);
        }

        auto snapshotFile = result.Value();
        ReadQueue->GetInvoker()->Invoke(context->Wrap(~FromFunctor([=] ()
            {
                VERIFY_THREAD_AFFINITY(ReadThread);

                try {
                    snapshotFile->Seek(offset, sSet);

                    TBlob data(length);
                    i32 bytesRead = snapshotFile->Read(data.begin(), length);
                    data.erase(data.begin() + bytesRead, data.end());

                    context->Response().Attachments().push_back(TSharedRef(MoveRV(data)));
                    context->SetResponseInfo("BytesRead: %d", bytesRead);

                    context->Reply();
                } catch (...) {
                    LOG_FATAL("IO error while reading snapshot (SnapshotId: %d)\n%s",
                        snapshotId,
                        ~CurrentExceptionMessage());
                }
            })));
    }

    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, GetChangeLogInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 changeLogId = request->changelogid();

        context->SetRequestInfo("ChangeLogId: %d",
            changeLogId);

        try {
            auto result = ChangeLogCache->Get(changeLogId);
            if (!result.IsOK()) {
                // TODO: cannot use ythrow here
                throw TServiceException(result);
            }

            auto changeLog = result.Value();
            i32 recordCount = changeLog->GetRecordCount();
        
            response->set_recordcount(recordCount);
        
            context->SetResponseInfo("RecordCount: %d", recordCount);
            context->Reply();
        } catch (...) {
            LOG_FATAL("IO error while getting changelog info (ChangeLogId: %d)\n%s",
                changeLogId,
                ~CurrentExceptionMessage());
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, ReadChangeLog)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        UNUSED(response);

        i32 changeLogId = request->changelogid();
        i32 startRecordId = request->startrecordid();
        i32 recordCount = request->recordcount();
    
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
        ReadQueue->GetInvoker()->Invoke(~context->Wrap(~FromFunctor([=] ()
            {
                VERIFY_THREAD_AFFINITY(ReadThread);

                try {
                    yvector<TSharedRef> recordData;
                    changeLog->Read(startRecordId, recordCount, &recordData);

                    context->Response().set_recordsread(recordData.ysize());
                    context->Response().Attachments().insert(
                        context->Response().Attachments().end(),
                        recordData.begin(),
                        recordData.end());

                    context->SetResponseInfo("RecordCount: %d", recordData.ysize());
                    context->Reply();
                } catch (...) {
                    LOG_FATAL("IO error while reading changelog (ChangeLogId: %d)\n%s",
                        changeLogId,
                        ~CurrentExceptionMessage());
                }
            })));
    }

    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, ApplyChanges)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TEpoch epoch = TEpoch::FromProto(request->epoch());
        i32 segmentId = request->segmentid();
        i32 recordCount = request->recordcount();
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

    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, PingFollower)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        i32 segmentId = request->segmentid();
        i32 recordCount = request->recordcount();
        TMetaVersion version(segmentId, recordCount);
        auto epoch = TEpoch::FromProto(request->epoch());
        i32 maxSnapshotId = request->maxsnapshotid();

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
                        FromMethod(&TThis::OnFollowerRecoveryFinished, TPtr(this))
                        ->Via(~EpochControlInvoker));
                }
                break;

            default:
                YUNREACHABLE();
        }

        response->set_status(status);

        // Reply with OK in any case.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMetaState::NProto, AdvanceSegment)
    {
        UNUSED(response);
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto epoch = TEpoch::FromProto(request->epoch());
        i32 segmentId = request->segmentid();
        i32 recordCount = request->recordcount();
        TMetaVersion version(segmentId, recordCount);
        bool createSnapshot = request->createsnapshot();

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
                        &TThis::DoAdvanceSegment,
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
    // End of RPC methods.

    void OnLeaderRecoveryFinished(TRecovery::EResult result)
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

    void OnFollowerRecoveryFinished(TRecovery::EResult result)
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
                request->set_segmentid(version.SegmentId);
                request->set_recordcount(version.RecordCount);
                request->set_epoch(epoch.ToProto());
                request->set_createsnapshot(false);
                request->Invoke()->Subscribe(FromMethod(
                    &TThis::OnRemoteAdvanceSegment,
                    TPtr(this),
                    followerId,
                    version));
            }

            MetaState->RotateChangeLog();
        }

        OnLeaderRecoveryComplete_.Fire();
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

    void OnFollowerCommit(TLeaderCommitter::EResult result, TCtxApplyChanges::TPtr context)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto& response = context->Response();

        switch (result) {
            case TCommitterBase::EResult::Committed:
                response.set_committed(true);
                context->Reply();
                break;

            case TCommitterBase::EResult::LateChanges:
                context->Reply(
                    TProxy::EErrorCode::InvalidVersion,
                    "Changes are late");
                break;

            case TCommitterBase::EResult::OutOfOrderChanges:
                context->Reply(
                    TProxy::EErrorCode::InvalidVersion,
                    "Changes are out of order");
                Restart();
                break;

            default:
                YUNREACHABLE();
        }
    }

    void Restart()
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

    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxAdvanceSegment::TPtr context)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto& response = context->Response();

        switch (result.ResultCode) {
            case TSnapshotCreator::EResultCode::OK:
                response.set_checksum(result.Checksum);
                context->Reply();
                break;
            case TSnapshotCreator::EResultCode::InvalidVersion:
                context->Reply(
                    TProxy::EErrorCode::InvalidVersion,
                    "Requested to create a snapshot for an invalid version");
                break;
            case TSnapshotCreator::EResultCode::AlreadyInProgress:
                context->Reply(
                    TProxy::EErrorCode::SnapshotAlreadyInProgress,
                    "Snapshot creation is already in progress");
                break;
            default:
                YUNREACHABLE();
        }
    }

    ECommitResult OnChangeCommit(TLeaderCommitter::EResult result)
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

    // IElectionCallbacks implementation.
    // TODO: get rid of these stupid names.
    void OnElectionStartLeading(const TEpoch& epoch)
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
            FromMethod(&TThis::OnLeaderRecoveryFinished, TPtr(this))
            ->Via(~EpochControlInvoker));
    }

    void OnElectionStopLeading()
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
                &TThis::WaitSnapshotCreation,
                TPtr(this),
                SnapshotCreator));
            SnapshotCreator.Reset();
        }
    }

    void OnElectionStartFollowing(TPeerId leaderId, const TEpoch& epoch)
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

    void OnElectionStopFollowing()
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
                &TThis::WaitSnapshotCreation,
                TPtr(this),
                SnapshotCreator));
            SnapshotCreator.Reset();
        }
    }
    // End of IElectionCallback methods.

    void StartControlEpoch(const TEpoch& epoch)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YASSERT(~EpochControlInvoker == NULL);
        EpochControlInvoker = New<TCancelableInvoker>(ControlInvoker);

        Epoch = epoch;
    }

    void StopControlEpoch()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LeaderId = NElection::InvalidPeerId;
        Epoch = TEpoch();

        YASSERT(~EpochControlInvoker != NULL);
        EpochControlInvoker->Cancel();
        EpochControlInvoker.Reset();
    }

    void StartStateEpoch()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YASSERT(~EpochStateInvoker == NULL);
        EpochStateInvoker = New<TCancelableInvoker>(GetStateInvoker());
    }

    void StopStateEpoch()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        YASSERT(~EpochStateInvoker != NULL);
        EpochStateInvoker->Cancel();
        EpochStateInvoker.Reset();
    }

    void OnApplyChange()
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

    TPeerPriority GetPriority()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto version = MetaState->GetReachableVersion();
        return ((TPeerPriority) version.SegmentId << 32) | version.RecordCount;
    }

    Stroka FormatPriority(TPeerPriority priority)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        i32 segmentId = (priority >> 32);
        i32 recordCount = priority & ((1ll << 32) - 1);
        return Sprintf("(%d, %d)", segmentId, recordCount);
    }

    // Blocks state thread until snapshot creation is finished.
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

        OnStartLeading_.Fire();
    }

    void DoStopLeading()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Leading || StateStatus == EPeerStatus::LeaderRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopStateEpoch();

        OnStopLeading_.Fire();
    }


    void DoStartFollowing()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Stopped);
        StateStatus = EPeerStatus::FollowerRecovery;

        StartStateEpoch();

        OnStartFollowing_.Fire();
    }

    void DoFollowerRecoveryComplete()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Following;

        OnFollowerRecoveryComplete_.Fire();
    }

    void DoStopFollowing()
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        YASSERT(StateStatus == EPeerStatus::Following || StateStatus == EPeerStatus::FollowerRecovery);
        StateStatus = EPeerStatus::Stopped;

        StopStateEpoch();

        OnStopFollowing_.Fire();
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReadThread);
};

////////////////////////////////////////////////////////////////////////////////

IMetaStateManager::TPtr CreatePersistentStateManager(
    const TPersistentStateManagerConfig& config,
    IInvoker* controlInvoker,
    IMetaState* metaState,
    NRpc::IRpcServer* server)
{
    return New<TPersistentStateManager>(
        config, controlInvoker, metaState, server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
