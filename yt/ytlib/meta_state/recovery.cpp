#include "stdafx.h"
#include "recovery.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"
#include "../misc/foreach.h"

// TODO: wrap with try/catch to handle IO errors

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TRecovery::TRecovery(
    const TMetaStateManagerConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr controlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , MetaState(metaState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , LeaderId(leaderId)
{
    YASSERT(~cellManager != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~changeLogCache != NULL);
    YASSERT(~snapshotStore != NULL);
    YASSERT(~controlInvoker != NULL);

    VERIFY_THREAD_AFFINITY(ControlThread);

    auto stateInvoker = metaState->GetStateInvoker();

    VERIFY_INVOKER_AFFINITY(stateInvoker, StateThread);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);

    CancelableStateInvoker = New<TCancelableInvoker>(stateInvoker);
    CancelableControlInvoker = New<TCancelableInvoker>(controlInvoker);
}

void TRecovery::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableControlInvoker->Cancel();
    CancelableStateInvoker->Cancel();
}

TRecovery::TAsyncResult::TPtr TRecovery::RecoverFromSnapshotAndChangeLog(
    TMetaVersion targetVersion,
    i32 snapshotId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto currentVersion = MetaState->GetVersion();

    LOG_INFO("Recovering state from %s to %s",
        ~currentVersion.ToString(),
        ~targetVersion.ToString());

    YASSERT(MetaState->GetVersion() <= targetVersion);

    // Check if loading a snapshot is preferable.
    // Currently this is done by comparing segmentIds and is subject to further optimization.
    if (snapshotId != NonexistingSnapshotId && currentVersion.SegmentId < snapshotId)
    {
        // Load the snapshot.
        LOG_DEBUG("Using snapshot %d for recovery", snapshotId);

        TSnapshotReader::TPtr snapshotReader = SnapshotStore->GetReader(snapshotId);
        if (~snapshotReader == NULL) {
            if (IsLeader()) {
                LOG_FATAL("Snapshot %d has vanished", snapshotId);
            }

            LOG_DEBUG("Snapshot is not found locally and will be downloaded");

            TSnapshotDownloader snapshotDownloader(
                // TODO: pass proper config
                TSnapshotDownloader::TConfig(),
                CellManager);

            auto snapshotWriter = SnapshotStore->GetWriter(snapshotId);
            auto snapshotResult = snapshotDownloader.GetSnapshot(
                snapshotId,
                ~snapshotWriter);

            if (snapshotResult != TSnapshotDownloader::EResult::OK) {
                LOG_ERROR("Error downloading snapshot (SnapshotId: %d, Result: %s)",
                    snapshotId,
                    ~snapshotResult.ToString());
                return New<TAsyncResult>(EResult::Failed);
            }

            SnapshotStore->UpdateMaxSnapshotId(snapshotId);

            snapshotReader = SnapshotStore->GetReader(snapshotId);
            if (~snapshotReader == NULL) {
                LOG_FATAL("Snapshot %d has vanished", snapshotId);
            }
        }

        snapshotReader->Open();
        auto* stream = &snapshotReader->GetStream();

        MetaState->Load(snapshotId, stream);

        // The reader reference is being held by the closure action.
        return RecoverFromChangeLog(
            targetVersion,
            snapshotReader->GetPrevRecordCount());
    } else {
        // Recovery using changelogs only.
        LOG_DEBUG("No snapshot is used for recovery");

        i32 prevRecordCount =
            currentVersion.SegmentId == 0
            ? NonexistingPrevRecordCount
            : UnknownPrevRecordCount;

        return RecoverFromChangeLog(
            targetVersion,
            prevRecordCount);
    }
}

TRecovery::TAsyncResult::TPtr TRecovery::RecoverFromChangeLog(
    TMetaVersion targetVersion,
    i32 expectedPrevRecordCount)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Iterate through the segments and apply the changelogs.
    for (i32 segmentId = MetaState->GetVersion().SegmentId;
         segmentId <= targetVersion.SegmentId;
         ++segmentId)
    {
        bool isFinal = segmentId == targetVersion.SegmentId;
        bool mayBeMissing = isFinal && targetVersion.RecordCount == 0 || !IsLeader();

        auto changeLog = ChangeLogCache->Get(segmentId);
        if (~changeLog == NULL) {
            if (!mayBeMissing) {
                LOG_FATAL("A required changelog %d is missing", segmentId);
            }

            LOG_INFO("Changelog %d is missing and will be created", segmentId);
            YASSERT(expectedPrevRecordCount != UnknownPrevRecordCount);
            changeLog = ChangeLogCache->Create(segmentId, expectedPrevRecordCount);
        }

        LOG_DEBUG("Found changelog (Id: %d, RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
            segmentId,
            changeLog->GetRecordCount(),
            changeLog->GetPrevRecordCount(),
            ~ToString(isFinal));

        if (expectedPrevRecordCount != UnknownPrevRecordCount &&
            changeLog->GetPrevRecordCount() != expectedPrevRecordCount)
        {
            LOG_FATAL("PrevRecordCount mismatch (Expected: %d, Actual: %d)",
                expectedPrevRecordCount,
                changeLog->GetPrevRecordCount());
        }

        if (!IsLeader()) {
            auto proxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
            
            auto request = proxy->GetChangeLogInfo();
            request->SetChangeLogId(segmentId);

            auto response = request->Invoke(Config.RpcTimeout)->Get();
            if (!response->IsOK()) {
                LOG_ERROR("Could not get changelog %d info from leader", segmentId);
                return New<TAsyncResult>(EResult::Failed);
            }

            i32 localRecordCount = changeLog->GetRecordCount();
            i32 remoteRecordCount = response->GetRecordCount();

            LOG_INFO("Changelog %d has %d local record(s), %d remote record(s)",
                segmentId,
                localRecordCount,
                remoteRecordCount);

            if (segmentId == targetVersion.SegmentId &&
                remoteRecordCount < targetVersion.RecordCount)
            {
                LOG_FATAL("Remote changelog has insufficient records to reach the requested state");
            }

            if (localRecordCount > remoteRecordCount) {
                changeLog->Truncate(remoteRecordCount);
                changeLog->Finalize();
                LOG_INFO("Local changelog is longer than expected, truncated to %d records (ChangeLogId: %d)",
                    remoteRecordCount,
                    segmentId);

                auto currentVersion = MetaState->GetVersion();
                YASSERT(currentVersion.SegmentId <= segmentId);

                if (currentVersion.SegmentId == segmentId && currentVersion.RecordCount > remoteRecordCount) {
                    LOG_INFO("Current state contains uncommitted changes, restarting with a clear one");

                    MetaState->Clear();
                    return FromMethod(&TRecovery::Run, TPtr(this))
                           ->AsyncVia(~CancelableControlInvoker)
                           ->Do();
                }
            }

            // Do not download more than actually needed.
            int desiredRecordCount =
                segmentId == targetVersion.SegmentId
                ? targetVersion.RecordCount
                : remoteRecordCount;
            
            if (localRecordCount < desiredRecordCount) {
                // TODO: provide config
                TChangeLogDownloader changeLogDownloader(
                    TChangeLogDownloader::TConfig(),
                    CellManager);
                auto changeLogResult = changeLogDownloader.Download(
                    TMetaVersion(segmentId, desiredRecordCount),
                    *changeLog);

                if (changeLogResult != TChangeLogDownloader::EResult::OK) {
                    LOG_ERROR("Error downloading changelog (ChangeLogId: %d, Result: %s)",
                        segmentId,
                        ~changeLogResult.ToString());
                    return New<TAsyncResult>(EResult::Failed);
                }
            }
        }

        if (!isFinal && !changeLog->IsFinalized()) {
            LOG_INFO("Finalizing an intermediate changelog %d", segmentId);
            changeLog->Finalize();
        }

        if (segmentId == targetVersion.SegmentId) {
            YASSERT(changeLog->GetRecordCount() == targetVersion.RecordCount);
        }

        ApplyChangeLog(*changeLog, changeLog->GetRecordCount());

        if (!isFinal) {
            MetaState->AdvanceSegment();
        }

        expectedPrevRecordCount = changeLog->GetRecordCount();
    }

    YASSERT(MetaState->GetVersion() == targetVersion);

    return New<TAsyncResult>(EResult::OK);
}

void TRecovery::ApplyChangeLog(
    TAsyncChangeLog& changeLog,
    i32 targetRecordCount)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(MetaState->GetVersion().SegmentId == changeLog.GetId());
    
    i32 startRecordId = MetaState->GetVersion().RecordCount;
    i32 recordCount = targetRecordCount - startRecordId;

    if (recordCount == 0)
        return;

    LOG_INFO("Reading records %d-%d from changelog %d",
        startRecordId,
        targetRecordCount - 1, 
        changeLog.GetId());

    yvector<TSharedRef> records;
    changeLog.Read(startRecordId, recordCount, &records);
    if (records.ysize() < recordCount) {
        LOG_FATAL("Not enough records in changelog"
            "(ChangeLogId: %d, StartRecordId: %d, ExpectedRecordCount: %d, ActualRecordCount: %d)",
            changeLog.GetId(),
            startRecordId,
            recordCount,
            records.ysize());
    }

    // TODO: timing
    LOG_INFO("Applying changes to meta state");

    for (i32 i = 0; i < recordCount; ++i)  {
        MetaState->ApplyChange(records[i]);
    }

    LOG_INFO("Finished applying changes");
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    const TMetaStateManagerConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr controlInvoker)
    : TRecovery(
        config,    
        cellManager,
        metaState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        controlInvoker)
{
    YASSERT(~cellManager != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~changeLogCache != NULL);
    YASSERT(~snapshotStore != NULL);
    YASSERT(~controlInvoker != NULL);

    VERIFY_THREAD_AFFINITY(ControlThread);
}

TRecovery::TAsyncResult::TPtr TLeaderRecovery::Run()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto version = MetaState->GetReachableVersion();
    i32 maxAvailableSnapshotId = SnapshotStore->GetMaxSnapshotId();
    YASSERT(maxAvailableSnapshotId <= version.SegmentId);

    return FromMethod(
               &TRecovery::RecoverFromSnapshotAndChangeLog,
               TPtr(this),
               version,
               maxAvailableSnapshotId)
           ->AsyncVia(~CancelableStateInvoker)
           ->Do();
}

bool TLeaderRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerRecovery::TFollowerRecovery(
    const TMetaStateManagerConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr controlInvoker)
    : TRecovery(
        config,
        cellManager,
        metaState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        controlInvoker)
    , Result(New<TAsyncResult>())
    , SyncReceived(false)
{
    YASSERT(~cellManager != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~changeLogCache != NULL);
    YASSERT(~snapshotStore != NULL);
    YASSERT(~controlInvoker != NULL);

    VERIFY_THREAD_AFFINITY(ControlThread);
}

TRecovery::TAsyncResult::TPtr TFollowerRecovery::Run()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Synchronizing with leader");

    TAutoPtr<TProxy> leaderProxy(CellManager->GetMasterProxy<TProxy>(LeaderId));
    auto request = leaderProxy->Sync();
    request->Invoke(Config.SyncTimeout)->Subscribe(
        FromMethod(&TFollowerRecovery::OnSync, TPtr(this))
        ->Via(~CancelableControlInvoker));

    return Result;
}

void TFollowerRecovery::OnSync(TProxy::TRspSync::TPtr response)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!response->IsOK()) {
        LOG_ERROR("Error synchronizing with leader (ErrorCode: %s)",
            ~response->GetErrorCode().ToString());
        Result->Set(EResult::Failed);
        return;
    }

    YASSERT(!SyncReceived);
    SyncReceived = true;

    TMetaVersion version(
        response->GetSegmentId(),
        response->GetRecordCount());
    auto epoch = TEpoch::FromProto(response->GetEpoch());
    i32 maxSnapshotId = response->GetMaxSnapshotId();

    if (epoch != Epoch) {
        LOG_ERROR("Invalid epoch (expected: %s, received: %s)",
            ~Epoch.ToString(),
            ~epoch.ToString());
        Result->Set(EResult::Failed);
        return;
    }

    LOG_INFO("Sync received (Version: %s, Epoch: %s, MaxSnapshotId: %d)",
        ~version.ToString(),
        ~epoch.ToString(),
        maxSnapshotId);

    PostponedVersion = version;
    YASSERT(PostponedChanges.ysize() == 0);

    i32 snapshotId = Max(maxSnapshotId, SnapshotStore->GetMaxSnapshotId());

    FromMethod(
        &TRecovery::RecoverFromSnapshotAndChangeLog,
        TPtr(this),
        PostponedVersion,
        snapshotId)
    ->AsyncVia(~CancelableStateInvoker)
    ->Do()->Apply(FromMethod(
        &TFollowerRecovery::OnSyncReached,
        TPtr(this)))
    ->Subscribe(FromMethod(
        &TAsyncResult::Set,
        Result));
}

TRecovery::TAsyncResult::TPtr TFollowerRecovery::OnSyncReached(EResult result)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (result != EResult::OK) {
        return New<TAsyncResult>(result);
    }

    LOG_INFO("Sync reached");

    return FromMethod(&TFollowerRecovery::CapturePostponedChanges, TPtr(this))
           ->AsyncVia(~CancelableControlInvoker)
           ->Do();
}

TRecovery::TAsyncResult::TPtr TFollowerRecovery::CapturePostponedChanges()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // TODO: use threshold?
    if (PostponedChanges.ysize() == 0) {
        LOG_INFO("No postponed changes left");
        return New<TAsyncResult>(EResult::OK);
    }

    THolder<TPostponedChanges> changes(new TPostponedChanges());
    changes->swap(PostponedChanges);

    LOG_INFO("Captured postponed changes (ChangeCount: %d)", changes->ysize());

    return FromMethod(
               &TFollowerRecovery::ApplyPostponedChanges,
               TPtr(this),
               changes)
           ->AsyncVia(~CancelableStateInvoker)
           ->Do();
}

TRecovery::TAsyncResult::TPtr TFollowerRecovery::ApplyPostponedChanges(
    TAutoPtr<TPostponedChanges> changes)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Applying postponed changes (ChangeCount: %d)", changes->ysize());
    
    FOREACH(const auto& change, *changes) {
        switch (change.Type) {
            case TPostponedChange::EType::Change: {
                auto version = MetaState->GetVersion();
                MetaState->LogChange(version, change.ChangeData);
                MetaState->ApplyChange(change.ChangeData);
                break;
            }

            case TPostponedChange::EType::SegmentAdvance:
                MetaState->RotateChangeLog();
                break;

            default:
                YUNREACHABLE();
        }
    }
   
    LOG_INFO("Finished applying postponed changes");

    return FromMethod(
               &TFollowerRecovery::CapturePostponedChanges,
               TPtr(this))
           ->AsyncVia(~CancelableControlInvoker)
           ->Do();
}

TRecovery::EResult TFollowerRecovery::PostponeSegmentAdvance(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!SyncReceived) {
        LOG_DEBUG("Segment advance received before sync, ignored");
        return EResult::OK;
    }

    if (PostponedVersion != version) {
        LOG_WARNING("Out-of-order postponed segment advance received (expected: %s, received: %s)",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return EResult::Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateSegmentAdvance());
    
    LOG_DEBUG("Enqueued postponed segment advance %s",
        ~PostponedVersion.ToString());

    ++PostponedVersion.SegmentId;
    PostponedVersion.RecordCount = 0;
    
    return EResult::OK;
}

TRecovery::EResult TFollowerRecovery::PostponeChange(
    const TMetaVersion& version,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!SyncReceived) {
        LOG_DEBUG("Change received before sync, ignored");
        return EResult::OK;
    }

    if (PostponedVersion != version) {
        LOG_WARNING("Out-of-order postponed change received (ExpectedVersion: %s, Version: %s)",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return EResult::Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateChange(changeData));
    
    LOG_DEBUG("Enqueued postponed change %s",
        ~PostponedVersion.ToString());

    ++PostponedVersion.RecordCount;

    return EResult::OK;
}

bool TFollowerRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
