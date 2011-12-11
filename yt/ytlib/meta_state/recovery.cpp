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
    const TPersistentStateManagerConfig& config,
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

        auto readerResult = SnapshotStore->GetReader(snapshotId);
        if (!readerResult.IsOK()) {
            if (IsLeader()) {
                LOG_FATAL("Snapshot is not available\n%s", ~readerResult.ToString());
            }

            LOG_DEBUG("Snapshot cannot be found locally and will be downloaded");

            TSnapshotDownloader snapshotDownloader(
                // TODO: pass proper config
                TSnapshotDownloader::TConfig(),
                CellManager);

            auto snapshotWriter = SnapshotStore->GetRawWriter(snapshotId);

            auto downloadResult = snapshotDownloader.GetSnapshot(snapshotId, ~snapshotWriter);
            if (downloadResult != TSnapshotDownloader::EResult::OK) {
                LOG_ERROR("Error downloading snapshot (SnapshotId: %d, Result: %s)",
                    snapshotId,
                    ~downloadResult.ToString());
                return New<TAsyncResult>(EResult::Failed);
            }

            SnapshotStore->UpdateMaxSnapshotId(snapshotId);

            readerResult = SnapshotStore->GetReader(snapshotId);
            if (!readerResult.IsOK()) {
                LOG_FATAL("Snapshot is not available\n%s", ~readerResult.ToString());
            }
        }

        auto snapshotReader = readerResult.Value();
        snapshotReader->Open();
        MetaState->Load(snapshotId, &snapshotReader->GetStream());

        // The reader reference is being held by the closure action.
        return RecoverFromChangeLog(    
            targetVersion,
            snapshotReader->GetPrevRecordCount());
    } else {
        // Recover using changelogs only.
        LOG_DEBUG("No snapshot is used for recovery");

        i32 prevRecordCount =
            currentVersion.SegmentId == 0
            ? NonexistingPrevRecordCount
            : UnknownPrevRecordCount;

        return RecoverFromChangeLog(targetVersion, prevRecordCount);
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

        TCachedAsyncChangeLog::TPtr changeLog;
        auto changeLogResult = ChangeLogCache->Get(segmentId);
        if (!changeLogResult.IsOK()) {
            if (!mayBeMissing) {
                LOG_FATAL("Changelog is not available\n%s", ~changeLogResult.ToString());
            }

            LOG_INFO("Changelog %d is missing and will be created", segmentId);
            YASSERT(expectedPrevRecordCount != UnknownPrevRecordCount);
            changeLog = ChangeLogCache->Create(segmentId, expectedPrevRecordCount);
        } else {
            changeLog = changeLogResult.Value();
        }

        LOG_DEBUG("Changelog found (ChangeLogId: %d, RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
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
            proxy->SetTimeout(Config.RpcTimeout);

            auto request = proxy->GetChangeLogInfo();
            request->set_changelogid(segmentId);

            auto response = request->Invoke()->Get();
            if (!response->IsOK()) {
                LOG_ERROR("Error getting changelog info from leader (ChangeLogId: %d)\n%s",
                    segmentId,
                    ~response->GetError().ToString());
                return New<TAsyncResult>(EResult::Failed);
            }

            i32 localRecordCount = changeLog->GetRecordCount();
            i32 remoteRecordCount = response->recordcount();

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
            }

            auto currentVersion = MetaState->GetVersion();
            YASSERT(currentVersion.SegmentId <= segmentId);

            // Check if the current state contains some changes that are not present in remote changelog.
            // If so, clear the state and restart recovery.
            if (currentVersion.SegmentId == segmentId && currentVersion.RecordCount > remoteRecordCount) {
                LOG_INFO("Current state contains uncommitted changes, restarting with a clear one");

                MetaState->Clear();
                return FromMethod(&TRecovery::Run, TPtr(this))
                        ->AsyncVia(~CancelableControlInvoker)
                        ->Do();
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

    FOREACH (const auto& changeData, records)  {
        auto version = MetaState->GetVersion();
        try {
            MetaState->ApplyChange(changeData);
        } catch (...) {
            LOG_DEBUG("Failed to apply the change during recovery (Version: %s)\n%s",
                ~version.ToString(),
                ~CurrentExceptionMessage());
        }
    }

    LOG_INFO("Finished applying changes");
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    const TPersistentStateManagerConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    IInvoker::TPtr controlInvoker)
    : TRecovery(
        config,
        cellManager,
        metaState,
        changeLogCache,
        snapshotStore,
        epoch,
        cellManager->GetSelfId(),
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
    const TPersistentStateManagerConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr controlInvoker,
    TMetaVersion targetVersion,
    i32 maxSnapshotId)
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
    , TargetVersion(targetVersion)
    , MaxSnapshotId(maxSnapshotId)
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

    PostponedVersion = TargetVersion;
    YASSERT(PostponedChanges.empty());

    FromMethod(
        &TRecovery::RecoverFromSnapshotAndChangeLog,
        TPtr(this),
        TargetVersion,
        MaxSnapshotId)
    ->AsyncVia(~CancelableStateInvoker)
    ->Do()->Apply(FromMethod(
        &TFollowerRecovery::OnSyncReached,
        TPtr(this)))
    ->Subscribe(FromMethod(
        &TAsyncResult::Set,
        Result));

    return Result;
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
    if (PostponedChanges.empty()) {
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
                try {
                    MetaState->ApplyChange(change.ChangeData);
                } catch (...) {
                    LOG_DEBUG("Failed to apply the change during recovery (Version: %s)\n%s",
                        ~version.ToString(),
                        ~CurrentExceptionMessage());
                }
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

    if (PostponedVersion > version) {
        LOG_DEBUG("Late segment advance received during recovery, ignored (ExpectedVersion: %s, ReceivedVersion: %s)",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return EResult::OK;
    }

    if (PostponedVersion < version) {
        LOG_WARNING("Out-of-order segment advance received during recovery (ExpectedVersion: %s, ReceivedVersion: %s)",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return EResult::Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateSegmentAdvance());
    
    LOG_DEBUG("Postponing segment advance (Version: %s)", ~PostponedVersion.ToString());

    ++PostponedVersion.SegmentId;
    PostponedVersion.RecordCount = 0;
    
    return EResult::OK;
}

TRecovery::EResult TFollowerRecovery::PostponeChanges(
    const TMetaVersion& version,
    const yvector<TSharedRef>& changes)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (PostponedVersion > version) {
        LOG_WARNING("Late changes received during recovery, ignored (ExpectedVersion: %s, ReceivedVersion: %s)",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return EResult::Failed;
    }

    if (PostponedVersion != version) {
        LOG_WARNING("Out-of-order changes received during recovery (ExpectedVersion: %s, ReceivedVersion: %s)",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return EResult::Failed;
    }

    LOG_DEBUG("Postponing changes (Version: %s, ChangeCount: %d)",
        ~PostponedVersion.ToString(),
        changes.ysize());

    FOREACH (const auto& change, changes) {
        PostponedChanges.push_back(TPostponedChange::CreateChange(change));
    }
    
    PostponedVersion.RecordCount += changes.ysize();

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
