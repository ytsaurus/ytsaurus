#include "recovery.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"

// TODO: wrap with try/catch to handle IO errors

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

// TODO: make configurable
static TDuration SyncTimeout = TDuration::MilliSeconds(5000);

////////////////////////////////////////////////////////////////////////////////

TRecovery::TRecovery(
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr serviceInvoker)
    : CellManager(cellManager)
    , MetaState(metaState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , LeaderId(leaderId)
    , CancelableServiceInvoker(New<TCancelableInvoker>(serviceInvoker))
    , CancelableStateInvoker(New<TCancelableInvoker>(metaState->GetInvoker()))
{ }

void TRecovery::Stop()
{
    CancelableServiceInvoker->Cancel();
    CancelableStateInvoker->Cancel();
}

TRecovery::TResult::TPtr TRecovery::RecoverFromSnapshot(
    TMetaVersion targetVersion,
    i32 snapshotId)
{
    LOG_INFO("Recovering state from %s to %s",
        ~MetaState->GetVersion().ToString(),
        ~targetVersion.ToString());

    YASSERT(MetaState->GetVersion() <= targetVersion);

    // Check if loading a snapshot is preferable.
    // Currently this is done by comparing segmentIds and is subject to further optimization.
    if (snapshotId != NonexistingSnapshotId && MetaState->GetVersion().SegmentId < snapshotId)
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

            TSnapshotWriter::TPtr snapshotWriter = SnapshotStore->GetWriter(snapshotId);
            TSnapshotDownloader::EResult snapshotResult = snapshotDownloader.GetSnapshot(
                snapshotId,
                ~snapshotWriter);

            if (snapshotResult != TSnapshotDownloader::EResult::OK) {
                LOG_ERROR("Error downloading snapshot (SnapshotId: %d, Result: %s)",
                    snapshotId,
                    ~snapshotResult.ToString());
                return New<TResult>(EResult::Failed);
            }

            snapshotReader = SnapshotStore->GetReader(snapshotId);
            if (~snapshotReader == NULL) {
                LOG_FATAL("Snapshot %d has vanished", snapshotId);
            }
        }

        snapshotReader->Open();
        TInputStream* stream = &snapshotReader->GetStream();

        // The reader reference is being held by the closure action.
        return MetaState
            ->Load(snapshotId, stream)
            ->Apply(
                FromMethod(
                    &TRecovery::RecoverFromChangeLog,
                    TPtr(this),
                    snapshotReader,
                    targetVersion,
                    snapshotReader->GetPrevRecordCount())
                ->AsyncVia(~CancelableStateInvoker));
    } else {
        // Recovery solely using changelogs.
        LOG_DEBUG("No snapshot is used for recovery");

        i32 prevRecordCount =
            targetVersion.SegmentId == 0
            ? NonexistingPrevRecordCount
            : UnknownPrevRecordCount;

        return RecoverFromChangeLog(
            TVoid(),
            TSnapshotReader::TPtr(),
            targetVersion,
            prevRecordCount);
    }
}

TRecovery::TResult::TPtr TRecovery::RecoverFromChangeLog(
    TVoid,
    TSnapshotReader::TPtr,
    TMetaVersion targetVersion,
    i32 expectedPrevRecordCount)
{
    // Iterate through the segments and apply the changelogs.
    for (i32 segmentId = MetaState->GetVersion().SegmentId;
         segmentId <= targetVersion.SegmentId;
         ++segmentId)
    {
        bool isFinal = segmentId == targetVersion.SegmentId;
        bool mayBeMissing = isFinal && targetVersion.RecordCount == 0 || !IsLeader();

        TCachedAsyncChangeLog::TPtr changeLog = ChangeLogCache->Get(segmentId);
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
            THolder<TProxy> leaderProxy(CellManager->GetMasterProxy<TProxy>(LeaderId));
            TProxy::TReqGetChangeLogInfo::TPtr request = leaderProxy->GetChangeLogInfo();
            request->SetSegmentId(segmentId);
             // TODO: timeout
            TProxy::TRspGetChangeLogInfo::TPtr response = request->Invoke()->Get();
            if (!response->IsOK()) {
                LOG_ERROR("Could not get changelog %d info from leader", segmentId);
                return New<TResult>(EResult::Failed);
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
                LOG_INFO("Local changelog %d is longer than expected, truncated to %d records",
                    segmentId,
                    remoteRecordCount);
            }

            // Do not download more than actually needed.
            int desiredRecordCount =
                segmentId == targetVersion.SegmentId
                ? targetVersion.RecordCount
                : remoteRecordCount;
            
            if (localRecordCount < desiredRecordCount) {
                TChangeLogDownloader changeLogDownloader(
                    TChangeLogDownloader::TConfig(),
                    CellManager);
                TChangeLogDownloader::EResult changeLogResult = changeLogDownloader.Download(
                    TMetaVersion(segmentId, desiredRecordCount),
                    *changeLog);

                if (changeLogResult != TChangeLogDownloader::EResult::OK) {
                    LOG_ERROR("Error downloading changelog (ChangeLogId: %d, Result: %s)",
                        segmentId,
                        ~changeLogResult.ToString());
                    return New<TResult>(EResult::Failed);
                }
            }
        }

        if (!isFinal && !changeLog->IsFinalized()) {
            LOG_WARNING("Forcing finalization of an intermediate changelog %d", segmentId);
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

    return New<TResult>(EResult::OK);
}

void TRecovery::ApplyChangeLog(
    TAsyncChangeLog& changeLog,
    i32 targetRecordCount)
{
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
        LOG_FATAL("Not enough records in the changelog"
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
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr serviceInvoker)
    : TRecovery(
        cellManager,
        metaState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        serviceInvoker)
{ }

TRecovery::TResult::TPtr TLeaderRecovery::Run()
{
    TMetaVersion version = MetaState->GetNextVersion();

    i32 maxAvailableSnapshotId = SnapshotStore->GetMaxSnapshotId();
    YASSERT(maxAvailableSnapshotId <= version.SegmentId);

    return FromMethod(
               &TRecovery::RecoverFromSnapshot,
               TPtr(this),
               version,
               maxAvailableSnapshotId)
           ->AsyncVia(~CancelableStateInvoker)
           ->Do();
}

bool TLeaderRecovery::IsLeader() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerRecovery::TFollowerRecovery(
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    TPeerId leaderId,
    IInvoker::TPtr serviceInvoker)
    : TRecovery(
        cellManager,
        metaState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        serviceInvoker)
    , Result(New<TResult>())
    , SyncReceived(false)
{ }

TRecovery::TResult::TPtr TFollowerRecovery::Run()
{
    LOG_INFO("Requesting sync from leader");

    TDelayedInvoker::Get()->Submit(
        FromMethod(&TFollowerRecovery::OnSyncTimeout, TPtr(this))
        ->Via(~CancelableServiceInvoker),
        SyncTimeout);

    TAutoPtr<TProxy> leaderProxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    TProxy::TReqScheduleSync::TPtr request = leaderProxy->ScheduleSync();
    request->SetPeerId(CellManager->GetSelfId());
    request->Invoke();

    return Result;
}

void TFollowerRecovery::OnSyncTimeout()
{
    if (SyncReceived)
        return;

    LOG_INFO("Sync timeout");
    
    Result->Set(EResult::Failed);
}

void TFollowerRecovery::Sync(
    const TMetaVersion& version,
    const TEpoch& epoch,
    i32 maxSnapshotId)
{
    if (SyncReceived) {
        LOG_WARNING("Duplicate sync received");
        return;
    }
        
    SyncReceived = true;

    LOG_INFO("Sync received (Version: %s, Epoch: %s, MaxSnapshotId: %d)",
        ~version.ToString(),
        ~epoch.ToString(),
        maxSnapshotId);

    PostponedVersion = version;
    YASSERT(PostponedChanges.ysize() == 0);

    i32 snapshotId = Max(maxSnapshotId, SnapshotStore->GetMaxSnapshotId());

    FromMethod(
        &TRecovery::RecoverFromSnapshot,
        TPtr(this),
        PostponedVersion,
        snapshotId)
    ->AsyncVia(~CancelableStateInvoker)
    ->Do()->Apply(FromMethod(
        &TFollowerRecovery::OnSyncReached,
        TPtr(this)))
    ->Subscribe(FromMethod(
        &TResult::Set,
        Result));
}

TRecovery::TResult::TPtr TFollowerRecovery::OnSyncReached(EResult result)
{
    if (result != EResult::OK) {
        return New<TResult>(result);
    }

    LOG_INFO("Sync reached");

    return FromMethod(&TFollowerRecovery::CapturePostponedChanges, TPtr(this))
           ->AsyncVia(~CancelableServiceInvoker)
           ->Do();
}

TRecovery::TResult::TPtr TFollowerRecovery::CapturePostponedChanges()
{
    // TODO: use threshold?
    if (PostponedChanges.ysize() == 0) {
        LOG_INFO("No postponed changes left");
        return New<TResult>(EResult::OK);
    }

    THolder<TPostponedChanges> changes(new TPostponedChanges());
    changes->swap(PostponedChanges);

    LOG_INFO("Captured %d postponed changes", changes->ysize());

    return FromMethod(
               &TFollowerRecovery::ApplyPostponedChanges,
               TPtr(this),
               changes)
           ->AsyncVia(~CancelableStateInvoker)
           ->Do();
}

TRecovery::TResult::TPtr TFollowerRecovery::ApplyPostponedChanges(
    TAutoPtr<TPostponedChanges> changes)
{
    LOG_INFO("Applying %d postponed changes", changes->ysize());
    
    for (TPostponedChanges::const_iterator it = changes->begin();
         it != changes->end();
         ++it)
    {
        const TPostponedChange& change = *it;
        switch (change.Type) {
            case TPostponedChange::EType::Change:
                MetaState->LogChange(change.ChangeData);
                MetaState->ApplyChange(change.ChangeData);
                break;

            case TPostponedChange::EType::SegmentAdvance:
                MetaState->RotateChangeLog();
                break;

            default:
                YASSERT(false);
                break;
        }
    }
   
    LOG_INFO("Finished applying postponed changes");

    return FromMethod(
               &TFollowerRecovery::CapturePostponedChanges,
               TPtr(this))
           ->AsyncVia(~CancelableServiceInvoker)
           ->Do();
}

TRecovery::EResult TFollowerRecovery::PostponeSegmentAdvance(
    const TMetaVersion& version)
{
    if (!SyncReceived) {
        LOG_DEBUG("Postponed segment advance received before sync, ignored");
        return EResult::OK;
    }

    if (PostponedVersion != version) {
        LOG_WARNING("Out-of-order postponed segment advance received (ExpectedVersion: %s, Version: %s)",
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
    if (!SyncReceived) {
        LOG_DEBUG("Postponed change received before sync, ignored");
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
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
