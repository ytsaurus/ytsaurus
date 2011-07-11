#include "recovery.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"
#include "../misc/string.h"

// TODO: wrap with try/catch to handle IO errors

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

// TODO: make configurable
static TDuration SyncTimeout = TDuration::MilliSeconds(5000);

////////////////////////////////////////////////////////////////////////////////

TRecovery::TRecovery(
    TCellManager::TPtr cellManager,
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TMasterEpoch epoch,
    TMasterId leaderId,
    IInvoker::TPtr invoker,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr workQueue)
    : CellManager(cellManager)
    , MasterState(masterState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , LeaderId(leaderId)
    , ServiceInvoker(invoker)
    , EpochInvoker(epochInvoker)
    , WorkQueue(workQueue)
{ }

TRecovery::TResult::TPtr TRecovery::RecoverFromSnapshot(
    TMasterStateId targetStateId,
    i32 snapshotId)
{
    LOG_INFO("Recovering state from %s to %s",
        ~MasterState->GetStateId().ToString(),
        ~targetStateId.ToString());

    YASSERT(MasterState->GetStateId() <= targetStateId);

    // Check if loading a snapshot is preferable.
    // Currently this is done by comparing segmentIds and is subject to further optimization.
    if (snapshotId != NonexistingSnapshotId && MasterState->GetStateId().SegmentId < snapshotId)
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
                // TODO: ToString()
                LOG_ERROR("Error downloading snapshot (SnapshotId: %d, Result: %s)",
                    snapshotId,
                    ~snapshotResult.ToString());
                return new TResult(EResult::Failed);
            }

            snapshotReader = SnapshotStore->GetReader(snapshotId);
            if (~snapshotReader == NULL) {
                LOG_FATAL("Snapshot %d has vanished", snapshotId);
            }
        }

        snapshotReader->Open();
        TInputStream& stream = snapshotReader->GetStream();

        // The reader reference is being held by the closure action.
        return MasterState->Load(snapshotId, stream)->Apply(FromMethod(
                  &TRecovery::RecoverFromChangeLog,
                  TPtr(this),
                  snapshotReader,
                  targetStateId,
                  snapshotReader->GetPrevRecordCount())->AsyncVia(WorkQueue));
    } else {
        // Recovery solely using changelogs.
        LOG_DEBUG("No snapshot is used for recovery");

        i32 prevRecordCount =
            targetStateId.SegmentId == 0
            ? NonexistingPrevRecordCount
            : UnknownPrevRecordCount;

        return RecoverFromChangeLog(
            TVoid(),
            TSnapshotReader::TPtr(),
            targetStateId,
            prevRecordCount);
    }
}

TRecovery::TResult::TPtr TRecovery::RecoverFromChangeLog(
    TVoid,
    TSnapshotReader::TPtr,
    TMasterStateId targetStateId,
    i32 expectedPrevRecordCount)
{
    // Iterate through the segments and apply the changelogs.
    for (i32 segmentId = MasterState->GetStateId().SegmentId;
         segmentId <= targetStateId.SegmentId;
         ++segmentId)
    {
        bool isFinal = segmentId == targetStateId.SegmentId;
        bool mayBeMissing = isFinal && targetStateId.ChangeCount == 0 || !IsLeader();

        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            if (!mayBeMissing) {
                LOG_FATAL("A required changelog %d is missing", segmentId);
            }

            LOG_INFO("Changelog %d is missing and will be created", segmentId);
            YASSERT(expectedPrevRecordCount != UnknownPrevRecordCount);
            cachedChangeLog = ChangeLogCache->Create(segmentId, expectedPrevRecordCount);
        }

        TAsyncChangeLog& changeLog = cachedChangeLog->GetWriter();

        LOG_DEBUG("Found changelog (Id: %d, RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
            segmentId,
            changeLog.GetRecordCount(),
            changeLog.GetPrevRecordCount(),
            ~ToString(isFinal));

        if (changeLog.GetPrevRecordCount() != expectedPrevRecordCount) {
            LOG_FATAL("PrevRecordCount mismatch (Expected: %d, Actual: %d)",
                expectedPrevRecordCount,
                changeLog.GetPrevRecordCount());
        }

        if (!IsLeader()) {
            THolder<TProxy> leaderProxy(CellManager->GetMasterProxy<TProxy>(LeaderId));
            TProxy::TReqGetChangeLogInfo::TPtr request = leaderProxy->GetChangeLogInfo();
            request->SetSegmentId(segmentId);
             // TODO: timeout
            TProxy::TRspGetChangeLogInfo::TPtr response = request->Invoke()->Get();
            if (!response->IsOK()) {
                LOG_ERROR("Could not get changelog %d info from leader", segmentId);
                return new TResult(EResult::Failed);
            }

            i32 localRecordCount = changeLog.GetRecordCount();
            i32 remoteRecordCount = response->GetRecordCount();

            LOG_INFO("Changelog %d has %d local record(s), %d remote record(s)",
                segmentId,
                localRecordCount,
                remoteRecordCount);

            if (segmentId == targetStateId.SegmentId &&
                remoteRecordCount < targetStateId.ChangeCount)
            {
                LOG_FATAL("Remote changelog has insufficient records to reach the requested state");
            }

            i32 targetChangeCount =
                segmentId == targetStateId.SegmentId
                ? targetStateId.ChangeCount
                : remoteRecordCount;

            if (remoteRecordCount < targetChangeCount) {
                changeLog.Truncate(remoteRecordCount);
                changeLog.Finalize();
                LOG_INFO("Local changelog %d is longer than expected, truncated to %d records",
                    segmentId,
                    remoteRecordCount);
            } else if (localRecordCount < targetChangeCount) {
                // TODO: extract method
                TChangeLogDownloader changeLogDownloader(
                    TChangeLogDownloader::TConfig(),
                    CellManager);
                TChangeLogDownloader::EResult changeLogResult = changeLogDownloader.Download(
                    TMasterStateId(segmentId, targetChangeCount),
                    changeLog);

                if (changeLogResult != TChangeLogDownloader::EResult::OK) {
                    LOG_ERROR("Error downloading changelog (ChangeLogId: %d, Result: %s)",
                        segmentId,
                        ~changeLogResult.ToString());
                    return new TResult(EResult::Failed);
                }
            }
        }

        if (!isFinal && !changeLog.IsFinalized()) {
            LOG_WARNING("Forcing finalization of an intermediate changelog %d", segmentId);
            changeLog.Finalize();
        }

        // Apply the whole changelog.
        ApplyChangeLog(changeLog, changeLog.GetRecordCount());

        if (!isFinal) {
            MasterState->AdvanceSegment();
        }

        expectedPrevRecordCount = changeLog.GetRecordCount();
    }

    YASSERT(MasterState->GetStateId() == targetStateId);

    return new TResult(EResult::OK);
}

void TRecovery::ApplyChangeLog(
    TAsyncChangeLog& changeLog,
    i32 targetChangeCount)
{
    YASSERT(MasterState->GetStateId().SegmentId == changeLog.GetId());
    
    i32 startRecordId = MasterState->GetStateId().ChangeCount;
    i32 recordCount = targetChangeCount - startRecordId;

    if (recordCount == 0)
        return;

    LOG_INFO("Reading records %d-%d from changelog %d",
        startRecordId,
        targetChangeCount - 1, 
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
    LOG_INFO("Applying changes to master state");

    for (i32 i = 0; i < recordCount; ++i)  {
        MasterState->ApplyChange(records[i]);
    }

    LOG_INFO("Finished applying changes");
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    TCellManager::TPtr cellManager,
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TMasterEpoch epoch,
    TMasterId leaderId,
    IInvoker::TPtr serviceInvoker,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr workQueue)
    : TRecovery(
        cellManager,
        masterState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        serviceInvoker,
        epochInvoker,
        workQueue)
{ }

TRecovery::TResult::TPtr TLeaderRecovery::Run()
{
    TMasterStateId stateId = MasterState->GetAvailableStateId();

    i32 maxAvailableSnapshotId = SnapshotStore->GetMaxSnapshotId();
    YASSERT(maxAvailableSnapshotId <= stateId.SegmentId);

    return FromMethod(
               &TRecovery::RecoverFromSnapshot,
               TPtr(this),
               stateId,
               maxAvailableSnapshotId)
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(WorkQueue)
           ->Do();
}

bool TLeaderRecovery::IsLeader() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerRecovery::TFollowerRecovery(
    TCellManager::TPtr cellManager,
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TMasterEpoch epoch,
    TMasterId leaderId,
    IInvoker::TPtr serviceInvoker,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr workQueue)
    : TRecovery(
        cellManager,
        masterState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        serviceInvoker,
        epochInvoker,
        workQueue)
    , Result(new TResult())
    , SyncReceived(false)
{ }

TRecovery::TResult::TPtr TFollowerRecovery::Run()
{
    LOG_INFO("Requesting sync from leader");

    TDelayedInvoker::Get()->Submit(
        FromMethod(&TFollowerRecovery::OnSyncTimeout, TPtr(this))->Via(ServiceInvoker),
        SyncTimeout);

    TAutoPtr<TProxy> leaderProxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    TProxy::TReqScheduleSync::TPtr request = leaderProxy->ScheduleSync();
    request->SetMasterId(CellManager->GetSelfId());
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
    const TMasterStateId& stateId,
    const TMasterEpoch& epoch,
    i32 maxSnapshotId)
{
    if (SyncReceived) {
        LOG_WARNING("Duplicate sync received");
        return;
    }
        
    SyncReceived = true;

    LOG_INFO("Sync received (StateId: %s, Epoch: %s, MaxSnapshotId: %d)",
        ~stateId.ToString(),
        ~StringFromGuid(epoch),
        maxSnapshotId);

    PostponedStateId = stateId;
    YASSERT(PostponedChanges.ysize() == 0);

    i32 snapshotId = Max(maxSnapshotId, SnapshotStore->GetMaxSnapshotId());

    FromMethod(
        &TRecovery::RecoverFromSnapshot,
        TPtr(this),
        PostponedStateId,
        snapshotId)
    ->AsyncVia(EpochInvoker)
    ->AsyncVia(WorkQueue)
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
        return new TResult(result);
    }

    LOG_INFO("Sync reached");

    return FromMethod(&TFollowerRecovery::CapturePostponedChanges, TPtr(this))
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(ServiceInvoker)
           ->Do();
}

TRecovery::TResult::TPtr TFollowerRecovery::CapturePostponedChanges()
{
    // TODO: use threshold?
    if (PostponedChanges.ysize() == 0) {
        LOG_INFO("No postponed changes left");
        return new TResult(EResult::OK);
    }

    THolder<TPostponedChanges> changes(new TPostponedChanges());
    changes->swap(PostponedChanges);

    LOG_INFO("Captured %d postponed changes", changes->ysize());

    return FromMethod(
               &TFollowerRecovery::ApplyPostponedChanges,
               TPtr(this),
               changes)
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(WorkQueue)
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
                MasterState->LogAndApplyChange(change.ChangeData);
                break;

            case TPostponedChange::EType::SegmentAdvance:
                MasterState->RotateChangeLog();
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
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(ServiceInvoker)
           ->Do();
}

TRecovery::EResult TFollowerRecovery::PostponeSegmentAdvance(
    const TMasterStateId& stateId)
{
    if (!SyncReceived) {
        LOG_DEBUG("Postponed segment advance received before sync, ignored");
        return EResult::OK;
    }

    if (PostponedStateId != stateId) {
        LOG_WARNING("Out-of-order postponed segment advance received (ExpectedStateId: %s, StateId: %s)",
            ~PostponedStateId.ToString(),
            ~stateId.ToString());
        return EResult::Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateSegmentAdvance());
    
    LOG_DEBUG("Enqueued postponed segment advance %s",
        ~PostponedStateId.ToString());

    ++PostponedStateId.SegmentId;
    PostponedStateId.ChangeCount = 0;
    
    return EResult::OK;
}

TRecovery::EResult TFollowerRecovery::PostponeChange(
    const TMasterStateId& stateId,
    const TSharedRef& changeData)
{
    if (!SyncReceived) {
        LOG_DEBUG("Postponed change received before sync, ignored");
        return EResult::OK;
    }

    if (PostponedStateId != stateId) {
        LOG_WARNING("Out-of-order postponed change received (ExpectedStateId: %s, StateId: %s)",
            ~PostponedStateId.ToString(),
            ~stateId.ToString());
        return EResult::Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateChange(changeData));
    
    LOG_DEBUG("Enqueued postponed change %s",
        ~PostponedStateId.ToString());

    ++PostponedStateId.ChangeCount;

    return EResult::OK;
}

bool TFollowerRecovery::IsLeader() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
