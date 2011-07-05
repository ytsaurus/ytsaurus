#include "recovery.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"

#include "../actions/action_util.h"
#include "../misc/serialize.h"
#include "../misc/string.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MasterRecovery");

////////////////////////////////////////////////////////////////////////////////

// TODO: make configurable
static TDuration SyncTimeout = TDuration::MilliSeconds(1000);

////////////////////////////////////////////////////////////////////////////////

TMasterRecovery::TMasterRecovery(
    const TSnapshotDownloader::TConfig& snapshotDownloaderConfig,
    const TChangeLogDownloader::TConfig& changeLogDownloaderConfig,
    TCellManager::TPtr cellManager,
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore* snapshotStore,
    TMasterEpoch epoch,
    TMasterId leaderId,
    IInvoker::TPtr invoker,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr workQueue)
    : SyncReceived(false)
    , SnapshotDownloaderConfig(snapshotDownloaderConfig)
    , ChangeLogDownloaderConfig(changeLogDownloaderConfig)
    , CellManager(cellManager)
    , MasterState(masterState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , LeaderId(leaderId)
    , ServiceInvoker(invoker)
    , EpochInvoker(epochInvoker)
    , WorkQueue(workQueue)
    , Result(new TResult())
{ }

TMasterRecovery::TResult::TPtr TMasterRecovery::RecoverLeader(TMasterStateId stateId)
{
    FromMethod(&TMasterRecovery::RecoverLeaderFromSnapshot, TPtr(this), stateId)
        ->Via(EpochInvoker)
        ->Via(WorkQueue)
        ->Do();
    return Result;
}

void TMasterRecovery::RecoverLeaderFromSnapshot(TMasterStateId targetStateId)
{
    LOG_INFO("Recovering leader state from %s to %s",
        ~MasterState->GetStateId().ToString(),
        ~targetStateId.ToString());

    // TODO: wrap with try/catch to handle IO errors

    YASSERT(MasterState->GetStateId() <= targetStateId);

    i32 maxAvailableSnapshotId = SnapshotStore->GetMaxSnapshotId();
    YASSERT(maxAvailableSnapshotId <= targetStateId.SegmentId);
    
    //i32 prevRecordCount = -1;

    // TODO: extract method
    if (MasterState->GetStateId().SegmentId < maxAvailableSnapshotId) {
        TSnapshotReader::TPtr snapshotReader = SnapshotStore->GetReader(maxAvailableSnapshotId);
        if (snapshotReader.Get() == NULL) {
            LOG_FATAL("The latest snapshot %d has vanished", maxAvailableSnapshotId);
        }

        snapshotReader->Open();
        TInputStream& stream = snapshotReader->GetStream();

        // we need to remember snapshotReader until Load is finished
        MasterState->Load(maxAvailableSnapshotId, stream)->Subscribe(FromMethod(
            &TMasterRecovery::RecoverLeaderFromChangeLog,
            TPtr(this),
            snapshotReader,
            targetStateId));
        //prevRecordCount = snapshotReader->GetPrevRecordCount();
    } else {
        RecoverLeaderFromChangeLog(TVoid(), TSnapshotReader::TPtr(), targetStateId);
    }
}

void TMasterRecovery::RecoverLeaderFromChangeLog(
    TVoid,
    TSnapshotReader::TPtr,
    TMasterStateId targetStateId)
{
    for (i32 segmentId = MasterState->GetStateId().SegmentId;
         segmentId <= targetStateId.SegmentId;
         ++segmentId)
    {
        // TODO: extract method
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            if (segmentId < targetStateId.SegmentId ||
                segmentId == targetStateId.SegmentId &&
                targetStateId.ChangeCount != 0)
            {
                LOG_FATAL("A required changelog %d is missing", segmentId);
            }
            //YASSERT(prevRecordCount != -1);
            cachedChangeLog = ChangeLogCache->Create(segmentId, -1/*prevRecordCount*/);
        }

        TChangeLog::TPtr changeLog = cachedChangeLog->GetChangeLog();
        // TODO: fixme
        /*
        if (prevRecordCount != -1 &&
            changeLog->GetPrevStateId().ChangeCount != prevRecordCount)
        {
            // TODO: log
            return E_Failed;
        }
        prevRecordCount = changeLog->GetRecordCount();
        */

        if (segmentId != targetStateId.SegmentId && !changeLog->IsFinalized()) {
            LOG_WARNING("Changelog %d is not finalized", segmentId);
        }

        // Apply the whole changelog.
        ApplyChangeLog(changeLog, changeLog->GetRecordCount());

        if (segmentId < targetStateId.SegmentId) {
            MasterState->AdvanceSegment();
        }
    }

    YASSERT(MasterState->GetStateId() == targetStateId);

    Result->Set(E_OK);
}

void TMasterRecovery::ApplyChangeLog(
    TChangeLog::TPtr changeLog,
    i32 targetChangeCount)
{
    YASSERT(MasterState->GetStateId().SegmentId == changeLog->GetId());
    
    i32 startRecordId = MasterState->GetStateId().ChangeCount;
    i32 recordCount = targetChangeCount - startRecordId;

    if (recordCount == 0)
        return;

    LOG_INFO("Reading records %d-%d from changelog %d",
        startRecordId,
        targetChangeCount - 1, 
        changeLog->GetId());

    yvector<TSharedRef> records;
    changeLog->Read(startRecordId, recordCount, &records);
    if (records.ysize() < recordCount) {
        LOG_FATAL("Could not read changelog %d starting from record %d "
            "(expected: %d records, received %d records)",
            changeLog->GetId(),
            startRecordId,
            recordCount,
            records.ysize());
    }

    LOG_INFO("Applying changes to master state");
    for (i32 i = 0; i < recordCount; ++i)  {
        MasterState->ApplyChange(records[i]);
    }

    LOG_INFO("Finished applying changes");
}

TMasterRecovery::TResult::TPtr TMasterRecovery::RecoverFollower()
{
    LOG_INFO("Requesting sync from leader");

    TDelayedInvoker::Get()->Submit(
        FromMethod(&TMasterRecovery::OnSyncTimeout, TPtr(this))->Via(ServiceInvoker),
        SyncTimeout);

    TAutoPtr<TProxy> leaderProxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    TProxy::TReqScheduleSync::TPtr request = leaderProxy->ScheduleSync();
    request->SetMasterId(CellManager->GetSelfId());
    request->Invoke();

    return Result;
}

void TMasterRecovery::OnSyncTimeout()
{
    if (SyncReceived)
        return;

    LOG_INFO("Sync timeout");
    
    Result->Set(E_Failed);
}

void TMasterRecovery::Sync(
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
        &TMasterRecovery::RecoverFollowerFromSnapshot,
        TPtr(this),
        PostponedStateId,
        snapshotId)
    ->Via(EpochInvoker)
    ->Via(WorkQueue)
    ->Do();
}

TMasterRecovery::EResult TMasterRecovery::PostponeSegmentAdvance(const TMasterStateId& stateId)
{
    if (!SyncReceived) {
        LOG_DEBUG("Postponed segment advance received before sync, ignored");
        return E_OK;
    }

    if (PostponedStateId != stateId) {
        LOG_WARNING("Out-of-order postponed segment advance received (ExpectedStateId: %s, StateId: %s)",
            ~PostponedStateId.ToString(),
            ~stateId.ToString());
        return E_Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateSegmentAdvance());
    
    LOG_DEBUG("Enqueued postponed segment advance %s",
        ~PostponedStateId.ToString());

    ++PostponedStateId.SegmentId;
    PostponedStateId.ChangeCount = 0;
    
    return E_OK;
}

TMasterRecovery::EResult TMasterRecovery::PostponeChange(
    const TMasterStateId& stateId,
    const TSharedRef& changeData)
{
    if (!SyncReceived) {
        LOG_DEBUG("Postponed change received before sync, ignored");
        return E_OK;
    }

    if (PostponedStateId != stateId) {
        LOG_WARNING("Out-of-order postponed change received (ExpectedStateId: %s, StateId: %s)",
            ~PostponedStateId.ToString(),
            ~stateId.ToString());
        return E_Failed;
    }

    PostponedChanges.push_back(TPostponedChange::CreateChange(changeData));
    
    LOG_DEBUG("Enqueued postponed change %s",
        ~PostponedStateId.ToString());

    ++PostponedStateId.ChangeCount;

    return E_OK;
}

void TMasterRecovery::RecoverFollowerFromSnapshot(
    TMasterStateId targetStateId,
    i32 snapshotId)
{
    LOG_INFO("Recovering follower state from %s to %s",
        ~MasterState->GetStateId().ToString(),
        ~targetStateId.ToString());

    // TODO: wrap with try/catch to handle IO errors

    YASSERT(MasterState->GetStateId() <= targetStateId);
    i32 prevRecordCount = -1;
    
    // TODO: extract method
    if (MasterState->GetStateId().SegmentId < snapshotId) {
        TSnapshotReader::TPtr snapshotReader = SnapshotStore->GetReader(snapshotId);
        if (snapshotReader.Get() == NULL) {
            TSnapshotDownloader snapshotDownloader(
                SnapshotDownloaderConfig, CellManager);

            TSnapshotWriter::TPtr snapshotWriter =
                SnapshotStore->GetWriter(snapshotId);
            TSnapshotDownloader::EResult snapshotResult =
                snapshotDownloader.GetSnapshot(snapshotId, ~snapshotWriter);

            if (snapshotResult != TSnapshotDownloader::OK) {
                LOG_ERROR("Error %d while downloading snapshot %d",
                    snapshotResult, snapshotId);

                Result->Set(E_Failed);
                return;
            }

            snapshotReader = SnapshotStore->GetReader(snapshotId);
            if (snapshotReader.Get() == NULL) {
                LOG_FATAL("Latest snapshot %d has vanished", snapshotId);
            }
            prevRecordCount = snapshotReader->GetPrevRecordCount();
        }

        snapshotReader->Open();
        TInputStream& stream = snapshotReader->GetStream();

        // we need to remember snapshotReader until Load is finished
        MasterState->Load(snapshotId, stream)->Subscribe(FromMethod(
            &TMasterRecovery::RecoverFollowerFromChangeLog,
            TPtr(this),
            snapshotReader,
            targetStateId));
    } else {
        RecoverFollowerFromChangeLog(TVoid(), TSnapshotReader::TPtr(), targetStateId);
    }
}

void TMasterRecovery::RecoverFollowerFromChangeLog(
    TVoid,
    TSnapshotReader::TPtr,
    TMasterStateId targetStateId)
{
    for (i32 segmentId = MasterState->GetStateId().SegmentId;
         segmentId <= targetStateId.SegmentId;
         ++segmentId)
    {
        // TODO: extract method
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            // This is Info, not a Warning!
            // A follower may be far behind the leader.
            LOG_INFO("Could not load the latest changelog %d, an empty one is created",
                segmentId);
            // TODO: pass prev
            //YASSERT(prevRecordCount != -1);
            cachedChangeLog = ChangeLogCache->Create(segmentId, -1 /*prevRecordCount*/);
        }

        TChangeLog::TPtr changeLog = cachedChangeLog->GetChangeLog();
/*
        if (changeLog->GetPrevStateId().ChangeCount != prevRecordCount) {
            // TODO: log
            return new TResult(E_Failed);
        }
        prevRecordCount = changeLog->GetRecordCount();
*/
        // Getting right record count in the changelog
        // TODO: extract method
        THolder<TProxy> leaderProxy(CellManager->GetMasterProxy<TProxy>(LeaderId));
        TProxy::TReqGetChangeLogInfo::TPtr request = leaderProxy->GetChangeLogInfo();
        request->SetSegmentId(segmentId);
        TProxy::TRspGetChangeLogInfo::TPtr response = request->Invoke()->Get(); // TODO: timeout
        if (!response->IsOK()) {
            LOG_ERROR("Could not get changelog %d info from leader",
                        segmentId);

            Result->Set(E_Failed);
            return;
        }

        i32 localRecordCount = changeLog->GetRecordCount();
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

        // TODO: use changeLogWriter instead
        if (remoteRecordCount < targetChangeCount) {
            changeLog->Truncate(remoteRecordCount);
            // TODO: finalize?
            // TODO: this could only happen with the last changelog
        } else if (localRecordCount < targetChangeCount) {
            // TODO: extract method
            TChangeLogDownloader changeLogDownloader(ChangeLogDownloaderConfig, CellManager);
            TChangeLogDownloader::EResult changeLogResult = changeLogDownloader.Download(
                TMasterStateId(segmentId, targetChangeCount),
                cachedChangeLog->GetWriter());

            if (changeLogResult != TChangeLogDownloader::OK) {
                // TODO: tostring
                LOG_ERROR("Error %d while downloading changelog %d",
                    (int) changeLogResult,
                    segmentId);

                Result->Set(E_Failed);
                return;
            }
        }

        if (segmentId != targetStateId.SegmentId && !changeLog->IsFinalized()) {
            LOG_WARNING("Changelog %d was not finalized", segmentId);
            cachedChangeLog->GetWriter().Finalize();
        }

        ApplyChangeLog(changeLog, targetChangeCount);

        if (segmentId < targetStateId.SegmentId) {
            MasterState->AdvanceSegment();
        }
    }

    YASSERT(MasterState->GetStateId() == targetStateId);

    FromMethod(
        &TMasterRecovery::CapturePostponedChanges,
        TPtr(this))
    ->Via(EpochInvoker)
    ->Via(ServiceInvoker)
    ->Do();
}

void TMasterRecovery::CapturePostponedChanges()
{
    // TODO: use threshold?
    if (PostponedChanges.ysize() == 0) {
        LOG_INFO("No postponed changes left");
        Result->Set(E_OK);
        return;
    }

    THolder<TPostponedChanges> changes(new TPostponedChanges());
    changes->swap(PostponedChanges);

    LOG_INFO("Captured %d postponed changes", changes->ysize());

    FromMethod(
        &TMasterRecovery::ApplyPostponedChanges,
        TPtr(this),
        changes)
    ->Via(EpochInvoker)
    ->Via(WorkQueue)
    ->Do();
}

void TMasterRecovery::ApplyPostponedChanges(TAutoPtr<TPostponedChanges> changes)
{
    LOG_INFO("Applying %d postponed change(s)", changes->ysize());
    
    for (TPostponedChanges::const_iterator it = changes->begin();
         it != changes->end();
         ++it)
    {
        const TPostponedChange& change = *it;
        switch (change.Type) {
            case TPostponedChange::T_Change:
                MasterState->LogAndApplyChange(change.ChangeData);
                break;

            case TPostponedChange::T_SegmentAdvance:
                MasterState->RotateChangeLog();
                break;

            default:
                YASSERT(false);
                break;
        }
    }
   
    LOG_INFO("Finished applying postponed changes");

    FromMethod(
        &TMasterRecovery::CapturePostponedChanges,
        TPtr(this))
    ->Via(EpochInvoker)
    ->Via(ServiceInvoker)
    ->Do();
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
