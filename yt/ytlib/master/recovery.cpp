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

TMasterRecovery::TMasterRecovery(
    const TSnapshotDownloader::TConfig& snapshotDownloaderConfig,
    const TChangeLogDownloader::TConfig& changeLogDownloaderConfig,
    TCellManager::TPtr cellManager,
    TDecoratedMasterState* decoratedState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore* snapshotStore,
    TMasterEpoch epoch,
    TMasterId leaderId,
    IInvoker::TPtr invoker,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr workQueue)
    : SnapshotDownloaderConfig(snapshotDownloaderConfig)
    , ChangeLogDownloaderConfig(changeLogDownloaderConfig)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , LeaderId(leaderId)
    , ServiceInvoker(invoker)
    , EpochInvoker(epochInvoker)
    , WorkQueue(workQueue)
{}

TMasterRecovery::TResult::TPtr TMasterRecovery::RecoverLeader(TMasterStateId stateId)
{
    return FromMethod(&TMasterRecovery::DoRecoverLeader, TPtr(this), stateId)
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(WorkQueue)
           ->Do();
}

TMasterRecovery::EResult TMasterRecovery::DoRecoverLeader(TMasterStateId targetStateId)
{
    LOG_INFO("Recovering leader state from %s to %s",
               ~DecoratedState->GetStateId().ToString(),
               ~targetStateId.ToString());

    // TODO: wrap with try/catch to handle IO errors

    YASSERT(DecoratedState->GetStateId() <= targetStateId);

    i32 maxAvailableSnapshotId = SnapshotStore->GetMaxSnapshotId();
    YASSERT(maxAvailableSnapshotId <= targetStateId.SegmentId);
    i32 prevRecordCount = -1;

    // TODO: extract method
    if (DecoratedState->GetStateId().SegmentId < maxAvailableSnapshotId) {
        THolder<TSnapshotReader> snapshotReader(SnapshotStore->GetReader(maxAvailableSnapshotId));
        if (~snapshotReader == NULL) {
            LOG_FATAL("The latest snapshot %d has vanished", maxAvailableSnapshotId);
        }

        snapshotReader->Open();
        DecoratedState->Load(maxAvailableSnapshotId, snapshotReader->GetStream());
        prevRecordCount = snapshotReader->GetPrevRecordCount();
    }

    YASSERT(DecoratedState->GetStateId().SegmentId >= maxAvailableSnapshotId);

    for (i32 segmentId = DecoratedState->GetStateId().SegmentId;
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
            cachedChangeLog = ChangeLogCache->Create(segmentId, prevRecordCount);
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
        */
        prevRecordCount = changeLog->GetRecordCount();

        if (segmentId != targetStateId.SegmentId && !changeLog->IsFinalized()) {
            LOG_WARNING("Changelog %d is not finalized", segmentId);
        }

        ApplyChangeLog(
            changeLog,
            DecoratedState->GetStateId().ChangeCount,
            changeLog->GetRecordCount() - DecoratedState->GetStateId().ChangeCount);

        if (segmentId < targetStateId.SegmentId) {
            DecoratedState->NextSegment();
        }
    }

    YASSERT(DecoratedState->GetStateId() == targetStateId);

    return E_OK;
}

// TODO: consider passing first/last instead of first/count
void TMasterRecovery::ApplyChangeLog(
    TChangeLog::TPtr changeLog,
    i32 startRecordId,
    i32 recordCount)
{
    YASSERT(DecoratedState->GetStateId().SegmentId == changeLog->GetId());
    YASSERT(DecoratedState->GetStateId().ChangeCount == startRecordId);

    if (recordCount == 0)
        return;

    LOG_INFO("Reading records %d:%d from changelog %d",
               startRecordId, startRecordId + recordCount - 1, changeLog->GetId());

    TBlob dataHolder;
    yvector<TRef> records;
    changeLog->Read(startRecordId, recordCount, &dataHolder, &records);
    if (records.ysize() < recordCount) {
        LOG_FATAL("Could not read changelog %d starting from record %d"
                   "(expected: %d records, got %d records)",
                   changeLog->GetId(), startRecordId, recordCount, records.ysize());
    }

    LOG_INFO("Applying changes to master state");
    for (i32 i = 0; i < recordCount; ++i)  {
        DecoratedState->ApplyChange(records[i]);
    }

    LOG_INFO("Finished applying changes");
}

TMasterRecovery::TResult::TPtr TMasterRecovery::RecoverFollower()
{
    LOG_INFO("Requesting state id from leader");

    TAutoPtr<TProxy> leaderProxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    TProxy::TReqGetCurrentState::TPtr request = leaderProxy->GetCurrentState();
    // TODO: timeout
    return request->Invoke()->Apply(
               FromMethod(&TMasterRecovery::OnGetCurrentStateResponse, TPtr(this))
               ->AsyncVia(EpochInvoker)
               ->AsyncVia(ServiceInvoker));
}

void TMasterRecovery::PostponeChange(const TSharedRef& change, const TMasterStateId& stateId)
{
    PostponedChanges.push_back(TPostponedChange(change, stateId));
}

TMasterRecovery::TResult::TPtr TMasterRecovery::OnGetCurrentStateResponse(
    TProxy::TRspGetCurrentState::TPtr response)
{
    if (!response->IsOK()) {
        LOG_WARNING("Error %s requesting state id from leader",
            ~response->GetErrorCode().ToString());
        return new TResult(E_Failed);
    }

    TMasterEpoch epoch = GuidFromProtoGuid(response->GetEpoch());
    if (epoch != Epoch) {
        LOG_WARNING("Leader replied with a wrong epoch (expected: %s, received: %s)",
            ~StringFromGuid(Epoch),
            ~StringFromGuid(epoch));
        return new TResult(E_Failed);
    }

    CurrentPostponedStateId = TMasterStateId(
        response->GetSegmentId(),
        response->GetChangeCount());
    LOG_INFO("Postponed state is %s", ~CurrentPostponedStateId.ToString());

    i32 maxSnapshotId = Max(response->GetMaxSnapshotId(), SnapshotStore->GetMaxSnapshotId());
  
    YASSERT(PostponedChanges.ysize() == 0);

    return FromMethod(
               &TMasterRecovery::DoRecoverFollower,
               TPtr(this),
               CurrentPostponedStateId,
               maxSnapshotId)
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(WorkQueue)
           ->Do();
}

TMasterRecovery::TResult::TPtr TMasterRecovery::DoRecoverFollower(
    TMasterStateId targetStateId,
    i32 maxSnapshotId)
{
    LOG_INFO("Recovering follower state from %s to %s",
        ~DecoratedState->GetStateId().ToString(),
        ~targetStateId.ToString());

    // TODO: wrap with try/catch to handle IO errors

    YASSERT(DecoratedState->GetStateId() <= targetStateId);
    i32 prevRecordCount = -1;
    
    // TODO: extract method
    if (DecoratedState->GetStateId().SegmentId < maxSnapshotId) {
        THolder<TSnapshotReader> snapshotReader(SnapshotStore->GetReader(maxSnapshotId));
        if (~snapshotReader == NULL) {
            TSnapshotDownloader snapshotDownloader(
                SnapshotDownloaderConfig, CellManager);
            THolder<TSnapshotWriter> snapshotWriter(
                SnapshotStore->GetWriter(maxSnapshotId));
            TSnapshotDownloader::EResult snapshotResult =
                snapshotDownloader.GetSnapshot(maxSnapshotId, ~snapshotWriter);

            if (snapshotResult != TSnapshotDownloader::OK) {
                LOG_ERROR("Error %d while downloading snapshot %d",
                    snapshotResult, maxSnapshotId);
                return new TResult(E_Failed);
            }

            snapshotReader = SnapshotStore->GetReader(maxSnapshotId);
            if (~snapshotReader == NULL) {
                LOG_FATAL("Latest snapshot %d has vanished", maxSnapshotId);
            }
            prevRecordCount = snapshotReader->GetPrevRecordCount();
        }

        snapshotReader->Open();
        DecoratedState->Load(maxSnapshotId, snapshotReader->GetStream());
    }

    for (i32 segmentId = DecoratedState->GetStateId().SegmentId;
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
            cachedChangeLog = ChangeLogCache->Create(segmentId, prevRecordCount);
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
            return new TResult(E_Failed);
        }

        i32 recordCount = response->GetRecordCount();
        // TODO: use changeLogWriter instead
        if (changeLog->GetRecordCount() > recordCount) {
            LOG_INFO("Changelog %d has %d records while %d are expected, truncating",
                       segmentId, changeLog->GetRecordCount(), recordCount);
            changeLog->Truncate(recordCount);
            // TODO: finalize?
            // TODO: this could only happen with the last changelog
        }

        // TODO: extract method
        TChangeLogDownloader changeLogDownloader(ChangeLogDownloaderConfig, CellManager);
        TChangeLogDownloader::EResult changeLogResult = changeLogDownloader.Download(
            TMasterStateId(segmentId, recordCount),
            &cachedChangeLog->GetWriter());

        if (changeLogResult != TChangeLogDownloader::OK) {
            LOG_ERROR("Error %d while downloading changelog %d",
                        (int) changeLogResult, segmentId);
            return new TResult(E_Failed);
        }

        if (segmentId != targetStateId.SegmentId && !changeLog->IsFinalized()) {
            LOG_WARNING("Changelog %d was not finalized", segmentId);
            cachedChangeLog->GetWriter().Close();
        }

        ApplyChangeLog(
            changeLog,
            DecoratedState->GetStateId().ChangeCount,
            recordCount - DecoratedState->GetStateId().ChangeCount);

        if (segmentId < targetStateId.SegmentId) {
            DecoratedState->NextSegment();
        }
    }

    YASSERT(DecoratedState->GetStateId() == targetStateId);

    return FromMethod(&TMasterRecovery::CapturePostponedChanges, this)
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(ServiceInvoker)
           ->Do();
}

TMasterRecovery::TResult::TPtr TMasterRecovery::CapturePostponedChanges()
{
    // TODO: use threshold?
    if (PostponedChanges.ysize() == 0) {
        LOG_INFO("No postponed changes left");
        return new TResult(E_OK);
    }

    // TODO: vector type has changed
    THolder< yvector<TBlob> > changes(new yvector<TBlob>());
    changes->swap(PostponedChanges);

    LOG_INFO("Captured %d postponed changes", changes->ysize());

    return FromMethod(&TMasterRecovery::ApplyPostponedChanges, TPtr(this), changes)
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(WorkQueue)
           ->Do();
}

// TODO: vector type has changed
TMasterRecovery::TResult::TPtr TMasterRecovery::ApplyPostponedChanges(
    TAutoPtr< yvector< TBlob > > changes)
{
    LOG_INFO("Applying postponed changes %d-%d",
        DecoratedState->GetStateId().ChangeCount,
        DecoratedState->GetStateId().ChangeCount + changes->ysize() - 1);
    
    // TODO: add to changelog?
    for (i32 i = 0; i < changes->ysize(); ++i) {
        // TODO: write this change to local changelog
        // TODO: sometimes we have to switch to next changelog (via DecoratedMasterState)
        DecoratedState->ApplyChange((*changes)[i]);
    }
    
    LOG_INFO("Finished applying postponed changes");

    return FromMethod(&TMasterRecovery::CapturePostponedChanges, TPtr(this))
           ->AsyncVia(EpochInvoker)
           ->AsyncVia(ServiceInvoker)
           ->Do();
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
