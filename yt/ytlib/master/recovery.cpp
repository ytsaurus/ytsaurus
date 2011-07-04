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
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore* snapshotStore,
    TMasterEpoch epoch,
    TMasterId leaderId,
    IInvoker::TPtr invoker,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr workQueue)
    : IsPostponing(false)
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
        ~MasterState->GetStateId().ToString(),
        ~targetStateId.ToString());

    // TODO: wrap with try/catch to handle IO errors

    YASSERT(MasterState->GetStateId() <= targetStateId);

    i32 maxAvailableSnapshotId = SnapshotStore->GetMaxSnapshotId();
    YASSERT(maxAvailableSnapshotId <= targetStateId.SegmentId);
    i32 prevRecordCount = -1;

    // TODO: extract method
    if (MasterState->GetStateId().SegmentId < maxAvailableSnapshotId) {
        THolder<TSnapshotReader> snapshotReader(SnapshotStore->GetReader(maxAvailableSnapshotId));
        if (~snapshotReader == NULL) {
            LOG_FATAL("The latest snapshot %d has vanished", maxAvailableSnapshotId);
        }

        snapshotReader->Open();
        MasterState->Load(maxAvailableSnapshotId, snapshotReader->GetStream());
        prevRecordCount = snapshotReader->GetPrevRecordCount();
    }

    YASSERT(MasterState->GetStateId().SegmentId >= maxAvailableSnapshotId);

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

        // Apply the whole changelog.
        ApplyChangeLog(changeLog, changeLog->GetRecordCount());

        if (segmentId < targetStateId.SegmentId) {
            MasterState->AdvanceSegment();
        }
    }

    YASSERT(MasterState->GetStateId() == targetStateId);

    return E_OK;
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

    TBlob dataHolder;
    yvector<TRef> records;
    changeLog->Read(startRecordId, recordCount, &dataHolder, &records);
    if (records.ysize() < recordCount) {
        LOG_FATAL("Could not read changelog %d starting from record %d "
            "(expected: %d records, received %d records)",
            changeLog->GetId(),
            startRecordId,
            recordCount,
            records.ysize());
    }

    // TODO: kill this hack once changelog returns shared refs
    TSharedRef::TBlobPtr dataHolder2 = new TBlob();
    dataHolder.swap(*dataHolder2);

    LOG_INFO("Applying changes to master state");
    for (i32 i = 0; i < recordCount; ++i)  {
        MasterState->ApplyChange(TSharedRef(dataHolder2, records[i]));
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

TMasterRecovery::EResult TMasterRecovery::PostponeSegmentAdvance(const TMasterStateId& stateId)
{
    if (!IsPostponing)
        return E_OK;

    // TODO: drop once rpc becomes ordered
    if (PostponedStateId > stateId) {
        LOG_WARNING("Late postponed segment advance received (expected state: %s, received: %s)",
            ~PostponedStateId.ToString(),
            ~stateId.ToString());
        return E_Failed;
    }

    if (PostponedStateId != stateId) {
        LOG_WARNING("Out-of-order postponed segment advance received (expected state: %s, received: %s)",
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
    if (!IsPostponing)
        return E_OK;

    // TODO: drop once rpc becomes ordered
    if (PostponedStateId > stateId) {
        LOG_WARNING("Late postponed change received (expected state: %s, received: %s)",
            ~PostponedStateId.ToString(),
            ~stateId.ToString());
        return E_Failed;
    }

    if (PostponedStateId != stateId) {
        LOG_WARNING("Out-of-order postponed change received (expected state: %s, received: %s)",
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

    PostponedStateId = TMasterStateId(
        response->GetSegmentId(),
        response->GetChangeCount());
    IsPostponing = true;

    LOG_INFO("Postponed state is %s", ~PostponedStateId.ToString());

    i32 maxSnapshotId = Max(
        response->GetMaxSnapshotId(),
        SnapshotStore->GetMaxSnapshotId());
  
    YASSERT(PostponedChanges.ysize() == 0);

    return
        FromMethod(
            &TMasterRecovery::DoRecoverFollower,
            TPtr(this),
            PostponedStateId,
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
        ~MasterState->GetStateId().ToString(),
        ~targetStateId.ToString());

    // TODO: wrap with try/catch to handle IO errors

    YASSERT(MasterState->GetStateId() <= targetStateId);
    i32 prevRecordCount = -1;
    
    // TODO: extract method
    if (MasterState->GetStateId().SegmentId < maxSnapshotId) {
        TAutoPtr<TSnapshotReader> snapshotReader(SnapshotStore->GetReader(maxSnapshotId));
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
        MasterState->Load(maxSnapshotId, snapshotReader->GetStream());
    }

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
                &cachedChangeLog->GetWriter());

            if (changeLogResult != TChangeLogDownloader::OK) {
                // TODO: tostring
                LOG_ERROR("Error %d while downloading changelog %d",
                    (int) changeLogResult,
                    segmentId);
                return new TResult(E_Failed);
            }
        }

        if (segmentId != targetStateId.SegmentId && !changeLog->IsFinalized()) {
            LOG_WARNING("Changelog %d was not finalized", segmentId);
            cachedChangeLog->GetWriter().Close();
        }

        ApplyChangeLog(changeLog, targetChangeCount);

        if (segmentId < targetStateId.SegmentId) {
            MasterState->AdvanceSegment();
        }
    }

    YASSERT(MasterState->GetStateId() == targetStateId);

    return
        FromMethod(&TMasterRecovery::CapturePostponedChanges, this)
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

    THolder<TPostponedChanges> changes(new TPostponedChanges());
    changes->swap(PostponedChanges);

    LOG_INFO("Captured %d postponed changes", changes->ysize());

    return
        FromMethod(&TMasterRecovery::ApplyPostponedChanges, TPtr(this), changes)
        ->AsyncVia(EpochInvoker)
        ->AsyncVia(WorkQueue)
        ->Do();
}

TMasterRecovery::TResult::TPtr TMasterRecovery::ApplyPostponedChanges(
    TAutoPtr<TPostponedChanges> changes)
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
                MasterState->AdvanceSegment();
                break;

            default:
                YASSERT(false);
                break;
        }
    }
   
    LOG_INFO("Finished applying postponed changes");

    return
        FromMethod(&TMasterRecovery::CapturePostponedChanges, TPtr(this))
        ->AsyncVia(EpochInvoker)
        ->AsyncVia(ServiceInvoker)
        ->Do();
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
