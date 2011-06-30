#include "change_log_downloader.h"
#include "change_log_writer.h"

#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChangeLogDownloader");

//////////////////////////////////////////////////////////////////////////////////

TChangeLogDownloader::TChangeLogDownloader(
    const TConfig& config,
    TCellManager::TPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{ }

TChangeLogDownloader::EResult TChangeLogDownloader::Download(
    TMasterStateId stateId,
    TChangeLogWriter* changeLogWriter)
{
    LOG_DEBUG("Requested %d record(s) in changelog %d",
                stateId.ChangeCount, stateId.SegmentId);

    TChangeLog::TPtr changeLog = changeLogWriter->GetChangeLog();
    YASSERT(changeLog->GetId() == stateId.SegmentId);
    if (changeLog->GetRecordCount() >= stateId.ChangeCount) {
        LOG_DEBUG("Local changelog already contains %d record(s), no download needed",
                    changeLog->GetRecordCount());
        return OK;
    }

    TMasterId sourceId = GetChangeLogSource(stateId);
    if (sourceId == InvalidMasterId) {
        return ChangeLogNotFound;
    }

    return DownloadChangeLog(stateId, sourceId, changeLogWriter);
}

TMasterId TChangeLogDownloader::GetChangeLogSource(TMasterStateId stateId)
{
    TAsyncResult<TMasterId>::TPtr asyncResult = new TAsyncResult<TMasterId>();
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter();

    for (TMasterId i = 0; i < CellManager->GetMasterCount(); ++i) {
        LOG_DEBUG("Requesting changelog info from master %d", i);

        TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(i);
        TProxy::TReqGetChangeLogInfo::TPtr request = proxy->GetChangeLogInfo();
        request->SetSegmentId(stateId.SegmentId);
        awaiter->Await(request->Invoke(Config.LookupTimeout), FromMethod(
            &TChangeLogDownloader::OnResponse,
            awaiter, asyncResult, i, stateId));
    }

    awaiter->Complete(FromMethod(
        &TChangeLogDownloader::OnComplete,
        stateId, asyncResult));

    return asyncResult->Get();
}

TChangeLogDownloader::EResult TChangeLogDownloader::DownloadChangeLog(
    TMasterStateId stateId,
    TMasterId sourceId,
    TChangeLogWriter* changeLogWriter)
{
    TChangeLog::TPtr changLog = changeLogWriter->GetChangeLog();
    i32 downloadedRecordCount = changLog->GetRecordCount();

    LOG_DEBUG("Started downloading records %d:%d from master %d",
                changLog->GetRecordCount(), stateId.ChangeCount - 1, sourceId);

    TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(sourceId);
    while (downloadedRecordCount < stateId.ChangeCount) {
        TProxy::TReqReadChangeLog::TPtr request = proxy->ReadChangeLog();
        request->SetSegmentId(stateId.SegmentId);
        request->SetStartRecordId(downloadedRecordCount);
        i32 desiredChunkSize = Min(
            Config.RecordsPerRequest,
            stateId.ChangeCount - downloadedRecordCount);
        request->SetRecordCount(desiredChunkSize);

        LOG_DEBUG("Requesting records %d:%d",
                    downloadedRecordCount, downloadedRecordCount + desiredChunkSize - 1);

        TProxy::TRspReadChangeLog::TPtr response = request->Invoke(Config.ReadTimeout)->Get();

        if (!response->IsOK()) {
            TProxy::EErrorCode errorCode = response->GetErrorCode();
            if (response->IsServiceError()) {
                // TODO: drop ToValue()
                switch (errorCode.ToValue()) {
                    case TProxy::EErrorCode::InvalidSegmentId:
                        LOG_WARNING("Master %d does not have changelog %d anymore",
                            sourceId,
                            stateId.SegmentId);
                        return ChangeLogUnavailable;

                    case TProxy::EErrorCode::IOError:
                        LOG_WARNING("IO error occurred on master %d during downloading changelog %d",
                            sourceId,
                            stateId.SegmentId);
                        return RemoteError;

                    default:
                        LOG_FATAL("Unknown error code %s received from master %d",
                            ~errorCode.ToString(),
                            sourceId);
                        break;
                }
            } else {
                LOG_WARNING("Error %s reading snapshot from master %d",
                    ~errorCode.ToString(),
                    sourceId);
                return RemoteError;
            }
        }

        yvector<TSharedRef>& attachments = response->Attachments();
        if (attachments.ysize() == 0) {
            LOG_WARNING("Master %d does not have %d records of changelog %d anymore",
                sourceId,
                stateId.ChangeCount,
                stateId.SegmentId);
            return ChangeLogUnavailable;
        }

        if (attachments.ysize() != desiredChunkSize) {
            // Continue anyway.
            LOG_DEBUG("Received records %d:%d while %d records were requested",
				downloadedRecordCount,
				downloadedRecordCount + attachments.ysize() - 1,
				desiredChunkSize);
        } else {
            LOG_DEBUG("Received records %d:%d",
                downloadedRecordCount,
                downloadedRecordCount + attachments.ysize() - 1);
        }

        TChangeLogWriter::TAppendResult::TPtr appendResult;
        for (i32 i = 0; i < attachments.ysize(); ++i) {
            TSharedRef attachment = attachments[i];
            appendResult = changeLogWriter->Append(downloadedRecordCount, attachment);
            ++downloadedRecordCount;
        }

        if (downloadedRecordCount >= stateId.ChangeCount) {
            TChangeLogWriter::EResult result = appendResult->Get();
            YASSERT(result == TChangeLogWriter::OK);
        }
    }

    LOG_DEBUG("Download complete");

    return OK;
}

void TChangeLogDownloader::OnResponse(
    TProxy::TRspGetChangeLogInfo::TPtr response,
    TParallelAwaiter::TPtr awaiter,
    TAsyncResult<TMasterId>::TPtr asyncResult,
    TMasterId masterId,
    TMasterStateId stateId)
{
    if (!response->IsOK()) {
        LOG_DEBUG("Error %s requesting info on changelog %d from master %d",
            ~response->GetErrorCode().ToString(),
            stateId.SegmentId,
            masterId);
        return;
    }

    i32 recordCount = response->GetRecordCount();
    if (recordCount < stateId.ChangeCount) {
        LOG_DEBUG("Master %d has only %d record(s) while %d records needed",
            masterId,
            recordCount,
            stateId.ChangeCount);
        return;
    }

    LOG_DEBUG("Found appropriate download source at master %d (RecordCount: %d)",
        masterId,
        recordCount);

    asyncResult->Set(masterId);
    awaiter->Cancel();
}

void TChangeLogDownloader::OnComplete(
    TMasterStateId stateId,
    TAsyncResult<TMasterId>::TPtr asyncResult)
{
    LOG_DEBUG("Unable to find %d record(s) in changelog %d at any master",
                stateId.SegmentId, stateId.ChangeCount);
    asyncResult->Set(InvalidMasterId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
