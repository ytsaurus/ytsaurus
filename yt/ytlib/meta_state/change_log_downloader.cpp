#include "stdafx.h"
#include "change_log_downloader.h"
#include "async_change_log.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

//////////////////////////////////////////////////////////////////////////////////

TChangeLogDownloader::TChangeLogDownloader(
    TConfig* config,
    TCellManager::TPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{
    YASSERT(~cellManager != NULL);
}

TChangeLogDownloader::EResult TChangeLogDownloader::Download(
    TMetaVersion version,
    TAsyncChangeLog& changeLog)
{
    LOG_INFO("Requested %d records in changelog %d",
        version.RecordCount,
        version.SegmentId);

    YASSERT(changeLog.GetId() == version.SegmentId);

    if (changeLog.GetRecordCount() >= version.RecordCount) {
        LOG_INFO("Local changelog already contains %d records, no download needed",
            changeLog.GetRecordCount());
        return EResult::OK;
    }

    TPeerId sourceId = GetChangeLogSource(version);
    if (sourceId == NElection::InvalidPeerId) {
        return EResult::ChangeLogNotFound;
    }

    return DownloadChangeLog(version, sourceId, changeLog);
}

TPeerId TChangeLogDownloader::GetChangeLogSource(TMetaVersion version)
{
    auto asyncResult = New< TFuture<TPeerId> >();
    auto awaiter = New<TParallelAwaiter>();

    for (TPeerId i = 0; i < CellManager->GetPeerCount(); ++i) {
        LOG_INFO("Requesting changelog info from peer %d", i);

        auto proxy = CellManager->GetMasterProxy<TProxy>(i);
        proxy->SetTimeout(Config->LookupTimeout);

        auto request = proxy->GetChangeLogInfo();
        request->set_changelogid(version.SegmentId);
        awaiter->Await(request->Invoke(), FromMethod(
            &TChangeLogDownloader::OnResponse,
            awaiter,
            asyncResult,
            i,
            version));
    }

    awaiter->Complete(FromMethod(
        &TChangeLogDownloader::OnComplete,
        asyncResult));

    return asyncResult->Get();
}

TChangeLogDownloader::EResult TChangeLogDownloader::DownloadChangeLog(
    TMetaVersion version,
    TPeerId sourceId,
    TAsyncChangeLog& changeLog)
{
    i32 downloadedRecordCount = changeLog.GetRecordCount();

    LOG_INFO("Started downloading records %d-%d from peer %d",
        changeLog.GetRecordCount(),
        version.RecordCount - 1,
        sourceId);

    auto proxy = CellManager->GetMasterProxy<TProxy>(sourceId);
    proxy->SetTimeout(Config->ReadTimeout);

    while (downloadedRecordCount < version.RecordCount) {
        auto request = proxy->ReadChangeLog();
        request->set_changelogid(version.SegmentId);
        request->set_startrecordid(downloadedRecordCount);
        i32 desiredChunkSize = Min(
            Config->RecordsPerRequest,
            version.RecordCount - downloadedRecordCount);
        request->set_recordcount(desiredChunkSize);

        LOG_DEBUG("Requesting records %d-%d",
            downloadedRecordCount,
            downloadedRecordCount + desiredChunkSize - 1);

        auto response = request->Invoke()->Get();

        if (!response->IsOK()) {
            auto error = response->GetError();
            if (NRpc::IsServiceError(error)) {
                switch (EErrorCode(error.GetCode())) {
                    case EErrorCode::NoSuchChangeLog:
                        LOG_WARNING("Peer %d does not have changelog %d anymore",
                            sourceId,
                            version.SegmentId);
                        return EResult::ChangeLogUnavailable;

                    default:
                        LOG_FATAL("Unexpected error received from peer %d\n%s",
                            sourceId,
                            ~error.ToString());
                        break;
                }
            } else {
                LOG_WARNING("Error %s reading snapshot from peer %d",
                    ~error.ToString(),
                    sourceId);
                return EResult::RemoteError;
            }
        }

        auto& attachments = response->Attachments();
        if (attachments.ysize() == 0) {
            LOG_WARNING("Peer %d does not have %d records of changelog %d anymore",
                sourceId,
                version.RecordCount,
                version.SegmentId);
            return EResult::ChangeLogUnavailable;
        }

        if (attachments.ysize() != desiredChunkSize) {
            // Continue anyway.
            LOG_DEBUG("Received records %d-%d while %d records were requested",
                downloadedRecordCount,
                downloadedRecordCount + attachments.ysize() - 1,
                desiredChunkSize);
        } else {
            LOG_DEBUG("Received records %d-%d",
                downloadedRecordCount,
                downloadedRecordCount + attachments.ysize() - 1);
        }

        for (i32 i = 0; i < attachments.ysize(); ++i) {
            changeLog.Append(downloadedRecordCount, attachments[i]);
            ++downloadedRecordCount;
        }
    }

    LOG_INFO("Finished downloading changelog");

    return EResult::OK;
}

void TChangeLogDownloader::OnResponse(
    TProxy::TRspGetChangeLogInfo::TPtr response,
    TParallelAwaiter::TPtr awaiter,
    TFuture<TPeerId>::TPtr asyncResult,
    TPeerId peerId,
    TMetaVersion version)
{
    if (!response->IsOK()) {
        LOG_INFO("Error %s requesting info on changelog %d from peer %d",
            ~response->GetError().ToString(),
            version.SegmentId,
            peerId);
        return;
    }

    i32 recordCount = response->recordcount();
    if (recordCount < version.RecordCount) {
        LOG_INFO("Peer %d has only %d records while %d records needed",
            peerId,
            recordCount,
            version.RecordCount);
        return;
    }

    LOG_INFO("An appropriate download source found (PeerId: %d, RecordCount: %d)",
        peerId,
        recordCount);

    asyncResult->Set(peerId);
    awaiter->Cancel();
}

void TChangeLogDownloader::OnComplete(
    TFuture<TPeerId>::TPtr asyncResult)
{
    LOG_INFO("Unable to find requested records at any master");

    asyncResult->Set(NElection::InvalidPeerId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
