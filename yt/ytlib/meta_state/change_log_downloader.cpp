#include "stdafx.h"
#include "config.h"
#include "change_log_downloader.h"
#include "async_change_log.h"
#include "meta_version.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/election/cell_manager.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("/meta_state");

//////////////////////////////////////////////////////////////////////////////////

TChangeLogDownloader::TChangeLogDownloader(
    TChangeLogDownloaderConfig* config,
    TCellManager* cellManager)
    : Config(config)
    , CellManager(cellManager)
{
    YASSERT(cellManager);
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
    auto awaiter = New<TParallelAwaiter>(&Profiler, "/changelog_source_lookup_time");

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        LOG_INFO("Requesting changelog info from peer %d", id);

        auto request =
            CellManager->GetMasterProxy<TProxy>(id)
            ->GetChangeLogInfo()
            ->SetTimeout(Config->LookupTimeout);
        request->set_change_log_id(version.SegmentId);
        awaiter->Await(
            request->Invoke(),
            EscapeYPath(CellManager->GetPeerAddress(id)),
            BIND(
                &TChangeLogDownloader::OnResponse,
                awaiter,
                asyncResult,
                id,
                version));
    }

    awaiter->Complete(BIND(&TChangeLogDownloader::OnComplete, asyncResult));

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
    proxy->SetDefaultTimeout(Config->ReadTimeout);

    while (downloadedRecordCount < version.RecordCount) {
        auto request = proxy->ReadChangeLog();
        request->set_change_log_id(version.SegmentId);
        request->set_start_record_id(downloadedRecordCount);
        i32 desiredChunkSize = Min(
            Config->RecordsPerRequest,
            version.RecordCount - downloadedRecordCount);
        request->set_record_count(desiredChunkSize);

        LOG_DEBUG("Requesting records %d-%d",
            downloadedRecordCount,
            downloadedRecordCount + desiredChunkSize - 1);

        auto response = request->Invoke().Get();

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
                LOG_WARNING("Error reading changelog from peer %d\n%s",
                    sourceId,
                    ~error.ToString());
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
    TParallelAwaiter::TPtr awaiter,
    TFuture<TPeerId>::TPtr asyncResult,
    TPeerId peerId,
    TMetaVersion version,
    TProxy::TRspGetChangeLogInfo::TPtr response)
{
    if (!response->IsOK()) {
        LOG_INFO("Error %s requesting info on changelog %d from peer %d",
            ~response->GetError().ToString(),
            version.SegmentId,
            peerId);
        return;
    }

    i32 recordCount = response->record_count();
    if (recordCount < version.RecordCount) {
        LOG_INFO("Peer %d has only %d records while %d records needed",
            peerId,
            recordCount,
            version.RecordCount);
        return;
    }

    LOG_INFO("Peer %d has %d records, which is enough",
        peerId,
        recordCount);

    asyncResult->Set(peerId);
    awaiter->Cancel();
}

void TChangeLogDownloader::OnComplete(
    TFuture<TPeerId>::TPtr asyncResult)
{
    LOG_INFO("Unable to find requested records at any peer");

    asyncResult->Set(NElection::InvalidPeerId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
