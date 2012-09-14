#include "stdafx.h"
#include "config.h"
#include "change_log_downloader.h"
#include "async_change_log.h"
#include "meta_version.h"
#include "private.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/election/cell_manager.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

//////////////////////////////////////////////////////////////////////////////////

TChangeLogDownloader::TChangeLogDownloader(
    TChangeLogDownloaderConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , ControlInvoker(controlInvoker)
{
    YCHECK(config);
    YCHECK(cellManager);
    YCHECK(ControlInvoker);
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
    auto promise = NewPromise<TPeerId>();
    auto awaiter = New<TParallelAwaiter>(
        ControlInvoker,
        &Profiler,
        "/changelog_source_lookup_time");

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        LOG_INFO("Requesting changelog info from peer %d", id);

        TProxy proxy(CellManager->GetMasterChannel(id));
        proxy.SetDefaultTimeout(Config->LookupTimeout);
        
        auto request = proxy.GetChangeLogInfo();
        request->set_change_log_id(version.SegmentId);
        awaiter->Await(
            request->Invoke(),
            EscapeYPathToken(CellManager->GetPeerAddress(id)),
            BIND(
                &TChangeLogDownloader::OnResponse,
                awaiter,
                promise,
                id,
                version));
    }

    awaiter->Complete(BIND(&TChangeLogDownloader::OnComplete, promise));

    return promise.Get();
}

TChangeLogDownloader::EResult TChangeLogDownloader::DownloadChangeLog(
    TMetaVersion version,
    TPeerId sourceId,
    TAsyncChangeLog& changeLog)
{
    int downloadedRecordCount = changeLog.GetRecordCount();

    LOG_INFO("Started downloading records %d-%d from peer %d",
        changeLog.GetRecordCount(),
        version.RecordCount - 1,
        sourceId);

    TProxy proxy(CellManager->GetMasterChannel(sourceId));
    proxy.SetDefaultTimeout(Config->ReadTimeout);

    while (downloadedRecordCount < version.RecordCount) {
        auto request = proxy.ReadChangeLog();
        request->set_change_log_id(version.SegmentId);
        request->set_start_record_id(downloadedRecordCount);
        int desiredChunkSize = Min(
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
                        LOG_FATAL(error, "Unexpected error received from peer %d",
                            sourceId);
                        break;
                }
            } else {
                LOG_WARNING(error, "Error reading changelog from peer %d",
                    sourceId);
                return EResult::RemoteError;
            }
        }

        YASSERT(response->Attachments().size() == 1);
        // Don't forget to unpack obtained refs
        std::vector<TSharedRef> attachments;
        UnpackRefs(response->Attachments().front(), &attachments);
        if (attachments.empty()) {
            LOG_WARNING("Peer %d does not have %d records of changelog %d anymore",
                sourceId,
                version.RecordCount,
                version.SegmentId);
            return EResult::ChangeLogUnavailable;
        }

        int attachmentCount = static_cast<int>(attachments.size());
        if (attachmentCount != desiredChunkSize) {
            // Continue anyway.
            LOG_DEBUG("Received records %d-%d while %d records were requested",
                downloadedRecordCount,
                downloadedRecordCount + attachmentCount - 1,
                desiredChunkSize);
        } else {
            LOG_DEBUG("Received records %d-%d",
                downloadedRecordCount,
                downloadedRecordCount + attachmentCount - 1);
        }

        for (int i = 0; i < static_cast<int>(attachments.size()); ++i) {
            changeLog.Append(downloadedRecordCount, attachments[i]);
            ++downloadedRecordCount;
        }
    }

    LOG_INFO("Finished downloading changelog");

    return EResult::OK;
}

void TChangeLogDownloader::OnResponse(
    TParallelAwaiterPtr awaiter,
    TPromise<TPeerId> promise,
    TPeerId peerId,
    TMetaVersion version,
    TProxy::TRspGetChangeLogInfoPtr response)
{
    if (!response->IsOK()) {
        LOG_WARNING(response->GetError(), "Error requesting changelog %d info from peer %d",
            version.SegmentId,
            peerId);
        return;
    }

    int recordCount = response->record_count();
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

    promise.Set(peerId);
    awaiter->Cancel();
}

void TChangeLogDownloader::OnComplete(
    TPromise<TPeerId> promise)
{
    LOG_INFO("Unable to find requested records at any peer");

    promise.Set(NElection::InvalidPeerId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
