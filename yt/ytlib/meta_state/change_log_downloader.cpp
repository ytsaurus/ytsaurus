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

TError TChangeLogDownloader::Download(
    const TMetaVersion& version,
    TAsyncChangeLog* changeLog)
{
    LOG_INFO("Requested %d records in changelog %d",
        version.RecordCount,
        version.SegmentId);

    YCHECK(changeLog->GetId() == version.SegmentId);

    if (changeLog->GetRecordCount() >= version.RecordCount) {
        LOG_INFO("Local changelog already contains %d records, no download needed",
            changeLog->GetRecordCount());
        return TError();
    }

    auto sourceId = GetChangeLogSource(version);
    if (sourceId == NElection::InvalidPeerId) {
        return TError("Changelog is not found: %d", version.SegmentId);
    }

    return DownloadChangeLog(version, sourceId, changeLog);
}

TPeerId TChangeLogDownloader::GetChangeLogSource(const TMetaVersion& version)
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
            CellManager->GetPeerAddress(id),
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

TError TChangeLogDownloader::DownloadChangeLog(
    const TMetaVersion& version,
    TPeerId sourceId,
    TAsyncChangeLog* changeLog)
{
    int downloadedRecordCount = changeLog->GetRecordCount();

    LOG_INFO("Started downloading records %d-%d from peer %d",
        changeLog->GetRecordCount(),
        version.RecordCount - 1,
        sourceId);

    TProxy proxy(CellManager->GetMasterChannel(sourceId));
    proxy.SetDefaultTimeout(Config->ReadTimeout);

    while (downloadedRecordCount < version.RecordCount) {
        int desiredChunkSize = std::min(
            Config->RecordsPerRequest,
            version.RecordCount - downloadedRecordCount);

        LOG_DEBUG("Requesting records %d-%d",
            downloadedRecordCount,
            downloadedRecordCount + desiredChunkSize - 1);

        auto req = proxy.ReadChangeLog();
        req->set_change_log_id(version.SegmentId);
        req->set_start_record_id(downloadedRecordCount);
        req->set_record_count(desiredChunkSize);

        auto rsp = req->Invoke().Get();
        if (!rsp->IsOK()) {
            return TError("Error reading changelog %d from peer %d",
                version.SegmentId,
                sourceId)
                << *rsp;
        }

        YCHECK(rsp->Attachments().size() == 1);

        std::vector<TSharedRef> attachments;
        UnpackRefs(rsp->Attachments().front(), &attachments);

        if (attachments.empty()) {
            return TError("Peer %d does not have %d records of changelog %d anymore",
                sourceId,
                version.RecordCount,
                version.SegmentId);
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
            changeLog->Append(downloadedRecordCount, attachments[i]);
            ++downloadedRecordCount;
        }
    }

    LOG_INFO("Finished downloading changelog");

    return TError();
}

void TChangeLogDownloader::OnResponse(
    TParallelAwaiterPtr awaiter,
    TPromise<TPeerId> promise,
    TPeerId peerId,
    const TMetaVersion& version,
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
