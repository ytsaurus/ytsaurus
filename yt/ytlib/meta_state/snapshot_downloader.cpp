#include "stdafx.h"
#include "snapshot_downloader.h"
#include "private.h"
#include "config.h"
#include "snapshot.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/election/cell_manager.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/bind.h>
#include <ytlib/actions/future.h>

#include <util/system/fs.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TSnapshotDownloader::TSnapshotDownloader(
    TSnapshotDownloaderConfigPtr config,
    TCellManagerPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{
    YCHECK(config);
    YCHECK(cellManager);
}

TError TSnapshotDownloader::DownloadSnapshot(
    i32 snapshotId,
    const Stroka& fileName)
{
    auto snapshotInfo = GetSnapshotInfo(snapshotId);
    auto sourceId = snapshotInfo.SourceId;
    if (sourceId == NElection::InvalidPeerId) {
        return TError("Snapshot is not found: %d", snapshotId);
    }

    return DownloadSnapshot(fileName, snapshotId, snapshotInfo);
}

TSnapshotDownloader::TSnapshotInfo TSnapshotDownloader::GetSnapshotInfo(i32 snapshotId)
{
    auto promise = NewPromise<TSnapshotInfo>();
    auto awaiter = New<TParallelAwaiter>();

    LOG_INFO("Getting snapshot %d info from peers", snapshotId);
    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        LOG_INFO("Requesting snapshot info from peer %d", peerId);

        TProxy proxy(CellManager->GetMasterChannel(peerId));
        proxy.SetDefaultTimeout(Config->LookupTimeout);

        auto request = proxy.GetSnapshotInfo();
        request->set_snapshot_id(snapshotId);
        awaiter->Await(request->Invoke(), BIND(
            &TSnapshotDownloader::OnSnapshotInfoResponse,
            awaiter,
            promise,
            peerId));
    }
    LOG_INFO("Snapshot info requests sent");

    awaiter->Complete(BIND(
        &TSnapshotDownloader::OnSnapshotInfoComplete,
        snapshotId,
        promise));

    return promise.Get();
}

void TSnapshotDownloader::OnSnapshotInfoResponse(
    TParallelAwaiterPtr awaiter,
    TPromise<TSnapshotInfo> promise,
    TPeerId peerId,
    TProxy::TRspGetSnapshotInfoPtr response)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!response->IsOK()) {
        LOG_INFO(response->GetError(), "Error requesting snapshot info from peer %d",
            peerId);
        return;
    }
    
    i64 length = response->length();
    
    LOG_INFO("Got snapshot info from peer %d (Length: %" PRId64 ")",
        peerId,
        length);

    promise.Set(TSnapshotInfo(peerId, length));
    awaiter->Cancel();
}

void TSnapshotDownloader::OnSnapshotInfoComplete(
    i32 snapshotId,
    TPromise<TSnapshotInfo> promise)
{
    LOG_INFO("Could not get snapshot %d info from peers", snapshotId);

    promise.Set(TSnapshotInfo(NElection::InvalidPeerId, -1));
}

TError TSnapshotDownloader::DownloadSnapshot(
    const Stroka& fileName,
    i32 snapshotId,
    const TSnapshotInfo& snapshotInfo)
{
    YASSERT(snapshotInfo.Length >= 0);
    
    auto sourceId = snapshotInfo.SourceId;

    TAutoPtr<TFile> file;
    try {
        file = new TFile(fileName, CreateAlways | WrOnly | Seq);
        file->Resize(snapshotInfo.Length);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "IO error opening snapshot %d for writing",
            snapshotId);
    }

    TBufferedFileOutput output(*file);
    
    auto error = WriteSnapshot(snapshotId, snapshotInfo.Length, sourceId, &output);
    if (!error.IsOK()) {
        return error;
    }

    try {
        output.Flush();
        file->Flush();
        file->Close();
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Error closing snapshot %d",
            snapshotId);
    }

    return TError();
}

TError TSnapshotDownloader::WriteSnapshot(
    i32 snapshotId,
    i64 snapshotLength,
    i32 sourceId,
    TOutputStream* output)
{
    LOG_INFO("Started downloading snapshot %d from peer %d (Length: %" PRId64 ")",
        snapshotId,
        sourceId,
        snapshotLength);

    TProxy proxy(CellManager->GetMasterChannel(sourceId));
    proxy.SetDefaultTimeout(Config->ReadTimeout);

    i64 downloadedLength = 0;
    while (downloadedLength < snapshotLength) {
        auto req = proxy.ReadSnapshot();
        req->set_snapshot_id(snapshotId);
        req->set_offset(downloadedLength);
        i32 blockSize = static_cast<i32>(std::min(
            static_cast<i64>(Config->BlockSize),
            snapshotLength - downloadedLength));
        req->set_length(blockSize);

        auto rsp = req->Invoke().Get();
        if (!rsp->IsOK()) {
            return TError("Error downloading snapshot from peer %d",
                sourceId)
                << *rsp;
        }
        
        const auto& attachments = rsp->Attachments();
        TRef block(attachments.at(0));
        if (static_cast<i32>(block.Size()) != blockSize) {
            LOG_WARNING("Snapshot block of wrong size received (Offset: %" PRId64 ", Size: %d, ExpectedSize: %d)",
                downloadedLength,
                static_cast<i32>(block.Size()),
                blockSize);
            // continue anyway
        } else {
            LOG_DEBUG("Snapshot block received (Offset: %" PRId64 ", Size: %d)",
                downloadedLength,
                blockSize);
        }

        try {
            output->Write(block.Begin(), block.Size());
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error writing snapshot %d",
                snapshotId);
        }

        downloadedLength += block.Size();
    }

    LOG_INFO("Finished downloading snapshot");

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
