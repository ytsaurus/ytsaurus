#include "stdafx.h"
#include "snapshot_downloader.h"
#include "common.h"
#include "snapshot.h"
#include "meta_state_manager_proxy.h"
#include "cell_manager.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/action_util.h>
#include <ytlib/actions/future.h>

#include <util/system/fs.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TSnapshotDownloader::TSnapshotDownloader(
    TSnapshotDownloaderConfigPtr config,
    TCellManagerPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{
    YASSERT(config);
    YASSERT(cellManager);
}

TSnapshotDownloader::EResult TSnapshotDownloader::DownloadSnapshot(
    i32 segmentId,
    TFile* snapshotFile)
{
    YASSERT(snapshotFile);

    auto snapshotInfo = GetSnapshotInfo(segmentId);
    auto sourceId = snapshotInfo.SourceId;
    if (sourceId == NElection::InvalidPeerId) {
        return EResult::SnapshotNotFound;
    }
    
    EResult result = DownloadSnapshot(segmentId, snapshotInfo, snapshotFile);
    if (result != EResult::OK) {
        return result;
    }

    return EResult::OK;
}

TSnapshotDownloader::TSnapshotInfo TSnapshotDownloader::GetSnapshotInfo(i32 snapshotId)
{
    auto asyncResult = New< TFuture<TSnapshotInfo> >();
    auto awaiter = New<TParallelAwaiter>();

    LOG_INFO("Getting snapshot %s info from peers");
    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        LOG_INFO("Requesting snapshot info from peer %d", peerId);

        auto request =
            CellManager->GetMasterProxy<TProxy>(peerId)
            ->GetSnapshotInfo()
            ->SetTimeout(Config->LookupTimeout);
        request->set_snapshot_id(snapshotId);
        awaiter->Await(request->Invoke(), FromMethod(
            &TSnapshotDownloader::OnSnapshotInfoResponse,
            awaiter, asyncResult, peerId));
    }
    LOG_INFO("Snapshot info requests sent");

    awaiter->Complete(FromMethod(
        &TSnapshotDownloader::OnSnapshotInfoComplete,
        snapshotId,
        asyncResult));

    return asyncResult->Get();
}

void TSnapshotDownloader::OnSnapshotInfoResponse(
    TProxy::TRspGetSnapshotInfo::TPtr response,
    TParallelAwaiter::TPtr awaiter,
    TFuture<TSnapshotInfo>::TPtr asyncResult,
    TPeerId peerId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!response->IsOK()) {
        LOG_INFO("Error requesting snapshot info from peer %d\n%s",
            peerId,
            ~response->GetError().ToString());
        return;
    }
    
    i64 length = response->length();
    
    LOG_INFO("Got snapshot info from peer %d (Length: %" PRId64 ")",
        peerId,
        length);

    asyncResult->Set(TSnapshotInfo(peerId, length));
    awaiter->Cancel();
}

void TSnapshotDownloader::OnSnapshotInfoComplete(
    i32 segmentId,
    TFuture<TSnapshotInfo>::TPtr asyncResult)
{
    LOG_INFO("Could not get snapshot %d info from peers", segmentId);

    asyncResult->Set(TSnapshotInfo(NElection::InvalidPeerId, -1));
}

TSnapshotDownloader::EResult TSnapshotDownloader::DownloadSnapshot(
    i32 segmentId,
    TSnapshotInfo snapshotInfo,
    TFile* snapshotFile)
{
    YASSERT(snapshotInfo.Length >= 0);
    
    TPeerId sourceId = snapshotInfo.SourceId;

    snapshotFile->Resize(snapshotInfo.Length);
    TBufferedFileOutput writer(*snapshotFile);
    
    auto result = WriteSnapshot(segmentId, snapshotInfo.Length, sourceId, writer);
    if (result != EResult::OK) {
        return result;
    }

    try {
        writer.Flush();
        snapshotFile->Flush();
        snapshotFile->Close();
    } catch (const std::exception& ex) {
        LOG_FATAL("Error closing snapshot %d\n%s",
            segmentId,
            ex.what());
    }

    return EResult::OK;
}

TSnapshotDownloader::EResult TSnapshotDownloader::WriteSnapshot(
    i32 snapshotId,
    i64 snapshotLength,
    i32 sourceId,
    TOutputStream& output)
{
    LOG_INFO("Started downloading snapshot %d from peer %d (Length: %" PRId64 ")",
        snapshotId,
        sourceId,
        snapshotLength);

    auto proxy = CellManager->GetMasterProxy<TProxy>(sourceId);
    proxy->SetDefaultTimeout(Config->ReadTimeout);

    i64 downloadedLength = 0;
    while (downloadedLength < snapshotLength) {
        auto request = proxy->ReadSnapshot();
        request->set_snapshot_id(snapshotId);
        request->set_offset(downloadedLength);
        i32 blockSize = Min(Config->BlockSize, (i32)(snapshotLength - downloadedLength));
        request->set_length(blockSize);
        auto response = request->Invoke()->Get();

        if (!response->IsOK()) {
            auto error = response->GetError();
            if (NRpc::IsServiceError(error)) {
                switch (error.GetCode()) {
                    case EErrorCode::NoSuchSnapshot:
                        LOG_WARNING("Peer %d does not have snapshot %d anymore",
                            sourceId,
                            snapshotId);
                        return EResult::SnapshotUnavailable;

                    default:
                        LOG_FATAL("Unexpected error received from peer %d\n%s",
                            sourceId,
                            ~error.ToString());
                        break;
                }
            } else {
                LOG_WARNING("Error reading snapshot at peer %d\n%s",
                    sourceId,
                    ~error.ToString());
                return EResult::RemoteError;
            }
        }
        
        const yvector<TSharedRef>& attachments = response->Attachments();
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
            output.Write(block.Begin(), block.Size());
        } catch (const std::exception& ex) {
            LOG_FATAL("Error writing snapshot %d\n%s",
                snapshotId,
                ex.what());
        }

        downloadedLength += block.Size();
    }

    LOG_INFO("Finished downloading snapshot");

    return EResult::OK;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
