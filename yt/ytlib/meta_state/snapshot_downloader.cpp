#include "stdafx.h"
#include "snapshot_downloader.h"
#include "snapshot.h"
#include "meta_state_manager_rpc.h"

#include "../actions/action_util.h"
#include "../actions/future.h"

#include <util/system/fs.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TSnapshotDownloader::TSnapshotDownloader(
    TConfig* config,
    TCellManager::TPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{
    YASSERT(cellManager);
}

TSnapshotDownloader::EResult TSnapshotDownloader::GetSnapshot(
    i32 segmentId,
    TFile* snapshotFile)
{
    YASSERT(snapshotFile);

    TSnapshotInfo snapshotInfo = GetSnapshotInfo(segmentId);
    TPeerId sourceId = snapshotInfo.SourceId;
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

    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        LOG_INFO("Requesting snapshot info from peer %d", peerId);

        auto proxy = CellManager->GetMasterProxy<TProxy>(peerId);
        proxy->SetTimeout(Config->LookupTimeout);

        auto request = proxy->GetSnapshotInfo();
        request->set_snapshotid(snapshotId);
        awaiter->Await(request->Invoke(), FromMethod(
            &TSnapshotDownloader::OnResponse,
            awaiter, asyncResult, peerId));
    }

    awaiter->Complete(FromMethod(
        &TSnapshotDownloader::OnComplete,
        snapshotId,
        asyncResult));

    return asyncResult->Get();
}

void TSnapshotDownloader::OnResponse(
    TProxy::TRspGetSnapshotInfo::TPtr response,
    TParallelAwaiter::TPtr awaiter,
    TFuture<TSnapshotInfo>::TPtr asyncResult,
    TPeerId peerId)
{
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

void TSnapshotDownloader::OnComplete(
    i32 segmentId,
    TFuture<TSnapshotInfo>::TPtr asyncResult)
{
    LOG_INFO("Could not get snapshot %d info from masters", segmentId);

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
    } catch (...) {
        LOG_FATAL("Error closing snapshot writer (SnapshotId: %d)\n%s",
            segmentId,
            ~CurrentExceptionMessage());
    }

    return EResult::OK;
}

TSnapshotDownloader::EResult TSnapshotDownloader::WriteSnapshot(
    i32 snapshotId,
    i64 snapshotLength,
    i32 sourceId,
    TOutputStream& output)
{
    LOG_INFO("Started downloading snapshot (SnapshotId: %d, Length: %" PRId64 ", PeerId: %d)",
            snapshotId,
            snapshotLength,
            sourceId);

    auto proxy = CellManager->GetMasterProxy<TProxy>(sourceId);
    proxy->SetTimeout(Config->ReadTimeout);

    i64 downloadedLength = 0;
    while (downloadedLength < snapshotLength) {
        auto request = proxy->ReadSnapshot();
        request->set_snapshotid(snapshotId);
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
        } catch (...) {
            LOG_FATAL("Error writing snapshot\n%s", ~CurrentExceptionMessage());
        }

        downloadedLength += block.Size();
    }

    LOG_INFO("Finished downloading snapshot");

    return EResult::OK;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
