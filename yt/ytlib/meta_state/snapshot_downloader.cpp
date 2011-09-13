#include "snapshot_downloader.h"
#include "snapshot.h"
#include "meta_state_manager_rpc.h"

#include "../actions/action_util.h"
#include "../actions/async_result.h"

#include <util/system/fs.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TSnapshotDownloader::TSnapshotDownloader(
    const TConfig& config,
    TCellManager::TPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{}

TSnapshotDownloader::EResult TSnapshotDownloader::GetSnapshot(
    i32 segmentId,
    TSnapshotWriter* snapshotWriter)
{
    TSnapshotInfo snapshotInfo = GetSnapshotInfo(segmentId);
    TPeerId sourceId = snapshotInfo.SourceId;
    if (sourceId == InvalidPeerId) {
        return EResult::SnapshotNotFound;
    }
    
    EResult result = DownloadSnapshot(segmentId, snapshotInfo, snapshotWriter);
    if (result != EResult::OK) {
        return result;
    }

    return EResult::OK;
}

TSnapshotDownloader::TSnapshotInfo TSnapshotDownloader::GetSnapshotInfo(i32 snapshotId)
{
    auto asyncResult = New< TAsyncResult<TSnapshotInfo> >();
    auto awaiter = New<TParallelAwaiter>();

    for (TPeerId i = 0; i < CellManager->GetPeerCount(); ++i) {
        LOG_INFO("Requesting snapshot info from peer %d", i);

        auto proxy = CellManager->GetMasterProxy<TProxy>(i);
        auto request = proxy->GetSnapshotInfo();
        request->SetSnapshotId(snapshotId);
        awaiter->Await(request->Invoke(Config.LookupTimeout), FromMethod(
            &TSnapshotDownloader::OnResponse,
            awaiter, asyncResult, i));
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
    TAsyncResult<TSnapshotInfo>::TPtr asyncResult,
    TPeerId peerId)
{
    if (!response->IsOK()) {
        // We have no snapshot id to log it here
        LOG_INFO("Error %s requesting snapshot info from peer %d",
            ~response->GetErrorCode().ToString(),
            peerId);
        return;
    }
    
    i64 length = response->GetLength();
    ui64 checksum = response->GetChecksum();
    i32 prevRecordCount = response->GetPrevRecordCount();
    
    LOG_INFO("Got snapshot info from peer %d (Length: %" PRId64 ", Checksum: %" PRIx64 ")",
        peerId,
        length,
        checksum);

    asyncResult->Set(TSnapshotInfo(peerId, length, prevRecordCount, checksum));
    awaiter->Cancel();
}

void TSnapshotDownloader::OnComplete(
    i32 segmentId,
    TAsyncResult<TSnapshotInfo>::TPtr asyncResult)
{
    LOG_INFO("Could not get snapshot %d info from masters", segmentId);

    asyncResult->Set(TSnapshotInfo(InvalidPeerId, -1, 0, 0));
}

TSnapshotDownloader::EResult TSnapshotDownloader::DownloadSnapshot(
    i32 segmentId,
    TSnapshotInfo snapshotInfo,
    TSnapshotWriter* snapshotWriter)
{
    YASSERT(snapshotInfo.Length >= 0);
    
    TPeerId sourceId = snapshotInfo.SourceId;
    try {
        snapshotWriter->Open(snapshotInfo.PrevRecordCount);
    } catch (const yexception& ex) {
        LOG_ERROR("Could not open snapshot writer: %s", ex.what());
        return EResult::IOError;
    }

    TOutputStream& output = snapshotWriter->GetStream();
    EResult result = WriteSnapshot(segmentId, snapshotInfo.Length, sourceId, output);
    if (result != EResult::OK) {
        return result;
    }

    try {
        snapshotWriter->Close();
    } catch (const yexception& ex) {
        LOG_ERROR("Could not close snapshot writer: %s", ex.what());
        return EResult::IOError;
    }

    if (snapshotWriter->GetChecksum() != snapshotInfo.Checksum) {
        LOG_ERROR(
            "Incorrect checksum in snapshot %d from peer %d, "
            "expected %" PRIx64 ", got %" PRIx64,
            segmentId, sourceId, snapshotInfo.Checksum, snapshotWriter->GetChecksum());
        return EResult::IncorrectChecksum;
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
    i64 downloadedLength = 0;
    while (downloadedLength < snapshotLength) {
        auto request = proxy->ReadSnapshot();
        request->SetSnapshotId(snapshotId);
        request->SetOffset(downloadedLength);
        i32 blockSize = Min(Config.BlockSize, (i32)(snapshotLength - downloadedLength));
        request->SetLength(blockSize);
        auto response = request->Invoke(Config.ReadTimeout)->Get();

        if (!response->IsOK()) {
            TProxy::EErrorCode errorCode = response->GetErrorCode();
            if (response->IsServiceError()) {
                switch (errorCode.ToValue()) {
                    case TProxy::EErrorCode::InvalidSegmentId:
                        LOG_WARNING(
                            "Peer %d does not have snapshot %d anymore",
                            sourceId,
                            snapshotId);
                        return EResult::SnapshotUnavailable;

                    case TProxy::EErrorCode::IOError:
                        LOG_WARNING(
                            "IO error occurred on peer %d during downloading snapshot %d",
                            sourceId,
                            snapshotId);
                        return EResult::RemoteError;

                    default:
                        LOG_FATAL("Unknown error code %s received from peer %d",
                            ~errorCode.ToString(),
                            sourceId);
                        break;
                }
            } else {
                LOG_WARNING("RPC error %s reading snapshot from peer %d",
                    ~errorCode.ToString(),
                    sourceId);
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
        } catch (const yexception& ex) {
            LOG_ERROR("Exception occurred while writing to output: %s",
                ex.what());
            return EResult::IOError;
        }

        downloadedLength += block.Size();
    }

    LOG_INFO("Finished downloading snapshot");

    return EResult::OK;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
