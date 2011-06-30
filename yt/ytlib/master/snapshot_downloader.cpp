#include "snapshot_downloader.h"
#include "snapshot.h"
#include "master_state_manager_rpc.h"

#include "../actions/action_util.h"
#include "../actions/async_result.h"
#include "../logging/log.h"

#include <util/system/fs.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("SnapshotDownloader");

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
    TMasterId sourceId = snapshotInfo.SourceId;
    if (sourceId == InvalidMasterId) {
        return SnapshotNotFound;
    }
    
    LOG_DEBUG("Downloading snapshot %d from master %d", segmentId, sourceId);

    EResult result = DownloadSnapshot(segmentId, snapshotInfo, snapshotWriter);
    if (result != OK) {
        return result;
    }

    LOG_DEBUG("Finished downloading snapshot %d from master %d",
                segmentId, sourceId);

    return OK;
}

TSnapshotDownloader::TSnapshotInfo TSnapshotDownloader::GetSnapshotInfo(i32 snapshotId)
{
    TAsyncResult<TSnapshotInfo>::TPtr asyncResult = new TAsyncResult<TSnapshotInfo>();
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter();

    for (TMasterId i = 0; i < CellManager->GetMasterCount(); ++i) {
        LOG_DEBUG("Requesting snapshot info from master %d", i);

        TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(i);
        TProxy::TReqGetSnapshotInfo::TPtr request = proxy->GetSnapshotInfo();
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
    TMasterId masterId)
{
    if (!response->IsOK()) {
        // We have no snapshot id to log it here
        LOG_DEBUG("Error %s requesting snapshot info from master %d",
            ~response->GetErrorCode().ToString(),
            masterId);
        return;
    }
    
    i64 length = response->GetLength();
    ui64 checksum = response->GetChecksum();
    i32 prevRecordCount = response->GetPrevRecordCount();
    
    LOG_DEBUG("Got snapshot info from master %d (length: %" PRId64 ", checksum: %" PRIx64 ")",
        masterId,
        length,
        checksum);

    asyncResult->Set(TSnapshotInfo(masterId, length, prevRecordCount, checksum));
    awaiter->Cancel();
}

void TSnapshotDownloader::OnComplete(
    i32 segmentId,
    TAsyncResult<TSnapshotInfo>::TPtr asyncResult)
{
    LOG_DEBUG("Could not get snapshot %d info from masters",
        segmentId);
    asyncResult->Set(TSnapshotInfo(InvalidMasterId, -1, 0, 0));
}

TSnapshotDownloader::EResult TSnapshotDownloader::DownloadSnapshot(
    i32 segmentId,
    TSnapshotInfo snapshotInfo,
    TSnapshotWriter* snapshotWriter)
{
    YASSERT(snapshotInfo.Length >= 0);
    
    TMasterId sourceId = snapshotInfo.SourceId;
    try {
        snapshotWriter->Open(snapshotInfo.PrevRecordCount);
    } catch (const yexception& ex) {
        LOG_ERROR("Could not open snapshot writer: %s", ex.what());
        return IOError;
    }

    TOutputStream& output = snapshotWriter->GetStream();
    EResult result = WriteSnapshot(segmentId, snapshotInfo.Length, sourceId, output);
    if (result != OK) {
        return result;
    }

    try {
        snapshotWriter->Close();
    } catch (const yexception& ex) {
        LOG_ERROR("Could not close snapshot writer: %s", ex.what());
        return IOError;
    }

    if (snapshotWriter->GetChecksum() != snapshotInfo.Checksum) {
        LOG_ERROR(
            "Incorrect checksum in snapshot %d from master %d, "
            "expected %" PRIx64 ", got %" PRIx64,
            segmentId, sourceId, snapshotInfo.Checksum, snapshotWriter->GetChecksum());
        return IncorrectChecksum;
    }
    
    return OK;
}

TSnapshotDownloader::EResult TSnapshotDownloader::WriteSnapshot(
    i32 snapshotId,
    i64 snapshotLength,
    i32 sourceId,
    TOutputStream& output)
{
    TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(sourceId);
    i64 downloadedLength = 0;
    while (downloadedLength < snapshotLength) {
        TProxy::TReqReadSnapshot::TPtr request = proxy->ReadSnapshot();
        request->SetSnapshotId(snapshotId);
        request->SetOffset(downloadedLength);
        i32 blockSize = Min(Config.BlockSize, (i32)(snapshotLength - downloadedLength));
        request->SetLength(blockSize);
        TProxy::TRspReadSnapshot::TPtr response =
            request->Invoke(Config.ReadTimeout)->Get();

        if (!response->IsOK()) {
            TProxy::EErrorCode errorCode = response->GetErrorCode();
            if (response->IsServiceError()) {
                switch (errorCode.ToValue()) {
                    case TProxy::EErrorCode::InvalidSegmentId:
                        LOG_WARNING(
                            "Master %d does not have snapshot %d anymore",
                            sourceId,
                            snapshotId);
                        return SnapshotUnavailable;

                    case TProxy::EErrorCode::IOError:
                        LOG_WARNING(
                            "IO error occurred on master %d during downloading snapshot %d",
                            sourceId,
                            snapshotId);
                        return RemoteError;

                    default:
                        LOG_FATAL("Unknown error code %s received from master %d",
                            ~errorCode.ToString(),
                            sourceId);
                        break;
                }
            } else {
                LOG_DEBUG("RPC error %s reading snapshot from master %d",
                    ~errorCode.ToString(),
                    sourceId);
                return RemoteError;
            }
        }
        
        const yvector<TSharedRef>& attachments = response->Attachments();
        TRef block(attachments.at(0));

        if (static_cast<i32>(block.Size()) != blockSize) {
            LOG_WARNING("Received block with offset %" PRId64 " and size %d while size %d was expected",
                downloadedLength,
                static_cast<i32>(block.Size()),
                blockSize);
            // continue anyway
        } else {
            LOG_DEBUG("Received block with offset %" PRId64 " and size %d",
                downloadedLength,
                blockSize);
        }

        try {
            output.Write(block.Begin(), block.Size());
        } catch (const yexception& ex) {
            LOG_ERROR("Exception occurred while writing to output: %s",
                ex.what());
            return IOError;
        }

        downloadedLength += block.Size();
    }
    return OK;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
