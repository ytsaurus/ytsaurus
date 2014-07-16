#include "stdafx.h"
#include "snapshot_download.h"
#include "private.h"
#include "config.h"
#include "snapshot.h"
#include "snapshot_discovery.h"
#include "file_snapshot_store.h"

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <ytlib/election/cell_manager.h>

#include <server/hydra/snapshot_service_proxy.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDownloader
    : public TRefCounted
{
public:
    TSnapshotDownloader(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        TFileSnapshotStorePtr fileStore)
        : Config_(config)
        , CellManager_(cellManager)
        , FileStore_(fileStore)
        , Logger(HydraLogger)
    {
        YCHECK(Config_);
        YCHECK(CellManager_);
        YCHECK(FileStore_);

        Logger.AddTag("CellGuid: %v", CellManager_->GetCellGuid());
    }

    TAsyncError Run(int snapshotId)
    {
        return BIND(&TSnapshotDownloader::DoRun, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

private:
    TDistributedHydraManagerConfigPtr Config_;
    TCellManagerPtr CellManager_;
    TFileSnapshotStorePtr FileStore_;

    NLog::TLogger Logger;


    TError DoRun(int snapshotId)
    {
        try {
            auto params = WaitFor(DiscoverSnapshot(Config_, CellManager_, snapshotId));
            if (params.SnapshotId == NonexistingSegmentId) {
                THROW_ERROR_EXCEPTION("Unable to find a download source for snapshot %d",
                    snapshotId);
            }

            auto writer = FileStore_->CreateRawWriter(snapshotId);

            LOG_INFO("Downloading %" PRId64 " bytes from peer %d",
                params.CompressedLength,
                params.PeerId);

            TSnapshotServiceProxy proxy(CellManager_->GetPeerChannel(params.PeerId));
            proxy.SetDefaultTimeout(Config_->SnapshotDownloader->RpcTimeout);

            i64 downloadedLength = 0;
            while (downloadedLength < params.CompressedLength) {
                auto req = proxy.ReadSnapshot();
                req->set_snapshot_id(snapshotId);
                req->set_offset(downloadedLength);
                i64 blockSize = std::min(
                    Config_->SnapshotDownloader->BlockSize,
                    params.CompressedLength - downloadedLength);
                req->set_length(blockSize);

                auto rsp = WaitFor(req->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error downloading snapshot");

                const auto& attachments = rsp->Attachments();
                YCHECK(attachments.size() == 1);

                const auto& block = attachments[0];
                if (block.Size() != blockSize) {
                    LOG_WARNING("Snapshot block of wrong size received (Offset: %" PRId64 ", Size: %" PRISZT ", ExpectedSize: %" PRId64 ")",
                        downloadedLength,
                        block.Size(),
                        blockSize);
                    // Continue anyway.
                } else {
                    LOG_DEBUG("Snapshot block received (Offset: %" PRId64 ", Size: %" PRId64 ")",
                        downloadedLength,
                        blockSize);
                }

                writer->GetStream()->Write(block.Begin(), block.Size());

                downloadedLength += blockSize;
            }

            writer->Close();

            FileStore_->ConfirmSnapshot(snapshotId);

            LOG_INFO("Snapshot downloaded successfully");

            return TError();
        } catch (const std::exception& ex) {
            return TError("Error downloading snapshot %d", snapshotId)
                << ex;
        }
    }

};

TAsyncError DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore,
    int snapshotId)
{
    auto downloader = New<TSnapshotDownloader>(
        config,
        cellManager,
        fileStore);
    return downloader->Run(snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
