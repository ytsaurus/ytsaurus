#include "stdafx.h"
#include "snapshot_download.h"
#include "private.h"
#include "config.h"
#include "snapshot.h"
#include "snapshot_discovery.h"

#include <core/concurrency/fiber.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

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
        ISnapshotStorePtr snapshotStore)
        : Config(config)
        , CellManager(cellManager)
        , SnapshotStore(snapshotStore)
        , Logger(HydraLogger)
    {
        YCHECK(Config);
        YCHECK(CellManager);
        YCHECK(SnapshotStore);

        Logger.AddTag(Sprintf("CellGuid: %s",
            ~ToString(CellManager->GetCellGuid())));
    }

    TAsyncError Run(int snapshotId)
    {
        return BIND(&TSnapshotDownloader::DoRun, MakeStrong(this))
            .AsyncVia(HydraIOQueue->GetInvoker())
            .Run(snapshotId);
    }

private:
    TDistributedHydraManagerConfigPtr Config;
    TCellManagerPtr CellManager;
    ISnapshotStorePtr SnapshotStore;

    NLog::TTaggedLogger Logger;

    TError DoRun(int snapshotId)
    {
        try {
            auto asyncSnapshotInfo = DiscoverSnapshot(Config, CellManager, snapshotId);
            auto snapshotInfo = WaitFor(asyncSnapshotInfo);
            if (snapshotInfo.SnapshotId == NonexistingSegmentId) {
                THROW_ERROR_EXCEPTION("Unable to find a download source for snapshot %d",
                    snapshotId);
            }

            auto writer = SnapshotStore->CreateRawWriter(snapshotId);

            LOG_INFO("Downloading %" PRId64 " bytes from peer %d",
                snapshotInfo.Length,
                snapshotInfo.PeerId);

            THydraServiceProxy proxy(CellManager->GetPeerChannel(snapshotInfo.PeerId));
            proxy.SetDefaultTimeout(Config->SnapshotDownloader->RpcTimeout);

            i64 downloadedLength = 0;
            while (downloadedLength < snapshotInfo.Length) {
                auto req = proxy.ReadSnapshot();
                req->set_snapshot_id(snapshotId);
                req->set_offset(downloadedLength);
                i64 blockSize = std::min(
                    Config->SnapshotDownloader->BlockSize,
                    snapshotInfo.Length - downloadedLength);
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

            SnapshotStore->ConfirmSnapshot(snapshotId);

            LOG_INFO("Snapshot downloaded successfully");

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

};

TAsyncError DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    ISnapshotStorePtr snapshotStore,
    int snapshotId)
{
    auto downloader = New<TSnapshotDownloader>(
        config,
        cellManager,
        snapshotStore);
    return downloader->Run(snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
