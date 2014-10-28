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

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoDownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore,
    int snapshotId)
{
    try {
        auto params = WaitFor(DiscoverSnapshot(config, cellManager, snapshotId));
        if (params.SnapshotId == NonexistingSegmentId) {
            THROW_ERROR_EXCEPTION("Unable to find a download source for snapshot %v",
                snapshotId);
        }

        auto writer = fileStore->CreateRawWriter(snapshotId);

        LOG_INFO("Downloading %v bytes from peer %v",
            params.CompressedLength,
            params.PeerId);

        TSnapshotServiceProxy proxy(cellManager->GetPeerChannel(params.PeerId));
        proxy.SetDefaultTimeout(config->SnapshotDownloadRpcTimeout);

        i64 downloadedLength = 0;
        while (downloadedLength < params.CompressedLength) {
            auto req = proxy.ReadSnapshot();
            req->set_snapshot_id(snapshotId);
            req->set_offset(downloadedLength);
            i64 blockSize = std::min(
                config->SnapshotDownloadBlockSize,
                params.CompressedLength - downloadedLength);
            req->set_length(blockSize);

            auto rsp = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error downloading snapshot");

            const auto& attachments = rsp->Attachments();
            YCHECK(attachments.size() == 1);

            const auto& block = attachments[0];
            if (block.Size() != blockSize) {
                LOG_WARNING("Snapshot block of wrong size received (Offset: %v, Size: %v, ExpectedSize: %v)",
                    downloadedLength,
                    block.Size(),
                    blockSize);
                // Continue anyway.
            } else {
                LOG_DEBUG("Snapshot block received (Offset: %v, Size: %v)",
                    downloadedLength,
                    blockSize);
            }

            writer->GetStream()->Write(block.Begin(), block.Size());

            downloadedLength += blockSize;
        }

        writer->Close();

        fileStore->ConfirmSnapshot(snapshotId);

        LOG_INFO("Snapshot downloaded successfully");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error downloading snapshot %v", snapshotId)
           << ex;
    }
}

} // namespace

TAsyncError DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore,
    int snapshotId)
{
    return BIND(DoDownloadSnapshot)
        .Guarded()
        .AsyncVia(GetHydraIOInvoker())
        .Run(config, cellManager, fileStore, snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
