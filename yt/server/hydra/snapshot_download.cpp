#include "snapshot_download.h"
#include "private.h"
#include "config.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "snapshot_discovery.h"

#include <yt/server/hydra/snapshot_service_proxy.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

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
        auto params = WaitFor(DiscoverSnapshot(config, cellManager, snapshotId))
            .ValueOrThrow();
        auto writer = fileStore->CreateRawWriter(snapshotId);

        WaitFor(writer->Open())
            .ThrowOnError();

        YT_LOG_INFO("Downloading %v bytes from peer %v",
            params.CompressedLength,
            params.PeerId);

        TSnapshotServiceProxy proxy(cellManager->GetPeerChannel(params.PeerId));
        proxy.SetDefaultTimeout(config->SnapshotDownloadRpcTimeout);

        i64 downloadedLength = 0;
        while (downloadedLength < params.CompressedLength) {
            auto req = proxy.ReadSnapshot();
            req->set_snapshot_id(snapshotId);
            req->set_offset(downloadedLength);
            i64 desiredBlockSize = std::min(
                config->SnapshotDownloadBlockSize,
                params.CompressedLength - downloadedLength);
            req->set_length(desiredBlockSize);

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error downloading snapshot");
            const auto& rsp = rspOrError.Value();

            const auto& attachments = rsp->Attachments();
            YCHECK(attachments.size() == 1);

            const auto& block = attachments[0];
            YT_LOG_DEBUG("Snapshot block received (Offset: %v, Size: %v)",
                downloadedLength,
                block.Size());

            WaitFor(writer->Write(block))
                .ThrowOnError();

            downloadedLength += block.Size();
        }

        WaitFor(writer->Close())
            .ThrowOnError();

        YT_LOG_INFO("Snapshot downloaded successfully");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error downloading snapshot %v", snapshotId)
           << ex;
    }
}

} // namespace

TFuture<void> DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore,
    int snapshotId)
{
    return BIND(DoDownloadSnapshot)
        .AsyncVia(GetCurrentInvoker())
        .Run(config, cellManager, fileStore, snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
