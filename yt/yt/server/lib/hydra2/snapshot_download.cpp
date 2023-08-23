#include "snapshot_download.h"
#include "private.h"
#include "snapshot_discovery.h"

#include <yt/yt/server/lib/hydra2/hydra_service_proxy.h>

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHydra2 {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoDownloadSnapshot(
    const TDistributedHydraManagerConfigPtr& config,
    const TCellManagerPtr& cellManager,
    const ISnapshotStorePtr& store,
    int snapshotId,
    TLogger logger)
{
    const auto& Logger = logger.WithTag("SnapshotId: %v, SelfPeerId: %v",
        snapshotId,
        cellManager->GetSelfPeerId());

    // Non-voting peers usually peek into another peer's remote snapshot store.
    // They definitely shouldn't download anything anywhere.
    // TODO(shakurov):
    // Strictly speaking, this check is incorrect, as it disables non-voting peers
    // with *local* stores from downloading snapshots, either.
    // However, non-voting peers with local snapshot stores are currently not used
    // in practice, and there's no way to distinguish remote and local store
    // at the moment (at least, there's no non-ugly way).
    if (!cellManager->GetSelfConfig()->Voting) {
        THROW_ERROR_EXCEPTION("Snapshot downloading by non-voting peers is prohibited");
    }

    try {
        YT_LOG_INFO("Will download snapshot from peers");

        auto params = WaitFor(DiscoverSnapshot(config, cellManager, snapshotId))
            .ValueOrThrow();

        auto writer = store->CreateWriter(snapshotId, params.Meta);
        WaitFor(writer->Open())
            .ThrowOnError();

        YT_LOG_INFO("Downloading snapshot from peer (CompressedLength: %v, PeerId: %v)",
            params.CompressedLength,
            params.PeerId);

        TInternalHydraServiceProxy proxy(cellManager->GetPeerChannel(params.PeerId));
        proxy.SetDefaultTimeout(config->SnapshotDownloadTotalStreamingTimeout);
        proxy.DefaultServerAttachmentsStreamingParameters() = NRpc::TStreamingParameters{
            .WindowSize = config->SnapshotDownloadWindowSize,
            .ReadTimeout = config->SnapshotDownloadStreamingStallTimeout
        };
        proxy.SetDefaultResponseCodec(config->SnapshotDownloadStreamingCompressionCodec);
        proxy.SetDefaultEnableLegacyRpcCodecs(false);

        auto req = proxy.ReadSnapshot();
        req->set_snapshot_id(snapshotId);

        auto inputStream = WaitFor(CreateRpcClientInputStream(std::move(req)))
            .ValueOrThrow();

        PipeInputToOutput(inputStream, writer);

        WaitFor(writer->Close())
            .ThrowOnError();

        YT_LOG_INFO("Snapshot downloaded successfully");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error downloading snapshot %v", snapshotId)
           << ex;
    }
}

} // namespace

///////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    ISnapshotStorePtr store,
    int snapshotId,
    TLogger logger)
{
    return BIND(DoDownloadSnapshot)
        .AsyncVia(GetCurrentInvoker())
        .Run(std::move(config), std::move(cellManager), std::move(store), snapshotId, std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
