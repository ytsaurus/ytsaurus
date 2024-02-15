#include "snapshot_download.h"
#include "private.h"
#include "snapshot_discovery.h"
#include "helpers.h"
#include "hydra_service_proxy.h"
#include "config.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NElection;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoDownloadSnapshot(
    const TDistributedHydraManagerConfigPtr& config,
    TDistributedHydraManagerOptions options,
    const TCellManagerPtr& cellManager,
    const ISnapshotStorePtr& store,
    int snapshotId,
    TLogger logger)
{
    const auto& Logger = logger.WithTag("SnapshotId: %v, SelfPeerId: %v",
        snapshotId,
        cellManager->GetSelfPeerId());

    auto isPersistenceEnabled = IsPersistenceEnabled(cellManager, options);
    if (!isPersistenceEnabled) {
        THROW_ERROR_EXCEPTION("Snapshot downloading by observers is prohibited");
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
    TDistributedHydraManagerOptions options,
    TCellManagerPtr cellManager,
    ISnapshotStorePtr store,
    int snapshotId,
    TLogger logger)
{
    return BIND(DoDownloadSnapshot)
        .AsyncVia(GetCurrentInvoker())
        .Run(std::move(config), std::move(options), std::move(cellManager), std::move(store), snapshotId, std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
