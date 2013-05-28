#include "stdafx.h"
#include "snapshot_downloader.h"
#include "scheduler.h"
#include "config.h"

#include <ytlib/scheduler/helpers.h>

#include <ytlib/file_client/file_reader.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/ytree/ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSnapshotDownloader::TSnapshotDownloader(
    TSchedulerConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap,
    TOperationPtr operation)
    : Config(config)
    , Bootstrap(bootstrap)
    , Operation(operation)
    , Logger(SchedulerLogger)
{
    YCHECK(bootstrap);
    YCHECK(operation);

    Logger.AddTag(Sprintf("OperationId: %s", ~ToString(operation->GetOperationId())));
}

TFuture<void> TSnapshotDownloader::Run()
{
    return BIND(&TSnapshotDownloader::Download, MakeStrong(this))
           .AsyncVia(Bootstrap->GetScheduler()->GetSnapshotIOInvoker())
           .Run()
           // TODO(babenko): remove this ugly hack
           .Apply(BIND([] (TVoid) {}));
}

TVoid TSnapshotDownloader::Download()
{
    LOG_INFO("Checking snapshot existence");

    auto snapshotPath = GetSnapshotPath(Operation->GetOperationId());
    TObjectServiceProxy proxy(Bootstrap->GetMasterChannel());

    // Check existence.
    bool exists;
    {
        auto req = TYPathProxy::Exists(snapshotPath);
        auto rsp = proxy.Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error checking snapshot for existence");
        exists = rsp->value();
    }

    if (!exists) {
        LOG_INFO("Snapshot does not exist");
        return TVoid();
    }

    LOG_INFO("Snapshot found");

    try {

        auto reader = New<NFileClient::TSyncReader>();
        reader->Open(
            Config->SnapshotReader,
            Bootstrap->GetMasterChannel(),
            nullptr,
            CreateClientBlockCache(New<TClientBlockCacheConfig>()),
            snapshotPath);

        i64 size = reader->GetSize();

        LOG_INFO("Downloading %" PRId64 " bytes", size);

        Operation->Snapshot() = TBlob();
        auto& blob = *Operation->Snapshot();
        blob.Reserve(size);

        while (true) {
            auto block = reader->Read();
            if (!block)
                break;
            blob.Append(block);
        }

        LOG_INFO("Snapshot loaded");
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error loading snapshot");
        Operation->Snapshot() = Null;
    }
    
    return TVoid();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
