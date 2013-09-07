#include "stdafx.h"
#include "snapshot_downloader.h"
#include "scheduler.h"
#include "config.h"

#include <ytlib/concurrency/fiber.h>

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

    Logger.AddTag(Sprintf("OperationId: %s",
        ~ToString(operation->GetOperationId())));
}

void TSnapshotDownloader::Run()
{
    LOG_INFO("Starting downloading snapshot");

    auto snapshotPath = GetSnapshotPath(Operation->GetOperationId());
    auto reader = New<TAsyncReader>(
        Config->SnapshotReader,
        Bootstrap->GetMasterChannel(),
        CreateClientBlockCache(New<TClientBlockCacheConfig>()),
        nullptr,
        snapshotPath);

    {
        auto result = WaitFor(reader->AsyncOpen());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
        
    i64 size = reader->GetSize();

    LOG_INFO("Snapshot reader opened (Size: %" PRId64 ")", size);
    
    Operation->Snapshot() = TBlob();

    try {
        auto& blob = *Operation->Snapshot();
        blob.Reserve(size);

        while (true) {
            auto blockOrError = WaitFor(reader->AsyncRead());
            THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError);
            auto block = blockOrError.GetValue();
            if (!block)
                break;
            blob.Append(block);
        }

        LOG_INFO("Snapshot downloaded successfully");
    } catch (...) {
        Operation->Snapshot().Reset();
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
