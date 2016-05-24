#include "snapshot_downloader.h"
#include "config.h"
#include "scheduler.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/api/file_reader.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NScheduler {

using namespace NApi;
using namespace NConcurrency;

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

    Logger.AddTag("OperationId: %v", operation->GetId());
}

void TSnapshotDownloader::Run()
{
    LOG_INFO("Starting downloading snapshot");

    auto client = Bootstrap->GetMasterClient();

    auto snapshotPath = GetSnapshotPath(Operation->GetId());

    IFileReaderPtr reader;
    {
        TFileReaderOptions options;
        options.Config = Config->SnapshotReader;
        reader = client->CreateFileReader(snapshotPath, options);
    }

    WaitFor(reader->Open())
        .ThrowOnError();

    LOG_INFO("Snapshot reader opened");

    try {
        std::vector<TSharedRef> blocks;
        while (true) {
            auto blockOrError = WaitFor(reader->Read());
            THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError);
            auto block = blockOrError.Value();
            if (!block)
                break;
            blocks.push_back(block);
        }

        Operation->Snapshot() = MergeRefs(blocks);

        LOG_INFO("Snapshot downloaded successfully");
    } catch (...) {
        Operation->Snapshot().Reset();
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
