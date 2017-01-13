#include "snapshot_downloader.h"
#include "config.h"
#include "scheduler.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/api/file_reader.h>
#include <yt/ytlib/api/native_client.h>

#include <yt/ytlib/scheduler/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSnapshotDownloader::TSnapshotDownloader(
    TSchedulerConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap,
    const TOperationId& operationId)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , OperationId_(operationId)
    , Logger(NLogging::TLogger(SchedulerLogger)
        .AddTag("OperationId: %v", operationId))
{
    YCHECK(bootstrap);
}

TSharedRef TSnapshotDownloader::Run()
{
    LOG_INFO("Starting downloading snapshot");

    auto client = Bootstrap_->GetMasterClient();

    auto snapshotPath = GetSnapshotPath(OperationId_);

    IFileReaderPtr reader;
    {
        TFileReaderOptions options;
        options.Config = Config_->SnapshotReader;
        reader = client->CreateFileReader(snapshotPath, options);
    }

    WaitFor(reader->Open())
        .ThrowOnError();

    LOG_INFO("Snapshot reader opened");

    std::vector<TSharedRef> blocks;
    while (true) {
        auto blockOrError = WaitFor(reader->Read());
        auto block = blockOrError.ValueOrThrow();
        if (!block)
            break;
        blocks.push_back(block);
    }

    LOG_INFO("Snapshot downloaded successfully");

    struct TSnapshotDataTag { };
    return MergeRefsToRef<TSnapshotDataTag>(blocks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
