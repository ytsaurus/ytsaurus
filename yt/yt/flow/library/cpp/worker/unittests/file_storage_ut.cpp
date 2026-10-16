#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>
#include <yt/yt/flow/library/cpp/worker/file_storage.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NYT::NFlow::NWorker {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TFileStorageAdapterTest, ThrowingStorageNamesConfigurationAndObject)
{
    auto storage = CreateThrowingFileStorage();
    auto result = WaitFor(storage->GetOrCreate(
        NFileStorage::TFileStorageObjectId("source:v1:object"),
        [] (const std::string&) {
            return MakeFuture<void>(TError());
        }));

    EXPECT_FALSE(result.IsOK());
    EXPECT_THAT(result.GetMessage(), ::testing::HasSubstr("worker.file_storage"));
    EXPECT_THAT(result.GetMessage(), ::testing::HasSubstr("source:v1:object"));
}

TEST(TFileStorageAdapterTest, MapsCapacityAlertToStatusLeaf)
{
    TTempDir root;
    auto config = New<NFileStorage::TFileStorageConfig>();
    config->Path = root.Name();
    config->SoftSizeLimit = 1;
    config->HardSizeLimit = 1;
    config->CleanupPeriod = TDuration::Hours(1);

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto storage = CreateWorkerFileStorage(
        config,
        queue->GetInvoker(),
        NLogging::TLogger("FileStorageAdapterTest"),
        {},
        statusProfiler->WithPrefix("/file_storage"));

    auto result = WaitFor(storage->GetOrCreate(
        NFileStorage::TFileStorageObjectId("source:v1:large"),
        [] (const std::string& path) {
            TOFStream output((TFsPath(path) / "file").GetPath());
            output << "xx";
            output.Finish();
            return MakeFuture<void>(TError());
        }));

    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(statusProfiler->GetStatus().Errors.contains("/file_storage/capacity"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
