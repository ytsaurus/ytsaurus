#include "file_storage.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TThrowingFileStorage
    : public NFileStorage::IFileStorage
{
public:
    TFuture<NFileStorage::IFileStorageObjectPtr> GetOrCreate(
        NFileStorage::TFileStorageObjectId id,
        NFileStorage::TFileStorageFiller /*filler*/) override
    {
        return MakeFuture<NFileStorage::IFileStorageObjectPtr>(
            TError("File storage object %Qv requires worker.file_storage configuration",
                id.Underlying()));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

NFileStorage::IFileStoragePtr CreateWorkerFileStorage(
    NFileStorage::TFileStorageConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler)
{
    return NFileStorage::CreateFileStorage(
        std::move(config),
        std::move(invoker),
        std::move(logger),
        std::move(profiler),
        std::move(statusProfiler));
}

NFileStorage::IFileStoragePtr CreateThrowingFileStorage()
{
    return New<TThrowingFileStorage>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
