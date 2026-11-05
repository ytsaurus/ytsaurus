#pragma once

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

NFileStorage::IFileStoragePtr CreateWorkerFileStorage(
    NFileStorage::TFileStorageConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler);

NFileStorage::IFileStoragePtr CreateThrowingFileStorage();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
