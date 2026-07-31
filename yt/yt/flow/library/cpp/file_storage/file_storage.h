#pragma once

#include "config.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/logging/log.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <functional>
#include <optional>

namespace NYT::NFlow::NFileStorage {

////////////////////////////////////////////////////////////////////////////////

using TFileStorageFiller = std::function<TFuture<void>(const std::string& stagingPath)>;

struct IFileStorageObject
    : public TRefCounted
{
    virtual const TFileStorageObjectId& GetId() const = 0;
    virtual const std::string& GetPath() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileStorageObject);

struct IFileStorage
    : public TRefCounted
{
    // One process must exclusively own each configured cache root.
    virtual TFuture<IFileStorageObjectPtr> GetOrCreate(
        TFileStorageObjectId id,
        TFileStorageFiller filler) = 0;

    virtual TFuture<IFileStorageObjectPtr> GetOrCreate(
        TFileStorageObjectId id,
        std::optional<i64> /*expectedSize*/,
        TFileStorageFiller filler)
    {
        return GetOrCreate(std::move(id), std::move(filler));
    }
};

DEFINE_REFCOUNTED_TYPE(IFileStorage);

IFileStoragePtr CreateFileStorage(
    TFileStorageConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileStorage
