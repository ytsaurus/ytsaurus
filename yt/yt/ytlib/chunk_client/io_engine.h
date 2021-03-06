#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <util/system/file.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IIOEngine
    : public TRefCounted
{
    virtual TFuture<TSharedMutableRef> Pread(
        const std::shared_ptr<TFileHandle>& handle,
        i64 size,
        i64 offset,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<void> Pwrite(
        const std::shared_ptr<TFileHandle>& handle,
        const TSharedRef& data,
        i64 offset,
        i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<void> Pwritev(
        const std::shared_ptr<TFileHandle>& handle,
        const std::vector<TSharedRef>& data,
        i64 offset,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<bool> FlushData(
        const std::shared_ptr<TFileHandle>& handle,
        i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<bool> Flush(
        const std::shared_ptr<TFileHandle>& handle,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        const TString& fileName,
        EOpenMode mode,
        i64 preallocateSize = -1,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<void> Close(
        const std::shared_ptr<TFileHandle>& handle,
        i64 newSize = -1,
        bool flush = false) = 0;

    virtual TFuture<void> FlushDirectory(const TString& path) = 0;

    virtual TFuture<TSharedMutableRef> ReadAll(
        const TString& fileName,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual bool IsSick() const = 0;

    virtual TFuture<void> Fallocate(
        const std::shared_ptr<TFileHandle>& handle,
        i64 newSize) = 0;

    virtual const IInvokerPtr& GetWritePoolInvoker() = 0;
};

DEFINE_REFCOUNTED_TYPE(IIOEngine)

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId = "default",
    NProfiling::TProfiler profiler = {},
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
