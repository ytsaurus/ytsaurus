#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/misc/ref.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IIOEngine
    : public TRefCounted
{
    virtual TFuture<TSharedMutableRef> Pread(
        const std::shared_ptr<TFileHandle>& fh,
        size_t len, i64 offset,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<void> Pwrite(
        const std::shared_ptr<TFileHandle>& fh,
        const TSharedRef& data, i64 offset,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<bool> FlushData(const std::shared_ptr<TFileHandle>& fh, i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<bool> Flush(const std::shared_ptr<TFileHandle>& fh, i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        const TString& fName, EOpenMode oMode, i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual bool IsSick() const = 0;

    virtual TFuture<void> Close(const std::shared_ptr<TFileHandle>& fh, i64 newSize = -1, bool flush = false) = 0;
    virtual TFuture<void> FlushDirectory(const TString& path) = 0;
};

DEFINE_REFCOUNTED_TYPE(IIOEngine)

IIOEnginePtr CreateIOEngine(
    EIOEngineType ioType, 
    const NYTree::INodePtr& ioConfig, 
    const TString& locationId = "default", 
    const NProfiling::TProfiler& profiler = NProfiling::TProfiler(),
    const NLogging::TLogger& logger = NLogging::TLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
