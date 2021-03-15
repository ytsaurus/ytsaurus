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
    struct TReadRequest
    {
        FHANDLE Handle;
        i64 Offset;
        TSharedMutableRef Data;
    };

    struct TWriteRequest
    {
        FHANDLE Handle;
        i64 Offset;
        TSharedRef Data;
    };

    struct TVectorizedWriteRequest
    {
        FHANDLE Handle;
        i64 Offset;
        std::vector<TSharedRef> Data;
    };

    virtual TFuture<void> Read(
        const TReadRequest& request,
        i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<void> ReadMany(
        const std::vector<TReadRequest>& requests,
        i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<TSharedMutableRef> ReadAll(
        const TString& fileName,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<void> Write(
        const TWriteRequest& request,
        i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<void> WriteVectorized(
        const TVectorizedWriteRequest& request,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<void> FlushData(
        FHANDLE handle,
        i64 priority = std::numeric_limits<i64>::max()) = 0;
    virtual TFuture<void> Flush(
        FHANDLE hand1le,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        const TString& fileName,
        EOpenMode mode,
        i64 preallocateSize = -1,
        i64 priority = std::numeric_limits<i64>::max()) = 0;

    virtual TFuture<void> Close(
        std::shared_ptr<TFileHandle> handle,
        i64 newSize = -1,
        bool flush = false) = 0;

    virtual TFuture<void> FlushDirectory(const TString& path) = 0;

    virtual bool IsSick() const = 0;

    virtual TFuture<void> Fallocate(
        FHANDLE handle,
        i64 newSize) = 0;

    virtual const IInvokerPtr& GetAuxPoolInvoker() = 0;
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
