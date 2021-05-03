#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <util/system/file.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFlushFileMode,
    (All)
    (Data)
);

struct IIOEngine
    : public TRefCounted
{
    static constexpr auto DefaultPriority = Max<i64>();

    struct TReadRequest
    {
        FHANDLE Handle;
        i64 Offset = -1;
        TSharedMutableRef Buffer;
        bool UseDirectIO = false;
    };

    struct TWriteRequest
    {
        FHANDLE Handle;
        i64 Offset = -1;
        std::vector<TSharedRef> Buffers;
    };

    struct TOpenRequest
    {
        TString Path;
        EOpenMode Mode = {};
    };

    struct TCloseRequest
    {
        std::shared_ptr<TFileHandle> Handle;
        std::optional<i64> Size = {};
        bool Flush = false;
    };

    struct TAllocateRequest
    {
        FHANDLE Handle;
        i64 Size = -1;
    };

    struct TFlushFileRequest
    {
        FHANDLE Handle;
        EFlushFileMode Mode;
    };

    struct TFlushDirectoryRequest
    {
        TString Path;
    };


    virtual TFuture<void> Read(
        std::vector<TReadRequest> requests,
        i64 priority = DefaultPriority) = 0;
    virtual TFuture<void> Write(
        TWriteRequest request,
        i64 priority = DefaultPriority) = 0;

    virtual TFuture<void> FlushFile(
        TFlushFileRequest request,
        i64 priority = DefaultPriority) = 0;
    virtual TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        i64 priority = DefaultPriority) = 0;

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        TOpenRequest request,
        i64 priority = DefaultPriority) = 0;
    virtual TFuture<void> Close(
        TCloseRequest request,
        i64 priority = DefaultPriority) = 0;

    virtual TFuture<void> Allocate(
        TAllocateRequest request,
        i64 priority = DefaultPriority) = 0;

    virtual bool IsSick() const = 0;

    virtual const IInvokerPtr& GetAuxPoolInvoker() = 0;

    // Extension methods
    TFuture<TSharedMutableRef> ReadAll(
        const TString& path,
        i64 priority = DefaultPriority);
};

DEFINE_REFCOUNTED_TYPE(IIOEngine)

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId = "default",
    NProfiling::TProfiler profiler = {},
    NLogging::TLogger logger = {});

std::vector<EIOEngineType> GetSupportedIOEngineTypes();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
