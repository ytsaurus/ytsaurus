#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <util/system/file.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TIOEngineHandle final
    : public TFileHandle
{
public:
    TIOEngineHandle() = default;
    TIOEngineHandle(const TString& fName, EOpenMode oMode) noexcept;

    bool IsOpenForDirectIO() const;

private:
    bool OpenForDirectIO_ = false;
};

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
        TIOEngineHandlePtr Handle;
        i64 Offset = -1;
        i64 Size = -1;
    };

    struct TWriteRequest
    {
        TIOEngineHandlePtr Handle;
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
        TIOEngineHandlePtr Handle;
        std::optional<i64> Size = {};
        bool Flush = false;
    };

    struct TAllocateRequest
    {
        TIOEngineHandlePtr Handle;
        i64 Size = -1;
    };

    struct TFlushFileRequest
    {
        TIOEngineHandlePtr Handle;
        EFlushFileMode Mode;
    };

    struct TFlushDirectoryRequest
    {
        TString Path;
    };

    struct TDefaultReadTag
    { };

    virtual TFuture<std::vector<TSharedRef>> Read(
        std::vector<TReadRequest> requests,
        i64 priority,
        NYTAlloc::EMemoryZone memoryZone,
        TRefCountedTypeCookie tagCookie) = 0;
    virtual TFuture<void> Write(
        TWriteRequest request,
        i64 priority = DefaultPriority) = 0;

    virtual TFuture<void> FlushFile(
        TFlushFileRequest request,
        i64 priority = DefaultPriority) = 0;
    virtual TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        i64 priority = DefaultPriority) = 0;

    virtual TFuture<TIOEngineHandlePtr> Open(
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
    template <class TTag = TDefaultReadTag>
    TFuture<std::vector<TSharedRef>> Read(
        std::vector<TReadRequest> requests,
        i64 priority = DefaultPriority,
        NYTAlloc::EMemoryZone memoryZone = NYTAlloc::EMemoryZone::Normal);

    TFuture<TSharedRef> ReadAll(
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

#define IO_ENGINE_INL_H_
#include "io_engine-inl.h"
#undef IO_ENGINE_INL_H_
