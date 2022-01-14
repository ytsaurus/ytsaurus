#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/misc/workload.h>

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
    static void MarkOpenForDirectIO(EOpenMode* oMode);

private:
    const bool OpenForDirectIO_ = false;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFlushFileMode,
    (All)
    (Data)
);

struct IIOEngine
    : public TRefCounted
{
    using TSessionId = TGuid;

    struct TReadRequest
    {
        TIOEngineHandlePtr Handle;
        i64 Offset = -1;
        i64 Size = -1;
    };

    struct TReadResponse
    {
        std::vector<TSharedRef> OutputBuffers;
        // NB: This contains page size padding.
        i64 PaddedByteCount = 0;
        i64 IOCount = 0;
    };

    struct TWriteRequest
    {
        TIOEngineHandlePtr Handle;
        i64 Offset = -1;
        std::vector<TSharedRef> Buffers;
        bool Flush = false;
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

    struct TFlushFileRangeRequest
    {
        TIOEngineHandlePtr Handle;
        i64 Offset = -1;
        i64 Size = -1;
    };

    struct TFlushDirectoryRequest
    {
        TString Path;
    };

    struct TDefaultReadTag
    { };

    virtual TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId = {}) = 0;
    virtual TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TSessionId sessionId = {}) = 0;

    virtual TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;
    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TSessionId sessionId = {}) = 0;
    virtual TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;
    virtual TFuture<void> Close(
        TCloseRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual bool IsSick() const = 0;

    virtual const IInvokerPtr& GetAuxPoolInvoker() = 0;

    // Extension methods.
    template <class TTag = TDefaultReadTag>
    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TSessionId sessionId = {});

    TFuture<TSharedRef> ReadAll(
        const TString& path,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TSessionId sessionId = {});
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
