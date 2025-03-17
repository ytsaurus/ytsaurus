#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref.h>

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

DEFINE_ENUM(ELockFileMode,
    (Shared)
    (Exclusive)
    (Unlock)
);

using TIOSessionId = TGuid;

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
    i64 PaddedBytes = 0;
    i64 IORequests = 0;
};

struct TWriteRequest
{
    TIOEngineHandlePtr Handle;
    i64 Offset = -1;
    std::vector<TSharedRef> Buffers;
    bool Flush = false;
};

struct TWriteResponse
{
    i64 IOWriteRequests = 0;
    i64 IOSyncRequests = 0;
    i64 WrittenBytes = 0;
};

struct TOpenRequest
{
    TString Path;
    EOpenMode Mode = OpenExisting;
};

struct TCloseRequest
{
    TIOEngineHandlePtr Handle;
    std::optional<i64> Size;
    bool Flush = false;
};

struct TCloseResponse
{
    i64 IOSyncRequests = 0;
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

struct TFlushFileResponse
{
    i64 IOSyncRequests = 0;
};

struct TFlushFileRangeRequest
{
    TIOEngineHandlePtr Handle;
    i64 Offset = -1;
    i64 Size = -1;
    bool Async = false;
};

struct TFlushFileRangeResponse
{
    i64 IOSyncRequests = 0;
};

struct TFlushDirectoryRequest
{
    TString Path;
};

struct TFlushDirectoryResponse
{
    i64 IOSyncRequests = 0;
};

struct TLockRequest
{
    TIOEngineHandlePtr Handle;
    ELockFileMode Mode;
    bool Nonblocking = false;
};

struct TResizeRequest
{
    TIOEngineHandlePtr Handle;
    i64 Size = -1;
};

struct TDefaultReadTag
{ };

struct IIOEngine
    : public TRefCounted
{
    virtual TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TIOSessionId sessionId = {},
        bool useDedicatedAllocations = false) = 0;
    virtual TFuture<TWriteResponse> Write(
        TWriteRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TIOSessionId sessionId = {}) = 0;

    virtual TFuture<TFlushFileResponse> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;
    virtual TFuture<TFlushFileRangeResponse> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TIOSessionId sessionId = {}) = 0;
    virtual TFuture<TFlushDirectoryResponse> FlushDirectory(
        TFlushDirectoryRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;
    virtual TFuture<TCloseResponse> Close(
        TCloseRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual TFuture<void> Lock(
        TLockRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual TFuture<void> Resize(
        TResizeRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) = 0;

    virtual bool IsSick() const = 0;

    virtual void Reconfigure(const NYTree::INodePtr& dynamicIOConfig) = 0;

    virtual const IInvokerPtr& GetAuxPoolInvoker() = 0;

    virtual i64 GetTotalReadBytes() const = 0;
    virtual i64 GetTotalWrittenBytes() const = 0;
    virtual EDirectIOPolicy UseDirectIOForReads() const = 0;

    virtual bool IsInFlightReadRequestLimitExceeded() const = 0;
    virtual bool IsInFlightWriteRequestLimitExceeded() const = 0;

    virtual i64 GetInFlightReadRequestCount() const  = 0;
    virtual i64 GetReadRequestLimit() const  = 0;

    virtual i64 GetInFlightWriteRequestCount() const  = 0;
    virtual i64 GetWriteRequestLimit() const  = 0;

    // Extension methods.
    TFuture<TReadResponse> ReadAll(
        const TString& path,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TIOSessionId sessionId = {});
};

DEFINE_REFCOUNTED_TYPE(IIOEngine)

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId = "default",
    NProfiling::TProfiler profiler = {},
    NLogging::TLogger logger = {});

std::vector<EIOEngineType> GetSupportedIOEngineTypes();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
