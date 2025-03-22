#include "io_engine.h"
#include "io_engine_base.h"
#include "io_engine_uring.h"
#include "io_request_slicer.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_detail.h>

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/client/misc/workload.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>

#include <array>

#ifdef _linux_
    #include <sys/uio.h>
#endif

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

struct TInternalReadResponse {
    i64 IORequests = 0;
};

////////////////////////////////////////////////////////////////////////////////

TIOEngineHandle::TIOEngineHandle(const TString& fName, EOpenMode oMode) noexcept
    : TFileHandle(fName, oMode)
    , OpenForDirectIO_(oMode & DirectAligned)
{ }

bool TIOEngineHandle::IsOpenForDirectIO() const
{
    return OpenForDirectIO_;
}

void TIOEngineHandle::MarkOpenForDirectIO(EOpenMode* oMode)
{
    *oMode |= DirectAligned;
}

////////////////////////////////////////////////////////////////////////////////

TWriteResponse DoWriteImpl(
    const TWriteRequest& request,
    i64 maxBytesPerWrite,
    std::optional<i64> simulatedMaxBytesPerWrite,
    const TIOEngineSensorsPtr& sensors,
    bool enablePwritev);
TFlushFileResponse DoFlushFile(
    const TFlushFileRequest& request,
    bool enableSync,
    const TIOEngineSensorsPtr& sensors);
TFlushFileRangeResponse DoFlushFileRange(
    const TFlushFileRangeRequest& request,
    bool enableSync,
    const TIOEngineSensorsPtr& sensors);

std::vector<TSharedMutableRef> AllocateReadBuffers(
    const std::vector<TReadRequest>& requests,
    TRefCountedTypeCookie tagCookie,
    bool useDedicatedAllocations)
{
    bool shouldBeAligned = std::any_of(
        requests.begin(),
        requests.end(),
        [] (const TReadRequest& request) {
            return request.Handle->IsOpenForDirectIO();
        });

    auto allocate = [&] (size_t size) {
        TSharedMutableRefAllocateOptions options{
            .InitializeStorage = false
        };
        return shouldBeAligned
            ? TSharedMutableRef::AllocatePageAligned(size, options, tagCookie)
            : TSharedMutableRef::Allocate(size, options, tagCookie);
    };

    std::vector<TSharedMutableRef> results;
    results.reserve(requests.size());

    if (useDedicatedAllocations) {
        for (const auto& request : requests) {
            results.push_back(allocate(request.Size));
        }
        return results;
    }

    // Collocate blocks in single buffer.
    i64 totalSize = 0;
    for (const auto& request : requests) {
        totalSize += shouldBeAligned
            ? AlignUp<i64>(request.Size, DefaultPageSize)
            : request.Size;
    }

    auto buffer = allocate(totalSize);
    i64 offset = 0;
    for (const auto& request : requests) {
        results.push_back(buffer.Slice(offset, offset + request.Size));
        offset += shouldBeAligned
            ? AlignUp<i64>(request.Size, DefaultPageSize)
            : request.Size;
    }
    return results;
}

TInternalReadResponse DoRead(
    const TReadRequest& request,
    TSharedMutableRef buffer,
    std::optional<i64> simulatedMaxBytesPerRead,
    i64 maxBytesPerRead,
    EWorkloadCategory category,
    TIOSessionId sessionId,
    TRequestCounterGuard requestCounterGuard,
    const TIOEngineSensorsPtr& sensors,
    const NLogging::TLogger& Logger)
{
    YT_VERIFY(std::ssize(buffer) == request.Size);

    Y_UNUSED(requestCounterGuard);

    sensors->UpdateKernelStatistics();

    auto toReadRemaining = std::ssize(buffer);
    auto fileOffset = request.Offset;
    i64 bufferOffset = 0;

    TInternalReadResponse response;

    NFS::WrapIOErrors([&] {
        while (toReadRemaining > 0) {
            auto toRead = static_cast<ui32>(Min(toReadRemaining, maxBytesPerRead));

            i64 reallyRead;
            {
                TRequestStatsGuard statsGuard(sensors->ReadSensors);
                NTracing::TNullTraceContextGuard nullTraceContextGuard;
                reallyRead = HandleEintr(::pread, *request.Handle, buffer.Begin() + bufferOffset, toRead, fileOffset);
                ++response.IORequests;

                YT_LOG_DEBUG_IF(category == EWorkloadCategory::UserInteractive,
                    "Finished reading from disk (Handle: %v, ReadBytes: %v, ReadSessionId: %v, ReadTime: %v)",
                    static_cast<FHANDLE>(*request.Handle),
                    reallyRead,
                    sessionId,
                    statsGuard.GetElapsedTime());
            }

            if (reallyRead < 0) {
                // TODO(aozeritsky): ythrow is placed here consciously.
                // WrapIOErrors rethrows some kind of arcadia-style exception.
                // So in order to keep the old behaviour we should use ythrow or
                // rewrite WrapIOErrors.
                ythrow TFileError();
            }

            if (reallyRead == 0) {
                break;
            }

            sensors->RegisterReadBytes(reallyRead);
            if (simulatedMaxBytesPerRead) {
                reallyRead = Min(reallyRead, *simulatedMaxBytesPerRead);
            }

            fileOffset += reallyRead;
            bufferOffset += reallyRead;
            toReadRemaining -= reallyRead;
        }
    });

    if (toReadRemaining > 0) {
        THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request")
            << TErrorAttribute("to_read_remaining", toReadRemaining)
            << TErrorAttribute("max_bytes_per_read", maxBytesPerRead)
            << TErrorAttribute("request_size", request.Size)
            << TErrorAttribute("request_offset", request.Offset)
            << TErrorAttribute("file_size", request.Handle->GetLength())
            << TErrorAttribute("handle", static_cast<FHANDLE>(*request.Handle));
    }

    return response;
}

TWriteResponse DoWrite(
    const TWriteRequest& request,
    i64 maxBytesPerWrite,
    TRequestCounterGuard requestCounterGuard,
    bool syncFlush,
    bool asyncFlush,
    bool enableSync,
    std::optional<i64> simulatedMaxBytesPerWrite,
    const TIOEngineSensorsPtr& sensors,
    bool enablePwritev)
{
    auto guard = std::move(requestCounterGuard);
    auto writeResponse = DoWriteImpl(
        request,
        maxBytesPerWrite,
        simulatedMaxBytesPerWrite,
        sensors,
        enablePwritev);

    if ((syncFlush || asyncFlush) && writeResponse.WrittenBytes) {
        auto flushFileRangeResponse = DoFlushFileRange(
            TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writeResponse.WrittenBytes,
                .Async = !syncFlush && asyncFlush,
            },
            enableSync,
            sensors);

        writeResponse.IOSyncRequests += flushFileRangeResponse.IOSyncRequests;
    }

    return writeResponse;
}

TWriteResponse DoWriteImpl(
    const TWriteRequest& request,
    i64 maxBytesPerWrite,
    std::optional<i64> simulatedMaxBytesPerWrite,
    const TIOEngineSensorsPtr& sensors,
    bool enablePwritev)
{
    sensors->UpdateKernelStatistics();

    auto fileOffset = request.Offset;

    TWriteResponse response;

    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;

        auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Buffers));

        int bufferIndex = 0;
        i64 bufferOffset = 0; // within current buffer

        while (toWriteRemaining > 0) {
            auto isPwritevSupported = [&] {
#ifdef _linux_
                return true;
#else
                return false;
#endif
            };

            auto pwritev = [&] {
#ifdef _linux_
                std::array<iovec, MaxIovCountPerRequest> iov;
                int iovCount = 0;
                i64 toWrite = 0;
                while (bufferIndex + iovCount < std::ssize(request.Buffers) &&
                       iovCount < std::ssize(iov) &&
                       toWrite < maxBytesPerWrite)
                {
                    const auto& buffer = request.Buffers[bufferIndex + iovCount];
                    auto& iovPart = iov[iovCount];
                    iovPart = {
                        .iov_base = const_cast<char*>(buffer.Begin()),
                        .iov_len = buffer.Size()
                    };
                    if (iovCount == 0) {
                        iovPart.iov_base = static_cast<char*>(iovPart.iov_base) + bufferOffset;
                        iovPart.iov_len -= bufferOffset;
                    }
                    if (toWrite + static_cast<i64>(iovPart.iov_len) > maxBytesPerWrite) {
                        iovPart.iov_len = maxBytesPerWrite - toWrite;
                    }
                    toWrite += iovPart.iov_len;
                    ++iovCount;
                }

                i64 reallyWritten;
                {
                    TRequestStatsGuard statsGuard(sensors->WriteSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyWritten = HandleEintr(::pwritev, *request.Handle, iov.data(), iovCount, fileOffset);
                }

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                sensors->RegisterWrittenBytes(reallyWritten);
                if (simulatedMaxBytesPerWrite) {
                    reallyWritten = Min(reallyWritten, *simulatedMaxBytesPerWrite);
                }

                while (reallyWritten > 0) {
                    const auto& buffer = request.Buffers[bufferIndex];
                    i64 toAdvance = Min(std::ssize(buffer) - bufferOffset, reallyWritten);
                    fileOffset += toAdvance;
                    bufferOffset += toAdvance;
                    reallyWritten -= toAdvance;
                    toWriteRemaining -= toAdvance;
                    if (bufferOffset == std::ssize(buffer)) {
                        ++bufferIndex;
                        bufferOffset = 0;
                    }
                }
#else
                YT_ABORT();
#endif
            };

            auto pwrite = [&] {
                const auto& buffer = request.Buffers[bufferIndex];
                auto toWrite = static_cast<ui32>(Min(toWriteRemaining, maxBytesPerWrite, std::ssize(buffer) - bufferOffset));

                i32 reallyWritten;
                {
                    TRequestStatsGuard statsGuard(sensors->WriteSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyWritten = HandleEintr(::pwrite, *request.Handle, const_cast<char*>(buffer.Begin()) + bufferOffset, toWrite, fileOffset);
                }

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                sensors->RegisterWrittenBytes(reallyWritten);
                fileOffset += reallyWritten;
                bufferOffset += reallyWritten;
                toWriteRemaining -= reallyWritten;
                if (bufferOffset == std::ssize(buffer)) {
                    ++bufferIndex;
                    bufferOffset = 0;
                }
            };

            if (enablePwritev && isPwritevSupported()) {
                pwritev();
            } else {
                pwrite();
            }

            ++response.IOWriteRequests;
        }
    });

    response.WrittenBytes = fileOffset - request.Offset;

    return response;
}

TFlushFileResponse DoFlushFile(
    const TFlushFileRequest& request,
    bool enableSync,
    const TIOEngineSensorsPtr& sensors)
{
    TFlushFileResponse response;

    sensors->UpdateKernelStatistics();
    if (!enableSync) {
        return response;
    }

    auto doFsync = [&] {
        TRequestStatsGuard statsGuard(sensors->SyncSensors);
        return HandleEintr(::fsync, *request.Handle);
    };

#ifdef _linux_
    auto doFdatasync = [&] {
        TRequestStatsGuard statsGuard(sensors->DataSyncSensors);
        return HandleEintr(::fdatasync, *request.Handle);
    };
#else
    auto doFdatasync = doFsync;
#endif

    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        int result;
        switch (request.Mode) {
            case EFlushFileMode::All:
                result = doFsync();
                break;
            case EFlushFileMode::Data:
                result = doFdatasync();
                break;
            default:
                YT_ABORT();
        }
        if (result != 0) {
            ythrow TFileError();
        }

        response.IOSyncRequests = 1;
    });

    return response;
}

TFlushFileRangeResponse DoFlushFileRange(
    const TFlushFileRangeRequest& request,
    bool enableSync,
    const TIOEngineSensorsPtr& sensors)
{
    TFlushFileRangeResponse response;

    sensors->UpdateKernelStatistics();
    if (!enableSync) {
        return response;
    }

#ifdef _linux_
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        int result = 0;
        {
            TRequestStatsGuard statsGuard(sensors->DataSyncSensors);
            const auto flags = request.Async
                ? SYNC_FILE_RANGE_WRITE
                : SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
            result = HandleEintr(::sync_file_range, *request.Handle, request.Offset, request.Size, flags);
        };
        if (result != 0) {
            ythrow TFileError();
        }

        response.IOSyncRequests = 1;
    });
#else
    Y_UNUSED(request);
#endif

    return response;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TReadResponse> IIOEngine::ReadAll(
    const TString& path,
    EWorkloadCategory category,
    TIOSessionId sessionId,
    TFairShareSlotId fairShareSlot)
{
    return Open({path, OpenExisting | RdOnly | Seq | CloseOnExec}, category)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& handle) {
            struct TReadAllBufferTag
            { };
            return Read(
                {{
                    handle,
                    0,
                    handle->GetLength(),
                    fairShareSlot,
                }},
                category,
                GetRefCountedTypeCookie<TReadAllBufferTag>(),
                sessionId)
                .Apply(BIND(
                    [=, this, this_ = MakeStrong(this), handle = handle]
                    (TReadResponse response)
                {
                    YT_VERIFY(response.OutputBuffers.size() == 1);
                    return Close({.Handle = handle}, category)
                        // ignore result as Close here won't trigger sync requests
                        .AsVoid()
                        .Apply(BIND([response = std::move(response)] () mutable {
                            return std::move(response);
                        }));
                }));
        }));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TThreadPoolIOEngineConfig)

struct TThreadPoolIOEngineConfig
    : public TIOEngineConfigBase
{
    int ReadThreadCount;
    int WriteThreadCount;
    int FairShareThreadCount;

    bool EnablePwritev;
    bool FlushAfterWrite;
    bool AsyncFlushAfterWrite;
    bool EnableSyncOnCloseWithWrite;

    // Request size in bytes.
    i64 DesiredRequestSize;
    i64 MinRequestSize;

    // Fair-share thread pool settings.
    double DefaultPoolWeight;
    double UserInteractivePoolWeight;

    REGISTER_YSON_STRUCT(TThreadPoolIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("read_thread_count", &TThis::ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("write_thread_count", &TThis::WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("fair_share_thread_count", &TThis::FairShareThreadCount)
            .GreaterThanOrEqual(1)
            .Default(4);

        registrar.Parameter("enable_pwritev", &TThis::EnablePwritev)
            .Default(true);
        registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
            .Default(false);
        registrar.Parameter("async_flush_after_write", &TThis::AsyncFlushAfterWrite)
            .Default(false);
        registrar.Parameter("enable_sync_on_close_with_write", &TThis::EnableSyncOnCloseWithWrite)
            .Default(true);

        registrar.Parameter("desired_request_size", &TThis::DesiredRequestSize)
            .GreaterThanOrEqual(4_KB)
            .Default(128_KB);
        registrar.Parameter("min_request_size", &TThis::MinRequestSize)
            .GreaterThanOrEqual(512)
            .Default(64_KB);

        registrar.Parameter("default_pool_weight", &TThis::DefaultPoolWeight)
            .GreaterThan(0)
            .Default(1);
        registrar.Parameter("user_interactive_pool_weight", &TThis::UserInteractivePoolWeight)
            .GreaterThanOrEqual(1)
            .Default(4);
    }
};

DEFINE_REFCOUNTED_TYPE(TThreadPoolIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TFixedPriorityExecutor
{
public:
    TFixedPriorityExecutor(
        const TThreadPoolIOEngineConfigPtr& config,
        const TString& locationId,
        NLogging::TLogger /*logger*/)
        : ReadThreadPool_(CreateThreadPool(config->ReadThreadCount, Format("IOR:%v", locationId)))
        , WriteThreadPool_(CreateThreadPool(config->WriteThreadCount, Format("IOW:%v", locationId)))
        , ReadInvoker_(CreatePrioritizedInvoker(ReadThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "fixed_priority_executor_reader"}, {"location_id", locationId}})))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "fixed_priority_executor_writer"}, {"location_id", locationId}})))
    { }

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOSessionId)
    {
        return CreateFixedPriorityInvoker(ReadInvoker_, GetBasicPriority(category));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOSessionId)
    {
        return CreateFixedPriorityInvoker(WriteInvoker_, GetBasicPriority(category));
    }

    void Reconfigure(const TThreadPoolIOEngineConfigPtr& config)
    {
        ReadThreadPool_->SetThreadCount(config->ReadThreadCount);
        WriteThreadPool_->SetThreadCount(config->WriteThreadCount);
    }

private:
    const IThreadPoolPtr ReadThreadPool_;
    const IThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr ReadInvoker_;
    const IPrioritizedInvokerPtr WriteInvoker_;
};

class TPoolWeightProvider
    : public IPoolWeightProvider
{
public:
    TPoolWeightProvider(double defaultPoolWeight, double userInteractivePoolWeight)
        : DefaultPoolWeight_(defaultPoolWeight)
        , UserInteractivePoolWeight_(userInteractivePoolWeight)
    { }

    double GetWeight(const std::string& poolName) override {
        if (poolName == "Default") {
            return DefaultPoolWeight_;
        } else if (poolName == "UserInteractive") {
            return UserInteractivePoolWeight_;
        } else {
            return 1.0;
        }
    }

private:
    const double DefaultPoolWeight_;
    const double UserInteractivePoolWeight_;
};

class TFairShareThreadPool
{
public:
    TFairShareThreadPool(
        TThreadPoolIOEngineConfigPtr config,
        const TString& locationId,
        NLogging::TLogger logger)
        : ReadThreadPool_(CreateTwoLevelFairShareThreadPool(
            config->ReadThreadCount,
            Format("FSH:%v", locationId),
            {
                New<TPoolWeightProvider>(config->DefaultPoolWeight, config->UserInteractivePoolWeight)
            }))
        , WriteThreadPool_(CreateThreadPool(config->WriteThreadCount, Format("IOW:%v", locationId)))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "io_fair_share_thread_pool_writer"}, {"location_id", locationId}})))
        , Logger(logger)
        , DefaultPool_{"Default", config->DefaultPoolWeight}
        , UserInteractivePool_{"UserInteractive", config->UserInteractivePoolWeight}
    { }

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOSessionId client)
    {
        const auto& pool = GetPoolByCategory(category);
        return ReadThreadPool_->GetInvoker(pool.Name, ToString(client));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOSessionId)
    {
        return CreateFixedPriorityInvoker(WriteInvoker_, GetBasicPriority(category));
    }

    void Reconfigure(const TThreadPoolIOEngineConfigPtr& config)
    {
        ReadThreadPool_->SetThreadCount(config->ReadThreadCount);
        WriteThreadPool_->SetThreadCount(config->WriteThreadCount);
    }

private:
    struct TPoolDescriptor
    {
        std::string Name;
        double Weight = 1.0;
    };

    const TPoolDescriptor& GetPoolByCategory(EWorkloadCategory category)
    {
        if (category == EWorkloadCategory::UserInteractive) {
            return UserInteractivePool_;
        }
        return DefaultPool_;
    }

private:
    const ITwoLevelFairShareThreadPoolPtr ReadThreadPool_;
    const IThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr WriteInvoker_;
    const NLogging::TLogger Logger;

    const TPoolDescriptor DefaultPool_;
    const TPoolDescriptor UserInteractivePool_;
};

template <typename TThreadPool, typename TRequestSlicer>
class TThreadPoolIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TThreadPoolIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TThreadPoolIOEngine(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBase(
            config,
            std::move(locationId),
            std::move(profiler),
            std::move(logger))
        , StaticConfig_(std::move(config))
        , Config_(StaticConfig_)
        , ThreadPool_(StaticConfig_, LocationId_, Logger)
        , RequestSlicer_(StaticConfig_->DesiredRequestSize, StaticConfig_->MinRequestSize)
    { }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TIOSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        std::vector<TFuture<TInternalReadResponse>> futures;
        futures.reserve(requests.size());

        auto invoker = ThreadPool_.GetReadInvoker(category, sessionId);

        i64 paddedBytes = 0;
        for (const auto& request : requests) {
            paddedBytes += GetPaddedSize(request.Offset, request.Size, DefaultPageSize);
        }
        auto buffers = AllocateReadBuffers(requests, tagCookie, useDedicatedAllocations);
        auto config = Config_.Acquire();

        for (int index = 0; index < std::ssize(requests); ++index) {
            for (auto& slice : RequestSlicer_.Slice(std::move(requests[index]), buffers[index])) {
                auto future = BIND([=, this, this_ = MakeStrong(this), category = category, sessionId = sessionId] (
                    const TReadRequest& request,
                    TSharedMutableRef buffer,
                    TWallTimer timer,
                    TRequestCounterGuard requestCounterGuard) {
                    const auto readWaitTime = timer.GetElapsedTime();
                    AddReadWaitTimeSample(readWaitTime);

                    YT_LOG_DEBUG_IF(category == EWorkloadCategory::UserInteractive,
                        "Started reading from disk (Handle: %v, RequestSize: %v, ReadSessionId: %v, ReadWaitTime: %v)",
                        static_cast<FHANDLE>(*request.Handle),
                        request.Size,
                        sessionId,
                        readWaitTime);

                    return DoRead(
                        std::move(request),
                        std::move(buffer),
                        config->SimulatedMaxBytesPerRead,
                        config->MaxBytesPerRead,
                        category,
                        sessionId,
                        std::move(requestCounterGuard),
                        Sensors_,
                        Logger);
                })
                    .AsyncVia(invoker)
                    .Run(
                        std::move(slice.Request),
                        std::move(slice.OutputBuffer),
                        TWallTimer(),
                        CreateInFlightRequestGuard(EIOEngineRequestType::Read));
                futures.push_back(std::move(future));
            }
        }

        TReadResponse response{
            .PaddedBytes = paddedBytes,
        };
        response.OutputBuffers.assign(buffers.begin(), buffers.end());

        return AllSucceeded(std::move(futures))
            .Apply(BIND([
                response = std::move(response)
            ] (const std::vector<TInternalReadResponse>& subresponses) mutable {
                for (const auto& subresponse: subresponses) {
                    response.IORequests += subresponse.IORequests;
                }

                return std::move(response);
            }));
    }

    TFuture<TWriteResponse> Write(
        TWriteRequest request,
        EWorkloadCategory category,
        TIOSessionId sessionId) override
    {
        YT_ASSERT(request.Handle);

        auto config = Config_.Acquire();
        bool useSyncOnClose = config->EnableSyncOnCloseWithWrite;

        if (!useSyncOnClose) {
            request.Flush = false;
        }

        std::vector<TFuture<TWriteResponse>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            auto future = BIND([=, this, this_ = MakeStrong(this)] (
                const TWriteRequest &request,
                TWallTimer timer,
                TRequestCounterGuard requestCounterGuard) {
                AddWriteWaitTimeSample(timer.GetElapsedTime());
                return DoWrite(
                    request,
                    config->MaxBytesPerWrite,
                    std::move(requestCounterGuard),
                    config->FlushAfterWrite && request.Flush,
                    config->AsyncFlushAfterWrite,
                    config->EnableSync,
                    config->SimulatedMaxBytesPerWrite,
                    Sensors_,
                    config->EnablePwritev);
            })
                .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                .Run(
                    std::move(slice),
                    TWallTimer(),
                    CreateInFlightRequestGuard(EIOEngineRequestType::Write));
            futures.push_back(std::move(future));
        }

        return AllSucceeded(std::move(futures))
            .Apply(BIND([] (const std::vector<TWriteResponse>& subresponses) {
                TWriteResponse response;

                for (const auto& subresponse: subresponses) {
                    response.IOWriteRequests += subresponse.IOWriteRequests;
                    response.IOSyncRequests += subresponse.IOSyncRequests;
                    response.WrittenBytes += subresponse.WrittenBytes;
                }

                return response;
            }));
    }

    TFuture<TFlushFileResponse> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category) override
    {
        return BIND(&DoFlushFile, std::move(request), StaticConfig_->EnableSync, Sensors_)
            .AsyncVia(ThreadPool_.GetWriteInvoker(category, {}))
            .Run();
    }

    virtual TFuture<TFlushFileRangeResponse> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category,
        TIOSessionId sessionId) override
    {
        std::vector<TFuture<TFlushFileRangeResponse>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&DoFlushFileRange, std::move(slice), StaticConfig_->EnableSync, Sensors_)
                    .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                    .Run());
        }

        return AllSucceeded(std::move(futures))
            .Apply(BIND([] (const std::vector<TFlushFileRangeResponse>& subresponses) {
                TFlushFileRangeResponse response;

                for (const auto& subresponse: subresponses) {
                    response.IOSyncRequests += subresponse.IOSyncRequests;
                }

                return response;
            }));
    }

private:
    const TConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<TConfig> Config_;

    TThreadPool ThreadPool_;
    TRequestSlicer RequestSlicer_;

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        auto config = UpdateYsonStruct(StaticConfig_, node);

        ThreadPool_.Reconfigure(config);
        Config_.Store(config);
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFairShareIOEngineRequestType,
    (Read)
    (Write)
    (Flush)
    (FlushFileRange)
);

class TFairShareHierarchicalThreadPoolIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TThreadPoolIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TFairShareHierarchicalThreadPoolIOEngine(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger,
        TFairShareHierarchicalSlotQueuePtr<TString> fairShareQueue)
        : TIOEngineBase(
            config,
            std::move(locationId),
            std::move(profiler),
            std::move(logger))
        , StaticConfig_(std::move(config))
        , Config_(StaticConfig_)
        , ThreadPool_(CreateThreadPool(StaticConfig_->FairShareThreadCount, Format("IOFS:%v", LocationId_)))
        , RequestSlicer_(StaticConfig_->DesiredRequestSize, StaticConfig_->MinRequestSize)
        , FairShareQueue_(std::move(fairShareQueue))
    {
        RunActions();
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TIOSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        std::vector<TFuture<TInternalReadResponse>> futures;
        futures.reserve(requests.size());

        i64 paddedBytes = 0;
        for (const auto& request : requests) {
            paddedBytes += GetPaddedSize(request.Offset, request.Size, DefaultPageSize);
        }

        auto buffers = AllocateReadBuffers(requests, tagCookie, useDedicatedAllocations);
        auto config = Config_.Acquire();
        auto guard = Guard(Lock_);

        for (int index = 0; index < std::ssize(requests); ++index) {
            for (auto& slice : RequestSlicer_.Slice(std::move(requests[index]), buffers[index])) {
                auto slotId = requests[index].FairShareSlotId;
                auto requestId = TGuid::Create();
                auto promise = CreateRequestPromise<TInternalReadResponse>(
                    slotId,
                    requestId,
                    EFairShareIOEngineRequestType::Read);
                auto requestSize = slice.Request.Size;
                futures.push_back(promise.ToFuture());

                auto callback = BIND([=, this, this_ = MakeStrong(this),
                    request = std::move(slice.Request),
                    buffer = std::move(slice.OutputBuffer),
                    timer = TWallTimer(),
                    category = category,
                    sessionId = sessionId,
                    requestCounterGuard = CreateInFlightRequestGuard(EIOEngineRequestType::Read)] () mutable {
                    const auto readWaitTime = timer.GetElapsedTime();
                    AddReadWaitTimeSample(readWaitTime);

                    YT_LOG_DEBUG_IF(category == EWorkloadCategory::UserInteractive,
                        "Started reading from disk (Handle: %v, RequestSize: %v, ReadSessionId: %v, ReadWaitTime: %v)",
                        static_cast<FHANDLE>(*request.Handle),
                        request.Size,
                        sessionId,
                        readWaitTime);

                    return DoRead(
                        std::move(request),
                        std::move(buffer),
                        config->SimulatedMaxBytesPerRead,
                        config->MaxBytesPerRead,
                        category,
                        sessionId,
                        std::move(requestCounterGuard),
                        Sensors_,
                        Logger);
                });

                EmplaceOrCrash(
                    ReadRequestStorage_,
                    requestId,
                    TRequestHandler<TInternalReadResponse>{
                        .Promise = std::move(promise),
                        .Callback = std::move(callback),
                        .Cost = requestSize,
                    });
                SlotIds_.emplace(slotId);
                SlotIdToRequestIds_[slotId].push_back({requestId, EFairShareIOEngineRequestType::Read});
            }
        }

        guard.Release();

        TReadResponse response{
            .PaddedBytes = paddedBytes,
        };
        response.OutputBuffers.assign(buffers.begin(), buffers.end());

        EventCount_.NotifyAll();

        return AllSucceeded(std::move(futures))
            .Apply(BIND([
                response = std::move(response)
            ] (const std::vector<TInternalReadResponse>& subresponses) mutable {
                for (const auto& subresponse: subresponses) {
                    response.IORequests += subresponse.IORequests;
                }

                return std::move(response);
            }));
    }

    TFuture<TWriteResponse> Write(
        TWriteRequest request,
        EWorkloadCategory /*category*/,
        TIOSessionId /*sessionId*/) override
    {
        YT_ASSERT(request.Handle);

        auto config = Config_.Acquire();
        bool useSyncOnClose = config->EnableSyncOnCloseWithWrite;

        if (!useSyncOnClose) {
            request.Flush = false;
        }

        auto guard = Guard(Lock_);
        std::vector<TFuture<TWriteResponse>> futures;
        auto slotId = request.FairShareSlotId;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            auto requestId = TGuid::Create();
            auto promise = CreateRequestPromise<TWriteResponse>(
                slotId,
                requestId,
                EFairShareIOEngineRequestType::Write);
            auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Buffers));

            futures.push_back(promise.ToFuture());

            auto callback = BIND([
                =,
                this,
                this_ = MakeStrong(this),
                request = std::move(slice),
                timer = TWallTimer(),
                requestCounterGuard = CreateInFlightRequestGuard(EIOEngineRequestType::Write)] () mutable {
                AddWriteWaitTimeSample(timer.GetElapsedTime());
                return DoWrite(
                    request,
                    config->MaxBytesPerWrite,
                    std::move(requestCounterGuard),
                    config->FlushAfterWrite && request.Flush,
                    config->AsyncFlushAfterWrite,
                    config->EnableSync,
                    config->SimulatedMaxBytesPerWrite,
                    Sensors_,
                    config->EnablePwritev);
            });

            EmplaceOrCrash(
                WriteRequestStorage_,
                requestId,
                TRequestHandler<TWriteResponse>{
                    .Promise = std::move(promise),
                    .Callback = std::move(callback),
                    .Cost = toWriteRemaining,
                });
            SlotIds_.emplace(slotId);
            SlotIdToRequestIds_[slotId].push_back({requestId, EFairShareIOEngineRequestType::Write});
        }

        guard.Release();
        EventCount_.NotifyAll();

        return AllSucceeded(std::move(futures))
            .Apply(BIND([] (const std::vector<TWriteResponse>& subresponses) {
                TWriteResponse response;

                for (const auto& subresponse: subresponses) {
                    response.IOWriteRequests += subresponse.IOWriteRequests;
                    response.IOSyncRequests += subresponse.IOSyncRequests;
                    response.WrittenBytes += subresponse.WrittenBytes;
                }

                return response;
            }));
    }

    TFuture<TFlushFileResponse> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory /*category*/) override
    {
        auto slotId = request.FairShareSlotId;
        auto requestId = TGuid::Create();
        auto promise = CreateRequestPromise<TFlushFileResponse>(
            slotId,
            requestId,
            EFairShareIOEngineRequestType::Flush);
        auto callback = BIND(&DoFlushFile, std::move(request), StaticConfig_->EnableSync, Sensors_);
        auto future = promise.ToFuture();
        auto guard = Guard(Lock_);

        EmplaceOrCrash(
            FlushFileRequestStorage_,
            requestId,
            TRequestHandler<TFlushFileResponse>{
                .Promise = std::move(promise),
                .Callback = std::move(callback),
            });
        SlotIds_.emplace(slotId);
        SlotIdToRequestIds_[slotId].push_back({requestId, EFairShareIOEngineRequestType::Flush});

        guard.Release();
        EventCount_.NotifyAll();

        return future;
    }

    virtual TFuture<TFlushFileRangeResponse> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory /*category*/,
        TIOSessionId /*sessionId*/) override
    {
        std::vector<TFuture<TFlushFileRangeResponse>> futures;
        auto slotId = request.FairShareSlotId;

        auto guard = Guard(Lock_);
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            auto requestId = TGuid::Create();
            auto promise = CreateRequestPromise<TFlushFileRangeResponse>(
                slotId,
                requestId,
                EFairShareIOEngineRequestType::FlushFileRange);

            futures.push_back(promise.ToFuture());

            auto callback = BIND(&DoFlushFileRange, std::move(slice), StaticConfig_->EnableSync, Sensors_);

            EmplaceOrCrash(
                FlushFileRangeRequestStorage_,
                requestId,
                TRequestHandler<TFlushFileRangeResponse>{
                    .Promise = std::move(promise),
                    .Callback = std::move(callback),
                });
            SlotIds_.emplace(slotId);
            SlotIdToRequestIds_[slotId].push_back({requestId, EFairShareIOEngineRequestType::FlushFileRange});
        }

        guard.Release();
        EventCount_.NotifyAll();

        return AllSucceeded(std::move(futures))
            .Apply(BIND([] (const std::vector<TFlushFileRangeResponse>& subresponses) {
                TFlushFileRangeResponse response;

                for (const auto& subresponse: subresponses) {
                    response.IOSyncRequests += subresponse.IOSyncRequests;
                }

                return response;
            }));
    }

    void EngineLoop()
    {
        auto finally = [this] (auto&& guard, auto&& cookie) {
            guard.Release();
            EventCount_.Wait(std::move(cookie), TDuration::Seconds(1));
            YT_UNUSED_FUTURE(BIND(&TFairShareHierarchicalThreadPoolIOEngine::EngineLoop, MakeStrong(this))
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run());
        };

        while (true) {
            auto cookie = EventCount_.PrepareWait();

            auto guard = Guard(Lock_);
            if (SlotIds_.empty()) {
                finally(std::move(guard), std::move(cookie));
                return;
            }

            // TODO(don-dron): For requests that are not explicitly marked up with slots, guaranteed priority
            // must be used. In the future, you need to exclude unmarked requests.
            auto slot = FairShareQueue_ && !SlotIds_.contains(TFairShareSlotId{})
                ? FairShareQueue_->PeekSlot(SlotIds_)
                : nullptr;
            auto slotId = slot ? slot->GetSlotId() : TFairShareSlotId{};
            auto requestsIt = SlotIdToRequestIds_.find(slotId);

            if (requestsIt == SlotIdToRequestIds_.end()) {
                finally(std::move(guard), std::move(cookie));
                return;
            }

            EventCount_.CancelWait();

            auto& requests = requestsIt->second;

            YT_VERIFY(!requests.empty());
            auto requestIdToType = requests.front();
            auto requestId = requestIdToType.first;
            auto requestType = requestIdToType.second;

            requests.pop_front();

            if (requests.empty()) {
                EraseOrCrash(SlotIds_, slotId);
                EraseOrCrash(SlotIdToRequestIds_, slotId);
            }

            switch (requestType) {
                case EFairShareIOEngineRequestType::Read:
                    HandleNextRequest(
                        slot,
                        std::move(guard),
                        requestId,
                        ReadRequestStorage_);
                    break;
                case EFairShareIOEngineRequestType::Write:
                    HandleNextRequest(
                        slot,
                        std::move(guard),
                        requestId,
                        WriteRequestStorage_);
                    break;
                case EFairShareIOEngineRequestType::Flush:
                    HandleNextRequest(
                        slot,
                        std::move(guard),
                        requestId,
                        FlushFileRequestStorage_);
                    break;
                case EFairShareIOEngineRequestType::FlushFileRange:
                    HandleNextRequest(
                        slot,
                        std::move(guard),
                        requestId,
                        FlushFileRangeRequestStorage_);
                    break;
                default:
                    YT_LOG_FATAL("Unknown request type (RequestType: %v)", requestType);
            }
        }
    }

private:
    template <class TResponse>
    struct TRequestHandler
    {
        TPromise<TResponse> Promise;
        TCallback<TResponse()> Callback;
        i64 Cost = 0;
    };

    const TConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<TConfig> Config_;

    std::atomic<int> LoopCount_ = 0;

    IThreadPoolPtr ThreadPool_;
    TIORequestSlicer RequestSlicer_;

    TFairShareHierarchicalSlotQueuePtr<TString> FairShareQueue_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TFairShareSlotId, std::deque<std::pair<TGuid, EFairShareIOEngineRequestType>>> SlotIdToRequestIds_;
    THashSet<TFairShareSlotId> SlotIds_;

    THashMap<TGuid, TRequestHandler<TInternalReadResponse>> ReadRequestStorage_;
    THashMap<TGuid, TRequestHandler<TWriteResponse>> WriteRequestStorage_;
    THashMap<TGuid, TRequestHandler<TFlushFileResponse>> FlushFileRequestStorage_;
    THashMap<TGuid, TRequestHandler<TFlushFileRangeResponse>> FlushFileRangeRequestStorage_;

    NThreading::TEventCount EventCount_;

    template <class TResponse>
    TPromise<TResponse> CreateRequestPromise(
        TFairShareSlotId slotId,
        TGuid requestId,
        EFairShareIOEngineRequestType requestType)
    {
        auto promise = NewPromise<TResponse>();
        promise.OnCanceled(BIND([=, this, this_ = MakeStrong(this)] (const TError& /*error*/) {
            auto guard = Guard(Lock_);
            auto slotIt = SlotIds_.find(slotId);
            if (slotIt != SlotIds_.end()) {
                auto& requests = SlotIdToRequestIds_[slotId];
                requests.erase(std::find_if(
                    requests.begin(),
                    requests.end(),
                    [&] (const auto& requestIdToType) {
                        return requestIdToType.first == requestId;
                    }));

                if (requests.empty()) {
                    SlotIds_.erase(slotIt);
                    EraseOrCrash(SlotIdToRequestIds_, slotId);
                }

                switch (requestType) {
                    case EFairShareIOEngineRequestType::Read:
                        ReadRequestStorage_.erase(requestId);
                        return;
                    case EFairShareIOEngineRequestType::Write:
                        WriteRequestStorage_.erase(requestId);
                        return;
                    case EFairShareIOEngineRequestType::Flush:
                        FlushFileRequestStorage_.erase(requestId);
                        return;
                    case EFairShareIOEngineRequestType::FlushFileRange:
                        FlushFileRangeRequestStorage_.erase(requestId);
                        return;
                    default:
                        YT_ABORT();
                }
            }
        }).Via(GetAuxPoolInvoker()));

        return promise;
    }

    template <class TResponse>
    void HandleNextRequest(
        const TFairShareHierarchicalSlotQueueSlotPtr<TString>& slot,
        TGuard<NThreading::TSpinLock> guard,
        TGuid requestId,
        THashMap<TGuid, TRequestHandler<TResponse>>& requestStorage)
    {
        auto requestHandlerIt = requestStorage.find(requestId);

        YT_VERIFY(requestHandlerIt != requestStorage.end());
        auto& requestHandler = requestHandlerIt->second;
        auto promise = std::move(requestHandler.Promise);
        auto callback = std::move(requestHandler.Callback);
        auto cost = requestHandler.Cost;

        requestStorage.erase(requestHandlerIt);

        if (promise.IsCanceled()) {
            return;
        } else {
            guard.Release();

            if (cost > 0 && slot) {
                FairShareQueue_->AccountSlot(slot, cost);
            }

            try {
                auto result = callback();
                promise.TrySet(std::move(result));
            } catch (const std::exception& ex) {
                promise.TrySet(TError(ex));
            }
        }
    }

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        auto config = UpdateYsonStruct(StaticConfig_, node);
        ThreadPool_->SetThreadCount(config->FairShareThreadCount);
        Config_.Store(config);
        RunActions();
    }

    void RunActions()
    {
        while (LoopCount_.load() < Config_.Acquire()->FairShareThreadCount) {
            LoopCount_.fetch_add(1);
            YT_UNUSED_FUTURE(BIND(&TFairShareHierarchicalThreadPoolIOEngine::EngineLoop, MakeStrong(this))
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    TProfiler profiler,
    NLogging::TLogger logger,
    TFairShareHierarchicalSlotQueuePtr<TString> fairShareQueue)
{
    using TClassicThreadPoolIOEngine = TThreadPoolIOEngine<TFixedPriorityExecutor, TDummyRequestSlicer>;
    using TFairShareThreadPoolIOEngine = TThreadPoolIOEngine<TFairShareThreadPool, TIORequestSlicer>;

    switch (engineType) {
        case EIOEngineType::ThreadPool:
            return CreateIOEngine<TClassicThreadPoolIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
#ifdef _linux_
        case EIOEngineType::Uring:
        case EIOEngineType::FairShareUring:
            return CreateIOEngineUring(
                engineType,
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
#endif
        case EIOEngineType::FairShareThreadPool:
            return CreateIOEngine<TFairShareThreadPoolIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
        case EIOEngineType::FairShareHierarchical:
            return CreateIOEngine<TFairShareHierarchicalThreadPoolIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger),
                std::move(fairShareQueue));
        default:
            THROW_ERROR_EXCEPTION("Unknown IO engine %Qlv",
                engineType);
    }
}

std::vector<EIOEngineType> GetSupportedIOEngineTypes()
{
    std::vector<EIOEngineType> result;
    result.push_back(EIOEngineType::ThreadPool);
    if (IsUringIOEngineSupported()) {
        result.push_back(EIOEngineType::Uring);
        result.push_back(EIOEngineType::FairShareUring);
    }
    result.push_back(EIOEngineType::FairShareThreadPool);
    result.push_back(EIOEngineType::FairShareHierarchical);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
