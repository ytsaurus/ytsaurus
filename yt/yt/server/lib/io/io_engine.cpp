#include "io_engine.h"
#include "io_engine_base.h"
#include "io_engine_uring.h"
#include "io_request_slicer.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>

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

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

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

TFuture<IIOEngine::TReadResponse> IIOEngine::ReadAll(
    const TString& path,
    EWorkloadCategory category,
    TSessionId sessionId)
{
    return Open({path, OpenExisting | RdOnly | Seq | CloseOnExec}, category)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& handle) {
            struct TReadAllBufferTag
            { };
            return Read(
                {{handle, 0, handle->GetLength()}},
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
    : public TIOEngineBaseCommonConfig
{
    int ReadThreadCount;
    int WriteThreadCount;

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

        registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
            .Default(false);
        registrar.Parameter("async_flush_after_write", &TThis::AsyncFlushAfterWrite)
            .Default(false);
        registrar.Parameter("enable_sync_on_close_with_write", &TThis::EnableSyncOnCloseWithWrite)
            .Default(false);

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

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(ReadInvoker_, GetBasicPriority(category));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
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

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId client)
    {
        const auto& pool = GetPoolByCategory(category);
        return ReadThreadPool_->GetInvoker(pool.Name, ToString(client));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
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
    : public TIOEngineBaseCommon
{
public:
    using TConfig = TThreadPoolIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TThreadPoolIOEngine(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBaseCommon(
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
        TSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        std::vector<TFuture<TCommonReadResponse>> futures;
        futures.reserve(requests.size());

        auto invoker = ThreadPool_.GetReadInvoker(category, sessionId);

        i64 paddedBytes = 0;
        for (const auto& request : requests) {
            paddedBytes += GetPaddedSize(request.Offset, request.Size, DefaultPageSize);
        }
        auto buffers = AllocateReadBuffers(requests, tagCookie, useDedicatedAllocations);

        for (int index = 0; index < std::ssize(requests); ++index) {
            for (auto& slice : RequestSlicer_.Slice(std::move(requests[index]), buffers[index])) {
                auto future = BIND(
                    &TThreadPoolIOEngine::DoRead,
                    MakeStrong(this),
                    std::move(slice.Request),
                    std::move(slice.OutputBuffer),
                    TWallTimer(),
                    category,
                    sessionId,
                    Passed(CreateInFlightRequestGuard(EIOEngineRequestType::Read)))
                    .AsyncVia(invoker)
                    .Run();
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
            ] (const std::vector<TCommonReadResponse>& subresponses) mutable {
                for (const auto& subresponse: subresponses) {
                    response.IORequests += subresponse.IORequests;
                }

                return std::move(response);
            }));
    }

    TFuture<TWriteResponse> Write(
        TWriteRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        YT_ASSERT(request.Handle);

        bool useSyncOnClose = Config_.Acquire()->EnableSyncOnCloseWithWrite;

        if (!useSyncOnClose) {
            request.Flush = false;
        }

        std::vector<TFuture<TWriteResponse>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            auto future = BIND(
                &TThreadPoolIOEngine::DoWriteImpl,
                MakeStrong(this),
                std::move(slice),
                TWallTimer(),
                Passed(CreateInFlightRequestGuard(EIOEngineRequestType::Write)))
                .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                .Run();
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
        return BIND(&TThreadPoolIOEngine::DoFlushFile, MakeStrong(this), std::move(request))
            .AsyncVia(ThreadPool_.GetWriteInvoker(category, {}))
            .Run();
    }

    virtual TFuture<TFlushFileRangeResponse> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        std::vector<TFuture<TFlushFileRangeResponse>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&TThreadPoolIOEngine::DoFlushFileRange, MakeStrong(this), std::move(slice))
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

    TWriteResponse DoWriteImpl(
        const TWriteRequest& request,
        TWallTimer timer,
        TRequestCounterGuard requestCounterGuard)
    {
        auto guard = std::move(requestCounterGuard);
        auto writeResponse = DoWrite(request, timer);

        auto config = Config_.Acquire();
        auto syncFlush = config->FlushAfterWrite && request.Flush;
        auto asyncFlush = config->AsyncFlushAfterWrite;

        if ((syncFlush || asyncFlush) && writeResponse.WrittenBytes) {
            auto flushFileRangeResponse = DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writeResponse.WrittenBytes,
                .Async = !syncFlush && asyncFlush,
            });

            writeResponse.IOSyncRequests += flushFileRangeResponse.IOSyncRequests;
        }

        return writeResponse;
    }

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        TIOEngineBaseCommon::DoReconfigure(node);

        auto config = UpdateYsonStruct(StaticConfig_, node);

        ThreadPool_.Reconfigure(config);
        Config_.Store(config);
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    TProfiler profiler,
    NLogging::TLogger logger)
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
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
