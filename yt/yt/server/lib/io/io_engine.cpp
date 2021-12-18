#include "io_engine.h"
#include "io_request_slicer.h"
#include "read_request_combiner.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/notification_handle.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/client/misc/workload.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <array>

#ifdef _linux_
    #include <contrib/libs/liburing/src/include/liburing.h>

    #include <sys/uio.h>

    #ifndef FALLOC_FL_CONVERT_UNWRITTEN
        #define FALLOC_FL_CONVERT_UNWRITTEN 0x4
    #endif
#endif

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto DefaultPageSize = 4_KB;

#ifdef _linux_

static constexpr auto UringEngineNotificationCount = 2;
static constexpr auto MaxIovCountPerRequest = 64;
static constexpr auto MaxUringConcurrentRequestsPerThread = 32;

// See SetRequestUserData/GetRequestUserData.
static constexpr auto MaxSubrequestCount = 1 << 16;
static constexpr auto TypicalSubrequestCount = 64;

#endif

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const auto& Logger = IOLogger;

#endif

////////////////////////////////////////////////////////////////////////////////

TIOEngineHandle::TIOEngineHandle(const TString& fName, EOpenMode oMode) noexcept
    : TFileHandle(fName, oMode)
    , OpenForDirectIO_(oMode & DirectAligned)
{ }

////////////////////////////////////////////////////////////////////////////////

TFuture<TSharedRef> IIOEngine::ReadAll(
    const TString& path,
    EWorkloadCategory category,
    TSessionId sessionId)
{
    return Open({path, OpenExisting | RdOnly | Seq | CloseOnExec}, category)
        .Apply(BIND([=, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& handle) {
            struct TReadAllBufferTag
            { };
            return Read<TReadAllBufferTag>(
                    {{handle, 0, handle->GetLength()}},
                    category,
                    NYTAlloc::EMemoryZone::Normal,
                    sessionId
                ).Apply(BIND(
                    [=, this_ = MakeStrong(this), handle = handle]
                    (const TReadResponse& response)
                {
                    YT_VERIFY(response.OutputBuffers.size() == 1);
                    return Close({handle}, category)
                        .Apply(BIND([buffers = response.OutputBuffers] {
                            return buffers[0];
                        }));
                }));
        }));
}

////////////////////////////////////////////////////////////////////////////////

class TIOEngineConfigBase
    : public NYTree::TYsonSerializable
{
public:
    int AuxThreadCount;
    int FsyncThreadCount;

    bool EnableSync;

    i64 MaxBytesPerRead;
    i64 MaxBytesPerWrite;

    // For tests only.
    std::optional<i64> SimulatedMaxBytesPerWrite;
    std::optional<i64> SimulatedMaxBytesPerRead;

    std::optional<TDuration> SickReadTimeThreshold;
    std::optional<TDuration> SickReadTimeWindow;
    std::optional<TDuration> SickWriteTimeThreshold;
    std::optional<TDuration> SickWriteTimeWindow;
    std::optional<TDuration> SicknessExpirationTimeout;

    TIOEngineConfigBase()
    {
        RegisterParameter("aux_thread_count", AuxThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("fsync_thread_count", FsyncThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);

        RegisterParameter("enable_sync", EnableSync)
            .Default(true);

        RegisterParameter("max_bytes_per_read", MaxBytesPerRead)
            .GreaterThanOrEqual(1)
            .Default(256_MB);
        RegisterParameter("max_bytes_per_write", MaxBytesPerWrite)
            .GreaterThanOrEqual(1)
            .Default(256_MB);

        RegisterParameter("simulated_max_bytes_per_read", SimulatedMaxBytesPerRead)
            .Default()
            .GreaterThan(0);
        RegisterParameter("simulated_max_bytes_per_write", SimulatedMaxBytesPerWrite)
            .Default()
            .GreaterThan(0);

        RegisterParameter("sick_read_time_threshold", SickReadTimeThreshold)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default();
        RegisterParameter("sick_read_time_window", SickReadTimeWindow)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default();
        RegisterParameter("sick_write_time_threshold", SickWriteTimeThreshold)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default();
        RegisterParameter("sick_write_time_window", SickWriteTimeWindow)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default();
        RegisterParameter("sickness_expiration_timeout", SicknessExpirationTimeout)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TThreadPoolIOEngineConfig)

class TThreadPoolIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int ReadThreadCount;
    int WriteThreadCount;

    bool EnablePwritev;

    // Request size in bytes.
    i64 DesiredRequestSize;
    i64 MinRequestSize;

    // Fair-share thread pool settings.
    double DefaultPoolWeight;
    double UserInteractivePoolWeight;

    TThreadPoolIOEngineConfig()
    {
        RegisterParameter("read_thread_count", ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("write_thread_count", WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);

        RegisterParameter("enable_pwritev", EnablePwritev)
            .Default(true);

        RegisterParameter("desired_request_size", DesiredRequestSize)
            .GreaterThanOrEqual(4_KB)
            .Default(128_KB);
        RegisterParameter("min_request_size", MinRequestSize)
            .GreaterThanOrEqual(512)
            .Default(64_KB);

        RegisterParameter("default_pool_weight", DefaultPoolWeight)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("user_interactive_pool_weight", UserInteractivePoolWeight)
            .GreaterThanOrEqual(1)
            .Default(4);
    }
};

DEFINE_REFCOUNTED_TYPE(TThreadPoolIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

DECLARE_REFCOUNTED_CLASS(TUringIOEngineConfig)

class TUringIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int UringThreadCount;

    //! Limits the number of concurrent (outstanding) #IIOEngine requests per a single uring thread.
    int MaxConcurrentRequestsPerThread;

    int DirectIOPageSize;

    TUringIOEngineConfig()
    {
        RegisterParameter("uring_thread_count", UringThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("max_concurrent_requests_per_thread", MaxConcurrentRequestsPerThread)
            .GreaterThan(0)
            .LessThanOrEqual(MaxUringConcurrentRequestsPerThread)
            .Default(22);

        RegisterParameter("direct_io_page_size", DirectIOPageSize)
            .GreaterThan(0)
            .Default(DefaultPageSize);
    }
};

DEFINE_REFCOUNTED_TYPE(TUringIOEngineConfig)

#endif

////////////////////////////////////////////////////////////////////////////////

i64 GetPaddedSize(i64 offset, i64 size, i64 alignment)
{
    return AlignUp(offset + size, alignment) - AlignDown(offset, alignment);
}

////////////////////////////////////////////////////////////////////////////////

class TInflightCounter
{
public:

    void Increment()
    {
        if (!State_) {
            return;
        }
        State_->Counter.fetch_add(1, std::memory_order_relaxed);
    }

    void Decrement()
    {
        if (!State_) {
            return;
        }
        State_->Counter.fetch_sub(1, std::memory_order_relaxed);
    }

    static TInflightCounter Create(TProfiler& profiler, const TString& name)
    {
        TInflightCounter obj;
        obj.State_ = New<TState>();
        profiler.AddFuncGauge(name, obj.State_, [state = obj.State_.Get()](){
            return state->Counter.load(std::memory_order_relaxed);
        });
        return obj;
    }


private:
    struct TState : public TRefCounted
    {
        std::atomic<i32> Counter;
    };

private:
    NYT::TIntrusivePtr<TState> State_;
};


struct TIOEngineSensors
{
    struct TRequestSensors
    {
        // Single request time.
        NProfiling::TEventTimer Timer;

        // Cumulative execution time of all requests.
        NProfiling::TTimeCounter TotalTimeCounter;

        // Total requests count.
        NProfiling::TCounter Counter;

        // Currently executing requests count.
        TInflightCounter InflightCounter;
    };

    NProfiling::TCounter WrittenBytesCounter;
    NProfiling::TCounter ReadBytesCounter;

    TRequestSensors ReadSensors;
    TRequestSensors WriteSensors;
    TRequestSensors SyncSensors;
    TRequestSensors DataSyncSensors;
    TRequestSensors IoSubmitSensors;
};

class TRequestStatsGuard
{
public:
    TRequestStatsGuard(TIOEngineSensors::TRequestSensors sensors)
        : Sensors_(std::move(sensors))
    {
        Sensors_.Counter.Increment();
        Sensors_.InflightCounter.Increment();
    }

    TRequestStatsGuard(TRequestStatsGuard&& other) = default;

    ~TRequestStatsGuard()
    {
        auto duration = Timer_.GetElapsedTime();
        Sensors_.Timer.Record(duration);
        Sensors_.TotalTimeCounter.Add(duration);
        Sensors_.InflightCounter.Decrement();
    }

private:
    TIOEngineSensors::TRequestSensors Sensors_;
    NProfiling::TWallTimer Timer_;
};

class TIOEngineBase
    : public IIOEngine
{
public:
    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        EWorkloadCategory category) override
    {
        return BIND(&TIOEngineBase::DoOpen, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
            .Run();
    }

    TFuture<void> Close(
        TCloseRequest request,
        EWorkloadCategory category) override
    {
        auto invoker = (request.Flush || request.Size) ? FsyncInvoker_ : AuxInvoker_;
        return BIND(&TIOEngineBase::DoClose, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(invoker, GetBasicPriority(category)))
            .Run();
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        EWorkloadCategory category) override
    {
        return BIND(&TIOEngineBase::DoFlushDirectory, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(FsyncInvoker_, GetBasicPriority(category)))
            .Run();
    }

    TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory category) override
    {
        return BIND(&TIOEngineBase::DoAllocate, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
            .Run();
    }

    bool IsSick() const override
    {
        return Sick_;
    }

    const IInvokerPtr& GetAuxPoolInvoker() override
    {
        return AuxThreadPool_->GetInvoker();
    }

protected:
    using TConfig = TIOEngineConfigBase;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    const TString LocationId_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;
    TIOEngineSensors Sensors;

    TIOEngineBase(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : LocationId_(std::move(locationId))
        , Logger(std::move(logger))
        , Profiler(std::move(profiler))
        , Config_(std::move(config))
        , AuxThreadPool_(New<TThreadPool>(Config_->AuxThreadCount, Format("IOA:%v", LocationId_)))
        , FsyncThreadPool_(New<TThreadPool>(Config_->FsyncThreadCount, Format("IOS:%v", LocationId_)))
        , AuxInvoker_(CreatePrioritizedInvoker(AuxThreadPool_->GetInvoker()))
        , FsyncInvoker_(CreatePrioritizedInvoker(FsyncThreadPool_->GetInvoker()))
    {
        InitProfilerSensors();
    }

    TIOEngineHandlePtr DoOpen(const TOpenRequest& request)
    {
        TIOEngineHandlePtr handle;
        {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            handle = New<TIOEngineHandle>(request.Path, request.Mode);
        }
        if (!handle->IsOpen()) {
            THROW_ERROR_EXCEPTION(
                "Cannot open %Qv with mode %v",
                request.Path,
                request.Mode)
                << TError::FromSystem();
        }
        return handle;
    }

    void DoFlushDirectory(const TFlushDirectoryRequest& request)
    {
        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            NFS::FlushDirectory(request.Path);
        });
    }

    void DoClose(const TCloseRequest& request)
    {
        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            if (request.Size) {
                request.Handle->Resize(*request.Size);
            }
            if (request.Flush && Config_->EnableSync) {
                request.Handle->Flush();
            }
            request.Handle->Close();
        });
    }

    void DoAllocate(const TAllocateRequest& request)
    {
#ifdef _linux_
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        int mode = EnableFallocateConvertUnwritten_.load() ? FALLOC_FL_CONVERT_UNWRITTEN : 0;
        int result = HandleEintr(::fallocate, *request.Handle, mode, 0, request.Size);
        if (result != 0) {
            if ((errno == EPERM || errno == EOPNOTSUPP) && mode == FALLOC_FL_CONVERT_UNWRITTEN) {
                if (EnableFallocateConvertUnwritten_.exchange(false)) {
                    YT_LOG_INFO(TError::FromSystem(), "fallocate call failed; disabling FALLOC_FL_CONVERT_UNWRITTEN mode");
                }
            } else {
                THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "fallocate call failed")
                    << TError::FromSystem();
            }
        }
#else
        Y_UNUSED(request);
#endif
    }

    void AddWriteWaitTimeSample(TDuration duration)
    {
        if (Config_->SickWriteTimeThreshold && Config_->SickWriteTimeWindow && Config_->SicknessExpirationTimeout && !Sick_) {
            if (duration > *Config_->SickWriteTimeThreshold) {
                auto now = GetInstant();
                auto guard = Guard(WriteWaitLock_);
                if (!SickWriteWaitStart_) {
                    SickWriteWaitStart_ = now;
                } else if (now - *SickWriteWaitStart_ > *Config_->SickWriteTimeWindow) {
                    auto error = TError("Write is too slow")
                        << TErrorAttribute("sick_write_wait_start", *SickWriteWaitStart_);
                    guard.Release();
                    SetSickFlag(error);
                }
            } else {
                auto guard = Guard(WriteWaitLock_);
                SickWriteWaitStart_.reset();
            }
        }
    }

    void AddReadWaitTimeSample(TDuration duration)
    {
        if (Config_->SickReadTimeThreshold && Config_->SickReadTimeWindow && Config_->SicknessExpirationTimeout && !Sick_) {
            if (duration > *Config_->SickReadTimeThreshold) {
                auto now = GetInstant();
                auto guard = Guard(ReadWaitLock_);
                if (!SickReadWaitStart_) {
                    SickReadWaitStart_ = now;
                } else if (now - *SickReadWaitStart_ > *Config_->SickReadTimeWindow) {
                    auto error = TError("Read is too slow")
                        << TErrorAttribute("sick_read_wait_start", *SickReadWaitStart_);
                    guard.Release();
                    SetSickFlag(error);
                }
            } else {
                auto guard = Guard(ReadWaitLock_);
                SickReadWaitStart_.reset();
            }
        }
    }

private:
    const TConfigPtr Config_;

    const TThreadPoolPtr AuxThreadPool_;
    const TThreadPoolPtr FsyncThreadPool_;
    const IPrioritizedInvokerPtr AuxInvoker_;
    const IPrioritizedInvokerPtr FsyncInvoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReadWaitLock_);
    std::optional<TInstant> SickReadWaitStart_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, WriteWaitLock_);
    std::optional<TInstant> SickWriteWaitStart_;

    std::atomic<bool> Sick_ = false;
    std::atomic<i64> SicknessCounter_ = 0;

    NProfiling::TGauge SickGauge_;
    NProfiling::TGauge SickEventsGauge_;

    std::atomic<bool> EnableFallocateConvertUnwritten_ = true;


    void InitProfilerSensors()
    {
        Profiler.AddFuncGauge("/sick", MakeStrong(this), [this] {
            return Sick_.load();
        });

        Profiler.AddFuncGauge("/sick_events", MakeStrong(this), [this] {
            return SicknessCounter_.load();
        });

        Sensors.WrittenBytesCounter = Profiler.Counter("/written_bytes");
        Sensors.ReadBytesCounter = Profiler.Counter("/read_bytes");

        auto makeRequestSensors = [] (TProfiler profiler) {
            TIOEngineSensors::TRequestSensors sensors;
            sensors.Timer = profiler.Timer("/time");
            sensors.TotalTimeCounter = profiler.TimeCounter("/total_time");
            sensors.Counter = profiler.Counter("/request_count");
            sensors.InflightCounter = TInflightCounter::Create(profiler, "/inflight_count");
            return sensors;
        };

        Sensors.ReadSensors = makeRequestSensors(Profiler.WithPrefix("/read"));
        Sensors.WriteSensors = makeRequestSensors(Profiler.WithPrefix("/write"));
        Sensors.SyncSensors = makeRequestSensors(Profiler.WithPrefix("/sync"));
        Sensors.DataSyncSensors = makeRequestSensors(Profiler.WithPrefix("/datasync"));
        Sensors.IoSubmitSensors = makeRequestSensors(Profiler.WithPrefix("/uring_io_submit"));
    }

    void SetSickFlag(const TError& error)
    {
        bool expected = false;
        if (Sick_.compare_exchange_strong(expected, true)) {
            ++SicknessCounter_;
            TDelayedExecutor::Submit(
                BIND(&TIOEngineBase::ResetSickFlag, MakeStrong(this)),
                *Config_->SicknessExpirationTimeout);

            YT_LOG_WARNING(error, "Sick flag set");
        }
    }

    void ResetSickFlag()
    {
        {
            auto guard = Guard(WriteWaitLock_);
            SickWriteWaitStart_.reset();
        }

        {
            auto guard = Guard(ReadWaitLock_);
            SickReadWaitStart_.reset();
        }

        Sick_ = false;

        YT_LOG_WARNING("Sick flag reset");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFixedPriorityExecutor
{
public:
    TFixedPriorityExecutor(
            TIntrusivePtr<TThreadPoolIOEngineConfig> config,
            const TString& locationId,
            NLogging::TLogger)
        : ReadThreadPool_(New<TThreadPool>(config->ReadThreadCount, Format("IOR:%v", locationId)))
        , WriteThreadPool_(New<TThreadPool>(config->WriteThreadCount, Format("IOW:%v", locationId)))
        , ReadInvoker_(CreatePrioritizedInvoker(ReadThreadPool_->GetInvoker()))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker()))
    { }

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(ReadInvoker_, GetBasicPriority(category));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(WriteInvoker_, GetBasicPriority(category));
    }

private:
    const TThreadPoolPtr ReadThreadPool_;
    const TThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr ReadInvoker_;
    const IPrioritizedInvokerPtr WriteInvoker_;
};

class TFairShareThreadPool
{
public:
    TFairShareThreadPool(
            TIntrusivePtr<TThreadPoolIOEngineConfig> config,
            const TString& locationId,
            NLogging::TLogger logger)
        : ReadThreadPool_(CreateTwoLevelFairShareThreadPool(config->ReadThreadCount, Format("FSH:%v", locationId)))
        , WriteThreadPool_(New<TThreadPool>(config->WriteThreadCount, Format("IOW:%v", locationId)))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker()))
        , Logger(logger)
        , DefaultPool_{"Default", config->DefaultPoolWeight}
        , UserInteractivePool_{"UserInteractive", config->UserInteractivePoolWeight}
    {
        YT_LOG_INFO("Creating FairShare thread pool for location %v read thread count %v",
            locationId, config->ReadThreadCount);
    }

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId client)
    {
        const auto& pool = GetPoolByCategory(category);
        return ReadThreadPool_->GetInvoker(pool.Name, pool.Weight, ToString(client));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(WriteInvoker_, GetBasicPriority(category));
    }

private:
    struct TPoolDesctriptor
    {
        TString Name;
        double Weight = 1.0;
    };

    const TPoolDesctriptor& GetPoolByCategory(EWorkloadCategory category)
    {
        if (category == EWorkloadCategory::UserInteractive) {
            return UserInteractivePool_;
        }
        return DefaultPool_;
    }

private:
    const ITwoLevelFairShareThreadPoolPtr ReadThreadPool_;
    const TThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr WriteInvoker_;
    NLogging::TLogger Logger;

    const TPoolDesctriptor DefaultPool_;
    const TPoolDesctriptor UserInteractivePool_;
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
        , Config_(std::move(config))
        , ThreadPool_(Config_, LocationId_, Logger)
        , RequestSlicer_(Config_->DesiredRequestSize, Config_->MinRequestSize)
    { }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        EMemoryZone memoryZone,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId) override
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(requests.size());

        auto invoker = ThreadPool_.GetReadInvoker(category, sessionId);

        i64 paddedBytesRead = 0;
        TSharedRefArray result;
        std::vector<TMutableRef> buffers;
        buffers.reserve(requests.size());
        {
            TMemoryZoneGuard zoneGuard(memoryZone);

            i64 totalSize = 0;
            for (const auto& request : requests) {
                totalSize += request.Size;
                paddedBytesRead += GetPaddedSize(request.Offset, request.Size, DefaultPageSize);
            }

            TSharedRefArrayBuilder resultBuilder(requests.size(), totalSize, tagCookie);
            for (const auto& request : requests) {
                buffers.push_back(resultBuilder.AllocateAndAdd(request.Size));
            }
            result = resultBuilder.Finish();
        }

        for (int index = 0; index < std::ssize(requests); ++index) {
            for (auto& slice : RequestSlicer_.Slice(std::move(requests[index]), buffers[index])) {
                futures.push_back(
                    BIND(&TThreadPoolIOEngine::DoRead,
                        MakeStrong(this),
                        std::move(slice.Request),
                        std::move(slice.OutputBuffer),
                        TWallTimer())
                    .AsyncVia(invoker)
                    .Run());
            }
        }

        return AllSucceeded(std::move(futures))
            .Apply(BIND([paddedBytesRead, result = std::move(result)] {
                return TReadResponse{
                    .OutputBuffers = result.ToVector(),
                    .PaddedBytesRead = paddedBytesRead
                };
            }));
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        std::vector<TFuture<void>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&TThreadPoolIOEngine::DoWrite, MakeStrong(this), std::move(slice), TWallTimer())
                .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category) override
    {
        return BIND(&TThreadPoolIOEngine::DoFlushFile, MakeStrong(this), std::move(request))
            .AsyncVia(ThreadPool_.GetWriteInvoker(category, {}))
            .Run();
    }

    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        std::vector<TFuture<void>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&TThreadPoolIOEngine::DoFlushFileRange, MakeStrong(this), std::move(slice))
                .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

private:
    const TConfigPtr Config_;
    TThreadPool ThreadPool_;
    TRequestSlicer RequestSlicer_;


    void DoRead(
        const TReadRequest& request,
        TMutableRef buffer,
        TWallTimer timer)
    {
        YT_VERIFY(std::ssize(buffer) == request.Size);

        AddReadWaitTimeSample(timer.GetElapsedTime());

        auto toReadRemaining = static_cast<i64>(buffer.Size());
        auto fileOffset = request.Offset;
        i64 bufferOffset = 0;

        NFS::ExpectIOErrors([&] {
            while (toReadRemaining > 0) {
                auto toRead = static_cast<ui32>(Min(toReadRemaining, Config_->MaxBytesPerRead));

                i64 reallyRead;
                {
                    TRequestStatsGuard statsGuard(Sensors.ReadSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyRead = HandleEintr(::pread, *request.Handle, buffer.Begin() + bufferOffset, toRead, fileOffset);
                }

                if (reallyRead < 0) {
                    // TODO(aozeritsky): ythrow is placed here consciously.
                    // ExpectIOErrors rethrows some kind of arcadia-style exception.
                    // So in order to keep the old behaviour we should use ythrow or
                    // rewrite ExpectIOErrors.
                    ythrow TFileError();
                }

                if (reallyRead == 0) {
                    break;
                }

                Sensors.ReadBytesCounter.Increment(reallyRead);
                if (Config_->SimulatedMaxBytesPerRead) {
                    reallyRead = Min(reallyRead, *Config_->SimulatedMaxBytesPerRead);
                }

                fileOffset += reallyRead;
                bufferOffset += reallyRead;
                toReadRemaining -= reallyRead;
            }
        });

        if (toReadRemaining > 0) {
            THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request");
        }
    }

    void DoWrite(
        const TWriteRequest& request,
        TWallTimer timer)
    {
        auto writtenBytes = DoWriteImpl(request, timer);
        if (request.Flush && writtenBytes) {
            YT_VERIFY(writtenBytes > 0);
            DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writtenBytes
            });
        }
    }

    i64 DoWriteImpl(
        const TWriteRequest& request,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        auto fileOffset = request.Offset;

        NFS::ExpectIOErrors([&] {
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
                           toWrite < Config_->MaxBytesPerWrite)
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
                        if (toWrite + static_cast<i64>(iovPart.iov_len) > Config_->MaxBytesPerWrite) {
                            iovPart.iov_len = Config_->MaxBytesPerWrite - toWrite;
                        }
                        toWrite += iovPart.iov_len;
                        ++iovCount;
                    }

                    i64 reallyWritten;
                    {
                        TRequestStatsGuard statsGuard(Sensors.WriteSensors);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwritev, *request.Handle, iov.data(), iovCount, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    Sensors.WrittenBytesCounter.Increment(reallyWritten);
                    if (Config_->SimulatedMaxBytesPerWrite) {
                        reallyWritten = Min(reallyWritten, *Config_->SimulatedMaxBytesPerWrite);
                    }

                    while (reallyWritten > 0) {
                        const auto& buffer = request.Buffers[bufferIndex];
                        i64 toAdvance = Min(static_cast<i64>(buffer.Size()) - bufferOffset, reallyWritten);
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
                    auto toWrite = static_cast<ui32>(Min(toWriteRemaining, Config_->MaxBytesPerWrite, static_cast<i64>(buffer.Size()) - bufferOffset));

                    i32 reallyWritten;
                    {
                        TRequestStatsGuard statsGuard(Sensors.WriteSensors);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwrite, *request.Handle, const_cast<char*>(buffer.Begin()) + bufferOffset, toWrite, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    Sensors.WrittenBytesCounter.Increment(reallyWritten);
                    fileOffset += reallyWritten;
                    bufferOffset += reallyWritten;
                    toWriteRemaining -= reallyWritten;
                    if (bufferOffset == std::ssize(buffer)) {
                        ++bufferIndex;
                        bufferOffset = 0;
                    }
                };

                if (Config_->EnablePwritev && isPwritevSupported()) {
                    pwritev();
                } else {
                    pwrite();
                }
            }
        });

        return fileOffset - request.Offset;
    }

    void DoFlushFile(const TFlushFileRequest& request)
    {
        if (!Config_->EnableSync) {
            return;
        }

        auto doFsync = [&] {
            TRequestStatsGuard statsGuard(Sensors.SyncSensors);
            return HandleEintr(::fsync, *request.Handle);
        };

#ifdef _linux_
        auto doFdatasync = [&] {
            TRequestStatsGuard statsGuard(Sensors.DataSyncSensors);
            return HandleEintr(::fdatasync, *request.Handle);
        };
#else
        auto doFdatasync = doFsync;
#endif

        NFS::ExpectIOErrors([&] {
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
        });
    }

    void DoFlushFileRange(const TFlushFileRangeRequest& request)
    {
        if (!Config_->EnableSync) {
            return;
        }

#ifdef _linux_
        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            int result = 0;
            {
                TRequestStatsGuard statsGuard(Sensors.DataSyncSensors);
                constexpr auto flags = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
                result = HandleEintr(::sync_file_range, *request.Handle, request.Offset, request.Size, flags);
            };
            if (result != 0) {
                ythrow TFileError();
            }
        });
#else

    Y_UNUSED(request);

#endif

    }
};

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

DEFINE_ENUM(EUringRequestType,
    (FlushFile)
    (Read)
    (Write)
    (Allocate)
);

class TUring
    : private TNonCopyable
{
public:
    explicit TUring(int queueSize)
    {
        auto result = HandleUringEintr(io_uring_queue_init, queueSize, &Uring_, /* flags */ 0);
        if (result < 0) {
            THROW_ERROR_EXCEPTION("Failed to initialize uring")
                << TError::FromSystem(-result);
        }

        CheckUringResult(
            io_uring_ring_dontfork(&Uring_),
            TStringBuf("io_uring_ring_dontfork"));
    }

    ~TUring()
    {
        io_uring_queue_exit(&Uring_);
    }

    TError TryRegisterBuffers(TRange<iovec> iovs)
    {
        int result = HandleUringEintr(io_uring_register_buffers, &Uring_, iovs.Begin(), iovs.Size());
        return result == 0 ? TError() : TError::FromSystem(-result);
    }

    io_uring_cqe* WaitCqe()
    {
        io_uring_cqe* cqe;
        CheckUringResult(
            HandleUringEintr(io_uring_wait_cqe, &Uring_, &cqe),
            TStringBuf("io_uring_wait_cqe"));
        return cqe;
    }

    io_uring_cqe* PeekCqe()
    {
        io_uring_cqe* cqe;
        bool result = ValidateUringNonBlockingResult(
            HandleUringEintr(io_uring_peek_cqe, &Uring_, &cqe),
            TStringBuf("io_uring_peek_cqe"));
        return result ? cqe : nullptr;
    }

    int GetSQSpaceLeft()
    {
        return io_uring_sq_space_left(&Uring_);
    }

    io_uring_sqe* TryGetSqe()
    {
        return io_uring_get_sqe(&Uring_);
    }

    void CqeSeen(io_uring_cqe* cqe)
    {
        io_uring_cqe_seen(&Uring_, cqe);
    }

    int Submit()
    {
        int count = 0;
        while (true) {
            int result = HandleUringEintr(io_uring_submit, &Uring_);
            CheckUringResult(result, TStringBuf("io_uring_submit"));
            if (result == 0) {
                break;
            }
            count += result;
        }
        return count;
    }

private:
    io_uring Uring_;

    template <class F,  class... Args>
    static auto HandleUringEintr(F f, Args&&... args) -> decltype(f(args...))
    {
        while (true) {
            auto result = f(std::forward<Args>(args)...);
            if (result != -EINTR) {
                return result;
            }
        }
    }

    static void CheckUringResult(int result, TStringBuf callName)
    {
        YT_LOG_FATAL_IF(result < 0, TError::FromSystem(-result), "Uring %Qv call failed",
            callName);
    }

    static bool ValidateUringNonBlockingResult(int result, TStringBuf callName)
    {
        if (result == -EAGAIN) {
            return false;
        }
        CheckUringResult(result, callName);
        return true;
    }
};

using TUringIovBuffer = std::array<iovec, MaxIovCountPerRequest>;

struct TUringRequest
    : public TIntrusiveLinkedListNode<TUringRequest>
{
    virtual ~TUringRequest() = 0;

    struct TRequestToNode
    {
        TIntrusiveLinkedListNode<TUringRequest>* operator() (TUringRequest* request) const {
            return request;
        }
    };

    EUringRequestType Type;
    TIOEngineSensors::TRequestSensors Sensors;

    void StartTimeTracker() {
        RequestTimeGuard_.emplace(Sensors);
    }

    void StopTimeTracker() {
        RequestTimeGuard_.reset();
    }

private:
    std::optional<TRequestStatsGuard> RequestTimeGuard_;
};

TUringRequest::~TUringRequest() { }

using TUringRequestPtr = std::unique_ptr<TUringRequest>;

template <typename TResponse>
struct TUringRequestBase
    : public TUringRequest
{
    virtual ~TUringRequestBase() = 0;

    const TPromise<TResponse> Promise = NewPromise<TResponse>();

    void TrySetSucceeded()
    {
        if (Promise.TrySet()) {
            YT_LOG_TRACE("Request succeeded (Request: %p)",
                this);
        }
    }

    void TrySetFailed(TError error)
    {
        if (Promise.TrySet(std::move(error))) {
            YT_LOG_TRACE(error, "Request failed (Request: %p)",
                this);
        }
    }

    void TrySetFailed(const io_uring_cqe* cqe)
    {
        YT_VERIFY(cqe->res < 0);
        TrySetFailed(TError::FromSystem(-cqe->res));
    }

    void TrySetFinished(const io_uring_cqe* cqe)
    {
        if (cqe->res >= 0) {
            TrySetSucceeded();
        } else {
            TrySetFailed(cqe);
        }
    }
};

template <typename TResponse>
TUringRequestBase<TResponse>::~TUringRequestBase() { }

struct TFlushFileUringRequest
    : public TUringRequestBase<void>
{
    IIOEngine::TFlushFileRequest FlushFileRequest;
};

struct TAllocateUringRequest
    : public TUringRequestBase<void>
{
    IIOEngine::TAllocateRequest AllocateRequest;
};

struct TWriteUringRequest
    : public TUringRequestBase<void>
{
    IIOEngine::TWriteRequest WriteRequest;
    int CurrentWriteSubrequestIndex = 0;
    TUringIovBuffer* WriteIovBuffer = nullptr;

    int FinishedSubrequestCount = 0;
};

struct TReadUringRequest
    : public TUringRequestBase<IIOEngine::TReadResponse>
{
    struct TReadSubrequestState
    {
        iovec Iov;
        TMutableRef Buffer;
    };

    std::vector<IIOEngine::TReadRequest> ReadSubrequests;
    TCompactVector<TReadSubrequestState, TypicalSubrequestCount> ReadSubrequestStates;
    TCompactVector<int, TypicalSubrequestCount> PendingReadSubrequestIndexes;
    TReadRequestCombiner ReadRequestCombiner;

    i64 PaddedBytesRead = 0;
    int FinishedSubrequestCount = 0;


    void TrySetReadSucceeded()
    {
        IIOEngine::TReadResponse response{
            .PaddedBytesRead = PaddedBytesRead,
            .OutputBuffers = std::move(ReadRequestCombiner.ReleaseOutputBuffers())
        };
        if (Promise.TrySet(std::move(response))) {
            YT_LOG_TRACE("Request succeeded (Request: %p)",
                this);
        }
    }
};

class TUringThreadPool
{
public:
    TUringThreadPool(
        TString threadNamePrefix,
        TUringIOEngineConfigPtr config,
        const TIOEngineSensors& sensors)
        : Config_(std::move(config))
        , ThreadNamePrefix_(std::move(threadNamePrefix))
        , Sensors(sensors)
        , Threads_(Config_->UringThreadCount)
    {
        StartThreads();
    }

    ~TUringThreadPool()
    {
        StopThreads();
    }

    void SubmitRequest(TUringRequestPtr request)
    {
        YT_LOG_TRACE("Request enqueued (Request: %v, Type: %v)",
            request.get(),
            request->Type);

        RequestQueue_.enqueue(std::move(request));

        if (!RequestNotificationHandleRaised_.exchange(true)) {
            RequestNotificationHandle_.Raise();
        }
    }

private:
    const TUringIOEngineConfigPtr Config_;
    const TString ThreadNamePrefix_;
    const TIOEngineSensors Sensors;

    // NB: -1 is reserved for LIBURING_UDATA_TIMEOUT.
    static constexpr intptr_t StopNotificationUserData = -2;
    static constexpr intptr_t RequestNotificationUserData = -3;

    class TUringThread
        : public TThread
    {
    public:
        TUringThread(TUringThreadPool* threadPool, int index)
            : TThread(Format("%v:%v", threadPool->ThreadNamePrefix_, index))
            , ThreadPool_(threadPool)
            , Config_(ThreadPool_->Config_)
            , Uring_(Config_->MaxConcurrentRequestsPerThread + UringEngineNotificationCount)
            , Sensors(ThreadPool_->Sensors)
        {
            InitIovBuffers();
            Start();
        }

    private:
        TUringThreadPool* const ThreadPool_;
        const TUringIOEngineConfigPtr Config_;

        TUring Uring_;

        // Linked list of requests that have subrequests yet to be started.
        TIntrusiveLinkedList<TUringRequest, TUringRequest::TRequestToNode> UndersubmittedRequests_;

        int PendingSubmissionsCount_ = 0;
        bool RequestNotificationReadArmed_ = false;

        TNotificationHandle StopNotificationHandle_{true};
        bool Stopping_ = false;

        std::array<TUringIovBuffer, MaxUringConcurrentRequestsPerThread + UringEngineNotificationCount> AllIovBuffers_;
        std::vector<TUringIovBuffer*> FreeIovBuffers_;

        static constexpr int StopNotificationIndex = 0;
        static constexpr int RequestNotificationIndex = 1;

        std::array<ui64, UringEngineNotificationCount> NotificationReadBuffer_;
        std::array<iovec, UringEngineNotificationCount> NotificationIov_;

        TIOEngineSensors Sensors;


        void InitIovBuffers()
        {
            FreeIovBuffers_.reserve(AllIovBuffers_.size());
            for (auto& buffer : AllIovBuffers_) {
                FreeIovBuffers_.push_back(&buffer);
            }
        }

        void StopPrologue() override
        {
            StopNotificationHandle_.Raise();
        }

        void ThreadMain() override
        {
            YT_LOG_INFO("Uring thread started");

            ArmStopNotificationRead();
            ArmRequestNotificationRead();
            SubmitSqes();

            do {
                ThreadMainStep();
            } while (!IsUringDrained());

            YT_LOG_INFO("Uring thread stopped");
        }

        bool IsUringDrained()
        {
            return Stopping_ && PendingSubmissionsCount_ == 0;
        }

        void ThreadMainStep()
        {
            auto cqe = GetCqe(true);
            while (cqe) {
                auto* userData = io_uring_cqe_get_data(&*cqe);
                if (userData == reinterpret_cast<void*>(StopNotificationUserData)) {
                    YT_VERIFY(cqe->res == sizeof(ui64));
                    HandleStop();
                } else if (userData == reinterpret_cast<void*>(RequestNotificationUserData)) {
                    YT_VERIFY(cqe->res == sizeof(ui64));
                    YT_VERIFY(RequestNotificationReadArmed_);
                    RequestNotificationReadArmed_ = false;
                } else {
                    HandleCompletion(&*cqe);
                }

                cqe = GetCqe(false);
            }

            while (UndersubmittedRequests_.GetSize() > 0 && CanHandleMoreSubmissions()) {
                YT_LOG_TRACE("Submitting extra request from undersubmitted list.");
                auto* request = UndersubmittedRequests_.GetFront();
                HandleRequest(request);
            }

            HandleSubmissions();
            SubmitSqes();
        }

        TUringRequestPtr TryDequeueRequest()
        {
            ThreadPool_->RequestNotificationHandleRaised_.store(false);

            TUringRequestPtr request;
            ThreadPool_->RequestQueue_.try_dequeue(request);
            if (!request) {
                return nullptr;
            }

            if (Stopping_) {
                YT_LOG_TRACE("Request dropped (Request: %v)",
                    request.get());
                return nullptr;
            } else {
                YT_LOG_TRACE("Request dequeued (Request: %v)",
                    request.get());
                return request;
            }
        }

        void HandleStop()
        {
            YT_LOG_INFO("Stop received by uring thread (PendingRequestCount: %v)",
                PendingSubmissionsCount_);

            YT_VERIFY(!Stopping_);
            Stopping_ = true;
        }

        bool CanHandleMoreSubmissions()
        {
            bool result = PendingSubmissionsCount_ < Config_->MaxConcurrentRequestsPerThread;

            YT_VERIFY(!result || Uring_.GetSQSpaceLeft() > 0);

            return result;
        }

        void HandleSubmissions()
        {
            while (true) {
                if (!CanHandleMoreSubmissions()) {
                    YT_LOG_TRACE("Cannot handle more submissions");
                    break;
                }

                auto request = TryDequeueRequest();
                if (!request) {
                    break;
                }

                HandleRequest(request.release());
            }

            ArmRequestNotificationRead();
        }

        void HandleRequest(TUringRequest* request)
        {
            switch (request->Type) {
                case EUringRequestType::Read:
                    HandleReadRequest(static_cast<TReadUringRequest*>(request));
                    break;
                case EUringRequestType::Write:
                    HandleWriteRequest(static_cast<TWriteUringRequest*>(request));
                    break;
                case EUringRequestType::FlushFile:
                    HandleFlushFileRequest(static_cast<TFlushFileUringRequest*>(request));
                    break;
                case EUringRequestType::Allocate:
                    HandleAllocateRequest(static_cast<TAllocateUringRequest*>(request));
                    break;
                default:
                    YT_ABORT();
            }
        }

        void HandleReadRequest(TReadUringRequest* request)
        {
            auto totalSubrequestCount = std::ssize(request->ReadSubrequests);

            YT_LOG_TRACE("Handling read request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
                request,
                request->FinishedSubrequestCount,
                totalSubrequestCount);

            if (request->Prev || UndersubmittedRequests_.GetFront() == request) {
                UndersubmittedRequests_.Remove(request);
            }

            if (request->FinishedSubrequestCount == totalSubrequestCount) {
                request->TrySetReadSucceeded();
                DisposeRequest(request);
                return;
            } else if (request->PendingReadSubrequestIndexes.empty()) {
                return;
            }

            YT_VERIFY(CanHandleMoreSubmissions());
            while (!request->PendingReadSubrequestIndexes.empty() &&
                   CanHandleMoreSubmissions())
            {
                auto subrequestIndex = request->PendingReadSubrequestIndexes.back();
                request->PendingReadSubrequestIndexes.pop_back();

                const auto& subrequest = request->ReadSubrequests[subrequestIndex];
                auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];
                auto& buffer = subrequestState.Buffer;

                auto* sqe = AllocateSqe();
                subrequestState.Iov = {
                    .iov_base = buffer.Begin(),
                    .iov_len = Min<size_t>(buffer.Size(), Config_->MaxBytesPerRead)
                };

                YT_LOG_TRACE("Submitting read operation (Request: %p/%v, FD: %v, Offset: %v, Buffer: %p@%v)",
                    request,
                    subrequestIndex,
                    static_cast<FHANDLE>(*subrequest.Handle),
                    subrequest.Offset,
                    subrequestState.Iov.iov_base,
                    subrequestState.Iov.iov_len);

                io_uring_prep_readv(
                    sqe,
                    *subrequest.Handle,
                    &subrequestState.Iov,
                    1,
                    subrequest.Offset);

                SetRequestUserData(sqe, request, subrequestIndex);
            }

            if (!request->PendingReadSubrequestIndexes.empty()) {
                UndersubmittedRequests_.PushBack(request);
            }
        }

        void HandleWriteRequest(TWriteUringRequest* request)
        {
            auto totalSubrequestCount = std::ssize(request->WriteRequest.Buffers);

            YT_LOG_TRACE("Handling write request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
                request,
                request->FinishedSubrequestCount,
                totalSubrequestCount);

            if (request->CurrentWriteSubrequestIndex == totalSubrequestCount) {
                ReleaseIovBuffer(request->WriteIovBuffer);
                request->TrySetSucceeded();
                DisposeRequest(request);
                return;
            }

            if (!request->WriteIovBuffer) {
                request->WriteIovBuffer = AllocateIovBuffer();
            }

            int iovCount = 0;
            i64 toWrite = 0;
            while (request->CurrentWriteSubrequestIndex + iovCount < totalSubrequestCount &&
                iovCount < std::ssize(*request->WriteIovBuffer) &&
                toWrite < Config_->MaxBytesPerWrite)
            {
                const auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex + iovCount];
                auto& iov = (*request->WriteIovBuffer)[iovCount];
                iov = {
                    .iov_base = const_cast<char*>(buffer.Begin()),
                    .iov_len = buffer.Size()
                };
                if (toWrite + static_cast<i64>(iov.iov_len) > Config_->MaxBytesPerWrite) {
                    iov.iov_len = Config_->MaxBytesPerWrite - toWrite;
                }
                toWrite += iov.iov_len;
                ++iovCount;
            }

            YT_LOG_TRACE("Submitting write operation (Request: %p, FD: %v, Offset: %v, Buffers: %v)",
                request,
                static_cast<FHANDLE>(*request->WriteRequest.Handle),
                request->WriteRequest.Offset,
                MakeFormattableView(
                    xrange(request->WriteIovBuffer->begin(), request->WriteIovBuffer->begin() + iovCount),
                    [] (auto* builder, const auto* iov) {
                        builder->AppendFormat("%p@%v",
                            iov->iov_base,
                            iov->iov_len);
                    }));

            auto* sqe = AllocateSqe();
            io_uring_prep_writev(
                sqe,
                *request->WriteRequest.Handle,
                &request->WriteIovBuffer->front(),
                iovCount,
                request->WriteRequest.Offset);

            SetRequestUserData(sqe, request);
        }

        static ui32 GetSyncFlags(EFlushFileMode mode)
        {
            switch (mode) {
                case EFlushFileMode::All:
                    return 0;
                case EFlushFileMode::Data:
                    return IORING_FSYNC_DATASYNC;
                default:
                    YT_ABORT();
            }
        }

        void HandleFlushFileRequest(TFlushFileUringRequest* request)
        {
            YT_LOG_TRACE("Handling flush file request (Request: %p)",
                request);

            YT_LOG_TRACE("Submitting flush file request (Request: %p, FD: %v, Mode: %v)",
                request,
                static_cast<FHANDLE>(*request->FlushFileRequest.Handle),
                request->FlushFileRequest.Mode);

            auto* sqe = AllocateSqe();
            io_uring_prep_fsync(sqe, *request->FlushFileRequest.Handle, GetSyncFlags(request->FlushFileRequest.Mode));
            SetRequestUserData(sqe, request);
        }

        void HandleAllocateRequest(TAllocateUringRequest* request)
        {
            YT_LOG_TRACE("Handling allocate request (Request: %p)",
                request);

            YT_LOG_TRACE("Submitting allocate request (Request: %p, FD: %v, Size: %v)",
                request,
                static_cast<FHANDLE>(*request->AllocateRequest.Handle),
                request->AllocateRequest.Size);

            auto* sqe = AllocateSqe();
            io_uring_prep_fallocate(sqe, *request->AllocateRequest.Handle, FALLOC_FL_CONVERT_UNWRITTEN, 0, request->AllocateRequest.Size);
            SetRequestUserData(sqe, request);
        }


        void HandleCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData<TUringRequest>(cqe);
            request->StopTimeTracker();
            switch (request->Type) {
                case EUringRequestType::Read:
                    HandleReadCompletion(cqe);
                    break;
                case EUringRequestType::Write:
                    HandleWriteCompletion(cqe);
                    break;
                case EUringRequestType::FlushFile:
                    HandleFlushFileCompletion(cqe);
                    break;
                case EUringRequestType::Allocate:
                    HandleAllocateCompletion(cqe);
                    break;
                default:
                    YT_ABORT();
            }
        }

        void HandleReadCompletion(const io_uring_cqe* cqe)
        {
            auto [request, subrequestIndex] = GetRequestUserData<TReadUringRequest>(cqe);

            YT_LOG_TRACE("Handling read completion (Request: %p/%v)",
                request,
                subrequestIndex);

            auto& subrequest = request->ReadSubrequests[subrequestIndex];
            auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];

            if (cqe->res == 0 && subrequestState.Buffer.Size() > 0) {
                auto error = request->ReadRequestCombiner.CheckEOF(subrequestState.Buffer);
                if (!error.IsOK()) {
                    YT_LOG_TRACE("Read subrequest failed at EOF (Request: %p/%v, Remaining: %v)",
                        request,
                        subrequestIndex,
                        subrequestState.Buffer.Size());
                    request->TrySetFailed(std::move(error));
                } else {
                    YT_LOG_TRACE("Read subrequest succeeded at EOF (Request: %p/%v, Remaining: %v)",
                        request,
                        subrequestIndex,
                        subrequestState.Buffer.Size());
                }
                subrequestState.Buffer = {};
                ++request->FinishedSubrequestCount;
            } else if (cqe->res < 0) {
                ++request->FinishedSubrequestCount;
                request->TrySetFailed(cqe);
            } else {
                i64 readSize = cqe->res;
                if (Config_->SimulatedMaxBytesPerRead) {
                    readSize = Min(readSize, *Config_->SimulatedMaxBytesPerRead);
                }
                Sensors.ReadBytesCounter.Increment(readSize);
                auto bufferSize = static_cast<i64>(subrequestState.Buffer.Size());
                subrequest.Offset += readSize;
                if (bufferSize == readSize) {
                    YT_LOG_TRACE("Read subrequest fully succeeded (Request: %p/%v, Size: %v)",
                        request,
                        subrequestIndex,
                        readSize);
                    subrequestState.Buffer = {};
                    ++request->FinishedSubrequestCount;
                } else {
                    YT_LOG_TRACE("Read subrequest partially succeeded (Request: %p/%v, Size: %v)",
                        request,
                        subrequestIndex,
                        readSize);
                    subrequestState.Buffer = subrequestState.Buffer.Slice(readSize, bufferSize);
                    request->PendingReadSubrequestIndexes.push_back(subrequestIndex);
                }
            }

            HandleReadRequest(request);
        }

        void HandleWriteCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData<TWriteUringRequest>(cqe);

            YT_LOG_TRACE("Handling write completion (Request: %p)",
                request);

            if (cqe->res < 0) {
                request->TrySetFailed(cqe);
                DisposeRequest(request);
                return;
            }

            i64 writtenSize = cqe->res;
            if (Config_->SimulatedMaxBytesPerWrite) {
                writtenSize = Min(writtenSize, *Config_->SimulatedMaxBytesPerWrite);
            }
            Sensors.WrittenBytesCounter.Increment(writtenSize);
            request->WriteRequest.Offset += writtenSize;

            while (writtenSize > 0) {
                auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex];
                auto bufferSize = static_cast<i64>(buffer.Size());
                if (bufferSize <= writtenSize) {
                    YT_LOG_TRACE("Write subrequest fully succeeded (Request: %p/%v, Size: %v)",
                        request,
                        request->CurrentWriteSubrequestIndex,
                        writtenSize);
                    writtenSize -= bufferSize;
                    buffer = {};
                    ++request->CurrentWriteSubrequestIndex;
                } else {
                    YT_LOG_TRACE("Write subrequest partially succeeded (Request: %p/%v, Size: %v)",
                        request,
                        request->CurrentWriteSubrequestIndex,
                        writtenSize);
                    buffer = buffer.Slice(writtenSize, bufferSize);
                    writtenSize = 0;
                }
            }

            HandleWriteRequest(request);
        }

        void HandleFlushFileCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData<TFlushFileUringRequest>(cqe);

            YT_LOG_TRACE("Handling sync completion (Request: %p)",
                request);

            request->TrySetFinished(cqe);
            DisposeRequest(request);
        }

        void HandleAllocateCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData<TAllocateUringRequest>(cqe);

            YT_LOG_TRACE("Handling allocate completion (Request: %p)",
                request);

            request->TrySetFinished(cqe);
            DisposeRequest(request);
        }


        void ArmNotificationRead(
            int notificationIndex,
            const TNotificationHandle& notificationHandle,
            intptr_t notificationUserData)
        {
            YT_VERIFY(notificationIndex >= 0);
            YT_VERIFY(notificationIndex < std::ssize(NotificationIov_));
            YT_VERIFY(notificationIndex < std::ssize(NotificationReadBuffer_));

            auto iov = &NotificationIov_[notificationIndex];
            *iov = {
                .iov_base = &NotificationReadBuffer_[notificationIndex],
                .iov_len = sizeof(NotificationReadBuffer_[notificationIndex])
            };

            auto* sqe = AllocateNonRequestSqe();
            io_uring_prep_readv(sqe, notificationHandle.GetFD(), iov, 1, 0);
            io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(notificationUserData));
        }

        void ArmStopNotificationRead()
        {
            YT_LOG_TRACE("Arming stop notification read");
            ArmNotificationRead(StopNotificationIndex, StopNotificationHandle_, StopNotificationUserData);
        }

        void ArmRequestNotificationRead()
        {
            if (!RequestNotificationReadArmed_ &&
                !Stopping_ &&
                CanHandleMoreSubmissions())
            {
                RequestNotificationReadArmed_ = true;
                YT_LOG_TRACE("Arming request notification read");
                ArmNotificationRead(RequestNotificationIndex, ThreadPool_->RequestNotificationHandle_, RequestNotificationUserData);
            }
        }

        TUringIovBuffer* AllocateIovBuffer()
        {
            YT_VERIFY(!FreeIovBuffers_.empty());
            auto* result = FreeIovBuffers_.back();
            FreeIovBuffers_.pop_back();
            return result;
        }

        void ReleaseIovBuffer(TUringIovBuffer* buffer)
        {
            if (buffer) {
                FreeIovBuffers_.push_back(buffer);
            }
        }

        std::optional<io_uring_cqe> GetCqe(bool wait)
        {
            auto* cqe = wait ? Uring_.WaitCqe() : Uring_.PeekCqe();
            if (!cqe) {
                YT_VERIFY(!wait);
                return std::nullopt;
            }
            auto userData = reinterpret_cast<intptr_t>(io_uring_cqe_get_data(cqe));
            if (userData != StopNotificationUserData && userData != RequestNotificationUserData) {
                --PendingSubmissionsCount_;
            }
            YT_VERIFY(PendingSubmissionsCount_ >= 0);
            YT_LOG_TRACE("CQE received (PendingRequestCount: %v)",
                PendingSubmissionsCount_);

            auto result = *cqe;
            Uring_.CqeSeen(cqe);
            return result;
        }

        io_uring_sqe* AllocateNonRequestSqe()
        {
            auto* sqe = Uring_.TryGetSqe();
            YT_VERIFY(sqe);
            return sqe;
        }

        io_uring_sqe* AllocateSqe()
        {
            auto* sqe = Uring_.TryGetSqe();
            YT_VERIFY(sqe);
            PendingSubmissionsCount_++;
            return sqe;
        }

        void SubmitSqes()
        {
            int count = 0;
            {
                TRequestStatsGuard statsGuard(Sensors.IoSubmitSensors);
                Uring_.Submit();
            }
            if (count > 0) {
                YT_LOG_TRACE("SQEs submitted (SqeCount: %v, PendingRequestCount: %v)",
                    count,
                    PendingSubmissionsCount_);
            }
        }

        static void SetRequestUserData(io_uring_sqe* sqe, TUringRequest* request, int subrequestIndex = 0)
        {
            request->StartTimeTracker();
            auto userData = reinterpret_cast<void*>(
                reinterpret_cast<uintptr_t>(request) |
                (static_cast<uintptr_t>(subrequestIndex) << 48));
            io_uring_sqe_set_data(sqe, userData);
        }

        template <typename TUringRequest>
        static std::tuple<TUringRequest*, int> GetRequestUserData(const io_uring_cqe* cqe)
        {
            constexpr ui64 requestMask = (1ULL << 48) - 1;
            auto userData = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
            return {
                reinterpret_cast<TUringRequest*>(userData & requestMask),
                userData >> 48
            };
        }

        void DisposeRequest(TUringRequest* request)
        {
            YT_LOG_TRACE("Request disposed (Request: %v)",
                request);
            delete request;
        }
    };

    using TUringThreadPtr = TIntrusivePtr<TUringThread>;

    std::vector<TUringThreadPtr> Threads_;

    TNotificationHandle RequestNotificationHandle_{true};
    std::atomic<bool> RequestNotificationHandleRaised_ = false;
    moodycamel::ConcurrentQueue<TUringRequestPtr> RequestQueue_;


    void StartThreads()
    {
        for (int threadIndex = 0; threadIndex < Config_->UringThreadCount; ++threadIndex) {
            Threads_[threadIndex] = New<TUringThread>(this, threadIndex);
        }
    }

    void StopThreads()
    {
        for (const auto& thread : Threads_) {
            thread->Stop();
        }
    }
};

using TUringThreadPoolPtr = std::unique_ptr<TUringThreadPool>;

class TUringIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TUringIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TUringIOEngine(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBase(
            config,
            std::move(locationId),
            std::move(profiler),
            std::move(logger))
        , Config_(std::move(config))
        , ThreadPool_(std::make_unique<TUringThreadPool>(
            Format("IOU:%v", LocationId_),
            Config_,
            Sensors))
    { }

    ~TUringIOEngine()
    {
        GetFinalizerInvoker()->Invoke(BIND([threadPool = std::move(ThreadPool_)] () mutable {
            threadPool.reset();
        }));
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory /*category*/,
        EMemoryZone memoryZone,
        TRefCountedTypeCookie tagCookie,
        TSessionId) override
    {
        if (std::ssize(requests) > MaxSubrequestCount) {
            return MakeFuture<TReadResponse>(TError("Too many read requests: %v > %v",
                requests.size(),
                MaxSubrequestCount));
        }

        auto uringRequest = std::make_unique<TReadUringRequest>();

        auto [handles, ioRequests] = uringRequest->ReadRequestCombiner.Combine(
            std::move(requests),
            Config_->DirectIOPageSize,
            memoryZone,
            tagCookie);
        YT_VERIFY(handles.size() == ioRequests.size());

        uringRequest->Type = EUringRequestType::Read;
        uringRequest->Sensors = Sensors.ReadSensors;
        uringRequest->ReadSubrequests.reserve(ioRequests.size());
        uringRequest->ReadSubrequestStates.reserve(ioRequests.size());
        uringRequest->PendingReadSubrequestIndexes.reserve(ioRequests.size());

        for (int index = 0; index < std::ssize(ioRequests); ++index) {
            const auto& ioRequest = ioRequests[index];
            uringRequest->PaddedBytesRead += GetPaddedSize(
                ioRequest.Offset,
                ioRequest.Size,
                handles[index]->IsOpenForDirectIO() ? Config_->DirectIOPageSize : DefaultPageSize);
            uringRequest->ReadSubrequests.push_back({
                .Handle = std::move(handles[index]),
                .Offset = ioRequest.Offset,
                .Size = ioRequest.Size
            });
            uringRequest->ReadSubrequestStates.push_back({
                .Buffer = ioRequest.ResultBuffer
            });
            uringRequest->PendingReadSubrequestIndexes.push_back(index);
        }

        return SubmitRequest<TReadResponse>(std::move(uringRequest));
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory /*category*/,
        TSessionId) override
    {
        auto uringRequest = std::make_unique<TWriteUringRequest>();
        uringRequest->Type = EUringRequestType::Write;
        uringRequest->Sensors = Sensors.WriteSensors;
        uringRequest->WriteRequest = std::move(request);
        return SubmitRequest<void>(std::move(uringRequest));
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory /*category*/) override
    {
        auto uringRequest = std::make_unique<TFlushFileUringRequest>();
        uringRequest->Type = EUringRequestType::FlushFile;
        uringRequest->Sensors = Sensors.SyncSensors;
        uringRequest->FlushFileRequest = std::move(request);
        return SubmitRequest<void>(std::move(uringRequest));
    }

    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest /*request*/,
        EWorkloadCategory /*category*/,
        TSessionId /*sessionId*/) override
    {
        // TODO (capone212): implement
        return VoidFuture;
    }

    TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory /*category*/) override
    {
        auto uringRequest = std::make_unique<TAllocateUringRequest>();
        uringRequest->Type = EUringRequestType::Allocate;
        uringRequest->AllocateRequest = std::move(request);
        return SubmitRequest<void>(std::move(uringRequest));
    }

private:
    const TConfigPtr Config_;

    TUringThreadPoolPtr ThreadPool_;


    template <typename TUringResponse, typename TUringRequest>
    TFuture<TUringResponse> SubmitRequest(TUringRequest request)
    {
        auto future = request->Promise.ToFuture();
        ThreadPool_->SubmitRequest(std::move(request));
        return future;
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T, typename... TParams>
IIOEnginePtr CreateIOEngine(const NYTree::INodePtr& ioConfig, TParams... params)
{
    auto config = New<typename T::TConfig>();
    config->SetDefaults();
    if (ioConfig) {
        config->Load(ioConfig);
    }

    return New<T>(std::move(config), std::forward<TParams>(params)...);
}

} // namespace

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
            return CreateIOEngine<TUringIOEngine>(
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

#ifdef _linux_

namespace {

bool IsUringIOEngineSupported()
{
    io_uring uring;
    auto result = io_uring_queue_init(1, &uring, /* flags */ 0);
    if (result < 0) {
        return false;
    }
    io_uring_queue_exit(&uring);
    return true;
}

} // namespace

#endif

std::vector<EIOEngineType> GetSupportedIOEngineTypes()
{
    std::vector<EIOEngineType> result;
    result.push_back(EIOEngineType::ThreadPool);
#ifdef _linux_
    if (IsUringIOEngineSupported()) {
        result.push_back(EIOEngineType::Uring);
    }
#endif
    result.push_back(EIOEngineType::FairShareThreadPool);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
