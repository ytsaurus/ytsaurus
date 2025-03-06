#include "io_engine_base.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/misc/tls.h>

#ifdef _linux_
    #include <sys/uio.h>
#endif

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

void TIOEngineConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("aux_thread_count", &TThis::AuxThreadCount)
        .GreaterThanOrEqual(1)
        .Default(1);
    registrar.Parameter("fsync_thread_count", &TThis::FsyncThreadCount)
        .GreaterThanOrEqual(1)
        .Default(1);

    registrar.Parameter("enable_sync", &TThis::EnableSync)
        .Default(true);

    registrar.Parameter("max_bytes_per_read", &TThis::MaxBytesPerRead)
        .GreaterThanOrEqual(1)
        .Default(256_MB);
    registrar.Parameter("max_bytes_per_write", &TThis::MaxBytesPerWrite)
        .GreaterThanOrEqual(1)
        .Default(256_MB);

    registrar.Parameter("simulated_max_bytes_per_read", &TThis::SimulatedMaxBytesPerRead)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("simulated_max_bytes_per_write", &TThis::SimulatedMaxBytesPerWrite)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("sick_read_time_threshold", &TThis::SickReadTimeThreshold)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sick_read_time_window", &TThis::SickReadTimeWindow)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sick_write_time_threshold", &TThis::SickWriteTimeThreshold)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sick_write_time_window", &TThis::SickWriteTimeWindow)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sickness_expiration_timeout", &TThis::SicknessExpirationTimeout)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();

    registrar.Parameter("use_direct_io_for_reads", &TThis::UseDirectIOForReads)
        .Default(EDirectIOPolicy::Never);

    registrar.Parameter("write_request_limit", &TThis::WriteRequestLimit)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("read_request_limit", &TThis::ReadRequestLimit)
        .Default(std::numeric_limits<i64>::max());
}

////////////////////////////////////////////////////////////////////////////////

void TInflightCounter::Increment()
{
    if (!State_) {
        return;
    }
    State_->Counter.fetch_add(1, std::memory_order::relaxed);
}

void TInflightCounter::Decrement()
{
    if (!State_) {
        return;
    }
    State_->Counter.fetch_sub(1, std::memory_order::relaxed);
}

TInflightCounter TInflightCounter::Create(TProfiler& profiler, const TString& name)
{
    TInflightCounter counter;
    counter.State_ = New<TState>();
    profiler.AddFuncGauge(name, counter.State_, [state = counter.State_.Get()] {
        return state->Counter.load(std::memory_order::relaxed);
    });
    return counter;
}

////////////////////////////////////////////////////////////////////////////////

void TIOEngineSensors::RegisterWrittenBytes(i64 count)
{
    WrittenBytesCounter.Increment(count);
    TotalWrittenBytesCounter.fetch_add(count, std::memory_order::relaxed);
}

void TIOEngineSensors::RegisterReadBytes(i64 count)
{
    ReadBytesCounter.Increment(count);
    TotalReadBytesCounter.fetch_add(count, std::memory_order::relaxed);
}

YT_PREVENT_TLS_CACHING void TIOEngineSensors::UpdateKernelStatistics()
{
    constexpr auto UpdatePeriod = TDuration::Seconds(1);

    thread_local std::optional<TInstant> LastUpdateInstant;
    thread_local TTaskDiskStatistics LastStatistics;

    auto now = TInstant::Now();
    if (!LastUpdateInstant || (now - *LastUpdateInstant) > UpdatePeriod) {
        if (LastUpdateInstant) {
            auto current = GetSelfThreadTaskDiskStatistics();

            KernelReadBytesCounter.Increment(current.ReadBytes - LastStatistics.ReadBytes);
            KernelWrittenBytesCounter.Increment(current.WriteBytes - LastStatistics.WriteBytes);

            LastStatistics = current;
        }

        LastUpdateInstant = now;
    }
}

////////////////////////////////////////////////////////////////////////////////

TRequestStatsGuard::TRequestStatsGuard(TIOEngineSensors::TRequestSensors sensors)
    : Sensors_(std::move(sensors))
{
    Sensors_.Counter.Increment();
    Sensors_.InflightCounter.Increment();
}

TRequestStatsGuard::~TRequestStatsGuard()
{
    auto duration = Timer_.GetElapsedTime();
    Sensors_.Timer.Record(duration);
    Sensors_.TotalTimeCounter.Add(duration);
    Sensors_.InflightCounter.Decrement();
}

TDuration TRequestStatsGuard::GetElapsedTime() const
{
    return Timer_.GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

TRequestCounterGuard::TRequestCounterGuard()
{
    Engine_ = nullptr;
}

TRequestCounterGuard::TRequestCounterGuard(TIntrusivePtr<TIOEngineBase> engine, EIOEngineRequestType requestType)
    : Engine_(std::move(engine))
    , RequestType_(requestType)
{
    YT_VERIFY(Engine_);

    switch (RequestType_) {
        case EIOEngineRequestType::Read:
            Engine_->InFlightReadRequestCount_.fetch_add(1);
            break;
        case EIOEngineRequestType::Write:
            Engine_->InFlightWriteRequestCount_.fetch_add(1);
            break;
        default:
            YT_ABORT();
    }
}

TRequestCounterGuard::TRequestCounterGuard(TRequestCounterGuard&& other)
{
    MoveFrom(std::move(other));
}

TRequestCounterGuard::~TRequestCounterGuard()
{
    Release();
}

TRequestCounterGuard& TRequestCounterGuard::operator=(TRequestCounterGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

void TRequestCounterGuard::Release()
{
    if (Engine_) {
        switch (RequestType_) {
            case EIOEngineRequestType::Read:
                Engine_->InFlightReadRequestCount_.fetch_sub(1);
                break;
            case EIOEngineRequestType::Write:
                Engine_->InFlightWriteRequestCount_.fetch_sub(1);
                break;
            default:
                YT_ABORT();
        }

        Engine_.Reset();
    }
}

void TRequestCounterGuard::MoveFrom(TRequestCounterGuard&& other)
{
    Engine_ = other.Engine_;
    RequestType_ = other.RequestType_;

    other.Engine_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TIOEngineHandlePtr> TIOEngineBase::Open(TOpenRequest request, EWorkloadCategory category)
{
    return BIND(&TIOEngineBase::DoOpen, MakeStrong(this), std::move(request))
        .AsyncVia(NConcurrency::CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
        .Run();
}

TFuture<IIOEngine::TCloseResponse>
TIOEngineBase::Close(TCloseRequest request, EWorkloadCategory category)
{
    auto invoker = (request.Flush || request.Size) ? FsyncInvoker_ : AuxInvoker_;
    return BIND(&TIOEngineBase::DoClose, MakeStrong(this), std::move(request))
        .AsyncVia(NConcurrency::CreateFixedPriorityInvoker(invoker, GetBasicPriority(category)))
        .Run();
}

TFuture<IIOEngine::TFlushDirectoryResponse>
TIOEngineBase::FlushDirectory(TFlushDirectoryRequest request, EWorkloadCategory category)
{
    return BIND(&TIOEngineBase::DoFlushDirectory, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(FsyncInvoker_, GetBasicPriority(category)))
        .Run();
}

TFuture<void> TIOEngineBase::Allocate(TAllocateRequest request, EWorkloadCategory category)
{
    return BIND(&TIOEngineBase::DoAllocate, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
        .Run();
}

TFuture<void> TIOEngineBase::Lock(TLockRequest request, EWorkloadCategory category)
{
    return BIND(&TIOEngineBase::DoLock, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
        .Run();
}

TFuture<void> TIOEngineBase::Resize(TResizeRequest request, EWorkloadCategory category)
{
    return BIND(&TIOEngineBase::DoResize, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
        .Run();
}

bool TIOEngineBase::IsSick() const
{
    return Sick_;
}

const IInvokerPtr& TIOEngineBase::GetAuxPoolInvoker()
{
    return AuxThreadPool_->GetInvoker();
}

i64 TIOEngineBase::GetTotalReadBytes() const
{
    return Sensors_->TotalReadBytesCounter.load(std::memory_order::relaxed);
}

i64 TIOEngineBase::GetTotalWrittenBytes() const
{
    return Sensors_->TotalWrittenBytesCounter.load(std::memory_order::relaxed);
}

EDirectIOPolicy TIOEngineBase::UseDirectIOForReads() const
{
    return Config_.Acquire()->UseDirectIOForReads;
}

bool TIOEngineBase::IsInFlightReadRequestLimitExceeded() const
{
    return InFlightReadRequestCount_.load(std::memory_order_relaxed) >= Config_.Acquire()->ReadRequestLimit;
}

i64 TIOEngineBase::GetInFlightReadRequestCount() const
{
    return InFlightReadRequestCount_.load(std::memory_order_relaxed);
}

i64 TIOEngineBase::GetReadRequestLimit() const
{
    return Config_.Acquire()->ReadRequestLimit;
}

bool TIOEngineBase::IsInFlightWriteRequestLimitExceeded() const
{
    return InFlightWriteRequestCount_.load(std::memory_order_relaxed) >= Config_.Acquire()->WriteRequestLimit;
}

i64 TIOEngineBase::GetInFlightWriteRequestCount() const
{
    return InFlightWriteRequestCount_.load(std::memory_order_relaxed);
}

i64 TIOEngineBase::GetWriteRequestLimit() const
{
    return Config_.Acquire()->WriteRequestLimit;
}

TIOEngineBase::TIOEngineBase(
    TConfigPtr config,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
    : LocationId_(std::move(locationId))
    , Logger(std::move(logger))
    , Profiler(std::move(profiler))
    , StaticConfig_(std::move(config))
    , Config_(StaticConfig_)
    , AuxThreadPool_(CreateThreadPool(StaticConfig_->AuxThreadCount, Format("IOA:%v", LocationId_)))
    , FsyncThreadPool_(CreateThreadPool(StaticConfig_->FsyncThreadCount, Format("IOS:%v", LocationId_)))
    , AuxInvoker_(CreatePrioritizedInvoker(AuxThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "io_engine_base_aux"}, {"location_id", LocationId_}})))
    , FsyncInvoker_(CreatePrioritizedInvoker(FsyncThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "io_engine_base_fsync"}, {"location_id", LocationId_}})))
{
    InitProfilerSensors();
}

TIOEngineHandlePtr TIOEngineBase::DoOpen(const TOpenRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    auto handle = [&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        return New<TIOEngineHandle>(request.Path, request.Mode);
    }();
    if (!handle->IsOpen()) {
        THROW_ERROR_EXCEPTION(
            "Cannot open %v",
            request.Path)
            << TErrorAttribute("mode", DecodeOpenMode(request.Mode))
            << TError::FromSystem();
    }
    return handle;
}


IIOEngine::TFlushDirectoryResponse
TIOEngineBase::DoFlushDirectory(const TFlushDirectoryRequest& request)
{
    TFlushDirectoryResponse response;

    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (StaticConfig_->EnableSync) {
            NFS::FlushDirectory(request.Path);
            response.IOSyncRequests = 1;
        }
    });

    return response;
}

IIOEngine::TCloseResponse TIOEngineBase::DoClose(const TCloseRequest& request)
{
    TCloseResponse response;

    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (request.Size) {
            request.Handle->Resize(*request.Size);
        }
        if (request.Flush && StaticConfig_->EnableSync) {
            TRequestStatsGuard statsGuard(Sensors_->SyncSensors);
            request.Handle->Flush();
            response.IOSyncRequests = 1;
        }
        request.Handle->Close();
    });

    return response;
}

void TIOEngineBase::DoAllocate(const TAllocateRequest& request)
{
    Sensors_->UpdateKernelStatistics();
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

int TIOEngineBase::GetLockOp(ELockFileMode mode)
{
    switch (mode) {
        case ELockFileMode::Shared:
            return LOCK_SH;
        case ELockFileMode::Exclusive:
            return LOCK_EX;
        case ELockFileMode::Unlock:
            return LOCK_UN;
        default:
            YT_ABORT();
    }
}

void TIOEngineBase::DoLock(const TLockRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        auto op = GetLockOp(request.Mode) + (request.Nonblocking ? LOCK_NB : 0);
        if (HandleEintr(::flock, *request.Handle, op) != 0) {
            ythrow TFileError();
        }
    });
}

void TIOEngineBase::DoResize(const TResizeRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        if (!request.Handle->Resize(request.Size)) {
            ythrow TFileError();
        }
    });
}

void TIOEngineBase::AddWriteWaitTimeSample(TDuration duration)
{
    auto config = Config_.Acquire();
    if (config->SickWriteTimeThreshold && config->SickWriteTimeWindow && config->SicknessExpirationTimeout && !Sick_) {
        if (duration > *config->SickWriteTimeThreshold) {
            auto now = GetInstant();
            auto guard = Guard(WriteWaitLock_);
            if (!SickWriteWaitStart_) {
                SickWriteWaitStart_ = now;
            } else if (now - *SickWriteWaitStart_ > *config->SickWriteTimeWindow) {
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

void TIOEngineBase::AddReadWaitTimeSample(TDuration duration)
{
    auto config = Config_.Acquire();
    if (config->SickReadTimeThreshold && config->SickReadTimeWindow && config->SicknessExpirationTimeout && !Sick_) {
        if (duration > *config->SickReadTimeThreshold) {
            auto now = GetInstant();
            auto guard = Guard(ReadWaitLock_);
            if (!SickReadWaitStart_) {
                SickReadWaitStart_ = now;
            } else if (now - *SickReadWaitStart_ > *config->SickReadTimeWindow) {
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

TRequestCounterGuard TIOEngineBase::CreateInFlightRequestGuard(EIOEngineRequestType requestType)
{
    return TRequestCounterGuard(MakeStrong(this), requestType);
}

void TIOEngineBase::Reconfigure(const NYTree::INodePtr& node)
{
    auto realConfig = NYTree::UpdateYsonStruct(StaticConfig_, node);

    AuxThreadPool_->SetThreadCount(realConfig->AuxThreadCount);
    FsyncThreadPool_->SetThreadCount(realConfig->FsyncThreadCount);

    Config_.Store(realConfig);

    DoReconfigure(node);
}

void TIOEngineBase::InitProfilerSensors()
{
    Profiler.AddFuncGauge("/sick", MakeStrong(this), [this] {
        return Sick_.load();
    });

    Profiler.AddFuncGauge("/alive", MakeStrong(this), [] {
        return 1;
    });

    Profiler.AddFuncCounter("/sick_events", MakeStrong(this), [this] {
        return SicknessCounter_.load();
    });

    Sensors_->WrittenBytesCounter = Profiler.Counter("/written_bytes");
    Sensors_->ReadBytesCounter = Profiler.Counter("/read_bytes");

    Sensors_->KernelWrittenBytesCounter = Profiler.Counter("/kernel_written_bytes");
    Sensors_->KernelReadBytesCounter = Profiler.Counter("/kernel_read_bytes");

    auto makeRequestSensors = [] (TProfiler profiler) {
        TIOEngineSensors::TRequestSensors sensors;
        sensors.Timer = profiler.Timer("/time");
        sensors.TotalTimeCounter = profiler.TimeCounter("/total_time");
        sensors.Counter = profiler.Counter("/request_count");
        sensors.InflightCounter = TInflightCounter::Create(profiler, "/inflight_count");
        return sensors;
    };

    Sensors_->ReadSensors = makeRequestSensors(Profiler.WithPrefix("/read"));
    Sensors_->WriteSensors = makeRequestSensors(Profiler.WithPrefix("/write"));
    Sensors_->SyncSensors = makeRequestSensors(Profiler.WithPrefix("/sync"));
    Sensors_->DataSyncSensors = makeRequestSensors(Profiler.WithPrefix("/datasync"));
    Sensors_->IOSubmitSensors = makeRequestSensors(Profiler.WithPrefix("/uring_io_submit"));
}

void TIOEngineBase::SetSickFlag(const TError& error)
{
    auto config = Config_.Acquire();

    if (!config->SicknessExpirationTimeout) {
        return;
    }

    if (!Sick_.exchange(true)) {
        ++SicknessCounter_;

        TDelayedExecutor::Submit(
            BIND(&TIOEngineBase::ResetSickFlag, MakeStrong(this)),
            *config->SicknessExpirationTimeout);

        YT_LOG_WARNING(error, "Sick flag set");
    }
}

void TIOEngineBase::ResetSickFlag()
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

////////////////////////////////////////////////////////////////////////////////

i64 GetPaddedSize(i64 offset, i64 size, i64 alignment)
{
    return AlignUp(offset + size, alignment) - AlignDown(offset, alignment);
}

////////////////////////////////////////////////////////////////////////////////

void TIOEngineBaseCommonConfig::Register(TRegistrar registrar)
{
        registrar.Parameter("enable_pwritev", &TThis::EnablePwritev)
            .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TIOEngineBaseCommon::TIOEngineBaseCommon(
    TConfigPtr config,
    TString locationId,
    TProfiler profiler,
    NLogging::TLogger logger)
    : TIOEngineBase(
        config,
        std::move(locationId),
        std::move(profiler),
        std::move(logger))
{ }

std::vector<TSharedMutableRef> TIOEngineBaseCommon::AllocateReadBuffers(
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

TCommonReadResponse TIOEngineBaseCommon::DoRead(
    const TReadRequest& request,
    TSharedMutableRef buffer,
    TWallTimer timer,
    EWorkloadCategory category,
    TSessionId sessionId,
    TRequestCounterGuard requestCounterGuard)
{
    YT_VERIFY(std::ssize(buffer) == request.Size);

    Y_UNUSED(requestCounterGuard);

    const auto readWaitTime = timer.GetElapsedTime();
    AddReadWaitTimeSample(readWaitTime);
    Sensors_->UpdateKernelStatistics();

    auto toReadRemaining = std::ssize(buffer);
    auto fileOffset = request.Offset;
    i64 bufferOffset = 0;

    YT_LOG_DEBUG_IF(category == EWorkloadCategory::UserInteractive,
        "Started reading from disk (Handle: %v, RequestSize: %v, ReadSessionId: %v, ReadWaitTime: %v)",
        static_cast<FHANDLE>(*request.Handle),
        request.Size,
        sessionId,
        readWaitTime);

    TCommonReadResponse response;

    NFS::WrapIOErrors([&] {
        auto config = Config_.Acquire();

        while (toReadRemaining > 0) {
            auto toRead = static_cast<ui32>(Min(toReadRemaining, config->MaxBytesPerRead));

            i64 reallyRead;
            {
                TRequestStatsGuard statsGuard(Sensors_->ReadSensors);
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

            Sensors_->RegisterReadBytes(reallyRead);
            if (StaticConfig_->SimulatedMaxBytesPerRead) {
                reallyRead = Min(reallyRead, *StaticConfig_->SimulatedMaxBytesPerRead);
            }

            fileOffset += reallyRead;
            bufferOffset += reallyRead;
            toReadRemaining -= reallyRead;
        }
    });

    if (toReadRemaining > 0) {
        THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request")
            << TErrorAttribute("to_read_remaining", toReadRemaining)
            << TErrorAttribute("max_bytes_per_read", StaticConfig_->MaxBytesPerRead)
            << TErrorAttribute("request_size", request.Size)
            << TErrorAttribute("request_offset", request.Offset)
            << TErrorAttribute("file_size", request.Handle->GetLength())
            << TErrorAttribute("handle", static_cast<FHANDLE>(*request.Handle));
    }

    return response;
}

TIOEngineBase::TWriteResponse TIOEngineBaseCommon::DoWrite(
    const TWriteRequest& request,
    TWallTimer timer)
{
    AddWriteWaitTimeSample(timer.GetElapsedTime());
    Sensors_->UpdateKernelStatistics();

    auto fileOffset = request.Offset;

    TWriteResponse response;

    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;

        auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Buffers));

        int bufferIndex = 0;
        i64 bufferOffset = 0; // within current buffer

        auto config = Config_.Acquire();
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
                        toWrite < config->MaxBytesPerWrite)
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
                    if (toWrite + static_cast<i64>(iovPart.iov_len) > config->MaxBytesPerWrite) {
                        iovPart.iov_len = config->MaxBytesPerWrite - toWrite;
                    }
                    toWrite += iovPart.iov_len;
                    ++iovCount;
                }

                i64 reallyWritten;
                {
                    TRequestStatsGuard statsGuard(Sensors_->WriteSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyWritten = HandleEintr(::pwritev, *request.Handle, iov.data(), iovCount, fileOffset);
                }

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                Sensors_->RegisterWrittenBytes(reallyWritten);
                if (StaticConfig_->SimulatedMaxBytesPerWrite) {
                    reallyWritten = Min(reallyWritten, *StaticConfig_->SimulatedMaxBytesPerWrite);
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
                auto toWrite = static_cast<ui32>(Min(toWriteRemaining, config->MaxBytesPerWrite, std::ssize(buffer) - bufferOffset));

                i32 reallyWritten;
                {
                    TRequestStatsGuard statsGuard(Sensors_->WriteSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyWritten = HandleEintr(::pwrite, *request.Handle, const_cast<char*>(buffer.Begin()) + bufferOffset, toWrite, fileOffset);
                }

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                Sensors_->RegisterWrittenBytes(reallyWritten);
                fileOffset += reallyWritten;
                bufferOffset += reallyWritten;
                toWriteRemaining -= reallyWritten;
                if (bufferOffset == std::ssize(buffer)) {
                    ++bufferIndex;
                    bufferOffset = 0;
                }
            };

            if (config->EnablePwritev && isPwritevSupported()) {
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

TIOEngineBase::TFlushFileResponse TIOEngineBaseCommon::DoFlushFile(const TFlushFileRequest& request)
{
    TFlushFileResponse response;

    Sensors_->UpdateKernelStatistics();
    if (!StaticConfig_->EnableSync) {
        return response;
    }

    auto doFsync = [&] {
        TRequestStatsGuard statsGuard(Sensors_->SyncSensors);
        return HandleEintr(::fsync, *request.Handle);
    };

#ifdef _linux_
    auto doFdatasync = [&] {
        TRequestStatsGuard statsGuard(Sensors_->DataSyncSensors);
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

TIOEngineBase::TFlushFileRangeResponse TIOEngineBaseCommon::DoFlushFileRange(const TFlushFileRangeRequest& request)
{
    TFlushFileRangeResponse response;

    Sensors_->UpdateKernelStatistics();
    if (!StaticConfig_->EnableSync) {
        return response;
    }

#ifdef _linux_
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        int result = 0;
        {
            TRequestStatsGuard statsGuard(Sensors_->DataSyncSensors);
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

// This should be called by derived classes in their DoReconfigure method.
void TIOEngineBaseCommon::DoReconfigure(const NYTree::INodePtr& node)
{
    auto config = UpdateYsonStruct(StaticConfig_, node);
    Config_.Store(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

