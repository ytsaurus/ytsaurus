#include "io_engine_base.h"

#include "huge_page_manager.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/misc/tls.h>

#include <cstring>

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

    registrar.Parameter("direct_io_block_size", &TThis::DirectIOBlockSize)
        .Alias("direct_io_page_size")
        .GreaterThan(0)
        .Default(4_KB);

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
    registrar.Parameter("use_direct_io_for_writes", &TThis::UseDirectIOForWrites)
        .Default(EDirectIOPolicy::Never);

    registrar.Parameter("total_request_limit", &TThis::TotalRequestLimit)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("write_request_limit", &TThis::WriteRequestLimit)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("read_request_limit", &TThis::ReadRequestLimit)
        .Default(std::numeric_limits<i64>::max());

    registrar.Postprocessor([] (TThis* config) {
        THROW_ERROR_EXCEPTION_IF(
            config->MaxBytesPerRead < config->DirectIOBlockSize,
            "\"max_bytes_per_read\" must be at least \"direct_io_block_size\" (MaxBytesPerRead: %v, DirectIOBlockSize: %v)",
            config->MaxBytesPerRead,
            config->DirectIOBlockSize);
        THROW_ERROR_EXCEPTION_IF(
            config->MaxBytesPerWrite < config->DirectIOBlockSize,
            "\"max_bytes_per_write\" must be at least \"direct_io_block_size\" (MaxBytesPerWrite: %v, DirectIOBlockSize: %v)",
            config->MaxBytesPerWrite,
            config->DirectIOBlockSize);
    });
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

TInflightCounter TInflightCounter::Create(TProfiler& profiler, const std::string& name)
{
    TInflightCounter counter;
    counter.State_ = New<TState>();
    profiler.AddFuncGauge(name, counter.State_, [state = counter.State_.Get()] {
        return state->Counter.load(std::memory_order::relaxed);
    });
    return counter;
}

////////////////////////////////////////////////////////////////////////////////

void TIOEngineSensors::RegisterWrittenBytes(i64 count, EWorkloadCategory category)
{
    WrittenBytesCounter[category].Increment(count);
    TotalWrittenBytesCounter.fetch_add(count, std::memory_order::relaxed);
}

void TIOEngineSensors::RegisterReadBytes(i64 count, EWorkloadCategory category)
{
    ReadBytesCounter[category].Increment(count);
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
    : TRequestStatsGuard(std::move(sensors), false)
{ }

TRequestStatsGuard::TRequestStatsGuard(TIOEngineSensors::TRequestSensors sensors, bool usingHugePages)
    : Sensors_(std::move(sensors))
    , UsingHugePages_(usingHugePages)
{
    Sensors_.Counter.Increment();
    Sensors_.InflightCounter.Increment();

    if (UsingHugePages_) {
        Sensors_.HugePageInflightCounter.Increment();
    }
}


TRequestStatsGuard::~TRequestStatsGuard()
{
    auto duration = Timer_.GetElapsedTime();
    Sensors_.Timer.Record(duration);
    Sensors_.TotalTimeCounter.Add(duration);
    Sensors_.InflightCounter.Decrement();

    if (UsingHugePages_) {
        Sensors_.HugePageTimer.Record(duration);
        Sensors_.HugePageInflightCounter.Decrement();
    }
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

TRequestCounterGuard::TRequestCounterGuard(TIntrusivePtr<TIOEngineBase> engine, EIOEngineRequestType requestType, EWorkloadCategory category)
    : Engine_(std::move(engine))
    , RequestType_(requestType)
    , Category_(category)
{
    YT_VERIFY(Engine_);

    switch (RequestType_) {
        case EIOEngineRequestType::Read:
            Engine_->InFlightReadRequestCount_.fetch_add(1);
            Engine_->Sensors_->InflightReadRequestSensors[category].Increment();
            break;
        case EIOEngineRequestType::Write:
            Engine_->InFlightWriteRequestCount_.fetch_add(1);
            Engine_->Sensors_->InflightWriteRequestSensors[category].Increment();
            break;
        default:
            YT_ABORT();
    }
}

TRequestCounterGuard::TRequestCounterGuard(TRequestCounterGuard&& other) noexcept
{
    MoveFrom(std::move(other));
}

TRequestCounterGuard::~TRequestCounterGuard()
{
    Release();
}

TRequestCounterGuard& TRequestCounterGuard::operator=(TRequestCounterGuard&& other) noexcept
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
                Engine_->Sensors_->InflightReadRequestSensors[Category_].Decrement();
                break;
            case EIOEngineRequestType::Write:
                Engine_->InFlightWriteRequestCount_.fetch_sub(1);
                Engine_->Sensors_->InflightWriteRequestSensors[Category_].Decrement();
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
    Category_ = other.Category_;

    other.Engine_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TIOEngineHandlePtr> TIOEngineBase::Open(TOpenRequest request, EWorkloadCategory category)
{
    return BIND(&TIOEngineBase::DoOpen, MakeStrong(this), std::move(request))
        .AsyncVia(NConcurrency::CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(category)))
        .Run();
}

TFuture<TCloseResponse>
TIOEngineBase::Close(TCloseRequest request, EWorkloadCategory category)
{
    auto invoker = (request.Flush || request.Size) ? FsyncInvoker_ : AuxInvoker_;
    return BIND(&TIOEngineBase::DoClose, MakeStrong(this), std::move(request), category)
        .AsyncVia(NConcurrency::CreateFixedPriorityInvoker(invoker, GetBasicPriority(category)))
        .Run();
}

TFuture<TFlushDirectoryResponse>
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

EDirectIOPolicy TIOEngineBase::UseDirectIOForWrites() const
{
    return Config_.Acquire()->UseDirectIOForWrites;
}

bool TIOEngineBase::IsInFlightRequestLimitExceeded() const
{
    return GetInFlightRequestCount() >= GetTotalRequestLimit();
}

i64 TIOEngineBase::GetInFlightRequestCount() const
{
    return GetInFlightWriteRequestCount() + GetInFlightReadRequestCount();
}

i64 TIOEngineBase::GetTotalRequestLimit() const
{
    return Config_.Acquire()->TotalRequestLimit;
}

bool TIOEngineBase::IsInFlightReadRequestLimitExceeded() const
{
    return InFlightReadRequestCount_.load(std::memory_order::relaxed) >= Config_.Acquire()->ReadRequestLimit;
}

i64 TIOEngineBase::GetInFlightReadRequestCount() const
{
    return InFlightReadRequestCount_.load(std::memory_order::relaxed);
}

i64 TIOEngineBase::GetReadRequestLimit() const
{
    return Config_.Acquire()->ReadRequestLimit;
}

bool TIOEngineBase::IsInFlightWriteRequestLimitExceeded() const
{
    return InFlightWriteRequestCount_.load(std::memory_order::relaxed) >= Config_.Acquire()->WriteRequestLimit;
}

i64 TIOEngineBase::GetInFlightWriteRequestCount() const
{
    return InFlightWriteRequestCount_.load(std::memory_order::relaxed);
}

i64 TIOEngineBase::GetWriteRequestLimit() const
{
    return Config_.Acquire()->WriteRequestLimit;
}

TIOEngineBase::TIOEngineBase(
    TConfigPtr config,
    std::string locationId,
    IHugePageManagerPtr hugePageManager,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
    : LocationId_(std::move(locationId))
    , HugePageManager_(std::move(hugePageManager))
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
            .With("mode", DecodeOpenMode(request.Mode))
            .With(TError::FromSystem());
    }
    return handle;
}


TFlushDirectoryResponse TIOEngineBase::DoFlushDirectory(const TFlushDirectoryRequest& request)
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

TCloseResponse TIOEngineBase::DoClose(const TCloseRequest& request, EWorkloadCategory category)
{
    TCloseResponse response;

    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (request.Size) {
            request.Handle->Resize(*request.Size);
        }
        if (request.Flush && StaticConfig_->EnableSync) {
            TRequestStatsGuard statsGuard(Sensors_->SyncSensors[category]);
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
                YT_TLOG_INFO("fallocate call failed; disabling FALLOC_FL_CONVERT_UNWRITTEN mode")
                    .With(TError::FromSystem());
            }
        } else {
            THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "fallocate call failed")
                .With(TError::FromSystem());
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

TSharedMutableRef TIOEngineBase::AllocateWriteBlob(
    i64 size,
    i64 directIoBlockSize)
{
    size = AlignUp(size, directIoBlockSize);

    TSharedMutableRef hugePageBlob;
    if (HugePageManager_ && HugePageManager_->IsEnabled() && HugePageManager_->GetHugePageBlobSize() >= size) {
        auto hugePageBlobReservingResult = HugePageManager_->ReserveHugePageBlob();
        if (hugePageBlobReservingResult.IsOK()) {
            hugePageBlob = hugePageBlobReservingResult.Value();
        } else {
            YT_TLOG_DEBUG("Failed to reserve huge page blob")
                .With(hugePageBlobReservingResult);
            return TSharedMutableRef::AllocateAligned(size, directIoBlockSize, {.InitializeStorage = false}, {});
        }
    } else {
        return TSharedMutableRef::AllocateAligned(size, directIoBlockSize, {.InitializeStorage = false}, {});
    }
    return hugePageBlob;
}

std::vector<TSharedRef> TIOEngineBase::PrepareDirectIOWriteBuffers(
    const std::vector<TSharedRef>& buffers,
    i64 directIoBlockSize)
{
    auto size = static_cast<i64>(GetByteSize(buffers));

    auto shouldCopy = [&] {
        if (size % directIoBlockSize != 0) {
            return true;
        }
        for (const auto& buffer : buffers) {
            if (buffer.Size() == 0) {
                continue;
            }
            if (reinterpret_cast<i64>(buffer.Begin()) % directIoBlockSize != 0 ||
                buffer.Size() % directIoBlockSize != 0)
            {
                return true;
            }
        }
        return false;
    };

    if (!shouldCopy()) {
        return buffers;
    }

    auto alignedSize = AlignUp(size, directIoBlockSize);
    auto writeBlob = AllocateWriteBlob(alignedSize, directIoBlockSize).Slice(0, alignedSize);
    auto* current = writeBlob.Begin();
    for (const auto& buffer : buffers) {
        if (buffer.Size() == 0) {
            continue;
        }
        memcpy(current, buffer.Begin(), buffer.Size());
        current += buffer.Size();
    }
    memset(current, 0, alignedSize - size);

    return {std::move(writeBlob)};
}

TSharedMutableRef TIOEngineBase::AllocateHugeBlob()
{
    TSharedMutableRef hugePageBlob;
    if (HugePageManager_ && HugePageManager_->IsEnabled()) {
        auto hugePageBlobReservingResult = HugePageManager_->ReserveHugePageBlob();
        if (hugePageBlobReservingResult.IsOK()) {
            hugePageBlob = hugePageBlobReservingResult.Value();
        } else {
            YT_TLOG_DEBUG("Failed to reserve huge page blob")
                .With(hugePageBlobReservingResult);
        }
    }
    return hugePageBlob;
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
                    .With("sick_write_wait_start", *SickWriteWaitStart_);
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
                    .With("sick_read_wait_start", *SickReadWaitStart_);
                guard.Release();
                SetSickFlag(error);
            }
        } else {
            auto guard = Guard(ReadWaitLock_);
            SickReadWaitStart_.reset();
        }
    }
}

TRequestCounterGuard TIOEngineBase::CreateInFlightRequestGuard(EIOEngineRequestType requestType, EWorkloadCategory category)
{
    return TRequestCounterGuard(MakeStrong(this), requestType, category);
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
    SickGauge_ = Profiler.Gauge("/sick");
    SickGauge_.Update(Sick_.load());

    Profiler.AddFuncCounter("/sick_events", MakeStrong(this), [this] {
        return SicknessCounter_.load();
    });

    Sensors_->KernelWrittenBytesCounter = Profiler.Counter("/kernel_written_bytes");
    Sensors_->KernelReadBytesCounter = Profiler.Counter("/kernel_read_bytes");

    auto makeRequestSensors = [] (TProfiler profiler) {
        TIOEngineSensors::TRequestSensors sensors;
        sensors.Timer = profiler.Timer("/time");
        sensors.HugePageTimer = profiler.Timer("/huge_page_time");
        sensors.TotalTimeCounter = profiler.TimeCounter("/total_time");
        sensors.Counter = profiler.Counter("/request_count");
        sensors.InflightCounter = TInflightCounter::Create(profiler, "/inflight_count");
        sensors.HugePageInflightCounter = TInflightCounter::Create(profiler, "/huge_page_inflight_count");
        return sensors;
    };

    Sensors_->IOSubmitSensors = makeRequestSensors(Profiler.WithPrefix("/uring_io_submit"));

    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        auto profilerCategory = Profiler.WithTag("category", FormatEnum(category));

        Sensors_->InflightReadRequestSensors[category] = TInflightCounter::Create(profilerCategory, "/inflight_read_request_count");
        Sensors_->InflightWriteRequestSensors[category] = TInflightCounter::Create(profilerCategory, "/inflight_write_request_count");

        Sensors_->WrittenBytesCounter[category] = profilerCategory.Counter("/written_bytes");
        Sensors_->ReadBytesCounter[category] = profilerCategory.Counter("/read_bytes");

        Sensors_->ReadSensors[category] = makeRequestSensors(profilerCategory.WithPrefix("/read"));
        Sensors_->WriteSensors[category] = makeRequestSensors(profilerCategory.WithPrefix("/write"));
        Sensors_->SyncSensors[category] = makeRequestSensors(profilerCategory.WithPrefix("/sync"));
        Sensors_->DataSyncSensors[category] = makeRequestSensors(profilerCategory.WithPrefix("/datasync"));
    }
}

void TIOEngineBase::SetSickFlag(const TError& error)
{
    auto config = Config_.Acquire();

    if (!config->SicknessExpirationTimeout) {
        return;
    }

    if (!Sick_.exchange(true)) {
        SickGauge_.Update(true);
        ++SicknessCounter_;

        TDelayedExecutor::Submit(
            BIND(&TIOEngineBase::ResetSickFlag, MakeStrong(this)),
            *config->SicknessExpirationTimeout);

        YT_TLOG_WARNING("Sick flag set")
            .With(error);
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
    SickGauge_.Update(false);

    YT_TLOG_WARNING("Sick flag reset");
}

////////////////////////////////////////////////////////////////////////////////

i64 GetPaddedSize(i64 offset, i64 size, i64 alignment)
{
    return AlignUp(offset + size, alignment) - AlignDown(offset, alignment);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
