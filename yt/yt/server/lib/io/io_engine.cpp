#include "io_engine.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/notification_handle.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

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

////////////////////////////////////////////////////////////////////////////////

static constexpr auto PageSize = 4_KBs;

#ifdef _linux_

static constexpr auto MaxIovCountPerRequest = 64;
static constexpr auto MaxUringQueueSize = 32;

// See SetRequestUserData/GetRequestUserData.
static constexpr auto MaxSubrequestCount = 1 << 16;
static constexpr auto TypicalSubrequestCount = 64;

#endif

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const auto& Logger = IOLogger;

#endif

////////////////////////////////////////////////////////////////////////////////

TFuture<TSharedMutableRef> IIOEngine::ReadAll(
    const TString& path,
    i64 priority)
{
    return Open({path, OpenExisting | RdOnly | Seq | CloseOnExec}, priority)
        .Apply(BIND([=, this_ = MakeStrong(this)] (const std::shared_ptr<TFileHandle>& handle) {
            struct TReadAllBufferTag
            { };
            auto buffer = TSharedMutableRef::Allocate<TReadAllBufferTag>(handle->GetLength(), false);
            return Read({{*handle, 0, buffer}}, priority)
                .Apply(BIND([=, this_ = MakeStrong(this), handle = handle, buffer = buffer] {
                    return Close({handle}, priority)
                        .Apply(BIND([buffer] {
                            return buffer;
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
    }
};

DEFINE_REFCOUNTED_TYPE(TThreadPoolIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUringIOEngineConfig)

class TUringIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int UringThreadCount;
    int UringQueueSize;

    //! Limits the number of concurrent (outstanding) IO requests per a single uring thread.
    int MaxConcurrentRequestsPerThread;

    //! A read request may indicate that DirectIO is required to serve it;
    //! see #IIOEngine::TReadRequest::UseDirectIO. In particular, the file
    //! must be opened with O_DIRECT flag. If, however, #IIOEngine::TReadRequest::Data
    //! is not page-aligned and is smaller than #LargeUnalignedDirectIOReadSize,
    //! the engine will use an internal pooled buffer of size
    //! #PooledDirectIOReadBufferSize to serve the read and copy the data.
    //! This parameter must be divisible by page size.
    //! The total memory footprint is
    //! |UringThreadCount * MaxConcurrentRequestsPerThread * PooledDirectIOReadBufferSize|.
    i64 PooledDirectIOReadBufferSize;

    //! See above. In case of an unaligned read of size at least #LargeUnalignedDirectIOReadSize
    //! the engine will allocate an aligned buffer, issue a read request into it, and then
    //! copy the data back to the user's buffer.
    i64 LargeUnalignedDirectIOReadSize;

    TUringIOEngineConfig()
    {
        RegisterParameter("uring_thread_count", UringThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("uring_queue_size", UringQueueSize)
            .GreaterThan(0)
            .Default(16);
        RegisterParameter("max_concurrent_requests_per_thread", MaxConcurrentRequestsPerThread)
            .GreaterThan(0)
            .Default(16);

        RegisterParameter("pooled_direct_io_read_buffer_size", PooledDirectIOReadBufferSize)
            .GreaterThan(0)
            .Default(16_KB);
        RegisterParameter("large_unaligned_direct_io_read_size", LargeUnalignedDirectIOReadSize)
            .GreaterThan(0)
            .Default(64_KB);

        RegisterPostprocessor([&] {
            if (PooledDirectIOReadBufferSize % PageSize != 0) {
                THROW_ERROR_EXCEPTION("\"direct_io_read_buffer_size\" must be divisible by page size");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TUringIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TIOEngineBase
    : public IIOEngine
{
public:
    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        TOpenRequest request,
        i64 priority) override
    {
        return BIND(&TIOEngineBase::DoOpen, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Close(
        TCloseRequest request,
        i64 priority)
    {
        return BIND(&TIOEngineBase::DoClose, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        i64 priority)
    {
        return BIND(&TIOEngineBase::DoFlushDirectory, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Allocate(
        TAllocateRequest request,
        i64 priority) override
    {
        return BIND(&TIOEngineBase::DoAllocate, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, priority))
            .Run();
    }

    virtual bool IsSick() const override
    {
        return Sick_;
    }

    virtual const IInvokerPtr& GetAuxPoolInvoker() override
    {
        return AuxThreadPool_->GetInvoker();
    }

protected:
    using TConfig = TIOEngineConfigBase;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    const TString LocationId_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;

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
        , AuxInvoker_(CreatePrioritizedInvoker(AuxThreadPool_->GetInvoker()))
    {
        Profiler.AddFuncGauge("/sick", MakeStrong(this), [this] {
            return Sick_.load();
        });

        Profiler.AddFuncGauge("/sick_events", MakeStrong(this), [this] {
            return SicknessCounter_.load();
        });
    }

    std::shared_ptr<TFileHandle> DoOpen(const TOpenRequest& request)
    {
        std::shared_ptr<TFileHandle> handle;
        {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            handle = std::make_shared<TFileHandle>(request.Path, request.Mode);
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
        if (HandleEintr(::fallocate, request.Handle, FALLOC_FL_CONVERT_UNWRITTEN, 0, request.Size) != 0) {
            YT_LOG_WARNING(TError::FromSystem(), "fallocate call failed");
        }
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
    const IPrioritizedInvokerPtr AuxInvoker_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, ReadWaitLock_);
    std::optional<TInstant> SickReadWaitStart_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, WriteWaitLock_);
    std::optional<TInstant> SickWriteWaitStart_;

    std::atomic<bool> Sick_ = false;
    std::atomic<i64> SicknessCounter_ = 0;

    NProfiling::TGauge SickGauge_;
    NProfiling::TGauge SickEventsGauge_;


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
        , ReadThreadPool_(New<TThreadPool>(Config_->ReadThreadCount, Format("IOR:%v", LocationId_)))
        , WriteThreadPool_(New<TThreadPool>(Config_->WriteThreadCount, Format("IOW:%v", LocationId_)))
        , ReadInvoker_(CreatePrioritizedInvoker(ReadThreadPool_->GetInvoker()))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker()))
        , PreadTimer_(Profiler.Timer("/pread_time"))
        , PwriteTimer_(Profiler.Timer("/pwrite_time"))
        , FdatasyncTimer_(Profiler.Timer("/fdatasync_time"))
        , FsyncTimer_(Profiler.Timer("/fsync_time"))
    { }

    virtual TFuture<void> Read(
        std::vector<TReadRequest> requests,
        i64 priority) override
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(requests.size());
        auto invoker = CreateFixedPriorityInvoker(ReadInvoker_, priority);
        for (auto&& request : requests) {
            futures.push_back(BIND(&TThreadPoolIOEngine::DoRead, MakeStrong(this), std::move(request), TWallTimer())
                .AsyncVia(invoker)
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

    virtual TFuture<void> Write(
        TWriteRequest request,
        i64 priority) override
    {
        return BIND(&TThreadPoolIOEngine::DoWrite, MakeStrong(this), std::move(request), TWallTimer())
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> FlushFile(
        TFlushFileRequest request,
        i64 priority) override
    {
        return BIND(&TThreadPoolIOEngine::DoFlushFile, MakeStrong(this), std::move(request))
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

private:
    const TConfigPtr Config_;
    const TThreadPoolPtr ReadThreadPool_;
    const TThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr ReadInvoker_;
    const IPrioritizedInvokerPtr WriteInvoker_;

    NProfiling::TEventTimer PreadTimer_;
    NProfiling::TEventTimer PwriteTimer_;
    NProfiling::TEventTimer FdatasyncTimer_;
    NProfiling::TEventTimer FsyncTimer_;


    void DoRead(
        const TReadRequest& request,
        TWallTimer timer)
    {
        AddReadWaitTimeSample(timer.GetElapsedTime());

        auto toReadRemaining = static_cast<i64>(request.Buffer.Size());
        auto fileOffset = request.Offset;
        i64 bufferOffset = 0;

        NFS::ExpectIOErrors([&] {
            while (toReadRemaining > 0) {
                auto toRead = static_cast<ui32>(Min(toReadRemaining, Config_->MaxBytesPerRead));

                i64 reallyRead;
                {
                    TEventTimer eventTimer(PreadTimer_);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyRead = HandleEintr(::pread, request.Handle, request.Buffer.Begin() + bufferOffset, toRead, fileOffset);
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
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Buffers));

            int bufferIndex = 0;
            auto fileOffset = request.Offset;
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
                    while (bufferIndex + iovCount < static_cast<int>(request.Buffers.size()) &&
                           iovCount < iov.size() &&
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
                        if (toWrite + iovPart.iov_len > Config_->MaxBytesPerWrite) {
                            iovPart.iov_len = Config_->MaxBytesPerWrite - toWrite;
                        }
                        toWrite += iovPart.iov_len;
                        ++iovCount;
                    }

                    i64 reallyWritten;
                    {
                        TEventTimer eventTimer(PwriteTimer_);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwritev, request.Handle, iov.data(), iovCount, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

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
                        if (bufferOffset == buffer.Size()) {
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
                        TEventTimer timer(PwriteTimer_);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwrite, request.Handle, const_cast<char*>(buffer.Begin()) + bufferOffset, toWrite, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    fileOffset += reallyWritten;
                    bufferOffset += reallyWritten;
                    toWriteRemaining -= reallyWritten;
                    if (bufferOffset == buffer.Size()) {
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
    }

    void DoFlushFile(const TFlushFileRequest& request)
    {
        if (Config_->EnableSync) {
            return;
        }

        auto doFsync = [&] {
            TEventTimer timer(FsyncTimer_);
            return HandleEintr(::fsync, request.Handle);
        };

#ifdef _linux_
        auto doFdatasync = [&] {
            TEventTimer timer(FdatasyncTimer_);
            return HandleEintr(::fdatasync, request.Handle);
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
};

struct TUringRequest
{
    using TReadSubrequest = IIOEngine::TReadRequest;

    struct TReadSubrequestState
    {
        // Owning for large buffers.
        // Non-owning for pooled buffers.
        TSharedMutableRef DirectIOBuffer;

        bool IsDirectIOBufferPooled() const
        {
            return DirectIOBuffer && !DirectIOBuffer.GetHolder();
        }

        bool IsDirectIOBufferLarge() const
        {
            return DirectIOBuffer && DirectIOBuffer.GetHolder();
        }
    };

    const TPromise<void> Promise = NewPromise<void>();

    EUringRequestType Type;

    // EUringRequestType::FlushFile
    IIOEngine::TFlushFileRequest FlushFileRequest;

    // EUringRequestType::Allocate
    IIOEngine::TAllocateRequest AllocateRequest;

    // EUringRequestType::Read
    std::vector<TReadSubrequest> ReadSubrequests;
    SmallVector<TReadSubrequestState, TypicalSubrequestCount> ReadSubrequestStates;
    SmallVector<int, TypicalSubrequestCount> PendingReadSubrequestIndexes;

    // EUringRequestType::Write
    IIOEngine::TWriteRequest WriteRequest;
    int CurrentWriteSubrequestIndex = 0;

    // EUringRequestType::Read
    // EUringRequestType::Write
    int FinishedSubrequestCount = 0;
};

using TUringRequestPtr = std::unique_ptr<TUringRequest>;

class TUringThreadPool
{
public:
    TUringThreadPool(
        TString threadNamePrefix,
        TUringIOEngineConfigPtr config)
        : Config_(std::move(config))
        , ThreadNamePrefix_(std::move(threadNamePrefix))
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

    // NB: -1 is reserved for LIBURING_UDATA_TIMEOUT.
    static constexpr intptr_t StopNotificationUserData = -2;
    static constexpr intptr_t RequestNotificationUserData = -3;

    struct TPooledDirectIOReadBufferTag
    { };

    struct TLargeDirectIOReadBufferTag
    { };

    class TUringThread
    {
    public:
        TUringThread(TUringThreadPool* threadPool, int index)
            : ThreadPool_(threadPool)
            , Index_(index)
            , Config_(ThreadPool_->Config_)
            , Thread_(std::make_unique<TThread>(&StaticThreadMain, this))
            , PooledDirectIOReadBuffers_(TSharedMutableRef::AllocatePageAligned<TPooledDirectIOReadBufferTag>(
                Config_->MaxConcurrentRequestsPerThread * Config_->PooledDirectIOReadBufferSize,
                false))
            , Uring_(Config_->UringQueueSize)
        {
            RegisterDirectIOBuffersWithUring();
            PopulateSpareDirectIOBuffers();
        }

        void Start()
        {
            Thread_->Start();
            StartedEvent_.Wait();
        }

        void Stop()
        {
            StopNotificationHandle_.Raise();
            Thread_->Join();
        }

    private:
        TUringThreadPool* const ThreadPool_;
        const int Index_;

        const TUringIOEngineConfigPtr Config_;
        const std::unique_ptr<TThread> Thread_;
        const TSharedMutableRef PooledDirectIOReadBuffers_;

        TUring Uring_;

        TEvent StartedEvent_;

        int PendingRequestCount_ = 0;
        bool SubmissionNotificationReadArmed_ = false;

        TNotificationHandle StopNotificationHandle_{true};
        bool Stopping_ = false;

        std::array<iovec, MaxUringQueueSize * MaxIovCountPerRequest> Iovs_;
        int UsedIovCount_ = 0;

        int PendingIORequestCount_ = 0;

        bool PooledDirectIOReadBufferRegistered_ = false;
        std::vector<TMutableRef> SparePooledDirectIOReadBuffers_;

        ui64 NotificationReadBuffer_;


        void RegisterDirectIOBuffersWithUring()
        {
            std::array<iovec, 1> iovs{
                iovec{
                    .iov_base = PooledDirectIOReadBuffers_.Begin(),
                    .iov_len = PooledDirectIOReadBuffers_.Size()
                }
            };
            auto error = Uring_.TryRegisterBuffers(MakeRange(iovs));
            if (error.IsOK()) {
                PooledDirectIOReadBufferRegistered_ = true;
            } else {
                YT_LOG_ERROR(error, "Failed to register direct IO read buffers");
            }
        }

        void PopulateSpareDirectIOBuffers()
        {
            SparePooledDirectIOReadBuffers_.reserve(Config_->MaxConcurrentRequestsPerThread);
            for (int bufferIndex = 0; bufferIndex < Config_->MaxConcurrentRequestsPerThread; ++bufferIndex) {
                SparePooledDirectIOReadBuffers_.emplace_back(
                    PooledDirectIOReadBuffers_.Begin() + bufferIndex * Config_->PooledDirectIOReadBufferSize,
                    Config_->PooledDirectIOReadBufferSize);
            }
        }


        static void* StaticThreadMain(void* opaqueThread)
        {
            auto* thread = static_cast<TUringThread*>(opaqueThread);
            thread->ThreadMain();
            return nullptr;
        }

        void ThreadMain()
        {
            TThread::SetCurrentThreadName(MakeThreadName().c_str());

            YT_LOG_INFO("Uring thread started");

            StartedEvent_.NotifyOne();

            ArmStopNotificationRead();
            ArmSubmissionNotificationRead();
            SubmitSqes();

            do {
                ThreadMainStep();
            } while (!IsUringDrained());

            YT_LOG_INFO("Uring thread stopped");
        }

        bool IsUringDrained()
        {
            return
                Stopping_ &&
                PendingRequestCount_ == (SubmissionNotificationReadArmed_ ? 1 : 0);
        }

        void ThreadMainStep()
        {
            auto* cqe = WaitForCqe();

            ResetIovs();

            auto* userData = io_uring_cqe_get_data(cqe);
            if (userData == reinterpret_cast<void*>(StopNotificationUserData)) {
                YT_VERIFY(cqe->res == sizeof(ui64));
                HandleStop();
            } else if (userData == reinterpret_cast<void*>(RequestNotificationUserData)) {
                YT_VERIFY(cqe->res == sizeof(ui64));
                YT_VERIFY(SubmissionNotificationReadArmed_);
                SubmissionNotificationReadArmed_ = false;
            } else {
                HandleCompletion(cqe);
            }

            Uring_.CqeSeen(cqe);

            HandleSubmissions();

            SubmitSqes();
        }

        std::unique_ptr<TUringRequest> TryDequeueRequest()
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
                PendingRequestCount_);

            YT_VERIFY(!Stopping_);
            Stopping_ = true;
        }

        bool CanHandleMoreSubmissions()
        {
            return
                Uring_.GetSQSpaceLeft() > 0 &&
                !SparePooledDirectIOReadBuffers_.empty() &&
                // +2 is for TNotificationHandle reads.
                PendingIORequestCount_ < Config_->MaxConcurrentRequestsPerThread + 2;
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

            ArmSubmissionNotificationRead();
        }

        void HandleRequest(TUringRequest* request)
        {
            switch (request->Type) {
                case EUringRequestType::Read:
                    HandleReadRequest(request);
                    break;
                case EUringRequestType::Write:
                    HandleWriteRequest(request);
                    break;
                case EUringRequestType::FlushFile:
                    HandleFlushFileRequest(request);
                    break;
                case EUringRequestType::Allocate:
                    HandleAllocateRequest(request);
                    break;
                default:
                    YT_ABORT();
            }
        }

        void HandleReadRequest(TUringRequest* request)
        {
            auto totalSubrequestCount = static_cast<int>(request->ReadSubrequests.size());

            YT_LOG_TRACE("Handling read request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
                request,
                request->FinishedSubrequestCount,
                totalSubrequestCount);

            if (request->FinishedSubrequestCount == totalSubrequestCount) {
                TrySetRequestSucceeded(request);
                DisposeRequest(request);
                return;
            }

            while (!request->PendingReadSubrequestIndexes.empty() &&
                   CanHandleMoreSubmissions())
            {
                auto subrequestIndex = request->PendingReadSubrequestIndexes.back();
                request->PendingReadSubrequestIndexes.pop_back();

                const auto& subrequest = request->ReadSubrequests[subrequestIndex];
                const auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];

                auto* sqe = AllocateSqe();

                if (subrequest.UseDirectIO &&
                    !(IsBufferDirectIOCapable(subrequest.Buffer) && IsRequestDirectIOCapable(subrequest.Offset, subrequest.Buffer.Size())))
                {
                    auto startOffset = AlignDown(subrequest.Offset, PageSize);
                    auto endOffset = AlignUp(subrequest.Offset + static_cast<i64>(subrequest.Buffer.Size()), PageSize);

                    i64 readSize;
                    if (subrequest.Buffer.Size() >= Config_->LargeUnalignedDirectIOReadSize) {
                        readSize = Min(endOffset - startOffset, AlignUp(Config_->MaxBytesPerRead, PageSize));
                        AllocateLargeDirectIOReadBuffer(request, subrequestIndex, readSize);
                    } else {
                        AllocatePooledDirectIOReadBuffer(request, subrequestIndex);
                        readSize = Min<i64>(endOffset - startOffset, static_cast<i64>(subrequestState.DirectIOBuffer.Size()));
                    }

                    YT_LOG_TRACE("Submitting read operation with direct IO buffer (Request: %p/%v, FD: %v, Offset: %v, Buffer: %p@%v)",
                        request,
                        subrequestIndex,
                        subrequest.Handle,
                        startOffset,
                        subrequestState.DirectIOBuffer.Begin(),
                        readSize);

                    if (PooledDirectIOReadBufferRegistered_ && subrequestState.IsDirectIOBufferPooled()) {
                        io_uring_prep_read_fixed(sqe, subrequest.Handle, subrequestState.DirectIOBuffer.Begin(), readSize, startOffset, /* buf_index */ 0);
                    } else {
                        auto* iov = AllocateIov();
                        *iov = {
                            .iov_base = subrequestState.DirectIOBuffer.Begin(),
                            .iov_len = static_cast<size_t>(readSize)
                        };
                        io_uring_prep_readv(sqe, subrequest.Handle, iov, 1, startOffset);
                    }
                } else {
                    auto* iov = AllocateIov();
                    *iov = {
                        .iov_base = subrequest.Buffer.Begin(),
                        .iov_len = Min(subrequest.Buffer.Size(), static_cast<size_t>(Config_->MaxBytesPerRead))
                    };

                    YT_LOG_TRACE("Submitting read operation (Request: %p/%v, FD: %v, Offset: %v, Buffer: %p@%v)",
                        request,
                        subrequestIndex,
                        subrequest.Handle,
                        subrequest.Offset,
                        iov->iov_base,
                        iov->iov_len);

                    io_uring_prep_readv(sqe, subrequest.Handle, iov, 1, subrequest.Offset);
                }

                SetRequestUserData(sqe, request, subrequestIndex);
            }
        }

        void HandleWriteRequest(TUringRequest* request)
        {
            auto totalSubrequestCount = static_cast<int>(request->WriteRequest.Buffers.size());

            YT_LOG_TRACE("Handling write request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
                request,
                request->FinishedSubrequestCount,
                totalSubrequestCount);

            if (request->CurrentWriteSubrequestIndex == totalSubrequestCount) {
                TrySetRequestSucceeded(request);
                DisposeRequest(request);
                return;
            }

            auto* iovBegin = GetCurrentIov();
            int iovCount = 0;
            i64 toWrite = 0;
            while (request->CurrentWriteSubrequestIndex + iovCount < totalSubrequestCount &&
                iovCount < MaxIovCountPerRequest &&
                toWrite < Config_->MaxBytesPerWrite)
            {
                const auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex + iovCount];
                auto* iov = AllocateIov();
                *iov = {
                    .iov_base = const_cast<char*>(buffer.Begin()),
                    .iov_len = buffer.Size()
                };
                if (toWrite + iov->iov_len > Config_->MaxBytesPerWrite) {
                    iov->iov_len = Config_->MaxBytesPerWrite - toWrite;
                }
                toWrite += iov->iov_len;
                ++iovCount;
            }

            YT_LOG_TRACE("Submitting write operation (Request: %p, FD: %v, Offset: %v, Buffers: %v)",
                request,
                request->WriteRequest.Handle,
                request->WriteRequest.Offset,
                MakeFormattableView(xrange(iovBegin, iovBegin + iovCount), [] (auto* builder, const auto* iov) {
                    builder->AppendFormat("%p@%v",
                        iov->iov_base,
                        iov->iov_len);
                }));

            auto* sqe = AllocateSqe();
            io_uring_prep_writev(sqe, request->WriteRequest.Handle, iovBegin, iovCount, request->WriteRequest.Offset);
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

        void HandleFlushFileRequest(TUringRequest* request)
        {
            YT_LOG_TRACE("Handling flush file request (Request: %p)",
                request);

            YT_LOG_TRACE("Submitting flush file request (Request: %p, FD: %v, Mode: %v)",
                request,
                request->FlushFileRequest.Handle,
                request->FlushFileRequest.Mode);

            auto* sqe = AllocateSqe();
            io_uring_prep_fsync(sqe, request->FlushFileRequest.Handle, GetSyncFlags(request->FlushFileRequest.Mode));
            SetRequestUserData(sqe, request);
        }

        void HandleAllocateRequest(TUringRequest* request)
        {
            YT_LOG_TRACE("Handling allocate request (Request: %p)",
                request);

            YT_LOG_TRACE("Submitting allocate request (Request: %p, FD: %v, Size: %v)",
                request,
                request->AllocateRequest.Handle,
                request->AllocateRequest.Size);

            auto* sqe = AllocateSqe();
            io_uring_prep_fallocate(sqe, request->AllocateRequest.Handle, FALLOC_FL_CONVERT_UNWRITTEN, 0, request->AllocateRequest.Size);
            SetRequestUserData(sqe, request);
        }


        void HandleCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData(cqe);
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
            auto [request, subrequestIndex] = GetRequestUserData(cqe);

            YT_LOG_TRACE("Handling read completion (Request: %p/%v)",
                request,
                subrequestIndex);

            auto& subrequest = request->ReadSubrequests[subrequestIndex];
            auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];

            bool disposeDirectIOBuffer = true;

            if (cqe->res == 0 && subrequest.Buffer.Size() > 0) {
                ++request->FinishedSubrequestCount;
                TrySetRequestFailed(request, TError(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request"));
            } else if (cqe->res < 0) {
                ++request->FinishedSubrequestCount;
                TrySetRequestFailed(request, cqe);
            } else {
                i64 readSize = cqe->res;
                if (Config_->SimulatedMaxBytesPerRead) {
                    readSize = Min(readSize, *Config_->SimulatedMaxBytesPerRead);
                }

                auto bufferSize = static_cast<i64>(subrequest.Buffer.Size());

                YT_VERIFY(!subrequest.UseDirectIO || readSize % PageSize == 0);

                if (subrequestState.DirectIOBuffer) {
                    YT_VERIFY(readSize % PageSize == 0);
                    auto alignmentGap = subrequest.Offset % PageSize;
                    readSize = Min(readSize - alignmentGap, bufferSize);
                    YT_LOG_TRACE("Copying data from direct IO read buffer (Request: %p/%v, Size: %v)",
                        request,
                        subrequestIndex,
                        readSize);
                    ::memcpy(subrequest.Buffer.Begin(), subrequestState.DirectIOBuffer.Begin() + alignmentGap, readSize);
                }

                subrequest.Offset += readSize;
                if (bufferSize == readSize) {
                    YT_LOG_TRACE("Read subrequest fully succeeded (Request: %p/%v, Size: %v)",
                        request,
                        subrequestIndex,
                        readSize);
                    subrequest.Buffer = {};
                    ++request->FinishedSubrequestCount;
                } else {
                    YT_LOG_TRACE("Read subrequest partially succeeded (Request: %p/%v, Size: %v)",
                        request,
                        subrequestIndex,
                        readSize);
                    subrequest.Buffer = subrequest.Buffer.Slice(readSize, bufferSize);
                    request->PendingReadSubrequestIndexes.push_back(subrequestIndex);
                    // NB: Don't dispose direct IO read buffer; HandleReadRequest will need it below.
                    disposeDirectIOBuffer = false;
                }
            }

            if (disposeDirectIOBuffer) {
                DisposeDirectIOReadBuffer(request, subrequestIndex);
            }

            HandleReadRequest(request);
        }

        void HandleWriteCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData(cqe);

            YT_LOG_TRACE("Handling write completion (Request: %p)",
                request);

            if (cqe->res < 0) {
                TrySetRequestFailed(request, cqe);
                DisposeRequest(request);
                return;
            }

            i64 writtenSize = cqe->res;
            if (Config_->SimulatedMaxBytesPerWrite) {
                writtenSize = Min(writtenSize, *Config_->SimulatedMaxBytesPerWrite);
            }

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
            auto [request, _] = GetRequestUserData(cqe);

            YT_LOG_TRACE("Handling sync completion (Request: %p)",
                request);

            TrySetRequestFinished(request, cqe);
            DisposeRequest(request);
        }

        void HandleAllocateCompletion(const io_uring_cqe* cqe)
        {
            auto [request, _] = GetRequestUserData(cqe);

            YT_LOG_TRACE("Handling allocate completion (Request: %p)",
                request);

            TrySetRequestFinished(request, cqe);
            DisposeRequest(request);
        }


        void ArmNotificationRead(
            const TNotificationHandle& notificationHandle,
            intptr_t notificationUserData)
        {
            auto* iov = AllocateIov();
            *iov = {
                .iov_base = &NotificationReadBuffer_,
                .iov_len = sizeof(NotificationReadBuffer_)
            };

            auto* sqe = AllocateSqe();
            io_uring_prep_readv(sqe, notificationHandle.GetFD(), iov, 1, 0);
            io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(notificationUserData));
        }

        void ArmStopNotificationRead()
        {
            YT_LOG_TRACE("Arming stop notification read");
            ArmNotificationRead(StopNotificationHandle_, StopNotificationUserData);
        }

        void ArmSubmissionNotificationRead()
        {
            if (!SubmissionNotificationReadArmed_ &&
                !Stopping_ &&
                CanHandleMoreSubmissions())
            {
                SubmissionNotificationReadArmed_ = true;
                YT_LOG_TRACE("Arming submission notification read");
                ArmNotificationRead(ThreadPool_->RequestNotificationHandle_, RequestNotificationUserData);
            }
        }


        TString MakeThreadName()
        {
            return Format("%v:%v", ThreadPool_->ThreadNamePrefix_, Index_);
        }


        void ResetIovs()
        {
            UsedIovCount_ = 0;
        }

        iovec* GetCurrentIov()
        {
            return &Iovs_[UsedIovCount_];
        }

        iovec* AllocateIov()
        {
            YT_VERIFY(UsedIovCount_ < Iovs_.size());
            return &Iovs_[UsedIovCount_++];
        }


        io_uring_cqe* WaitForCqe()
        {
            auto* cqe = Uring_.WaitCqe();
            YT_VERIFY(--PendingRequestCount_ >= 0);
            YT_LOG_TRACE("CQE received (PendingRequestCount: %v)",
                PendingRequestCount_);
            return cqe;
        }

        io_uring_sqe* AllocateSqe()
        {
            auto* sqe = Uring_.TryGetSqe();
            YT_VERIFY(sqe);
            return sqe;
        }

        void SubmitSqes()
        {
            int count = Uring_.Submit();
            if (count > 0) {
                PendingRequestCount_ += count;

                YT_LOG_TRACE("SQEs submitted (SqeCount: %v, PendingRequestCount: %v)",
                    count,
                    PendingRequestCount_);
            }
        }

        static void TrySetRequestSucceeded(TUringRequest* request)
        {
            if (request->Promise.TrySet()) {
                YT_LOG_TRACE("Request succeeded (Request: %p)",
                    request);
            }
        }

        static void TrySetRequestFailed(TUringRequest* request, const io_uring_cqe* cqe)
        {
            YT_VERIFY(cqe->res < 0);
            TrySetRequestFailed(request, TError::FromSystem(-cqe->res));
        }

        static void TrySetRequestFailed(TUringRequest* request, const TError& error)
        {
            if (request->Promise.TrySet(std::move(error))) {
                YT_LOG_TRACE(error, "Request failed (Request: %p)",
                    request);
            }
        }

        static void TrySetRequestFinished(TUringRequest* request, const io_uring_cqe* cqe)
        {
            if (cqe->res >= 0) {
                TrySetRequestSucceeded(request);
            } else {
                TrySetRequestFailed(request, cqe);
            }
        }


        static void SetRequestUserData(io_uring_sqe* sqe, TUringRequest* request, int subrequestIndex = 0)
        {
            auto userData = reinterpret_cast<void*>(
                reinterpret_cast<uintptr_t>(request) |
                (static_cast<uintptr_t>(subrequestIndex) << 48));
            io_uring_sqe_set_data(sqe, userData);
        }

        static std::tuple<TUringRequest*, int> GetRequestUserData(const io_uring_cqe* cqe)
        {
            auto userData = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
            return {
                reinterpret_cast<TUringRequest*>(userData & ~(1ULL << 48)),
                userData >> 48
            };
        }


        void DisposeRequest(TUringRequest* request)
        {
            YT_LOG_TRACE("Request disposed (Request: %v)",
                request);
            for (int subrequestIndex = 0; subrequestIndex < static_cast<int>(request->ReadSubrequestStates.size()); ++subrequestIndex) {
                DisposeDirectIOReadBuffer(request, subrequestIndex);
            }
            delete request;
        }


        void AllocatePooledDirectIOReadBuffer(TUringRequest* request, int subrequestIndex)
        {
            auto& state = request->ReadSubrequestStates[subrequestIndex];
            if (state.IsDirectIOBufferPooled()) {
                return;
            }

            DisposeDirectIOReadBuffer(request, subrequestIndex);

            YT_VERIFY(!SparePooledDirectIOReadBuffers_.empty());
            state.DirectIOBuffer = TSharedMutableRef(SparePooledDirectIOReadBuffers_.back(), nullptr);
            SparePooledDirectIOReadBuffers_.pop_back();

            YT_LOG_TRACE("Pooled direct IO read buffer allocated (Request: %p, Buffer: %p@%v, SpareBufferCount: %v)",
                request,
                state.DirectIOBuffer.Begin(),
                state.DirectIOBuffer.Size(),
                SparePooledDirectIOReadBuffers_.size());
        }

        void AllocateLargeDirectIOReadBuffer(TUringRequest* request, int subrequestIndex, i64 size)
        {
            auto& state = request->ReadSubrequestStates[subrequestIndex];
            if (state.IsDirectIOBufferLarge() && state.DirectIOBuffer.Size() >= static_cast<size_t>(size)) {
                return;
            }

            DisposeDirectIOReadBuffer(request, subrequestIndex);

            state.DirectIOBuffer = TSharedMutableRef::AllocatePageAligned<TLargeDirectIOReadBufferTag>(size, false);;

            YT_LOG_TRACE("Large direct IO read buffer allocated (Request: %p/%v, Buffer: %p@%v)",
                request,
                subrequestIndex,
                state.DirectIOBuffer.Begin(),
                state.DirectIOBuffer.Size());
        }

        void DisposeDirectIOReadBuffer(TUringRequest* request, int subrequestIndex)
        {
            auto& state = request->ReadSubrequestStates[subrequestIndex];

            if (state.IsDirectIOBufferLarge()) {
                YT_LOG_TRACE("Large direct IO read buffer disposed (Request: %p/%v, Buffer: %p@%v)",
                    request,
                    subrequestIndex,
                    state.DirectIOBuffer.Begin(),
                    state.DirectIOBuffer.Size());
            }

            if (state.IsDirectIOBufferPooled()) {
                YT_LOG_TRACE("Pooled direct IO read buffer disposed (Request: %p/%v, Buffer: %p, SpareBufferCount: %v)",
                    request,
                    subrequestIndex,
                    state.DirectIOBuffer.Begin(),
                    SparePooledDirectIOReadBuffers_.size());
                SparePooledDirectIOReadBuffers_.push_back(state.DirectIOBuffer);
            }

            state.DirectIOBuffer = {};
        }

        static bool IsBufferDirectIOCapable(TRef buffer)
        {
            return
                reinterpret_cast<uintptr_t>(buffer.Begin()) % PageSize == 0 &&
                buffer.Size() % PageSize == 0;
        }

        static bool IsRequestDirectIOCapable(i64 offset, i64 size)
        {
            return
                offset % PageSize == 0 &&
                size % PageSize == 0;
        }
    };

    std::vector<std::unique_ptr<TUringThread>> Threads_;

    TNotificationHandle RequestNotificationHandle_{true};
    std::atomic<bool> RequestNotificationHandleRaised_ = false;
    moodycamel::ConcurrentQueue<TUringRequestPtr> RequestQueue_;


    void StartThreads()
    {
        YT_LOG_INFO("Starting all uring threads");
        for (int threadIndex = 0; threadIndex < Config_->UringThreadCount; ++threadIndex) {
            Threads_[threadIndex] = std::make_unique<TUringThread>(this, threadIndex);
        }
        for (const auto& thread : Threads_) {
            thread->Start();
        }
        YT_LOG_INFO("All uring threads started");
    }

    void StopThreads()
    {
        YT_LOG_INFO("Stopping all uring threads");
        for (const auto& thread : Threads_) {
            thread->Stop();
        }
        YT_LOG_INFO("All uring threads stopped");
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
            Config_))
    { }

    ~TUringIOEngine()
    {
        GetFinalizerInvoker()->Invoke(BIND([threadPool = std::move(ThreadPool_)] () mutable {
            threadPool.reset();
        }));
    }

    virtual TFuture<void> Read(
        std::vector<TReadRequest> requests,
        i64 /* priority */) override
    {
        if (static_cast<int>(requests.size()) > MaxSubrequestCount) {
            return MakeFuture(TError("Too many read requests: %v > %v",
                requests.size(),
                MaxSubrequestCount));
        }

        auto uringRequest = std::make_unique<TUringRequest>();
        uringRequest->Type = EUringRequestType::Read;
        uringRequest->ReadSubrequests = std::move(requests);
        uringRequest->ReadSubrequestStates.resize(uringRequest->ReadSubrequests.size());
        uringRequest->PendingReadSubrequestIndexes.reserve(uringRequest->ReadSubrequests.size());
        int subrequestCount = static_cast<int>(uringRequest->ReadSubrequests.size());
        for (int index = subrequestCount - 1; index >= 0; --index) {
            uringRequest->PendingReadSubrequestIndexes.push_back(index);
        }
        return SubmitRequest(std::move(uringRequest));
    }

    virtual TFuture<void> Write(
        TWriteRequest request,
        i64 /* priority */) override
    {
        auto uringRequest = std::make_unique<TUringRequest>();
        uringRequest->Type = EUringRequestType::Write;
        uringRequest->WriteRequest = std::move(request);
        return SubmitRequest(std::move(uringRequest));
    }

    virtual TFuture<void> FlushFile(
        TFlushFileRequest request,
        i64 /* priority */) override
    {
        auto uringRequest = std::make_unique<TUringRequest>();
        uringRequest->Type = EUringRequestType::FlushFile;
        uringRequest->FlushFileRequest = std::move(request);
        return SubmitRequest(std::move(uringRequest));
    }

    virtual TFuture<void> Allocate(
        TAllocateRequest request,
        i64 /* priority */) override
    {
        auto uringRequest = std::make_unique<TUringRequest>();
        uringRequest->Type = EUringRequestType::Allocate;
        uringRequest->AllocateRequest = std::move(request);
        return SubmitRequest(std::move(uringRequest));
    }

private:
    const TConfigPtr Config_;

    TUringThreadPoolPtr ThreadPool_;


    TFuture<void> SubmitRequest(TUringRequestPtr request)
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

    switch (engineType) {
        case EIOEngineType::ThreadPool:
            return CreateIOEngine<TThreadPoolIOEngine>(
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
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
