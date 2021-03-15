#include "io_engine.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <util/generic/size_literals.h>

#include <array>

#ifdef _linux_
    #include <sys/uio.h>

    #ifndef FALLOC_FL_CONVERT_UNWRITTEN
        #define FALLOC_FL_CONVERT_UNWRITTEN 0x4
    #endif
#endif

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TIOEngineConfigBase
    : public NYTree::TYsonSerializable
{
public:
    int AuxThreadCount;

    bool EnableSync;

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

class TThreadedIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int ReadThreadCount;
    int WriteThreadCount;

    i64 MaxBytesPerRead;
    i64 MaxBytesPerWrite;

    bool EnablePwritev;

    TThreadedIOEngineConfig()
    {
        RegisterParameter("read_thread_count", ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("write_thread_count", WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);

        RegisterParameter("max_bytes_per_read", MaxBytesPerRead)
            .GreaterThanOrEqual(1)
            .Default(256_MB);
        RegisterParameter("max_bytes_per_write", MaxBytesPerWrite)
            .GreaterThanOrEqual(1)
            .Default(256_MB);

        RegisterParameter("enable_pwritev", EnablePwritev)
            .Default(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIOEngineBase
    : public IIOEngine
{
public:
    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        const TString& fileName,
        EOpenMode mode,
        i64 preallocateSize,
        i64 priority) override
    {
        return BIND(&TIOEngineBase::DoOpen, MakeStrong(this), fileName, mode, preallocateSize)
            .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Close(
        std::shared_ptr<TFileHandle> handle,
        i64 newSize,
        bool flush)
    {
        return BIND(&TIOEngineBase::DoClose, MakeStrong(this), std::move(handle), newSize, flush && Config_->EnableSync)
            .AsyncVia(GetAuxPoolInvoker())
            .Run();
    }

    virtual TFuture<void> FlushDirectory(const TString& path)
    {
        return BIND(&TIOEngineBase::DoFlushDirectory, MakeStrong(this), path)
            .AsyncVia(GetAuxPoolInvoker())
            .Run();
    }

    virtual TFuture<void> Fallocate(
        FHANDLE handle,
        i64 newSize) override
    {
        return BIND(&TIOEngineBase::DoFallocate, MakeStrong(this), handle)
            .AsyncVia(GetAuxPoolInvoker())
            .Run(newSize);
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

    std::shared_ptr<TFileHandle> DoOpen(const TString& fileName, EOpenMode mode, i64 preallocateSize)
    {
        std::shared_ptr<TFileHandle> handle;
        {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            handle = std::make_shared<TFileHandle>(fileName, mode);
        }
        if (!handle->IsOpen()) {
            THROW_ERROR_EXCEPTION(
                "Cannot open %Qv with mode %v",
                fileName,
                mode)
                << TError::FromSystem();
        }
        if (preallocateSize > 0) {
            YT_VERIFY(mode & WrOnly);
            DoFallocate(*handle, preallocateSize);
        }
        return handle;
    }

    void DoFlushDirectory(const TString& path)
    {
        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            NFS::FlushDirectory(path);
        });
    }

    void DoClose(const std::shared_ptr<TFileHandle>& handle, i64 newSize, bool flush)
    {
        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            if (newSize >= 0) {
                handle->Resize(newSize);
            }
            if (flush) {
                handle->Flush();
            }
            handle->Close();
        });
    }

    void DoFallocate(FHANDLE handle, i64 newSize)
    {
#ifdef _linux_
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (HandleEintr(::fallocate, handle, FALLOC_FL_CONVERT_UNWRITTEN, 0, newSize) != 0) {
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
};

////////////////////////////////////////////////////////////////////////////////

class TThreadedIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TThreadedIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TThreadedIOEngine(
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
        const TReadRequest& request,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoRead, MakeStrong(this), std::move(request), TWallTimer())
            .AsyncVia(CreateFixedPriorityInvoker(ReadInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> ReadMany(
        const std::vector<TReadRequest>& requests,
        i64 priority) override
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(requests.size());
        auto invoker = CreateFixedPriorityInvoker(ReadInvoker_, priority);
        for (const auto& request : requests) {
            futures.push_back(BIND(&TThreadedIOEngine::DoRead, MakeStrong(this), request, TWallTimer())
                .AsyncVia(invoker)
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

    virtual TFuture<TSharedMutableRef> ReadAll(
        const TString& fileName,
        i64 priority = std::numeric_limits<i64>::max()) override
    {
        return BIND(&TThreadedIOEngine::DoReadAll, MakeStrong(this), fileName, TWallTimer())
            .AsyncVia(CreateFixedPriorityInvoker(ReadInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Write(
        const TWriteRequest& request,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoWrite, MakeStrong(this), request, TWallTimer())
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> WriteVectorized(
        const TVectorizedWriteRequest& request,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoWriteVectorized, MakeStrong(this), request, TWallTimer())
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> FlushData(
        FHANDLE handle,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoFlushData, MakeStrong(this), handle)
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Flush(
        FHANDLE handle,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoFlush, MakeStrong(this), handle)
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

        auto toReadRemaining = static_cast<i64>(request.Data.Size());
        auto fileOffset = request.Offset;
        i64 dataOffset = 0;

        NFS::ExpectIOErrors([&] {
            while (toReadRemaining > 0) {
                auto toRead = static_cast<ui32>(Min(toReadRemaining, Config_->MaxBytesPerRead));

                i32 reallyRead;
                {
                    TEventTimer eventTimer(PreadTimer_);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyRead = HandleEintr(::pread, request.Handle, request.Data.Begin() + dataOffset, toRead, fileOffset);
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

                fileOffset += reallyRead;
                dataOffset += reallyRead;
                toReadRemaining -= reallyRead;
            }
        });

        if (toReadRemaining > 0) {
            THROW_ERROR_EXCEPTION(
                NFS::EErrorCode::IOError,
                "Unexpected end-of-file while tried to read %v bytes at offset %v",
                request.Data.Size(),
                request.Offset);
        }
    }

    void DoWrite(
        const TWriteRequest& request,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(request.Data.Size());
            auto fileOffset = request.Offset;
            i64 dataOffset = 0;

            while (toWriteRemaining > 0) {
                auto toWrite = static_cast<ui32>(Min(toWriteRemaining, Config_->MaxBytesPerWrite));

                i32 reallyWritten;
                {
                    TEventTimer eventTimer(PwriteTimer_);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyWritten = HandleEintr(::pwrite, request.Handle, const_cast<char*>(request.Data.Begin()) + dataOffset, toWrite, fileOffset);
                }

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                fileOffset += reallyWritten;
                dataOffset += reallyWritten;
                toWriteRemaining -= reallyWritten;
            }
        });
    }

    void DoWriteVectorized(
        const TVectorizedWriteRequest& request,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Data));

            int dataIndex = 0;
            auto fileOffset = request.Offset;
            i64 dataOffset = 0; // within data[dataIndex]

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
                    std::array<struct iovec, 128> iov;
                    int iovCnt = 0;
                    i64 toWrite = 0;
                    while (dataIndex + iovCnt < static_cast<int>(request.Data.size()) &&
                            iovCnt < iov.size() &&
                            toWrite < Config_->MaxBytesPerWrite)
                    {
                        const auto& dataPart = request.Data[dataIndex + iovCnt];
                        auto& iovPart = iov[iovCnt];
                        iovPart.iov_base = const_cast<char*>(dataPart.Begin());
                        iovPart.iov_len = dataPart.Size();
                        if (iovCnt == 0) {
                            iovPart.iov_base = static_cast<char*>(iovPart.iov_base) + dataOffset;
                            iovPart.iov_len -= dataOffset;
                        }
                        if (toWrite + iovPart.iov_len > Config_->MaxBytesPerWrite) {
                            iovPart.iov_len = Config_->MaxBytesPerWrite - toWrite;
                        }
                        ++iovCnt;
                        toWrite += iovPart.iov_len;
                    }

                    ssize_t reallyWritten;
                    {
                        TEventTimer eventTimer(PwriteTimer_);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwritev, request.Handle, iov.data(), iovCnt, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    while (reallyWritten > 0) {
                        const auto& dataPart = request.Data[dataIndex];
                        i64 toAdvance = std::min(static_cast<i64>(dataPart.Size()) - dataOffset, reallyWritten);
                        fileOffset += toAdvance;
                        dataOffset += toAdvance;
                        reallyWritten -= toAdvance;
                        toWriteRemaining -= toAdvance;
                        if (dataOffset == dataPart.Size()) {
                            ++dataIndex;
                            dataOffset = 0;
                        }
                    }
#else
                    YT_ABORT();
#endif
                };

                auto pwrite = [&] {
                    const auto& dataPart = request.Data[dataIndex];
                    auto toWrite = static_cast<ui32>(Min(toWriteRemaining, Config_->MaxBytesPerWrite, static_cast<i64>(dataPart.Size()) - dataOffset));

                    i32 reallyWritten;
                    {
                        TEventTimer timer(PwriteTimer_);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwrite, request.Handle, const_cast<char*>(request.Data[dataIndex].Begin()) + dataOffset, toWrite, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    fileOffset += reallyWritten;
                    dataOffset += reallyWritten;
                    toWriteRemaining -= reallyWritten;
                    if (dataOffset == dataPart.Size()) {
                        ++dataIndex;
                        dataOffset = 0;
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

    TSharedMutableRef DoReadAll(const TString& fileName, TWallTimer timer)
    {
        AddReadWaitTimeSample(timer.GetElapsedTime());

        auto mode = OpenExisting | RdOnly | Seq | CloseOnExec;
        auto file = DoOpen(fileName, mode, -1);

        struct TReadAllBufferTag
        { };
        auto data = TSharedMutableRef::Allocate<TReadAllBufferTag>(file->GetLength(), false);

        DoRead(TReadRequest{*file, 0, data}, timer);

        DoClose(file, -1, false);

        return data;
    }

    void DoFlushData(FHANDLE handle)
    {
        if (Config_->EnableSync) {
            return;
        }
        NFS::ExpectIOErrors([&] {
            TEventTimer timer(FdatasyncTimer_);
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
#ifdef _linux_
            int result = HandleEintr(::fdatasync, handle);
#else
            int result = HandleEintr(::fsync, handle);
#endif
            if (result != 0) {
                ythrow TFileError();
            }
        });
    }

    void DoFlush(FHANDLE handle)
    {
        if (!Config_->EnableSync) {
            return;
        }
        NFS::ExpectIOErrors([&] {
            TEventTimer timer(FsyncTimer_);
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            if (HandleEintr(::fsync, handle) != 0) {
                ythrow TFileError();
            }
        });
    }
};

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

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    TProfiler profiler,
    NLogging::TLogger logger)
{
    switch (engineType) {
        case EIOEngineType::ThreadPool:
            return CreateIOEngine<TThreadedIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
        default:
            THROW_ERROR_EXCEPTION("Unknown IO engine %Qlv",
                engineType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
