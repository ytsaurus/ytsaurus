#include "io_engine.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/fs.h>

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
using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

class TThreadedIOEngineConfig
    : public NYTree::TYsonSerializable
{
public:
    int ReadThreadCount;
    int WriteThreadCount;

    i64 MaxBytesPerRead;
    i64 MaxBytesPerWrite;

    bool EnableSync;
    bool EnablePwritev;

    std::optional<TDuration> SickReadTimeThreshold;
    std::optional<TDuration> SickReadTimeWindow;
    std::optional<TDuration> SickWriteTimeThreshold;
    std::optional<TDuration> SickWriteTimeWindow;
    std::optional<TDuration> SicknessExpirationTimeout;

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

        RegisterParameter("enable_sync", EnableSync)
            .Default(true);
        RegisterParameter("enable_pwritev", EnablePwritev)
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

class TIOEngineBase
    : public IIOEngine
{
protected:
    const NLogging::TLogger Logger;

    explicit TIOEngineBase(NLogging::TLogger logger)
        : Logger(std::move(logger))
    { }

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
            DoFallocate(handle, preallocateSize);
        }
        return handle;
    }

    void DoFallocate(
        const std::shared_ptr<TFileHandle>& handle,
        i64 newSize)
    {
#ifdef _linux_
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (fallocate(*handle, FALLOC_FL_CONVERT_UNWRITTEN, 0, newSize) != 0) {
            YT_LOG_WARNING(TError::FromSystem(), "fallocate call failed");
        }
#endif
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
};

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
        : TIOEngineBase(std::move(logger))
        , Config_(std::move(config))
        , ReadThreadPool_(New<TThreadPool>(Config_->ReadThreadCount, Format("IOR:%v", locationId)))
        , WriteThreadPool_(New<TThreadPool>(Config_->WriteThreadCount, Format("IOW:%v", locationId)))
        , ReadInvoker_(CreatePrioritizedInvoker(ReadThreadPool_->GetInvoker()))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker()))
        , PreadTimer_(profiler.Timer("/pread_time"))
        , PwriteTimer_(profiler.Timer("/pwrite_time"))
        , FdatasyncTimer_(profiler.Timer("/fdatasync_time"))
        , FsyncTimer_(profiler.Timer("/fsync_time"))
    {
        profiler.AddFuncGauge("/sick", MakeStrong(this), [this] {
            return Sick_.load();
        });

        profiler.AddFuncGauge("/sick_events", MakeStrong(this), [this] {
            return SicknessCounter_.load();
        });
    }

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        const TString& fileName,
        EOpenMode mode,
        i64 preallocateSize,
        i64 priority) override
    {
        const auto& invoker = (mode & RdOnly)
            ? ReadInvoker_
            : WriteInvoker_;

        return BIND(&TThreadedIOEngine::DoOpen, MakeStrong(this), fileName, mode, preallocateSize)
            .AsyncVia(CreateFixedPriorityInvoker(invoker, priority))
            .Run();
    }

    virtual TFuture<void> Close(
        const std::shared_ptr<TFileHandle>& handle,
        i64 newSize,
        bool flush)
    {
        return BIND(&TThreadedIOEngine::DoClose, MakeStrong(this), handle, newSize, flush && Config_->EnableSync)
            .AsyncVia(WriteInvoker_)
            .Run();
    }

    virtual TFuture<void> FlushDirectory(const TString& path)
    {
        return BIND(&TThreadedIOEngine::DoFlushDirectory, MakeStrong(this), path)
            .AsyncVia(WriteInvoker_)
            .Run();
    }

    virtual TFuture<TSharedMutableRef> Pread(
        const std::shared_ptr<TFileHandle>& handle,
        i64 size,
        i64 offset,
        i64 priority) override
    {
        TWallTimer timer;
        auto memoryZone = GetCurrentMemoryZone();
        return BIND(&TThreadedIOEngine::DoPread, MakeStrong(this), handle, size, offset, timer, memoryZone)
            .AsyncVia(CreateFixedPriorityInvoker(ReadInvoker_, priority))
            .Run();
    }

    virtual TFuture<TSharedMutableRef> ReadAll(
        const TString& fileName,
        i64 priority = std::numeric_limits<i64>::max()) override
    {
        TWallTimer timer;
        auto memoryZone = GetCurrentMemoryZone();
        return BIND(&TThreadedIOEngine::DoReadAll, MakeStrong(this), fileName, timer, memoryZone)
            .AsyncVia(CreateFixedPriorityInvoker(ReadInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Pwrite(
        const std::shared_ptr<TFileHandle>& handle,
        const TSharedRef& data,
        i64 offset,
        i64 priority) override
    {
        TWallTimer timer;

        return BIND(&TThreadedIOEngine::DoPwrite, MakeStrong(this), handle, data, offset, timer)
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<void> Pwritev(
        const std::shared_ptr<TFileHandle>& handle,
        const std::vector<TSharedRef>& data,
        i64 offset,
        i64 priority) override
    {
        TWallTimer timer;

        return BIND(&TThreadedIOEngine::DoPwritev, MakeStrong(this), handle, data, offset, timer)
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<bool> FlushData(
        const std::shared_ptr<TFileHandle>& handle,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoFlushData, MakeStrong(this), handle)
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<bool> Flush(
        const std::shared_ptr<TFileHandle>& handle,
        i64 priority) override
    {
        return BIND(&TThreadedIOEngine::DoFlush, MakeStrong(this), handle)
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual bool IsSick() const override
    {
        return Sick_;
    }

    virtual TFuture<void> Fallocate(
        const std::shared_ptr<TFileHandle>& handle,
        i64 newSize) override
    {
        return BIND(&TThreadedIOEngine::DoFallocate, MakeStrong(this), handle)
            .AsyncVia(WriteInvoker_)
            .Run(newSize);
    }

    virtual const IInvokerPtr& GetWritePoolInvoker() override
    {
        return WriteThreadPool_->GetInvoker();
    }

private:
    const TConfigPtr Config_;
    const TThreadPoolPtr ReadThreadPool_;
    const TThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr ReadInvoker_;
    const IPrioritizedInvokerPtr WriteInvoker_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, ReadWaitLock_);
    std::optional<TInstant> SickReadWaitStart_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, WriteWaitLock_);
    std::optional<TInstant> SickWriteWaitStart_;

    std::atomic<bool> Sick_ = false;
    std::atomic<i64> SicknessCounter_ = 0;

    NProfiling::TGauge SickGauge_;
    NProfiling::TGauge SickEventsGauge_;
    NProfiling::TEventTimer PreadTimer_;
    NProfiling::TEventTimer PwriteTimer_;
    NProfiling::TEventTimer FdatasyncTimer_;
    NProfiling::TEventTimer FsyncTimer_;

    bool IsDirectAligned(const std::shared_ptr<TFileHandle>& handle)
    {
#ifdef _linux_
        const long flags = ::fcntl(*handle, F_GETFL);
        return flags & O_DIRECT;
#else
        return false;
#endif
    }

    bool DoFlushData(const std::shared_ptr<TFileHandle>& handle)
    {
        TEventTimer timer(FdatasyncTimer_);
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (!Config_->EnableSync) {
            return true;
        }
        return handle->FlushData();
    }

    bool DoFlush(const std::shared_ptr<TFileHandle>& handle)
    {
        TEventTimer timer(FsyncTimer_);
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (!Config_->EnableSync) {
            return true;
        }
        return handle->Flush();
    }

    TSharedMutableRef DoPread(
        const std::shared_ptr<TFileHandle>& handle,
        i64 size,
        i64 fileOffset,
        TWallTimer timer,
        EMemoryZone memoryZone)
    {
        AddReadWaitTimeSample(timer.GetElapsedTime());

        TMemoryZoneGuard guard(memoryZone);

        struct TThreadedIOEngineReadBufferTag
        { };
        auto data = TSharedMutableRef::Allocate<TThreadedIOEngineReadBufferTag>(size, false);

        auto toReadRemaining = size;
        i64 dataOffset = 0;

        NFS::ExpectIOErrors([&] {
            while (toReadRemaining > 0) {
                auto toRead = static_cast<ui32>(Min(toReadRemaining, Config_->MaxBytesPerRead));

                i32 reallyRead;
                {
                    TEventTimer timer(PreadTimer_);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyRead = handle->Pread(data.Begin() + dataOffset, toRead, fileOffset);
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

        return data.Slice(0, size - toReadRemaining);
    }

    TSharedMutableRef DoReadAll(const TString& fileName, TWallTimer timer, EMemoryZone memoryZone)
    {
        AddReadWaitTimeSample(timer.GetElapsedTime());

        auto mode = OpenExisting | RdOnly | Seq | CloseOnExec;
        auto file = DoOpen(fileName, mode, -1);
        auto data = DoPread(file, file->GetLength(), 0, timer, memoryZone);
        DoClose(file, -1, false);
        return data;
    }

    void DoPwrite(
        const std::shared_ptr<TFileHandle>& handle,
        const TSharedRef& data,
        i64 fileOffset,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(data.Size());
            i64 dataOffset = 0;

            while (toWriteRemaining > 0) {
                auto toWrite = static_cast<ui32>(Min(toWriteRemaining, Config_->MaxBytesPerWrite));

                i32 reallyWritten;
                {
                    TEventTimer timer(PwriteTimer_);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyWritten = handle->Pwrite(const_cast<char*>(data.Begin()) + dataOffset, toWrite, fileOffset);
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

    void DoPwritev(
        const std::shared_ptr<TFileHandle>& handle,
        const std::vector<TSharedRef>& data,
        i64 fileOffset,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(GetByteSize(data));

            int dataIndex = 0;
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
                    while (dataIndex + iovCnt < static_cast<int>(data.size()) &&
                            iovCnt < iov.size() &&
                            toWrite < Config_->MaxBytesPerWrite)
                    {
                        const auto& dataPart = data[dataIndex + iovCnt];
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
                        TEventTimer timer(PwriteTimer_);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        // TODO(babenko): consider adding Pwritev to TFileHandle
                        reallyWritten = ::pwritev(*handle, iov.data(), iovCnt, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    while (reallyWritten > 0) {
                        const auto& dataPart = data[dataIndex];
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
                    const auto& dataPart = data[dataIndex];
                    auto toWrite = static_cast<ui32>(Min(toWriteRemaining, Config_->MaxBytesPerWrite, static_cast<i64>(dataPart.Size()) - dataOffset));

                    i32 reallyWritten;
                    {
                        TEventTimer timer(PwriteTimer_);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = handle->Pwrite(const_cast<char*>(data[dataIndex].Begin()) + dataOffset, toWrite, fileOffset);
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
                BIND(&TThreadedIOEngine::ResetSickFlag, MakeStrong(this)),
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
