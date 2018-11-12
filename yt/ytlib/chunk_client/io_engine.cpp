#include "io_engine.h"

#include <util/system/platform.h>

#ifdef _linux_
#include <yt/contrib/aio/aio_abi.h>

#include <sys/syscall.h>

#include <unistd.h>
#endif

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/align.h>
#include <yt/core/misc/fs.h>

#include <yt/core/profiling/profiler.h>

#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/align.h>
#include <util/system/sanitizers.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

struct TAioEngineDataBufferTag {};
struct TDefaultEngineDataBufferTag {};

#ifdef _linux_

int io_setup(unsigned nr, aio_context_t* ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

int io_submit(aio_context_t ctx, long nr,  struct iocb** iocbpp)
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

int io_getevents(
    aio_context_t ctx,
    long min_nr,
    long max_nr,
    struct io_event* events,
    struct timespec* timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

#endif

template <typename T>
bool IsAligned(T value, i64 alignment)
{
    return ::AlignDown<T>(value, alignment) == value;
}

class TThreadedIOEngineConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<int> ThreadCount; // COMPAT(aozeritsky)
    int ReadThreadCount;
    int WriteThreadCount;
    bool UseDirectIO;

    TNullable<TDuration> SickReadTimeThreshold;
    TNullable<TDuration> SickReadTimeWindow;
    TNullable<TDuration> SickWriteTimeThreshold;
    TNullable<TDuration> SickWriteTimeWindow;
    TNullable<TDuration> SicknessExpirationTimeout;

    TThreadedIOEngineConfig()
    {
        RegisterParameter("thread_count", ThreadCount)
            .Default(Null)
            .GreaterThanOrEqual(1);
        RegisterParameter("read_thread_count", ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("write_thread_count", WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("use_direct_io", UseDirectIO)
            .Default(false);

        RegisterParameter("sick_read_time_threshold", SickReadTimeThreshold)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);

        RegisterParameter("sick_read_time_window", SickReadTimeWindow)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);

        RegisterParameter("sick_write_time_threshold", SickWriteTimeThreshold)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);

        RegisterParameter("sick_write_time_window", SickWriteTimeWindow)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);

        RegisterParameter("sickness_expiration_timeout", SicknessExpirationTimeout)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);

        RegisterPostprocessor([&] () {
            if (ThreadCount && ReadThreadCount == 1 && WriteThreadCount == 1) {
                if (ThreadCount.Get() == 1) {
                    ThreadCount = 2;
                }

                ReadThreadCount = (ThreadCount.Get() + 1) / 2;
                WriteThreadCount = ThreadCount.Get() - ReadThreadCount;

                YCHECK(ReadThreadCount > 0);
                YCHECK(WriteThreadCount > 0);
                YCHECK(ReadThreadCount + WriteThreadCount == ThreadCount.Get());
            }
        });
    }
};

class TThreadedIOEngine
    : public IIOEngine
{
public:
    using TConfig = TThreadedIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TThreadedIOEngine(
        TConfigPtr config,
        const TString& locationId,
        const TProfiler& profiler,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , ReadThreadPool_(New<TThreadPool>(Config_->ReadThreadCount, Format("DiskIOR:%v", locationId)))
        , WriteThreadPool_(New<TThreadPool>(Config_->WriteThreadCount, Format("DiskIOW:%v", locationId)))
        , ReadInvoker_(CreatePrioritizedInvoker(ReadThreadPool_->GetInvoker()))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker()))
        , Profiler_(profiler)
        , Logger(logger)
        , UseDirectIO_(Config_->UseDirectIO)
    { }

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(
        const TString& fileName,
        EOpenMode mode,
        i64 priority) override
    {
        const auto& invoker = (mode & RdOnly)
            ? ReadInvoker_
            : WriteInvoker_;

        return BIND(&TThreadedIOEngine::DoOpen, MakeStrong(this), fileName, mode)
            .AsyncVia(CreateFixedPriorityInvoker(invoker, priority))
            .Run();
    }

    virtual TFuture<void> Close(
        const std::shared_ptr<TFileHandle>& handle,
        i64 newSize,
        bool flush)
    {
        return BIND(&TThreadedIOEngine::DoClose, MakeStrong(this), handle, newSize, flush)
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
        size_t len,
        i64 offset,
        i64 priority) override
    {
        TWallTimer timer;
        return BIND(&TThreadedIOEngine::DoPread, MakeStrong(this), handle, len, offset, timer)
            .AsyncVia(CreateFixedPriorityInvoker(ReadInvoker_, priority))
            .Run();
    }

    virtual TFuture<TSharedMutableRef> ReadAll(
        const TString& fileName,
        i64 priority = std::numeric_limits<i64>::max()) override
    {
        TWallTimer timer;
        return BIND(&TThreadedIOEngine::DoReadAll, MakeStrong(this), fileName, timer)
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

        auto useDirectIO = UseDirectIO_ || IsDirectAligned(handle);
        YCHECK(!useDirectIO || IsAligned(reinterpret_cast<ui64>(data.Begin()), Alignment_));
        YCHECK(!useDirectIO || IsAligned(data.Size(), Alignment_));
        YCHECK(!useDirectIO || IsAligned(offset, Alignment_));
        return BIND(&TThreadedIOEngine::DoPwrite, MakeStrong(this), handle, data, offset, timer)
            .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
            .Run();
    }

    virtual TFuture<bool> FlushData(
        const std::shared_ptr<TFileHandle>& handle,
        i64 priority) override
    {
        if (UseDirectIO_) {
            return TrueFuture;
        } else {
            return BIND(&TThreadedIOEngine::DoFlushData, MakeStrong(this), handle)
                .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
                .Run();
        }
    }

    virtual TFuture<bool> Flush(
        const std::shared_ptr<TFileHandle>& handle,
        i64 priority) override
    {
        if (UseDirectIO_) {
            return TrueFuture;
        } else {
            return BIND(&TThreadedIOEngine::DoFlush, MakeStrong(this), handle)
                .AsyncVia(CreateFixedPriorityInvoker(WriteInvoker_, priority))
                .Run();
        }
    }

    virtual bool IsSick() const override
    {
        return Sick_;
    }

private:
    const TConfigPtr Config_;
    const size_t MaxBytesPerRead = 1_GB;
    const TThreadPoolPtr ReadThreadPool_;
    const TThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr ReadInvoker_;
    const IPrioritizedInvokerPtr WriteInvoker_;
    const TProfiler Profiler_;
    const NLogging::TLogger Logger;

    const bool UseDirectIO_;
    const i64 Alignment_ = 4_KB;

    TSpinLock ReadWaitSpinLock_;
    TNullable<TInstant> SickReadWaitStart_;

    TSpinLock WriteWaitSpinLock_;
    TNullable<TInstant> SickWriteWaitStart_;

    std::atomic<bool> Sick_ = { false };
    std::atomic<i64> SicknessCounter_ = { 0 };

    NProfiling::TSimpleGauge SickGauge_{"/sick"};
    NProfiling::TSimpleGauge SickEventsCount_{"/sick_events"};

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
        return handle->FlushData();
    }

    bool DoFlush(const std::shared_ptr<TFileHandle>& handle)
    {
        return handle->Flush();
    }

    void DoClose(const std::shared_ptr<TFileHandle>& handle, i64 newSize, bool flush)
    {
        NFS::ExpectIOErrors([&]() {
            if (newSize >= 0) {
                handle->Resize(newSize);
            }
            if (flush) {
                handle->Flush();
            }
            handle->Close();
        });
    }

    void DoFlushDirectory(const TString& path)
    {
        NFS::ExpectIOErrors([&]() {
            NFS::FlushDirectory(path);
        });
    }

    std::shared_ptr<TFileHandle> DoOpen(const TString& fileName, EOpenMode mode)
    {
        auto handle = std::make_shared<TFileHandle>(fileName, mode);
        if (!handle->IsOpen()) {
            THROW_ERROR_EXCEPTION(
                "Cannot open %Qv with mode %v",
                fileName,
                mode)
                << TError::FromSystem();
        }
        if (UseDirectIO_ || mode & DirectAligned) {
            handle->SetDirect();
        }
        return handle;
    }

    TSharedMutableRef DoPread(const std::shared_ptr<TFileHandle>& handle, size_t numBytes, i64 offset, TWallTimer timer)
    {
        AddReadWaitTimeSample(timer.GetElapsedTime());

        auto data = TSharedMutableRef::Allocate<TDefaultEngineDataBufferTag>(numBytes + UseDirectIO_ * 3 * Alignment_, false);
        i64 from = offset;
        i64 to = offset + numBytes;

        bool useDirectIO = UseDirectIO_ || IsDirectAligned(handle);

        if (useDirectIO) {
            data = data.Slice(AlignUp(data.Begin(), Alignment_), data.End());
            from = ::AlignDown(offset, Alignment_);
            to = ::AlignUp(to, Alignment_);
        }

        size_t readPortion = to - from;
        auto delta = offset - from;

        size_t result;
        ui8* buf = reinterpret_cast<ui8*>(data.Begin());

        YCHECK(readPortion <= data.Size());

        NFS::ExpectIOErrors([&]() {
            while (readPortion > 0) {
                const i32 toRead = static_cast<i32>(Min(MaxBytesPerRead, readPortion));
                const i32 reallyRead = handle->Pread(buf, toRead, from);

                if (reallyRead < 0) {
                    // TODO(aozeritsky): ythrow is placed here consciously.
                    // ExpectIOErrors rethrows some kind of arcadia-style exception.
                    // So in order to keep the old behaviour we should use ythrow or
                    // rewrite ExpectIOErrors.
                    ythrow TFileError();
                }

                if (reallyRead == 0) { // file exausted
                    break;
                }

                buf += reallyRead;
                from += reallyRead;
                readPortion -= reallyRead;

                if (useDirectIO && reallyRead < toRead) {
                    if (reallyRead != ::AlignUp<i32>(reallyRead, Alignment_)) {
                        if (from == handle->GetLength()) {
                            break;
                        } else {
                            THROW_ERROR_EXCEPTION("Unaligned pread")
                                << TErrorAttribute("requested_bytes", toRead)
                                << TErrorAttribute("read_bytes", reallyRead);
                        }
                    }
                }
            }

            result = buf - reinterpret_cast<ui8*>(data.Begin()) - delta;
        });

        return data.Slice(delta, delta + Min(result, numBytes));
    }

    TSharedMutableRef DoReadAll(const TString& fileName, TWallTimer timer)
    {
        AddReadWaitTimeSample(timer.GetElapsedTime());

        EOpenMode mode = RdOnly;

        auto file = DoOpen(fileName, mode);
        auto data = DoPread(file, file->GetLength(), 0, timer);
        DoClose(file, -1, false);
        return data;
    }

    void DoPwrite(const std::shared_ptr<TFileHandle>& handle, const TSharedRef& data, i64 offset, TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());

        const ui8* buf = reinterpret_cast<const ui8*>(data.Begin());
        size_t numBytes = data.Size();

        NFS::ExpectIOErrors([&]() {
            while (numBytes) {
                const i32 toWrite = static_cast<i32>(Min(MaxBytesPerRead, numBytes));
                const i32 reallyWritten = handle->Pwrite(buf, toWrite, offset);

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                buf += reallyWritten;
                offset += reallyWritten;
                numBytes -= reallyWritten;
            }
        });
    }

    void AddWriteWaitTimeSample(TDuration duration)
    {
        if (Config_->SickWriteTimeThreshold && Config_->SickWriteTimeWindow && Config_->SicknessExpirationTimeout && !Sick_) {
            if (duration > *Config_->SickWriteTimeThreshold) {
                auto now = GetInstant();
                auto guard = Guard(WriteWaitSpinLock_);
                if (!SickWriteWaitStart_) {
                    SickWriteWaitStart_ = now;
                } else if (now - *SickWriteWaitStart_ > *Config_->SickWriteTimeWindow) {
                    auto error = TError("Write is too slow")
                        << TErrorAttribute("sick_write_wait_start", *SickWriteWaitStart_);
                    guard.Release();
                    SetSickFlag(error);
                }
            } else {
                auto guard = Guard(WriteWaitSpinLock_);
                SickWriteWaitStart_.Reset();
            }
        }

        UpdateSicknessProfiling();
    }

    void AddReadWaitTimeSample(TDuration duration)
    {
        if (Config_->SickReadTimeThreshold && Config_->SickReadTimeWindow && Config_->SicknessExpirationTimeout && !Sick_) {
            if (duration > *Config_->SickReadTimeThreshold) {
                auto now = GetInstant();
                auto guard = Guard(ReadWaitSpinLock_);
                if (!SickReadWaitStart_) {
                    SickReadWaitStart_ = now;
                } else if (now - *SickReadWaitStart_ > *Config_->SickReadTimeWindow) {
                    auto error = TError("Read is too slow")
                        << TErrorAttribute("sick_read_wait_start", *SickReadWaitStart_);
                    guard.Release();
                    SetSickFlag(error);
                }
            } else {
                auto guard = Guard(ReadWaitSpinLock_);
                SickReadWaitStart_.Reset();
            }
        }

        UpdateSicknessProfiling();
    }

    void SetSickFlag(const TError& error)
    {
        bool expected = false;
        if (Sick_.compare_exchange_strong(expected, true)) {
            ++SicknessCounter_;
            TDelayedExecutor::Submit(
                BIND(&TThreadedIOEngine::ResetSickFlag, MakeStrong(this)),
                *Config_->SicknessExpirationTimeout);

            LOG_WARNING(error, "Location is sick");
        }
    }

    void ResetSickFlag()
    {
        {
            auto guard = Guard(WriteWaitSpinLock_);
            SickWriteWaitStart_.Reset();
        }

        {
            auto guard = Guard(ReadWaitSpinLock_);
            SickReadWaitStart_.Reset();
        }

        Sick_ = false;

        LOG_WARNING("Reset sick flag");
    }

    void UpdateSicknessProfiling()
    {
        Profiler_.Update(SickGauge_, Sick_.load());
        Profiler_.Update(SickEventsCount_, SicknessCounter_.load());
    }
};

#ifdef _linux_

DECLARE_REFCOUNTED_STRUCT(IAioOperation)

struct IAioOperation
    : public TRefCounted
    , public iocb
{
    virtual void Start(TAsyncSemaphoreGuard&& guard) = 0;
    virtual void Complete(const io_event& ev) = 0;
    virtual void Fail(const std::exception& ex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAioOperation)

class TAioOperation
    : public IAioOperation
{
public:
    virtual void Start(TAsyncSemaphoreGuard&& guard) override
    {
        Guard_ = std::move(guard);
    }

    virtual void Complete(const io_event& ev) override
    {
        DoComplete(ev);
        Guard_.Release();
    }

    virtual void Fail(const std::exception& ex) override
    {
        DoFail(ex);
        Guard_.Release();
    }

private:
    TAsyncSemaphoreGuard Guard_;

    virtual void DoComplete(const io_event& ev) = 0;
    virtual void DoFail(const std::exception& ex) = 0;
};

class TAioReadOperation
    : public TAioOperation
{
public:
    TAioReadOperation(
        const std::shared_ptr<TFileHandle>& handle,
        size_t len,
        i64 offset,
        i64 alignment)
        : Data_(TSharedMutableRef::Allocate<TAioEngineDataBufferTag>(len + 3 * alignment, true))
        , FH_(handle)
        , Length_(len)
        , Offset_(offset)
        , From_(::AlignDown(offset, alignment))
        , To_(::AlignUp((i64)(offset + len), alignment))
        , Alignment_(alignment)
    {
        Data_ = Data_.Slice(AlignUp(Data_.Begin(), Alignment_), Data_.End());

        memset(static_cast<iocb*>(this), 0, sizeof(iocb));

        aio_fildes = static_cast<FHANDLE>(*handle);
        aio_lio_opcode = IOCB_CMD_PREAD;

        aio_buf = reinterpret_cast<ui64>(Data_.Begin());
        aio_offset = From_;
        aio_nbytes = To_ - From_;

        YCHECK(IsAligned(aio_buf, alignment));
        YCHECK(IsAligned(aio_nbytes, alignment));
        YCHECK(IsAligned(aio_offset, alignment));
    }

    TFuture<TSharedMutableRef> Result()
    {
        return Result_;
    }

private:
    TSharedMutableRef Data_;
    std::shared_ptr<TFileHandle> FH_;
    const size_t Length_;
    const i64 Offset_;

    const i64 From_;
    const i64 To_;

    const i64 Alignment_;

    TPromise<TSharedMutableRef> Result_ = NewPromise<TSharedMutableRef>();

    virtual void DoComplete(const io_event& ev) override
    {
        auto delta = Offset_ - From_;
        auto result = ev.res - delta;
        Data_ = Data_.Slice(delta, delta + Min(static_cast<size_t>(result), Length_));
        Result_.Set(Data_);
    }

    virtual void DoFail(const std::exception& ex) override
    {
        Result_.Set(TError(ex));
    }
};

class TAioWriteOperation
    : public TAioOperation
{
public:
    TAioWriteOperation(
        const std::shared_ptr<TFileHandle>& handle,
        const TSharedRef& data,
        i64 offset,
        i64 alignment)
        : Data_(data)
        , Fh_(handle)
    {
        memset(static_cast<iocb*>(this), 0, sizeof(iocb));

        aio_fildes = static_cast<FHANDLE>(*handle);
        aio_lio_opcode = IOCB_CMD_PWRITE;

        aio_buf = reinterpret_cast<ui64>(Data_.Begin());
        aio_offset = offset;
        aio_nbytes = data.Size();

        YCHECK(IsAligned(aio_buf, alignment));
        YCHECK(IsAligned(aio_nbytes, alignment));
        YCHECK(IsAligned(aio_offset, alignment));
    }

    TFuture<void> Result()
    {
        return Result_;
    }

private:
    TSharedRef Data_;
    std::shared_ptr<TFileHandle> Fh_;

    TPromise<void> Result_ = NewPromise<void>();

    virtual void DoComplete(const io_event& ev) override
    {
        Result_.Set();
    }

    virtual void DoFail(const std::exception& ex) override
    {
        Result_.Set(TError(ex));
    }
};

class TAioEngineConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxQueueSize;

    TAioEngineConfig()
    {
        RegisterParameter("max_queue_size", MaxQueueSize)
            .GreaterThanOrEqual(1)
            .Default(128);
    }
};

class TAioEngine
    : public IIOEngine
{
public:
    using TConfig = TAioEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TAioEngine(const TConfigPtr& config, const TString& locationId, const TProfiler& profiler)
        : Profiler_(profiler)
        , MaxQueueSize_(config->MaxQueueSize)
        , Semaphore_(New<TProfiledAsyncSemaphore>(MaxQueueSize_, Profiler_, "/waiting_ops"))
        , Thread_(TThread::TParams(StaticLoop, this).SetName(Format("DiskEvents:%v", locationId)))
        , ThreadPool_(New<TThreadPool>(1, Format("FileOpener:%v", locationId)))
    {
        auto ret = io_setup(MaxQueueSize_, &Ctx_);
        if (ret < 0) {
            THROW_ERROR_EXCEPTION("Cannot initialize AIO")
                << TError::FromSystem();
        }

        Start();
    }

    ~TAioEngine() override
    {
        io_destroy(Ctx_);
        Stop();
    }

    virtual TFuture<TSharedMutableRef> Pread(
        const std::shared_ptr<TFileHandle>& handle, size_t len, i64 offset, i64 priority) override
    {
        auto op = New<TAioReadOperation>(handle, len, offset, Alignment_);
        Submit(op);
        return op->Result();
    }

    virtual TFuture<void> Pwrite(
        const std::shared_ptr<TFileHandle>& handle, const TSharedRef& data, i64 offset, i64 priority) override
    {
        auto op = New<TAioWriteOperation>(handle, data, offset, Alignment_);
        Submit(op);
        return op->Result();
    }

    virtual TFuture<bool> FlushData(const std::shared_ptr<TFileHandle>& handle, i64 priority) override
    {
        return TrueFuture;
    }

    virtual TFuture<bool> Flush(const std::shared_ptr<TFileHandle>& handle, i64 priority) override
    {
        return TrueFuture;
    }

    virtual TFuture<std::shared_ptr<TFileHandle>> Open(const TString& fileName, EOpenMode mode, i64 priority) override
    {
        return BIND(&TAioEngine::DoOpen, MakeStrong(this), fileName, mode)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    virtual bool IsSick() const override
    {
        return false;
    }

    virtual TFuture<void> Close(const std::shared_ptr<TFileHandle>& handle, i64 newSize, bool /*flush*/)
    {
        return BIND(&TAioEngine::DoClose, MakeStrong(this), handle, newSize)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> FlushDirectory(const TString& path)
    {
        return BIND(&TAioEngine::DoFlushDirectory, MakeStrong(this), path)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    virtual TFuture<TSharedMutableRef> ReadAll(const TString& fileName, i64 priority)
    {
        return BIND(&TAioEngine::DoReadAll, MakeStrong(this), fileName, priority)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

private:
    const TProfiler Profiler_;

    aio_context_t Ctx_ = 0;
    const int MaxQueueSize_;

    TAsyncSemaphorePtr Semaphore_;
    std::atomic<bool> Alive_ = {true};

    const size_t Alignment_ = 4_KB;

    TThread Thread_;
    const TThreadPoolPtr ThreadPool_;

    std::shared_ptr<TFileHandle> DoOpen(const TString& fileName, EOpenMode mode)
    {
        auto handle = std::make_shared<TFileHandle>(fileName, mode);
        if (!handle->IsOpen()) {
            THROW_ERROR_EXCEPTION(
                "Cannot open %Qv with mode %v",
                fileName,
                mode)
                << TError::FromSystem();
        }
        handle->SetDirect();
        return handle;
    }

    TFuture<TSharedMutableRef> DoReadAll(const TString& fileName, i64 priority)
    {
        auto file = DoOpen(fileName, RdOnly);
        return Pread(file, file->GetLength(), 0, priority);
    }

    void DoClose(const std::shared_ptr<TFileHandle>& handle, i64 newSize)
    {
        NFS::ExpectIOErrors([&]() {
            if (newSize >= 0) {
                handle->Resize(newSize);
            }
            handle->Close();
        });
    }

    void DoFlushDirectory(const TString& path)
    {
        NFS::ExpectIOErrors([&]() {
            NFS::FlushDirectory(path);
        });
    }

    void Loop()
    {
        io_event events[MaxQueueSize_];
        while (Alive_.load(std::memory_order_relaxed)) {
            auto ret = GetEvents(events);
            if (ret < 0) {
                break;
            }

            for (int i = 0; i < ret; ++i) {
                auto* op = static_cast<IAioOperation*>(reinterpret_cast<iocb*>(events[i].obj));
                auto& ev = events[i];

                try {
                    NFS::ExpectIOErrors([&]() {
                        if (ev.res < 0) {
                            ythrow TSystemError(-ev.res);
                        }

                        op->Complete(ev);
                    });
                } catch (const std::exception& ex) {
                    op->Fail(ex);
                }

                op->Unref();
            }
        }
    }

    static void* StaticLoop(void* self)
    {
        reinterpret_cast<TAioEngine*>(self)->Loop();
        return nullptr;
    }

    int GetEvents(io_event* events)
    {
        int ret;
        while ((ret = io_getevents(Ctx_, 1, MaxQueueSize_, events, nullptr)) < 0 && errno == EINTR)
        { }

        YCHECK(ret >= 0 || errno == EINVAL);
        if (ret > 0) {
            NSan::Unpoison(events, sizeof(*events) * ret);
        }
        return ret;
    }

    void Start()
    {
        YCHECK(Alive_.load(std::memory_order_relaxed));
        Thread_.Start();
    }

    void Stop()
    {
        YCHECK(Alive_.load(std::memory_order_relaxed));
        Alive_.store(false, std::memory_order_relaxed);
        Thread_.Join();
    }

    void DoSubmit(struct iocb* cb)
    {
        struct iocb* cbs[1];
        cbs[0] = cb;
        auto ret = io_submit(Ctx_, 1, cbs);

        if (ret < 0) {
            ythrow TSystemError(LastSystemError());
        } else if (ret != 1) {
            THROW_ERROR_EXCEPTION("Unexpected return code from io_submit")
                << TErrorAttribute("code", ret);
        }
    }

    void OnSlotsAvailable(const IAioOperationPtr& op, TAsyncSemaphoreGuard&& guard)
    {
        op->Ref();
        op->Start(std::move(guard));

        try {
            NFS::ExpectIOErrors([&] {
                DoSubmit(op.Get());
            });
        } catch (const std::exception& ex) {
            op->Fail(ex);
            op->Unref();
        }
    }

    void Submit(const IAioOperationPtr& op)
    {
        Semaphore_->AsyncAcquire(BIND(&TAioEngine::OnSlotsAvailable, MakeStrong(this), op), GetSyncInvoker());
    }
};

#endif

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
    const NYTree::INodePtr& ioConfig,
    const TString& locationId,
    const TProfiler& profiler,
    const NLogging::TLogger& logger)
{
    switch (engineType) {
        case EIOEngineType::ThreadPool:
            return CreateIOEngine<TThreadedIOEngine>(ioConfig, locationId, profiler, logger);
#ifdef _linux_
        case EIOEngineType::Aio:
            return CreateIOEngine<TAioEngine>(ioConfig, locationId, profiler);
#endif
        default:
            THROW_ERROR_EXCEPTION("Unknown IO engine %Qlv", engineType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
