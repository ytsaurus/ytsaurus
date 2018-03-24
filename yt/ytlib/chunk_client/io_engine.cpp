#include "io_engine.h"

#include <util/system/platform.h>

#ifdef _linux_
#include <linux/aio_abi.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/align.h>
#include <yt/core/misc/fs.h>

#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/align.h>

namespace NYT {
namespace NChunkClient {

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
    : public NYTree::TYsonSerializableLite
{
public:
    int Threads;
    bool UseDirectIO;

    TThreadedIOEngineConfig()
    {
        RegisterParameter("threads", Threads)
            .GreaterThanOrEqual(1)
            .Default(1);
        RegisterParameter("use_direct_io", UseDirectIO)
            .Default(false);
    }
};

class TThreadedIOEngine
    : public IIOEngine
{
public:
    using TConfigType = TThreadedIOEngineConfig;

    explicit TThreadedIOEngine(const TConfigType& config)
        : ThreadPool_(config.Threads, "DiskIO")
        , UseDirectIO_(config.UseDirectIO)
    { }

    virtual std::shared_ptr<TFileHandle> Open(const TString& fName, EOpenMode oMode) override
    {
        auto fh = std::make_shared<TFileHandle>(fName, oMode);
        if (!fh->IsOpen()) {
            THROW_ERROR_EXCEPTION("Cannot open %Qv with mode %v",
                fName,
                oMode) << TError::FromSystem();
        }
        if (UseDirectIO_) {
            fh->SetDirect();
        }
        return fh;
    }

    virtual TFuture<TSharedMutableRef> Pread(const std::shared_ptr<TFileHandle>& fh, size_t len, i64 offset) override
    {
        return BIND(&TThreadedIOEngine::DoPread, MakeStrong(this), fh, len, offset)
            .AsyncVia(ThreadPool_.GetInvoker())
            .Run();
    }

    virtual TFuture<void> Pwrite(const std::shared_ptr<TFileHandle>& fh, const TSharedMutableRef& data, i64 offset) override
    {
        YCHECK(!UseDirectIO_ || IsAligned(reinterpret_cast<ui64>(data.Begin()), Alignment_));
        YCHECK(!UseDirectIO_ || IsAligned(data.Size(), Alignment_));
        YCHECK(!UseDirectIO_ || IsAligned(offset, Alignment_));
        return BIND(&TThreadedIOEngine::DoPwrite, MakeStrong(this), fh, data, offset)
            .AsyncVia(ThreadPool_.GetInvoker())
            .Run();
    }

    virtual TFuture<bool> FlushData(const std::shared_ptr<TFileHandle>& fh) override
    {
        if (UseDirectIO_) {
            return TrueFuture;
        } else {
            return BIND(&TThreadedIOEngine::DoFlushData, MakeStrong(this), fh)
                .AsyncVia(ThreadPool_.GetInvoker())
                .Run();
        }
    }

    virtual TFuture<bool> Flush(const std::shared_ptr<TFileHandle>& fh) override
    {
        if (UseDirectIO_) {
            return TrueFuture;
        } else {
            return BIND(&TThreadedIOEngine::DoFlush, MakeStrong(this), fh)
                .AsyncVia(ThreadPool_.GetInvoker())
                .Run();
        }
    }

private:
    const size_t MaxPortion_ = size_t(1 << 30);
    NConcurrency::TThreadPool ThreadPool_;

    bool UseDirectIO_;
    const i64 Alignment_ = 4_KB;

    bool DoFlushData(const std::shared_ptr<TFileHandle>& fh)
    {
        return fh->FlushData();
    }

    bool DoFlush(const std::shared_ptr<TFileHandle>& fh)
    {
        return fh->Flush();
    }

    TSharedMutableRef DoPread(const std::shared_ptr<TFileHandle>& fh, size_t numBytes, i64 offset)
    {
        auto data = TSharedMutableRef::Allocate<TDefaultEngineDataBufferTag>(numBytes + UseDirectIO_ * 3 * Alignment_, false);
        i64 from = offset;
        i64 to = offset + numBytes;

        if (UseDirectIO_) {
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
            while (readPortion) {
                const i32 toRead = static_cast<i32>(Min(MaxPortion_, readPortion));
                const i32 reallyRead = fh->Pread(buf, toRead, from);

                if (reallyRead < 0) {
                    // TODO(aozeritsky): ythrow is placed here consciously.
                    // ExpectIOErrors rethrows some kind of arcadia-style exception.
                    // So in order to keep the old behaviour we should use ythrow or
                    // rewrite ExpectIOErrors.
                    ythrow TFileError();
                }

                buf += reallyRead;
                from += reallyRead;
                readPortion -= reallyRead;

                if (reallyRead == 0) {
                    // End of file.
                    break;
                }

                // FIXME(savrus) Allow pread to break on page boundaries.
                if (reallyRead < toRead && UseDirectIO_) {
                    THROW_ERROR_EXCEPTION("Pread finished early")
                        << TErrorAttribute("use_direct_io", UseDirectIO_)
                        << TErrorAttribute("requested_bytes", toRead)
                        << TErrorAttribute("read_bytes", reallyRead);
                }
            }

            result = buf - reinterpret_cast<ui8*>(data.Begin()) - delta;
        });

        return data.Slice(delta, delta + Min(result, numBytes));
    }

    void DoPwrite(const std::shared_ptr<TFileHandle>& fh, const TSharedMutableRef& data, i64 offset)
    {
        const ui8* buf = reinterpret_cast<ui8*>(data.Begin());
        size_t numBytes = data.Size();

        NFS::ExpectIOErrors([&]() {
            while (numBytes) {
                const i32 toWrite = static_cast<i32>(Min(MaxPortion_, numBytes));
                const i32 reallyWritten = fh->Pwrite(buf, toWrite, offset);

                if (reallyWritten < 0) {
                    ythrow TFileError();
                }

                buf += reallyWritten;
                offset += reallyWritten;
                numBytes -= reallyWritten;
            }
        });
    }
};

#ifdef _linux_

DECLARE_REFCOUNTED_STRUCT(IAioOperation)

struct IAioOperation
    : public TRefCounted
    , public iocb
{
    virtual void Start(NConcurrency::TAsyncSemaphoreGuard&& guard) = 0;
    virtual void Complete(const io_event& ev) = 0;
    virtual void Fail(const std::exception& ex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAioOperation)

class TAioOperation
    : public IAioOperation
{
public:
    virtual void Start(NConcurrency::TAsyncSemaphoreGuard&& guard) override
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
    NConcurrency::TAsyncSemaphoreGuard Guard_;

    virtual void DoComplete(const io_event& ev) = 0;
    virtual void DoFail(const std::exception& ex) = 0;
};

class TAioReadOperation
    : public TAioOperation
{
public:
    TAioReadOperation(
        const std::shared_ptr<TFileHandle>& fh,
        size_t len,
        i64 offset,
        i64 alignment)
        : Data_(TSharedMutableRef::Allocate<TAioEngineDataBufferTag>(len + 3 * alignment, false))
        , FH_(fh)
        , Length_(len)
        , Offset_(offset)
        , From_(::AlignDown(offset, alignment))
        , To_(::AlignUp((i64)(offset + len), alignment))
        , Alignment_(alignment)
    {
        Data_ = Data_.Slice(AlignUp(Data_.Begin(), Alignment_), Data_.End());

        memset(static_cast<iocb*>(this), 0, sizeof(iocb));

        aio_fildes = static_cast<FHANDLE>(*fh);
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
        const std::shared_ptr<TFileHandle>& fh,
        const TSharedMutableRef& data,
        i64 offset,
        i64 alignment)
        : Data_(data)
        , Fh_(fh)
    {
        memset(static_cast<iocb*>(this), 0, sizeof(iocb));

        aio_fildes = static_cast<FHANDLE>(*fh);
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
    TSharedMutableRef Data_;
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
    : public NYTree::TYsonSerializableLite
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
    using TConfigType = TAioEngineConfig;

    explicit TAioEngine(const TAioEngineConfig& config)
        : MaxQueueSize_(config.MaxQueueSize)
        , Semaphore_(MaxQueueSize_)
        , Thread_(StaticLoop, this)
    {
        auto ret = io_setup(MaxQueueSize_, &Ctx_);
        if (ret < 0) {
            THROW_ERROR_EXCEPTION("Cannot initialize AIO") << TError::FromSystem();
        }

        Start();
    }

    ~TAioEngine() override
    {
        io_destroy(Ctx_);
        Stop();
    }

    virtual TFuture<TSharedMutableRef> Pread(const std::shared_ptr<TFileHandle>& fh, size_t len, i64 offset) override
    {
        auto op = New<TAioReadOperation>(fh, len, offset, Alignment_);
        Submit(op);
        return op->Result();
    }

    virtual TFuture<void> Pwrite(const std::shared_ptr<TFileHandle>& fh, const TSharedMutableRef& data, i64 offset) override
    {
        auto op = New<TAioWriteOperation>(fh, data, offset, Alignment_);
        Submit(op);
        return op->Result();
    }

    virtual TFuture<bool> FlushData(const std::shared_ptr<TFileHandle>& fh) override
    {
        return TrueFuture;
    }

    virtual TFuture<bool> Flush(const std::shared_ptr<TFileHandle>& fh) override
    {
        return TrueFuture;
    }

    virtual std::shared_ptr<TFileHandle> Open(const TString& fName, EOpenMode oMode) override
    {
        auto fh = std::make_shared<TFileHandle>(fName, oMode);
        if (!fh->IsOpen()) {
            THROW_ERROR_EXCEPTION("Cannot open %Qv with mode %v",
                fName,
                oMode) << TError::FromSystem();
        }
        fh->SetDirect();
        return fh;
    }

private:
    aio_context_t Ctx_ = 0;
    const int MaxQueueSize_;

    NConcurrency::TAsyncSemaphore Semaphore_;
    int Inflight_ = 0;
    std::atomic<bool> Alive_ = {true};

    const i64 Alignment_ = 4_KB;

    TThread Thread_;

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

    static void* StaticLoop(void * self)
    {
        reinterpret_cast<TAioEngine*>(self)->Loop();
        return nullptr;
    }

    int GetEvents(io_event * events)
    {
        int ret;
        while ((ret = io_getevents(Ctx_, 1, MaxQueueSize_, events, nullptr)) < 0 && errno == EINTR)
        { }

        YCHECK(ret >= 0 || errno == EINVAL);
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
            THROW_ERROR_EXCEPTION("Unexpected return code from io_submit") << TErrorAttribute("code", ret);
        }
    }

    void OnSlotsAvailable(const IAioOperationPtr& op, NConcurrency::TAsyncSemaphoreGuard&& guard)
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
        Semaphore_.AsyncAcquire(BIND(&TAioEngine::OnSlotsAvailable, MakeStrong(this), op), GetSyncInvoker());
    }
};

#endif

template <typename T>
IIOEnginePtr CreateIOEngine(const NYTree::INodePtr& ioConfig)
{
    typename T::TConfigType config;
    config.SetDefaults();
    if (ioConfig) {
        config.Load(ioConfig);
    }

    return New<T>(config);
}

IIOEnginePtr CreateIOEngine(NDataNode::EIOEngineType ioType, const NYTree::INodePtr& ioConfig)
{
    switch (ioType) {
        case NDataNode::EIOEngineType::ThreadPool:
            return CreateIOEngine<TThreadedIOEngine>(ioConfig);
    #ifdef _linux_
        case NDataNode::EIOEngineType::Aio:
            return CreateIOEngine<TAioEngine>(ioConfig);
    #endif
        default:
            THROW_ERROR_EXCEPTION("Unknown IO engine %Qlv", ioType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
