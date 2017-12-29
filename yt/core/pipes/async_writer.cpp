#include "async_writer.h"
#include "private.h"
#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"
#include "pipe.h"

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/proc.h>

#include <yt/core/profiling/timing.h>

#include <errno.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = PipesLogger;

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWriterState,
    (Active)
    (Closed)
    (Failed)
    (Aborted)
);

class TAsyncWriterImpl
    : public TRefCounted
{
public:
    explicit TAsyncWriterImpl(int fd)
        : FD_(fd)
    {
        BIND([=, this_ = MakeStrong(this)] () {
            InitWatcher();
        })
        .Via(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    explicit TAsyncWriterImpl(const TString& str)
    {
        BIND([=, this_ = MakeStrong(this)] () {
            if (!InitFD(str)) {
                return;
            }
            InitWatcher();
        })
        .Via(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    ~TAsyncWriterImpl()
    {
        YCHECK(State_ != EWriterState::Active || AbortRequested_);
    }

    int GetHandle() const
    {
        return FD_;
    }

    TFuture<void> Write(const TSharedRef& buffer)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(buffer.Size() > 0);

        auto promise = NewPromise<void>();
        BIND([=, this_ = MakeStrong(this)] () {
            YCHECK(WriteResultPromise_.IsSet());
            WriteResultPromise_ = promise;

            UpdateDurationCounter(&TotalIdleMilliseconds_);

            switch (State_) {
                case EWriterState::Aborted:
                    WriteResultPromise_.Set(TError(EErrorCode::Aborted, "Writer aborted")
                        << TErrorAttribute("fd", FD_));
                    return;

                case EWriterState::Failed:
                    WriteResultPromise_.Set(TError("Writer failed")
                        << TErrorAttribute("fd", FD_));
                    return;

                case EWriterState::Closed:
                    WriteResultPromise_.Set(TError("Writer closed")
                        << TErrorAttribute("fd", FD_));
                    return;

                case EWriterState::Active:
                    Buffer_ = buffer;
                    Position_ = 0;

                    if (!FDWatcher_->is_active()) {
                        FDWatcher_->start();
                    }

                    break;

                default:
                    Y_UNREACHABLE();
            };
        })
        .Via(TIODispatcher::Get()->GetInvoker())
        .Run();

        return promise.ToFuture();
    }

    TFuture<void> Close()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(WriteResultPromise_.IsSet());

        return BIND([=, this_ = MakeStrong(this)] () {
            if (State_ != EWriterState::Active) {
                return;
            }

            State_ = EWriterState::Closed;
            FDWatcher_->stop();
            SafeClose(FD_, false);
            FD_ = TPipe::InvalidFD;
        })
        .AsyncVia(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    TFuture<void> Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        AbortRequested_ = true;

        return BIND([=, this_ = MakeStrong(this)] () {
            if (State_ != EWriterState::Active)
                return;

            State_ = EWriterState::Aborted;
            FDWatcher_->stop();

            if (WriteResultPromise_.TrySet(TError(EErrorCode::Aborted, "Writer aborted")
               << TErrorAttribute("fd", FD_)))
            {
                UpdateDurationCounter(&TotalBusyMilliseconds_);
            } else {
                UpdateDurationCounter(&TotalIdleMilliseconds_);
            }

            YCHECK(TryClose(FD_, false));
            FD_ = TPipe::InvalidFD;
        })
        .AsyncVia(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    TFuture<TDuration> GetIdleDuration() const
    {
        return BIND([this, this_ = MakeStrong(this)] () {
            if (State_ == EWriterState::Active && WriteResultPromise_.IsSet()) {
                UpdateDurationCounter(&TotalIdleMilliseconds_);
            }
            return TDuration::MilliSeconds(TotalIdleMilliseconds_);
        })
        .AsyncVia(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    TFuture<TDuration> GetBusyDuration() const
    {
        return BIND([this, this_ = MakeStrong(this)] ()  {
            if (State_ == EWriterState::Active && !WriteResultPromise_.IsSet()) {
                UpdateDurationCounter(&TotalBusyMilliseconds_);
            }
            return TDuration::MilliSeconds(TotalBusyMilliseconds_);
        })
        .AsyncVia(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    i64 GetByteCount() const
    {
        return ByteCount_;
    }

private:
    int FD_ = -1;

    //! \note Thread-unsafe. Must be accessed from ev-thread only.
    TNullable<ev::io> FDWatcher_;

    TPromise<void> WriteResultPromise_ = MakePromise(TError());

    std::atomic<bool> AbortRequested_ = { false };
    EWriterState State_ = EWriterState::Active;

    TSharedRef Buffer_;
    int Position_ = 0;

    //! Start instant of current reader state (either busy or idle).
    mutable TCpuInstant StartTime_;

    mutable i64 TotalBusyMilliseconds_ = 0;
    mutable i64 TotalIdleMilliseconds_ = 0;

    std::atomic<i64> ByteCount_ = { 0 };

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);

    bool InitFD(const TString& str)
    {
        FD_ = HandleEintr(::open, str.c_str(), O_WRONLY |  O_CLOEXEC);
        if (FD_ == -1) {
            LOG_ERROR(TError::FromSystem(), "Failed to open async writer"
                "for named pipe %v", str);
            State_ = EWriterState::Failed;
            return false;
        }
        try {
            SafeMakeNonblocking(FD_);
        } catch (const std::exception& ex) {
            LOG_ERROR("Failed to set nonblocking mode %v", ex.what());
            return false;
        }
        return true;
    }

    void InitWatcher()
    {
        StartTime_ = GetCpuInstant();
        FDWatcher_.Emplace();
        FDWatcher_->set(FD_, ev::WRITE);
        FDWatcher_->set(TIODispatcher::Get()->GetEventLoop());
        FDWatcher_->set<TAsyncWriterImpl, &TAsyncWriterImpl::OnWrite>(this);
    }

    void OnWrite(ev::io&, int eventType)
    {
        VERIFY_THREAD_AFFINITY(EventLoop);
        YCHECK((eventType & ev::WRITE) == ev::WRITE);
        YCHECK(State_ == EWriterState::Active);

        if (Position_ < Buffer_.Size()) {
            DoWrite();
        } else {
            FDWatcher_->stop();
        }
    }

    void DoWrite()
    {
#ifdef _unix_
        YCHECK(Position_ < Buffer_.Size());

        ssize_t size = HandleEintr(::write, FD_, Buffer_.Begin() + Position_, Buffer_.Size() - Position_);

        if (size == -1) {
            if (errno == EAGAIN) {
                return;
            }

            auto error = TError("Writer failed")
                << TErrorAttribute("fd", FD_)
                << TError::FromSystem();
            LOG_ERROR(error);

            YCHECK(TryClose(FD_, false));
            FD_ = TPipe::InvalidFD;

            State_ = EWriterState::Failed;
            FDWatcher_->stop();
            SetResultPromise(error);
            return;
        }

        YCHECK(size > 0);
        ByteCount_ += size;
        Position_ += size;

        if (Position_ == Buffer_.Size()) {
            SetResultPromise();
        }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    }

    // NB: Not really const, but is called from duration getters.
    void UpdateDurationCounter(i64* counter) const
    {
        auto now = GetCpuInstant();
        auto duration = now - StartTime_;
        StartTime_ = now;
        *counter += CpuDurationToDuration(duration).MilliSeconds();
    }

    void SetResultPromise(const TError& error = TError())
    {
        WriteResultPromise_.Set(error);
        UpdateDurationCounter(&TotalBusyMilliseconds_);
    }
};

DEFINE_REFCOUNTED_TYPE(TAsyncWriterImpl)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TAsyncWriter(int fd)
    : Impl_(New<NDetail::TAsyncWriterImpl>(fd))
{ }

TAsyncWriter::TAsyncWriter(TNamedPipePtr ptr)
    : Impl_(New<NDetail::TAsyncWriterImpl>(ptr->GetPath()))
    , NamedPipeHolder_(ptr)
{ }

TAsyncWriter::~TAsyncWriter()
{
    // Abort does not fail.
    Impl_->Abort();
}

int TAsyncWriter::GetHandle() const
{
    return Impl_->GetHandle();
}

TFuture<void> TAsyncWriter::Write(const TSharedRef& buffer)
{
    return Impl_->Write(buffer);
}

TFuture<void> TAsyncWriter::Close()
{
    return Impl_->Close();
}

TFuture<void> TAsyncWriter::Abort()
{
    return Impl_->Abort();
}

TFuture<TDuration> TAsyncWriter::GetBusyDuration() const
{
    return Impl_->GetBusyDuration();
}

TFuture<TDuration> TAsyncWriter::GetIdleDuration() const
{
    return Impl_->GetIdleDuration();
}

i64 TAsyncWriter::GetByteCount() const
{
    return Impl_->GetByteCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
