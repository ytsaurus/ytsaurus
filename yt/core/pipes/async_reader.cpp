#include "async_reader.h"
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

DEFINE_ENUM(EReaderState,
    (Active)
    (EndOfStream)
    (Failed)
    (Aborted)
);

class TAsyncReaderImpl
    : public TRefCounted
{
public:
    explicit TAsyncReaderImpl(int fd)
        : FD_(fd)
    {
        BIND([=, this_ = MakeStrong(this)] () {
            InitWatcher();
        })
        .Via(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    explicit TAsyncReaderImpl(const TString& str)
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

    ~TAsyncReaderImpl()
    {
        YCHECK(State_ != EReaderState::Active || AbortRequested_);
    }

    int GetHandle() const
    {
        return FD_;
    }

    TFuture<size_t> Read(const TSharedMutableRef& buffer)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(buffer.Size() > 0);

        auto promise = NewPromise<size_t>();

        TIODispatcher::Get()->GetInvoker()->Invoke(BIND([=, this_ = MakeStrong(this)] () {
            YCHECK(ReadResultPromise_.IsSet());
            ReadResultPromise_ = promise;

            UpdateDurationCounter(&TotalIdleMilliseconds_);

            switch (State_) {
                case EReaderState::Aborted:
                    ReadResultPromise_.Set(TError(EErrorCode::Aborted, "Reader aborted")
                        << TErrorAttribute("fd", FD_));
                    break;

                case EReaderState::EndOfStream:
                    ReadResultPromise_.Set(0);
                    break;

                case EReaderState::Failed:
                    ReadResultPromise_.Set(Error_);
                    break;

                case EReaderState::Active:
                    Buffer_ = buffer;
                    Position_ = 0;
                    if (!FDWatcher_->is_active()) {
                        FDWatcher_->start();
                    }
                    break;

                default:
                    Y_UNREACHABLE();
            };
        }));

        return promise.ToFuture();
    }

    TFuture<void> Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        AbortRequested_ = true;
        return BIND([=, this_ = MakeStrong(this)] () {
                if (State_ == EReaderState::Active) {
                    State_ = EReaderState::Aborted;
                    FDWatcher_->stop();

                    if (ReadResultPromise_.TrySet(TError(EErrorCode::Aborted, "Reader aborted")
                        << TErrorAttribute("fd", FD_)))
                    {
                        UpdateDurationCounter(&TotalBusyMilliseconds_);
                    } else {
                        UpdateDurationCounter(&TotalIdleMilliseconds_);
                    }
                    Close();
                }
            })
            .AsyncVia(TIODispatcher::Get()->GetInvoker())
            .Run();
    }

    TFuture<TDuration> GetIdleDuration() const
    {
        return BIND([this, this_ = MakeStrong(this)] () {
            if (State_ == EReaderState::Active && ReadResultPromise_.IsSet()) {
                UpdateDurationCounter(&TotalIdleMilliseconds_);
            }
            return TDuration::MilliSeconds(TotalIdleMilliseconds_);
        })
        .AsyncVia(TIODispatcher::Get()->GetInvoker())
        .Run();
    }

    TFuture<TDuration> GetBusyDuration() const
    {
        return BIND([this, this_ = MakeStrong(this)] () {
            if (State_ == EReaderState::Active && !ReadResultPromise_.IsSet()) {
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

    TPromise<size_t> ReadResultPromise_ = MakePromise<size_t>(0);

    std::atomic<bool> AbortRequested_ = { false };
    EReaderState State_ = EReaderState::Active;
    TError Error_;

    TSharedMutableRef Buffer_;
    int Position_ = 0;

    //! Start instant of current reader state (either busy or idle).
    mutable TCpuInstant StartTime_;

    mutable i64 TotalBusyMilliseconds_ = 0;
    mutable i64 TotalIdleMilliseconds_ = 0;

    std::atomic<i64> ByteCount_ = { 0 };

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    void OnRead(ev::io&, int eventType)
    {
        VERIFY_THREAD_AFFINITY(EventLoop);
        YCHECK((eventType & ev::READ) == ev::READ);
        YCHECK(State_ == EReaderState::Active);

        while (!ReadResultPromise_.IsSet()) {
            DoRead();
            YCHECK(Position_ != 0 || ReadResultPromise_.IsSet());
        }
    }

    void DoRead()
    {
#ifdef _unix_
        YCHECK(Position_ < Buffer_.Size());
        YCHECK(!ReadResultPromise_.IsSet());

        ssize_t size = HandleEintr(::read, FD_, Buffer_.Begin() + Position_, Buffer_.Size() - Position_);

        if (size == -1) {
            if (errno == EAGAIN) {
                if (Position_ != 0) {
                    FDWatcher_->stop();
                    SetResultPromise(Position_);
                }
                return;
            }

            YCHECK(errno != EBADF);

            Error_ = TError("Reader failed")
                << TErrorAttribute("fd", FD_)
                << TError::FromSystem();
            LOG_ERROR(Error_);
            Close();

            State_ = EReaderState::Failed;
            FDWatcher_->stop();
            if (Position_ != 0) {
                SetResultPromise(Position_);
            } else {
                SetResultPromise(Error_);
            }
            return;
        }

        ByteCount_ += size;
        Position_ += size;

        if (size == 0) {
            State_ = EReaderState::EndOfStream;
            FDWatcher_->stop();
            Close();
            SetResultPromise(Position_);
        } else if (Position_ == Buffer_.Size()) {
            FDWatcher_->stop();
            SetResultPromise(Position_);
        }
#else
        THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    }

    bool InitFD(const TString& str)
    {
        FD_ = HandleEintr(::open, str.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC);
        if (FD_ == -1) {
            State_ = EReaderState::Failed;
            Error_ = TError("Open failed")
                << TErrorAttribute("path", str)
                << TError::FromSystem();
            LOG_ERROR(Error_);
            return false;
        }
        return true;
    }

    void InitWatcher()
    {
        StartTime_ = GetCpuInstant();
        FDWatcher_.Emplace();
        FDWatcher_->set(FD_, ev::READ);
        FDWatcher_->set(TIODispatcher::Get()->GetEventLoop());
        FDWatcher_->set<TAsyncReaderImpl, &TAsyncReaderImpl::OnRead>(this);
    }

    void Close()
    {
        YCHECK(TryClose(FD_, false));
        FD_ = TPipe::InvalidFD;
    }

    // NB: Not really const, but is called from duration getters.
    void UpdateDurationCounter(i64* counter) const
    {
        auto now = GetCpuInstant();
        auto duration = now - StartTime_;
        StartTime_ = now;
        *counter += CpuDurationToDuration(duration).MilliSeconds();
    }

    void SetResultPromise(size_t size)
    {
        ReadResultPromise_.Set(size);
        UpdateDurationCounter(&TotalBusyMilliseconds_);
    }

    void SetResultPromise(const TError& error)
    {
        ReadResultPromise_.Set(error);
        UpdateDurationCounter(&TotalBusyMilliseconds_);
    }
};

DEFINE_REFCOUNTED_TYPE(TAsyncReaderImpl)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(int fd)
    : Impl_(New<NDetail::TAsyncReaderImpl>(fd))
{ }

TAsyncReader::TAsyncReader(TNamedPipePtr ptr)
    : Impl_(New<NDetail::TAsyncReaderImpl>(ptr->GetPath()))
    , NamedPipeHolder_(ptr)
{ }

TAsyncReader::~TAsyncReader()
{
    // Abort does not fail.
    Impl_->Abort();
}

int TAsyncReader::GetHandle() const
{
    return Impl_->GetHandle();
}

TFuture<size_t> TAsyncReader::Read(const TSharedMutableRef& buffer)
{
    return Impl_->Read(buffer);
}

TFuture<void> TAsyncReader::Abort()
{
    return Impl_->Abort();
}

TFuture<TDuration> TAsyncReader::GetBusyDuration() const
{
    return Impl_->GetBusyDuration();
}

TFuture<TDuration> TAsyncReader::GetIdleDuration() const
{
    return Impl_->GetIdleDuration();
}

i64 TAsyncReader::GetByteCount() const
{
    return Impl_->GetByteCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
