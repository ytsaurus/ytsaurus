#include "stdafx.h"
#include "async_reader.h"

#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/pipe.h>
#include <core/misc/proc.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = PipesLogger;

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
            FDWatcher_.set(FD_, ev::READ);
            FDWatcher_.set(TIODispatcher::Get()->Impl_->GetEventLoop());
            FDWatcher_.set<TAsyncReaderImpl, &TAsyncReaderImpl::OnRead>(this);
            FDWatcher_.start();
        })
        .Via(TIODispatcher::Get()->Impl_->GetInvoker())
        .Run();
    }

    ~TAsyncReaderImpl()
    {
        YCHECK(State_ != EReaderState::Active);
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

        TIODispatcher::Get()->Impl_->GetInvoker()->Invoke(BIND([=, this_ = MakeStrong(this)] () {
            YCHECK(ReadResultPromise_.IsSet());
            ReadResultPromise_ = promise;

            switch (State_) {
                case EReaderState::Aborted:
                    ReadResultPromise_.Set(TError(EErrorCode::Aborted, "Reader aborted")
                        << TErrorAttribute("fd", FD_));
                    break;

                case EReaderState::EndOfStream:
                    ReadResultPromise_.Set(0);
                    break;

                case EReaderState::Failed:
                    ReadResultPromise_.Set(TError("Reader failed")
                        << TErrorAttribute("fd", FD_));
                    break;

                case EReaderState::Active:
                    Buffer_ = buffer;
                    Position_ = 0;
                    if (!FDWatcher_.is_active()) {
                        FDWatcher_.start();
                    }
                    break;

                default:
                    YUNREACHABLE();
            };
        }));

        return promise.ToFuture();
    }

    TFuture<void> Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND([=, this_ = MakeStrong(this)] () {
                if (State_ == EReaderState::Active) { 
                    State_ = EReaderState::Aborted;
                    FDWatcher_.stop();
                    ReadResultPromise_.TrySet(TError(EErrorCode::Aborted, "Reader aborted")
                        << TErrorAttribute("fd", FD_));
                    Close();
                }
            })
            .AsyncVia(TIODispatcher::Get()->Impl_->GetInvoker())
            .Run();
    }

private:
    int FD_;

    //! \note Thread-unsafe. Must be accessed from ev-thread only.
    ev::io FDWatcher_;

    TPromise<size_t> ReadResultPromise_ = MakePromise<size_t>(0);

    EReaderState State_ = EReaderState::Active;

    TSharedMutableRef Buffer_;
    int Position_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    void OnRead(ev::io&, int eventType)
    {
        VERIFY_THREAD_AFFINITY(EventLoop);
        YCHECK((eventType & ev::READ) == ev::READ);

        YCHECK(State_ == EReaderState::Active);

        if (Position_ < Buffer_.Size()) {
            DoRead();
        } else {
            FDWatcher_.stop();
        }
    }

    void DoRead()
    {
#ifndef _win_
        YCHECK(Position_ < Buffer_.Size());

        int size;
        do {
            size = ::read(FD_, Buffer_.Begin() + Position_, Buffer_.Size() - Position_);
        } while (size == -1 && errno == EINTR);

        if (size == -1) {
            if (errno == EAGAIN) {
                return;
            }

            YCHECK(errno != EBADF);

            auto error = TError("Reader failed")
                << TErrorAttribute("fd", FD_)
                << TError::FromSystem();
            LOG_ERROR(error);
            Close();

            State_ = EReaderState::Failed;
            FDWatcher_.stop();
            ReadResultPromise_.Set(error);
            return;
        }

        Position_ += size;

        if (size == 0) {
            State_ = EReaderState::EndOfStream;
            FDWatcher_.stop();
            Close();
            ReadResultPromise_.Set(Position_);
        } else if (Position_ == Buffer_.Size()) {
            ReadResultPromise_.Set(Buffer_.Size());
        }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    }

    void Close()
    {
        YCHECK(TryClose(FD_, false));
        FD_ = TPipe::InvalidFD;
    }
};

DEFINE_REFCOUNTED_TYPE(TAsyncReaderImpl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(int fd)
    : Impl_(New<NDetail::TAsyncReaderImpl>(fd))
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
