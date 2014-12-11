#include "stdafx.h"
#include "async_reader.h"

#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/proc.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = PipesLogger;

////////////////////////////////////////////////////////////////////////////////

class TAsyncReaderImpl
    : public TRefCounted
{
public:
    explicit TAsyncReaderImpl(int fd)
        : FD_(fd)
    {
        auto this_ = MakeStrong(this);
        BIND([=] () {
            UNUSED(this_);

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

    TFuture<TErrorOr<size_t>> Read(void* buffer, int length)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(length > 0);

        auto promise = NewPromise<TErrorOr<size_t>>();

        auto this_ = MakeStrong(this);
        BIND([=] () {
            UNUSED(this_);

            YCHECK(ReadResultPromise_.IsSet());
            ReadResultPromise_ = promise;

            switch (State_) {
                case EReaderState::Aborted:
                    ReadResultPromise_.Set(TError("Reader aborted")
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
                    Length_ = length;
                    Position_ = 0;
                    if (!FDWatcher_.is_active()) {
                        FDWatcher_.start();
                    }
                    break;

                default:
                    YUNREACHABLE();
            };
        })
        .Via(TIODispatcher::Get()->Impl_->GetInvoker())
        .Run();

        return promise.ToFuture();
    }

    TFuture<void> Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto this_ = MakeStrong(this);
        return BIND([=] () {
            UNUSED(this_);

            if (State_ != EReaderState::Active) {
                return;
            }

            State_ = EReaderState::Aborted;
            FDWatcher_.stop();
            ReadResultPromise_.TrySet(TError("Reader aborted")
                << TErrorAttribute("fd", FD_));

            YCHECK(TryClose(FD_));
        })
        .AsyncVia(TIODispatcher::Get()->Impl_->GetInvoker())
        .Run();
    }

private:
    DECLARE_ENUM(EReaderState,
        (Active)
        (EndOfStream)
        (Failed)
        (Aborted)
    );

    int FD_;

    //! \note Thread-unsafe. Must be accessed from ev-thread only.
    ev::io FDWatcher_;

    TPromise<TErrorOr<size_t>> ReadResultPromise_ = MakePromise<TErrorOr<size_t>>(0);

    EReaderState State_ = EReaderState::Active;

    void* Buffer_ = nullptr;
    int Length_ = 0;
    int Position_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    void OnRead(ev::io&, int eventType)
    {
        VERIFY_THREAD_AFFINITY(EventLoop);
        YCHECK((eventType & ev::READ) == ev::READ);

        YCHECK(State_ == EReaderState::Active);

        if (Position_ < Length_) {
            DoRead();
        } else {
            FDWatcher_.stop();
        }
    }

    void DoRead()
    {
        YCHECK(Position_ < Length_);

        int size;
        do {
            size = ::read(FD_, static_cast<char *>(Buffer_) + Position_, Length_ - Position_);
        } while (size == -1 && errno == EINTR);

        if (size == -1) {
            if (errno == EAGAIN) {
                return;
            }

            auto error = TError("Reader failed")
                << TErrorAttribute("fd", FD_)
                << TError::FromSystem();
            LOG_ERROR(error);

            State_ = EReaderState::Failed;
            FDWatcher_.stop();
            ReadResultPromise_.Set(error);
            return;
        }

        Position_ += size;

        if (size == 0) {
            State_ = EReaderState::EndOfStream;
            FDWatcher_.stop();
            ReadResultPromise_.Set(Position_);
        } else if (Position_ == Length_) {
            ReadResultPromise_.Set(Length_);
        }
    }

};

DEFINE_REFCOUNTED_TYPE(TAsyncReaderImpl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(int fd)
    : Impl_(New<NDetail::TAsyncReaderImpl>(fd))
{ }

TFuture<TErrorOr<size_t>> TAsyncReader::Read(void* buf, size_t len)
{
    return Impl_->Read(buf, len);
}

TFuture<void> TAsyncReader::Abort()
{
    return Impl_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
