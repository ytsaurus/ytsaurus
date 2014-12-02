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

static const auto& Logger = PipesLogger;

////////////////////////////////////////////////////////////////////////////////

class TAsyncReaderImpl
    : public TRefCounted
{
public:
    explicit TAsyncReaderImpl(int fd);
    ~TAsyncReaderImpl();

    TFuture<TErrorOr<size_t>> Read(void* buffer, int length);

    TFuture<void> Abort();

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

    TPromise<TErrorOr<size_t>> ReadResultPromise_;

    EReaderState State_;

    void* Buffer_;
    int Length_;
    int Position_;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    void OnRead(ev::io&, int);
    void DoRead();
};

TAsyncReaderImpl::TAsyncReaderImpl(int fd)
    : FD_(fd)
    , ReadResultPromise_(MakePromise<TErrorOr<size_t>>(0))
    , State_(EReaderState::Active)
    , Buffer_(nullptr)
    , Length_(0)
    , Position_(0)
{
    auto this_ = MakeStrong(this);
    BIND([this, this_] () {
        FDWatcher_.set(FD_, ev::READ);
        FDWatcher_.set(TIODispatcher::Get()->Impl_->GetEventLoop());
        FDWatcher_.set<TAsyncReaderImpl, &TAsyncReaderImpl::OnRead>(this);
        FDWatcher_.start();
    })
    .Via(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();
}

TAsyncReaderImpl::~TAsyncReaderImpl()
{
    YCHECK(State_ != EReaderState::Active);
}

TFuture<TErrorOr<size_t>> TAsyncReaderImpl::Read(void* buffer, int length)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(ReadResultPromise_.IsSet());
    YCHECK(length > 0);

    auto promise = NewPromise<TErrorOr<size_t>>();

    auto this_ = MakeStrong(this);
    BIND([=] () {
        // Explicitly use this_ to capture strong reference in callback.
        this_->ReadResultPromise_ = promise;

        switch (State_) {
        case EReaderState::Aborted:
            ReadResultPromise_.Set(TError("Reader aborted (FD: %v)", FD_));
            return;

        case EReaderState::EndOfStream:
            ReadResultPromise_.Set(0);
            return;

        case EReaderState::Failed:
            ReadResultPromise_.Set(TError("Reader failed (FD: %v)", FD_));
            return;

        case EReaderState::Active:
            Buffer_ = buffer;
            Length_ = length;
            Position_ = 0;
            DoRead();
            break;

        default:
            YUNREACHABLE();
        };
    })
    .Via(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();

    return promise.ToFuture();
}

TFuture<void> TAsyncReaderImpl::Abort()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto this_ = MakeStrong(this);
    return BIND([this, this_] () {
        if (State_ != EReaderState::Active) {
            return;
        }

        State_ = EReaderState::Aborted;
        FDWatcher_.stop();
        ReadResultPromise_.TrySet(TError("Reader aborted (FD: %v)", FD_));

        YCHECK(TryClose(FD_));
    })
    .AsyncVia(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();
}

void TAsyncReaderImpl::DoRead()
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

        auto error = TError("Reader failed (FD: %v)", FD_) << TError::FromSystem();
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

void TAsyncReaderImpl::OnRead(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::READ) == ev::READ);

    YCHECK(State_ == EReaderState::Active);

    if (Position_ < Length_) {
        DoRead();
    }
}

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
