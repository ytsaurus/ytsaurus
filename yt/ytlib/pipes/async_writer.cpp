#include "stdafx.h"
#include "async_writer.h"

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

class TAsyncWriterImpl
    : public TRefCounted
{
public:
    explicit TAsyncWriterImpl(int fd);
    ~TAsyncWriterImpl();

    TAsyncError Write(const void* buffer, int length);
    TAsyncError Close();

    TFuture<void> Abort();

private:
    DECLARE_ENUM(EWriterState,
        (Active)
        (Closed)
        (Failed)
        (Aborted)
    );

    int FD_;

    //! \note Thread-unsafe. Must be accessed from ev-thread only.
    ev::io FDWatcher_;

    TAsyncErrorPromise WriteResultPromise_;

    EWriterState State_;

    const void* Buffer_;
    int Length_;
    int Position_;


    void OnWrite(ev::io&, int eventType);

    void DoWrite();

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

////////////////////////////////////////////////////////////////////////////////

TAsyncWriterImpl::TAsyncWriterImpl(int fd)
    : FD_(fd)
    , WriteResultPromise_(MakePromise(TError()))
    , State_(EWriterState::Active)
    , Buffer_(nullptr)
    , Length_(0)
    , Position_(0)
{
    auto this_ = MakeStrong(this);
    BIND([this, this_] () {
        FDWatcher_.set(FD_, ev::WRITE);
        FDWatcher_.set(TIODispatcher::Get()->Impl_->GetEventLoop());
        FDWatcher_.set<TAsyncWriterImpl, &TAsyncWriterImpl::OnWrite>(this);
        FDWatcher_.start();
    })
    .Via(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();
}

TAsyncWriterImpl::~TAsyncWriterImpl()
{ 
    YCHECK(State_ != EWriterState::Active);
}

TAsyncError TAsyncWriterImpl::Write(const void* buffer, int length)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(WriteResultPromise_.IsSet());
    YCHECK(length > 0);

    auto promise = NewPromise<TError>();
    auto this_ = MakeStrong(this);

    BIND([=] () {
        // Explicitly use this_ to capture strong reference in callback.
        this_->WriteResultPromise_ = promise;

        switch (State_) {
        case EWriterState::Aborted:
            WriteResultPromise_.Set(TError("Writer aborted (FD: %v)", FD_));
            return;

        case EWriterState::Failed:
            WriteResultPromise_.Set(TError("Writer failed (FD: %v)", FD_));
            return;

        case EWriterState::Closed:
            WriteResultPromise_.Set(TError("Writer closed (FD: %v)", FD_));
            return;

        case EWriterState::Active:
            Buffer_ = buffer;
            Length_ = length;
            Position_ = 0;
            DoWrite();
            break;

        default:
            YUNREACHABLE();
        };
    })
    .Via(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();

    return promise.ToFuture();
}

TAsyncError TAsyncWriterImpl::Close()
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(WriteResultPromise_.IsSet());

    auto this_ = MakeStrong(this);
    return BIND([this, this_] () {
        if (State_ != EWriterState::Active) {
            return;
        }

        State_ = EWriterState::Closed;
        FDWatcher_.stop();
        SafeClose(FD_);
    })
    .Guarded()
    .AsyncVia(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();
}

TFuture<void> TAsyncWriterImpl::Abort()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto this_ = MakeStrong(this);
    return BIND([this, this_] () {
        if (State_ != EWriterState::Active) {
            return;
        }

        State_ = EWriterState::Aborted;
        FDWatcher_.stop();
        WriteResultPromise_.TrySet(TError("Writer aborted (FD: %v)", FD_));

        YCHECK(TryClose(FD_));
    })
    .AsyncVia(TIODispatcher::Get()->Impl_->GetInvoker())
    .Run();
}

void TAsyncWriterImpl::DoWrite()
{
    YCHECK(Position_ < Length_);

    int size;
    do {
        size = ::write(FD_, static_cast<const char *>(Buffer_) + Position_, Length_ - Position_);
    } while (size == -1 && errno == EINTR);

    if (size == -1) {
        if (errno == EAGAIN) {
            return;
        }

        auto error = TError("Writer failed (FD: %v)", FD_) << TError::FromSystem();
        LOG_ERROR(error);

        State_ = EWriterState::Failed;
        FDWatcher_.stop();
        WriteResultPromise_.Set(error);
        return;
    }

    YCHECK(size > 0);
    Position_ += size;

    if (Position_ == Length_) {
        WriteResultPromise_.Set(TError());
    }
}

void TAsyncWriterImpl::OnWrite(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::WRITE) == ev::WRITE);

    YCHECK(State_ == EWriterState::Active);

    if (Position_ < Length_) {
        DoWrite();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TAsyncWriter(int fd)
    : Impl_(New<NDetail::TAsyncWriterImpl>(fd))
{ }

TAsyncError TAsyncWriter::Write(const void* data, size_t size)
{
    return Impl_->Write(data, size);
}

TAsyncError TAsyncWriter::Close()
{
    return Impl_->Close();
}

TFuture<void> TAsyncWriter::Abort()
{
    return Impl_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
