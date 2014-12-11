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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = PipesLogger;

////////////////////////////////////////////////////////////////////////////////

class TAsyncWriterImpl
    : public TRefCounted
{
public:
    explicit TAsyncWriterImpl(int fd)
        : FD_(fd)
    {
        auto this_ = MakeStrong(this);
        BIND([=] () {
            UNUSED(this_);

            FDWatcher_.set(FD_, ev::WRITE);
            FDWatcher_.set(TIODispatcher::Get()->Impl_->GetEventLoop());
            FDWatcher_.set<TAsyncWriterImpl, &TAsyncWriterImpl::OnWrite>(this);
            FDWatcher_.start();
        })
        .Via(TIODispatcher::Get()->Impl_->GetInvoker())
        .Run();
    }

    ~TAsyncWriterImpl()
    {
        YCHECK(State_ != EWriterState::Active);
    }

    TAsyncError Write(const void* buffer, int length)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(length > 0);

        auto promise = NewPromise<TError>();
        auto this_ = MakeStrong(this);
        BIND([=] () {
            UNUSED(this_);

            YCHECK(WriteResultPromise_.IsSet());
            WriteResultPromise_ = promise;

            switch (State_) {
                case EWriterState::Aborted:
                    WriteResultPromise_.Set(TError("Writer aborted")
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

    TAsyncError Close()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(WriteResultPromise_.IsSet());

        auto this_ = MakeStrong(this);
        return BIND([=] () {
            UNUSED(this_);

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

    TFuture<void> Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto this_ = MakeStrong(this);
        return BIND([=] () {
            UNUSED(this_);

            if (State_ != EWriterState::Active) {
                return;
            }

            State_ = EWriterState::Aborted;
            FDWatcher_.stop();
            WriteResultPromise_.TrySet(TError("Writer aborted")
               << TErrorAttribute("fd", FD_));

            YCHECK(TryClose(FD_));
        })
        .AsyncVia(TIODispatcher::Get()->Impl_->GetInvoker())
        .Run();
    }

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

    TAsyncErrorPromise WriteResultPromise_ = MakePromise(TError());

    EWriterState State_ = EWriterState::Active;

    const void* Buffer_ = nullptr;
    int Length_ = 0;
    int Position_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);


    void OnWrite(ev::io&, int eventType)
    {
        VERIFY_THREAD_AFFINITY(EventLoop);
        YCHECK((eventType & ev::WRITE) == ev::WRITE);

        YCHECK(State_ == EWriterState::Active);

        if (Position_ < Length_) {
            DoWrite();
        } else {
            FDWatcher_.stop();
        }
    }

    void DoWrite()
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

            auto error = TError("Writer failed")
                << TErrorAttribute("fd", FD_)
                << TError::FromSystem();
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

};

DEFINE_REFCOUNTED_TYPE(TAsyncWriterImpl);

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
