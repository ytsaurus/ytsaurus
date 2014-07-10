#include "stdafx.h"
#include "async_writer.h"
#include "non_block_writer.h"

#include "async_io.h"
#include "io_dispatcher.h"
#include "private.h"

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>
#include <core/concurrency/thread_affinity.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TAsyncWriter::TImpl
    : public TAsyncIOBase
{
public:
    explicit TImpl(int fd);
    ~TImpl() override;

    TAsyncError Write(const void* data, size_t size);
    TAsyncError Close();

private:
    virtual void DoStart(ev::dynamic_loop& eventLoop) override;
    virtual void DoStop() override;

    virtual void OnRegistered(TError status) override;
    virtual void OnUnregister(TError status) override;

    void OnWrite(ev::io&, int);
    void OnStart(ev::async&, int);

    std::unique_ptr<NDetail::TNonblockingWriter> Writer_;
    ev::io FDWatcher_;
    ev::async StartWatcher_;

    TAsyncErrorPromise WritePromise_;
    TAsyncErrorPromise ClosePromise_;

    TError RegistrationError_;

    const void* Buffer_;
    size_t Length_;
    size_t Position_;

    TSpinLock Lock_;

    NLog::TTaggedLogger Logger;


    void SetPromises(TError error);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TImpl::TImpl(int fd)
    : Writer_(new NDetail::TNonblockingWriter(fd))
    , Buffer_(nullptr)
    , Length_(0)
    , Position_(0)
    , Logger(PipesLogger)
{
    Logger.AddTag("FD: %v", fd);

    FDWatcher_.set(fd, ev::WRITE);
}

TAsyncWriter::TImpl::~TImpl()
{ }

void TAsyncWriter::TImpl::OnRegistered(TError status)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if (!status.IsOK()) {
        RegistrationError_ = status;
        SetPromises(status);
    }
}

void TAsyncWriter::TImpl::OnUnregister(TError status)
{
    if (!status.IsOK()) {
        LOG_ERROR(status, "Failed to unregister the pipe writer");
    }
}

void TAsyncWriter::TImpl::DoStart(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(Lock_);

    StartWatcher_.set(eventLoop);
    StartWatcher_.set<TAsyncWriter::TImpl, &TAsyncWriter::TImpl::OnStart>(this);
    StartWatcher_.start();

    FDWatcher_.set(eventLoop);
    FDWatcher_.set<TAsyncWriter::TImpl, &TAsyncWriter::TImpl::OnWrite>(this);
    FDWatcher_.start();
}

void TAsyncWriter::TImpl::DoStop()
{
    FDWatcher_.stop();
    StartWatcher_.stop();

    Writer_->Close();
}

void TAsyncWriter::TImpl::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::ASYNC) == ev::ASYNC);

    // One can probably remove this guard
    // But this call should be rare
    TGuard<TSpinLock> guard(Lock_);

    YCHECK(State_ == EAsyncIOState::Started);

    YCHECK(!Writer_->IsClosed());
    FDWatcher_.start();
}

void TAsyncWriter::TImpl::OnWrite(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::WRITE) == ev::WRITE);

    TGuard<TSpinLock> guard(Lock_);

    YCHECK(State_ == EAsyncIOState::Started);

    if (ClosePromise_) {
        YCHECK(!WritePromise_ || WritePromise_.IsSet());

        Stop();
        ClosePromise_.Set(TError());

        return;
    }

    if (Position_ < Length_) {
        auto result = Writer_->Write(
            static_cast<const char*>(Buffer_) + Position_,
            Length_ - Position_);

        if (!result.IsOK()) {
            Stop();
            SetPromises(TError(result));
            return;
        }

        auto writeLength = result.Value();
        Position_ += writeLength;

        if (Position_ == Length_) {
            WritePromise_.Set(TError());
        }
    } else {
        FDWatcher_.stop();
    }
}

TAsyncError TAsyncWriter::TImpl::Write(const void* buf, size_t len)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    YCHECK(!ClosePromise_ && !Writer_->IsClosed());

    YCHECK(!WritePromise_ || WritePromise_.IsSet());

    YCHECK(State_ == EAsyncIOState::Started || State_ == EAsyncIOState::Created);

    YCHECK(len > 0);

    if (!RegistrationError_.IsOK()) {
        return MakeFuture(RegistrationError_);
    }

    if (WritePromise_ && !WritePromise_.Get().IsOK()) {
        return WritePromise_;
    }

    Buffer_ = buf;
    Length_ = len;
    Position_ = 0;

    WritePromise_ = NewPromise<TError>();
    if (State_ == EAsyncIOState::Started && !FDWatcher_.is_active()) {
        StartWatcher_.send();
    }
    return WritePromise_.ToFuture();
}

TAsyncError TAsyncWriter::TImpl::Close()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    YCHECK(!ClosePromise_);
    YCHECK(!WritePromise_ || WritePromise_.IsSet());

    if (!RegistrationError_.IsOK()) {
        return MakeFuture(RegistrationError_);
    }

    if (WritePromise_ && !WritePromise_.Get().IsOK()) {
        return MakeFuture(TError(WritePromise_.Get()));
    }

    ClosePromise_ = NewPromise<TError>();
    if (State_ == EAsyncIOState::Started && !FDWatcher_.is_active()) {
        StartWatcher_.send();
    }
    return ClosePromise_.ToFuture();
}

void TAsyncWriter::TImpl::SetPromises(TError error)
{
    if (WritePromise_) {
        WritePromise_.Set(error);
    }
    if (ClosePromise_) {
        ClosePromise_.Set(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TAsyncWriter(int fd)
    : Impl_(New<TImpl>(fd))
{
    Impl_->Register();
}

TAsyncWriter::~TAsyncWriter()
{
    if (Impl_) {
        Impl_->Unregister();
    }
}

TAsyncWriter::TAsyncWriter(const TAsyncWriter& other)
    : Impl_(other.Impl_)
{ }

TAsyncError TAsyncWriter::Write(const void* data, size_t size)
{
    return Impl_->Write(data, size);
}

TAsyncError TAsyncWriter::Close()
{
    return Impl_->Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
