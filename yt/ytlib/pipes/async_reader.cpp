#include "stdafx.h"
#include "async_reader.h"
#include "non_block_reader.h"

#include "async_io.h"
#include "io_dispatcher.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>

#include <core/logging/log.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TAsyncReader::TImpl
    : public TAsyncIOBase
{
public:
    explicit TImpl(int fd);
    virtual ~TImpl() override;

    TFuture<TErrorOr<size_t>> Read(void* buf, size_t len);

    TError Abort();

private:
    virtual void DoStart(ev::dynamic_loop& eventLoop) override;
    virtual void DoStop() override;

    virtual void OnRegistered(TError status) override;
    virtual void OnUnregister(TError status) override;

    void OnRead(ev::io&, int);
    void OnStart(ev::async&, int);

    bool CanReadSomeMore() const;
    TError GetState() const;

    std::unique_ptr<NDetail::TNonblockingReader> Reader_;
    ev::io FDWatcher_;
    ev::async StartWatcher_;

    TError RegistrationError_;
    TPromise<TErrorOr<size_t>> ReadPromise_;

    TSpinLock Lock_;

    NLog::TLogger Logger;

    bool ReachedEOF_;

    void* Buffer_;
    size_t Length_;
    size_t Position_;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

TAsyncReader::TImpl::TImpl(int fd)
    : Reader_(new NDetail::TNonblockingReader(fd))
    , Logger(PipesLogger)
    , ReachedEOF_(false)
    , Buffer_(nullptr)
    , Length_(0)
    , Position_(0)
{
    Logger.AddTag("FD: %v", fd);

    FDWatcher_.set(fd, ev::READ);
}

TAsyncReader::TImpl::~TImpl()
{ }

void TAsyncReader::TImpl::OnRegistered(TError status)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if (!status.IsOK()) {
        RegistrationError_ = status;
        if (ReadPromise_) {
            ReadPromise_.Set(status);
        }
    }
}

void TAsyncReader::TImpl::OnUnregister(TError status)
{
    if (!status.IsOK()) {
        LOG_ERROR(status, "Failed to unregister the pipe reader");
    }
}

void TAsyncReader::TImpl::DoStart(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(Lock_);

    StartWatcher_.set(eventLoop);
    StartWatcher_.set<TAsyncReader::TImpl, &TAsyncReader::TImpl::OnStart>(this);
    StartWatcher_.start();

    FDWatcher_.set(eventLoop);
    FDWatcher_.set<TAsyncReader::TImpl, &TAsyncReader::TImpl::OnRead>(this);
    FDWatcher_.start();
}

void TAsyncReader::TImpl::DoStop()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    FDWatcher_.stop();
    StartWatcher_.stop();

    Reader_->Close();
}

void TAsyncReader::TImpl::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::ASYNC) == ev::ASYNC);

    TGuard<TSpinLock> guard(Lock_);

    YCHECK(State_ == EAsyncIOState::Started);

    YCHECK(!Reader_->IsClosed());
    FDWatcher_.start();
}

void TAsyncReader::TImpl::OnRead(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::READ) == ev::READ);

    TGuard<TSpinLock> guard(Lock_);

    YCHECK(State_ == EAsyncIOState::Started);

    YCHECK(!ReachedEOF_);

    if (Position_ < Length_) {
        auto result = Reader_->Read(static_cast<char*>(Buffer_) + Position_, Length_ - Position_);

        if (!result.IsOK()) {
            Stop();
            if (!ReadPromise_) {
                ReadPromise_.Set(result);
            }
            return;
        }

        auto readLength = result.Value();
        Position_ += readLength;

        if (readLength == 0) {
            Stop();
            ReachedEOF_ = true;
        }
        if (readLength == 0 || Position_ == Length_) {
            ReadPromise_.Set(Position_);
        }
    } else {
        FDWatcher_.stop();
    }
}

TFuture<TErrorOr<size_t>> TAsyncReader::TImpl::Read(void* buf, size_t len)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if (!RegistrationError_.IsOK()) {
        return MakeFuture<TErrorOr<size_t>>(RegistrationError_);
    }

    if (ReachedEOF_) {
        return MakeFuture(TErrorOr<size_t>(0));
    }

    YCHECK(!Reader_->IsClosed());

    Buffer_ = buf;
    Length_ = len;
    Position_ = 0;

    ReadPromise_ = NewPromise<TErrorOr<size_t>>();

    if (State_ == EAsyncIOState::Started && !FDWatcher_.is_active()) {
        StartWatcher_.send();
    }

    return ReadPromise_.ToFuture();
}

TError TAsyncReader::TImpl::Abort()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    Unregister();

    if (!RegistrationError_.IsOK()) {
        return RegistrationError_;
    }

    if (State_ == EAsyncIOState::StartAborted) {
        // Close the reader if TAsyncReader was aborted before registering.
        return Reader_->Close();
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(int fd)
    : Impl_(New<TImpl>(fd))
{
    Impl_->Register();
}

TAsyncReader::~TAsyncReader()
{
    if (Impl_) {
        Impl_->Unregister();
    }
}

TAsyncReader::TAsyncReader(const TAsyncReader& other)
    : Impl_(other.Impl_)
{ }

TFuture<TErrorOr<size_t>> TAsyncReader::Read(void* buf, size_t len)
{
    return Impl_->Read(buf, len);
}

TError TAsyncReader::Abort()
{
    return Impl_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
