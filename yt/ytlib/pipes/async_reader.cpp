#include "stdafx.h"
#include "async_reader.h"
#include "non_block_reader.h"

#include "async_io.h"
#include "io_dispatcher.h"
#include "private.h"

#include <core/logging/tagged_logger.h>
#include <core/concurrency/thread_affinity.h>

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

    std::pair<TBlob, bool> Read(TBlob&& buffer);
    TAsyncError GetReadyEvent();

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
    TAsyncErrorPromise ReadyPromise_;

    TSpinLock Lock_;

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

TAsyncReader::TImpl::TImpl(int fd)
    : Reader_(new NDetail::TNonblockingReader(fd))
    , ReadyPromise_()
    , Logger(ReaderLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher_.set(fd, ev::READ);
}

TAsyncReader::TImpl::~TImpl()
{ }

void TAsyncReader::TImpl::OnRegistered(TError status)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if (!status.IsOK()) {
        if (ReadyPromise_) {
            ReadyPromise_.Set(status);
            ReadyPromise_.Reset();
        }
        RegistrationError_ = status;
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

    YCHECK(!Reader_->ReachedEOF());

    if (!Reader_->IsBufferFull()) {
        Reader_->ReadToBuffer();

        if (!CanReadSomeMore()) {
            Stop();
        }

        if (Reader_->IsReady()) {
            if (ReadyPromise_) {
                ReadyPromise_.Set(GetState());
                ReadyPromise_.Reset();
            }
        }
    } else {
        // Pause for a while.
        FDWatcher_.stop();
    }
}

std::pair<TBlob, bool> TAsyncReader::TImpl::Read(TBlob&& buffer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if (State_ == EAsyncIOState::Started && !FDWatcher_.is_active() && CanReadSomeMore()) {
        StartWatcher_.send();
    }

    return Reader_->GetRead(std::move(buffer));
}

TAsyncError TAsyncReader::TImpl::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    if ((State_ == EAsyncIOState::StartAborted) || !RegistrationError_.IsOK() || Reader_->IsReady()) {
        return MakePromise(GetState());
    }

    YCHECK(!ReadyPromise_);
    ReadyPromise_ = NewPromise<TError>();
    return ReadyPromise_.ToFuture();
}

TError TAsyncReader::TImpl::Abort()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock_);

    Unregister();

    if (State_ == EAsyncIOState::StartAborted) {
        // Close the reader if TAsyncReader was aborted before registering.
        Reader_->Close();
    }

    if (ReadyPromise_) {
        ReadyPromise_.Set(GetState());
        ReadyPromise_.Reset();
    }

    // Report the last reader error if any.
    if (Reader_->InFailedState()) {
        return TError::FromSystem(Reader_->GetLastSystemError());
    } else {
        return TError();
    }
}

bool TAsyncReader::TImpl::CanReadSomeMore() const
{
    return !Reader_->InFailedState() && !Reader_->ReachedEOF();
}

TError TAsyncReader::TImpl::GetState() const
{
    if (State_ == EAsyncIOState::StartAborted) {
        return TError("Pipe reader aborted during startup");
    } else if (!RegistrationError_.IsOK()) {
        return RegistrationError_;
    } else if (Reader_->ReachedEOF() || !Reader_->IsBufferEmpty()) {
        return TError();
    } else if (Reader_->InFailedState()) {
        return TError::FromSystem(Reader_->GetLastSystemError());
    } else {
        YUNREACHABLE();
    }
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

std::pair<TBlob, bool> TAsyncReader::Read(TBlob&& buffer)
{
    return Impl_->Read(std::move(buffer));
}

TAsyncError TAsyncReader::GetReadyEvent()
{
    return Impl_->GetReadyEvent();
}

TError TAsyncReader::Abort()
{
    return Impl_->Abort();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
