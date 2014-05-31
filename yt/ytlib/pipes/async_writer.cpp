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

static const size_t WriteBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TAsyncWriter::TImpl
    : public TAsyncIOBase
{
public:
    explicit TImpl(int fd);
    ~TImpl() override;

    bool Write(const void* data, size_t size);
    TAsyncError AsyncClose();
    TAsyncError GetReadyEvent();

private:
    virtual void DoStart(ev::dynamic_loop& eventLoop) override;
    virtual void DoStop() override;

    virtual void OnRegistered(TError status) override;
    virtual void OnUnregister(TError status) override;

    void OnWrite(ev::io&, int);
    void OnStart(ev::async&, int);

    void RestartWatcher();
    TError GetWriterStatus() const;
    bool HasJobToDo() const;

    // TODO(babenko): Writer -> Writer_ etc
    std::unique_ptr<NDetail::TNonblockingWriter> Writer;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TAsyncErrorPromise ReadyPromise;
    TAsyncErrorPromise ClosePromise;

    TError RegistrationError;
    bool NeedToClose;

    TSpinLock Lock;

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TImpl::TImpl(int fd)
    : Writer(new NDetail::TNonblockingWriter(fd))
    , ReadyPromise()
    , ClosePromise()
    , NeedToClose(false)
    , Logger(PipesLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::WRITE);
}

TAsyncWriter::TImpl::~TImpl()
{ }

void TAsyncWriter::TImpl::OnRegistered(TError status)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    if (!status.IsOK()) {
        if (ReadyPromise) {
            ReadyPromise.Set(status);
            ReadyPromise.Reset();
        }
        if (ClosePromise) {
            ClosePromise.Set(status);
            ClosePromise.Reset();
        }
        RegistrationError = status;
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

    TGuard<TSpinLock> guard(Lock);

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncWriter::TImpl, &TAsyncWriter::TImpl::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncWriter::TImpl, &TAsyncWriter::TImpl::OnWrite>(this);
    FDWatcher.start();
}

void TAsyncWriter::TImpl::DoStop()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    FDWatcher.stop();
    StartWatcher.stop();

    Writer->Close();
    NeedToClose = false;

    if (ClosePromise) {
        ClosePromise.Set(GetWriterStatus());
        ClosePromise.Reset();
    }
}

void TAsyncWriter::TImpl::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::ASYNC) == ev::ASYNC);

    // One can probably remove this guard
    // But this call should be rare
    TGuard<TSpinLock> guard(Lock);

    YCHECK(State_ == EAsyncIOState::Started);

    YCHECK(!Writer->IsClosed());
    FDWatcher.start();
}

void TAsyncWriter::TImpl::OnWrite(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::WRITE) == ev::WRITE);

    TGuard<TSpinLock> guard(Lock);

    YCHECK(State_ == EAsyncIOState::Started);

    if (HasJobToDo()) {
        Writer->WriteFromBuffer();

        if (Writer->IsFailed() || (NeedToClose && Writer->IsBufferEmpty())) {
            Stop();
        }

        // I should set promise only if there is no much to do: HasJobToDo() == false
        if (Writer->IsFailed() || !HasJobToDo()) {
            if (ReadyPromise) {
                ReadyPromise.Set(GetWriterStatus());
                ReadyPromise.Reset();
            }
        }
    } else {
        // I stop because these is nothing to do
        FDWatcher.stop();
    }
}

bool TAsyncWriter::TImpl::Write(const void* data, size_t size)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    YCHECK(State_ == EAsyncIOState::Started || State_ == EAsyncIOState::Created);

    YCHECK(!ReadyPromise);
    YCHECK(!NeedToClose);

    Writer->WriteToBuffer(static_cast<const char*>(data), size);

    if (!Writer->IsFailed()) {
        RestartWatcher();
    } else {
        YCHECK(Writer->IsClosed());
    }

    return !RegistrationError.IsOK() || Writer->IsFailed() || Writer->IsBufferFull();
}

TAsyncError TAsyncWriter::TImpl::AsyncClose()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    YCHECK(!ClosePromise);
    YCHECK(!NeedToClose);

    // Writer can be already closed due to encountered error
    if (Writer->IsClosed()) {
        return MakePromise<TError>(GetWriterStatus());
    }

    NeedToClose = true;

    RestartWatcher();

    ClosePromise = NewPromise<TError>();
    return ClosePromise.ToFuture();
}

TAsyncError TAsyncWriter::TImpl::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    if (!RegistrationError.IsOK() || Writer->IsFailed() || !HasJobToDo()) {
        return MakePromise<TError>(GetWriterStatus());
    } else {
        ReadyPromise = NewPromise<TError>();
        return ReadyPromise.ToFuture();
    }
}

void TAsyncWriter::TImpl::RestartWatcher()
{
    if (State_ == EAsyncIOState::Started && !FDWatcher.is_active() && HasJobToDo()) {
        StartWatcher.send();
    }
}

TError TAsyncWriter::TImpl::GetWriterStatus() const
{
    if (!RegistrationError.IsOK()) {
        return RegistrationError;
    } else if (Writer->IsFailed()) {
        return TError::FromSystem(Writer->GetLastSystemError());
    } else {
        return TError();
    }
}

bool TAsyncWriter::TImpl::HasJobToDo() const
{
    return !Writer->IsBufferEmpty() || NeedToClose;
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

bool TAsyncWriter::Write(const void* data, size_t size)
{
    return Impl_->Write(data, size);
}

TAsyncError TAsyncWriter::AsyncClose()
{
    return Impl_->AsyncClose();
}

TAsyncError TAsyncWriter::GetReadyEvent()
{
    return Impl_->GetReadyEvent();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
