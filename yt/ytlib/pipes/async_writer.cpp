#include "async_writer.h"
#include "non_block_writer.h"

#include "io_dispatcher.h"
#include "private.h"

namespace NYT {
namespace NPipes {

static const size_t WriteBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

TAsyncWriter::TAsyncWriter(int fd)
    : Writer(new NDetail::TNonblockingWriter(fd))
    , ReadyPromise()
    , ClosePromise()
    , NeedToClose(false)
    , Logger(WriterLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::WRITE);
}

TAsyncWriter::~TAsyncWriter()
{
}

void TAsyncWriter::OnRegistered(TError status)
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

void TAsyncWriter::OnUnregister(TError status)
{
    if (!status.IsOK()) {
        LOG_ERROR(status, "Failed to unregister");
    }
}

void TAsyncWriter::DoStart(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(Lock);

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncWriter, &TAsyncWriter::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncWriter, &TAsyncWriter::OnWrite>(this);
    FDWatcher.start();
}

void TAsyncWriter::DoStop()
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

void TAsyncWriter::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::ASYNC) == ev::ASYNC);

    // One can probably remove this guard
    // But this call should be rare
    TGuard<TSpinLock> guard(Lock);

    YCHECK(IsStarted());

    if (!Writer->IsClosed()) {
        FDWatcher.start();
    }
}

void TAsyncWriter::OnWrite(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::WRITE) == ev::WRITE);

    TGuard<TSpinLock> guard(Lock);

    YCHECK(IsStarted());

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

bool TAsyncWriter::Write(const void* data, size_t size)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    YCHECK(!IsStopped());

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

TAsyncError TAsyncWriter::AsyncClose()
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

TAsyncError TAsyncWriter::GetReadyEvent()
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

void TAsyncWriter::RestartWatcher()
{
    if (IsStarted() && !IsStopped() && !FDWatcher.is_active() && HasJobToDo()) {
        StartWatcher.send();
    }
}

TError TAsyncWriter::GetWriterStatus() const
{
    if (!RegistrationError.IsOK()) {
        return RegistrationError;
    } else if (Writer->IsFailed()) {
        return TError::FromSystem(Writer->GetLastSystemError());
    } else {
        return TError();
    }
}

bool TAsyncWriter::HasJobToDo() const
{
    return !Writer->IsBufferEmpty() || NeedToClose;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
