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
    , IsRegistered_()
    , NeedToClose(false)
    , Logger(WriterLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::WRITE);

    auto RegistrationErrorFuture = TIODispatcher::Get()->AsyncRegister(MakeStrong(this));
    RegistrationErrorFuture.Subscribe(
        BIND(&TAsyncWriter::OnRegistered, MakeStrong(this)));
}

void TAsyncWriter::OnRegistered(TError status)
{
    TGuard<TSpinLock> guard(WriteLock);
    if (status.IsOK()) {
        IsRegistered_ = true;
    } else {
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

TAsyncWriter::~TAsyncWriter()
{
    if (IsRegistered()) {
        LOG_DEBUG("Start unregistering");

        auto error = TIODispatcher::Get()->Unregister(*this);
        if (!error.IsOK()) {
            LOG_ERROR(error, "Failed to unregister");
        }
    }
}

void TAsyncWriter::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(WriteLock);

    YCHECK(!IsStopped());

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncWriter, &TAsyncWriter::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncWriter, &TAsyncWriter::OnWrite>(this);
    FDWatcher.start();

    LOG_DEBUG("Registered");
}

void TAsyncWriter::Stop()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(WriteLock);

    Close();
}

void TAsyncWriter::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::ASYNC == ev::ASYNC);

    YCHECK(IsRegistered());
    YCHECK(!IsStopped());

    if (!Writer->IsClosed()) {
        FDWatcher.start();
    }
}

void TAsyncWriter::OnWrite(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::WRITE == ev::WRITE);

    TGuard<TSpinLock> guard(WriteLock);

    YCHECK(IsRegistered());
    YCHECK(!IsStopped());

    if (HasJobToDo()) {
        Writer->WriteFromBuffer();

        if (Writer->IsFailed() || (NeedToClose && Writer->IsBufferEmpty())) {
            Close();
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
    YCHECK(!IsStopped());
    YCHECK(!NeedToClose);

    TGuard<TSpinLock> guard(WriteLock);

    YCHECK(!ReadyPromise);

    Writer->WriteToBuffer(static_cast<const char*>(data), size);

    // restart watcher
    if (!Writer->IsFailed()) {
        if (IsRegistered() && !IsStopped() && !FDWatcher.is_active() && HasJobToDo()) {
            StartWatcher.send();
        }
    } else {
        // this is not true
        YCHECK(Writer->IsClosed());
    }

    return !RegistrationError.IsOK() || Writer->IsFailed() || Writer->IsBufferFull();
}

TAsyncError TAsyncWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!IsStopped());

    TGuard<TSpinLock> guard(WriteLock);

    NeedToClose = true;
    YCHECK(!ReadyPromise);

    if (IsRegistered() && !FDWatcher.is_active()) {
        StartWatcher.send();
    }

    ClosePromise = NewPromise<TError>();
    return ClosePromise.ToFuture();
}

TAsyncError TAsyncWriter::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    if (!RegistrationError.IsOK() || Writer->IsFailed() || !HasJobToDo()) {
        return MakePromise<TError>(GetWriterStatus());
    } else {
        ReadyPromise = NewPromise<TError>();
        return ReadyPromise.ToFuture();
    }
}

void TAsyncWriter::Close()
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

bool TAsyncWriter::IsStopped() const
{
    return Writer->IsClosed();
}

bool TAsyncWriter::IsRegistered() const
{
    return IsRegistered_;
}

bool TAsyncWriter::HasJobToDo() const
{
    return !Writer->IsBufferEmpty() || NeedToClose;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
