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
    , NeedToClose(false)
    , Logger(WriterLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::WRITE);

    RegistrationError = TIODispatcher::Get()->AsyncRegister(this);
}

TAsyncWriter::~TAsyncWriter()
{
    Writer->Close();
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

    YCHECK(!Writer->IsClosed());

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

    FDWatcher.stop();
    StartWatcher.stop();
}

void TAsyncWriter::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::ASYNC == ev::ASYNC);

    YCHECK(IsRegistered());

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

    if (HasJobToDo()) {
        Writer->WriteFromBuffer();

        if (Writer->IsFailed() || (NeedToClose && Writer->IsBufferEmpty())) {
            Close();
        }

        if (ReadyPromise) {
            if (!Writer->IsFailed()) {
                ReadyPromise.Set(TError());
            } else {
                ReadyPromise.Set(TError::FromSystem(Writer->GetLastSystemError()));
            }
            ReadyPromise.Reset();
        }
    } else {
        // I stop because these is nothing to do
        FDWatcher.stop();
    }
}

bool TAsyncWriter::Write(const void* data, size_t size)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!NeedToClose);

    TGuard<TSpinLock> guard(WriteLock);

    YCHECK(!ReadyPromise);

    Writer->WriteToBuffer(static_cast<const char*>(data), size);

    // restart watcher
    if (!Writer->IsFailed()) {
        if (IsRegistered() && !Writer->IsClosed() && !FDWatcher.is_active() && HasJobToDo()) {
            StartWatcher.send();
        }
    } else {
        YCHECK(Writer->IsClosed());
    }

    return Writer->IsFailed() || Writer->IsBufferFull();
}

void TAsyncWriter::Close()
{
    Writer->Close();
    NeedToClose = false;
    FDWatcher.stop();
    StartWatcher.stop();
}

TAsyncError TAsyncWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    NeedToClose = true;
    YCHECK(!ReadyPromise);

    if (IsRegistered() && !FDWatcher.is_active()) {
        StartWatcher.send();
    }

    ReadyPromise = NewPromise<TError>();
    return ReadyPromise.ToFuture();
}

TAsyncError TAsyncWriter::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    if (!IsRegistered()) {
        return RegistrationError;
    }

    if (Writer->IsFailed()) {
        return MakePromise<TError>(TError::FromSystem(Writer->GetLastSystemError()));
    } else if (!Writer->IsBufferFull()) {
        return MakePromise<TError>(TError());
    } else {
        ReadyPromise = NewPromise<TError>();
        return ReadyPromise.ToFuture();
    }
}

bool TAsyncWriter::IsRegistered() const
{
    return RegistrationError.IsSet() && RegistrationError.Get().IsOK();
}

bool TAsyncWriter::HasJobToDo() const
{
    return !Writer->IsBufferEmpty() || NeedToClose;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
