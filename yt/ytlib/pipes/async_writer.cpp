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
    , NeedToClose(false)
    , Logger(WriterLogger)
{
    FDWatcher.set(fd, ev::WRITE);

    RegistrationError = TIODispatcher::Get()->AsyncRegister(this);
}

TAsyncWriter::~TAsyncWriter()
{
    Close();
}

void TAsyncWriter::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(WriteLock);

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncWriter, &TAsyncWriter::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncWriter, &TAsyncWriter::OnWrite>(this);
    FDWatcher.start();
}

void TAsyncWriter::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::ASYNC == ev::ASYNC);

    FDWatcher.start();
}

void TAsyncWriter::OnWrite(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::WRITE == ev::WRITE);

    TGuard<TSpinLock> guard(WriteLock);

    if (!Writer->IsBufferEmpty() || NeedToClose) {
        Writer->WriteFromBuffer();

        if (Writer->IsFailed() || (NeedToClose && Writer->IsBufferEmpty())) {
            Close();
        }

        if (ReadyPromise.HasValue()) {
            if (!Writer->IsFailed()) {
                ReadyPromise->Set(TError());
            } else {
                ReadyPromise->Set(TError::FromSystem(Writer->GetLastSystemError()));
            }
            ReadyPromise.Reset();
        }
    } else {
        // I stop because these is nothing to write
        FDWatcher.stop();
    }
}

bool TAsyncWriter::Write(const void* data, size_t size)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!NeedToClose);

    TGuard<TSpinLock> guard(WriteLock);

    YCHECK(!ReadyPromise.HasValue());

    Writer->WriteToBuffer(static_cast<const char*>(data), size);

    // restart watcher
    if (!Writer->IsFailed()) {
        if (!Writer->IsBufferEmpty() || NeedToClose) {
            StartWatcher.send();
        }
    } else {
        YCHECK(Writer->IsClosed());
    }

    return (Writer->IsFailed() || Writer->IsBufferFull());
}

void TAsyncWriter::Close()
{
    Writer->Close();
    NeedToClose = false;
    FDWatcher.stop();
}

TAsyncError TAsyncWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    NeedToClose = true;
    YCHECK(!ReadyPromise.HasValue());

    StartWatcher.send();

    ReadyPromise.Assign(NewPromise<TError>());
    return ReadyPromise->ToFuture();
}

TAsyncError TAsyncWriter::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    if (!RegistrationError.IsSet() || !RegistrationError.Get().IsOK()) {
        return RegistrationError;
    }

    if (Writer->IsFailed()) {
        return MakePromise<TError>(TError::FromSystem(Writer->GetLastSystemError()));
    } else if (!Writer->IsBufferFull()) {
        return MakePromise<TError>(TError());
    } else {
        ReadyPromise.Assign(NewPromise<TError>());
        return ReadyPromise->ToFuture();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
