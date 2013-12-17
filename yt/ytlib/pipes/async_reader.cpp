#include "async_reader.h"
#include "non_block_reader.h"

#include "io_dispatcher.h"
#include "private.h"

namespace NYT {
namespace NPipes {

TAsyncReader::TAsyncReader(int fd)
    : Reader(new NDetail::TNonBlockReader(fd))
    , ReadyPromise()
    , Logger(ReaderLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::READ);

    RegistrationError = TIODispatcher::Get()->AsyncRegister(this);
}

TAsyncReader::~TAsyncReader()
{ }

void TAsyncReader::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(ReadLock);

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncReader, &TAsyncReader::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncReader, &TAsyncReader::OnRead>(this);
    FDWatcher.start();
}

void TAsyncReader::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::ASYNC == ev::ASYNC);

    FDWatcher.start();
}

void TAsyncReader::OnRead(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType | ev::READ == ev::READ);

    TGuard<TSpinLock> guard(ReadLock);

    YCHECK(!Reader->ReachedEOF());

    if (!Reader->IsBufferFull()) {
        Reader->ReadToBuffer();

        if (!CanReadSomeMore()) {
            FDWatcher.stop();
            Reader->Close();
        }

        if (ReadyPromise) {
            if (Reader->IsReady()) {
                ReadyPromise.Set(GetState());
                ReadyPromise.Reset();
            }
        }
    } else {
        LOG_DEBUG("The internal buffer is full. Stop the watcher");

        // pause for a while
        FDWatcher.stop();
    }
}

std::pair<TBlob, bool> TAsyncReader::Read(TBlob&& buffer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    if (CanReadSomeMore()) {
        // ev_io_start is not thread-safe
        StartWatcher.send();
    }

    return Reader->GetRead(std::move(buffer));
}

TAsyncError TAsyncReader::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    if (!RegistrationError.IsSet() || !RegistrationError.Get().IsOK()) {
        return RegistrationError;
    }

    if (Reader->IsReady()) {
        return MakePromise(GetState());
    }

    LOG_DEBUG("Returning a new future");

    YCHECK(!ReadyPromise);
    ReadyPromise = NewPromise<TError>();
    return ReadyPromise.ToFuture();
}

TError TAsyncReader::Close()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    Reader->Close();

    if (ReadyPromise) {
        ReadyPromise.Set(TError("The reader was aborted"));
        ReadyPromise.Reset();
    }

    if (Reader->InFailedState()) {
        return TError::FromSystem(Reader->GetLastSystemError());
    } else {
        return TError();
    }
}

bool TAsyncReader::CanReadSomeMore() const
{
    return (!Reader->InFailedState() && !Reader->ReachedEOF());
}

TError TAsyncReader::GetState() const
{
    if (Reader->ReachedEOF() || !Reader->IsBufferEmpty()) {
        return TError();
    } else if (Reader->InFailedState()) {
        return TError::FromSystem(Reader->GetLastSystemError());
    } else {
        YCHECK(false);
        return TError();
    }
}


}
}
