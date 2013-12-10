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
    LOG_TRACE("Constructing...");

    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::READ);

    RegistrationError = TIODispatcher::Get()->AsyncRegister(this);
}

TAsyncReader::~TAsyncReader()
{
}

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
    YCHECK(eventType == ev::ASYNC);

    FDWatcher.start();
}

void TAsyncReader::OnRead(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(eventType == ev::READ);

    TGuard<TSpinLock> guard(ReadLock);

    LOG_DEBUG("Reading to buffer...");

    YCHECK(!Reader->ReachedEOF());

    if (!Reader->IsBufferFull()) {
        Reader->TryReadInBuffer();

        if (Reader->ReachedEOF()) {
            FDWatcher.stop();
            Reader->Close();
        }

        if (ReadyPromise.HasValue()) {
            if (Reader->IsReady()) {
                ReadyPromise->Set(GetState());
                ReadyPromise.Reset();
            }
        }
    } else {
        // pause for a while
        FDWatcher.stop();
    }
}

std::pair<TBlob, bool> TAsyncReader::Read()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    if (!Reader->ReachedEOF()) {
        // ev_io_start is not thread-safe
        StartWatcher.send();
    }

    return Reader->GetRead();
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

    ReadyPromise.Assign(NewPromise<TError>());
    return ReadyPromise->ToFuture();
}

TError TAsyncReader::GetState()
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
