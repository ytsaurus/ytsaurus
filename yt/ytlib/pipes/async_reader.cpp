#include "async_reader.h"
#include "non_block_reader.h"

#include "io_dispatcher.h"
#include "private.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(int fd)
    : Reader(new NDetail::TNonblockingReader(fd))
    , ReadyPromise()
    , IsAborted_(false)
    , IsRegistered_(false)
    , Logger(ReaderLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::READ);
}

TAsyncReader::~TAsyncReader()
{
    YCHECK(IsStopped());
}

void TAsyncReader::Register()
{
    TIODispatcher::Get()->AsyncRegister(MakeStrong(this)).Subscribe(
        BIND(&TAsyncReader::OnRegistered, MakeStrong(this)));
}

void TAsyncReader::Unregister()
{
    TGuard<TSpinLock> guard(Lock);

    if (IsRegistered()) {
        if (!IsStopped()) {
            LOG_DEBUG("Start unregistering");

            auto error = TIODispatcher::Get()->AsyncUnregister(MakeStrong(this));
            error.Subscribe(
                BIND(&TAsyncReader::OnUnregister, MakeStrong(this)));
        }
    } else {
        DoAbort();
    }
}

void TAsyncReader::DoAbort()
{
    IsAborted_ = true;
    if (!IsRegistered()) {
        // it is safe to just close the reader
        // because we never registered
        Reader->Close();
    }
}

void TAsyncReader::OnRegistered(TError status)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    if (status.IsOK()) {
        YCHECK(!IsAborted());
        YCHECK(IsRegistered());
    } else {
        YCHECK(!IsRegistered());

        if (ReadyPromise) {
            ReadyPromise.Set(status);
            ReadyPromise.Reset();
        }
        RegistrationError = status;
    }
}

void TAsyncReader::OnUnregister(TError status)
{
    if (!status.IsOK()) {
        LOG_ERROR(status, "Failed to unregister");
    }
}

void TAsyncReader::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(Lock);

    if (IsAborted()) {
        // We should FAIL the registration process
        THROW_ERROR_EXCEPTION("Reader is already aborted");
    }

    YCHECK(!IsRegistered());
    YCHECK(!IsStopped());

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncReader, &TAsyncReader::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncReader, &TAsyncReader::OnRead>(this);
    FDWatcher.start();


    IsRegistered_ = true;

    LOG_DEBUG("Registered");
}

void TAsyncReader::Stop()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    YCHECK(IsRegistered());
    YCHECK(!IsStopped());

    Close();
}

void TAsyncReader::OnStart(ev::async&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::ASYNC) == ev::ASYNC);

    TGuard<TSpinLock> guard(Lock);

    YCHECK(IsRegistered());

    if (IsAborted()) {
        return;
    }

    FDWatcher.start();
}

void TAsyncReader::OnRead(ev::io&, int eventType)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK((eventType & ev::READ) == ev::READ);

    TGuard<TSpinLock> guard(Lock);

    YCHECK(IsRegistered());

    if (IsAborted()) {
        return;
    }

    YCHECK(!Reader->ReachedEOF());

    if (!Reader->IsBufferFull()) {
        Reader->ReadToBuffer();

        if (!CanReadSomeMore()) {
            Stop();
        }

        if (Reader->IsReady()) {
            if (ReadyPromise) {
                ReadyPromise.Set(GetState());
                ReadyPromise.Reset();
            }
        }
    } else {
        // pause for a while
        FDWatcher.stop();
    }
}

std::pair<TBlob, bool> TAsyncReader::Read(TBlob&& buffer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    if (!IsRegistered()) {
        return std::make_pair(TBlob(), false);
    }

    if (!IsAborted() && CanReadSomeMore()) {
        // is it safe?
        if (!FDWatcher.is_active()) {
            StartWatcher.send();
        }
    }

    return Reader->GetRead(std::move(buffer));
}

TAsyncError TAsyncReader::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    if (IsAborted() || !RegistrationError.IsOK() || Reader->IsReady()) {
        return MakePromise(GetState());
    }

    YCHECK(!ReadyPromise);
    ReadyPromise = NewPromise<TError>();
    return ReadyPromise.ToFuture();
}

void TAsyncReader::Close()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    FDWatcher.stop();
    StartWatcher.stop();

    Reader->Close();
}

TError TAsyncReader::Abort()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(Lock);

    IsAborted_ = true;

    if (IsRegistered()) {
        if (!IsStopped()) {
            LOG_DEBUG("Start unregistering");

            auto error = TIODispatcher::Get()->AsyncUnregister(MakeStrong(this));
            error.Subscribe(
                BIND(&TAsyncReader::OnUnregister, MakeStrong(this)));
        }
    } else {
        Reader->Close();
    }

    if (ReadyPromise) {
        ReadyPromise.Set(GetState());
        ReadyPromise.Reset();
    }

    // report the last reader error if any
    if (Reader->InFailedState()) {
        return TError::FromSystem(Reader->GetLastSystemError());
    } else {
        return TError();
    }
}

bool TAsyncReader::CanReadSomeMore() const
{
    return !Reader->InFailedState() && !Reader->ReachedEOF();
}

TError TAsyncReader::GetState() const
{
    if (IsAborted()) {
        return TError("The reader was aborted");
    } else if (!RegistrationError.IsOK()) {
        return RegistrationError;
    } else if (Reader->ReachedEOF() || !Reader->IsBufferEmpty()) {
        return TError();
    } else if (Reader->InFailedState()) {
        return TError::FromSystem(Reader->GetLastSystemError());
    } else {
        YCHECK(false);
        return TError();
    }
}

bool TAsyncReader::IsAborted() const
{
    return IsAborted_;
}

bool TAsyncReader::IsRegistered() const
{
    return IsRegistered_;
}

bool TAsyncReader::IsStopped() const
{
    return Reader->IsClosed();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
