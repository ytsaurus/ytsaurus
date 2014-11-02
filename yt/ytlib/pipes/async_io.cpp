#include "stdafx.h"
#include "async_io.h"

#include "io_dispatcher.h"
#include "private.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TAsyncIOBase::TAsyncIOBase()
    : State_(EAsyncIOState::Created)
{ }

TAsyncIOBase::~TAsyncIOBase()
{
    YCHECK(
        State_ == EAsyncIOState::StartAborted ||
        State_ == EAsyncIOState::Stopped);
}

void TAsyncIOBase::Register()
{
    auto this_ = MakeStrong(this);
    TIODispatcher::Get()->AsyncRegister(this_).Subscribe(
        BIND(&TAsyncIOBase::OnRegistered, this_));
}

void TAsyncIOBase::Unregister()
{
    TGuard<TSpinLock> guard(FlagsLock_);

    if (State_ == EAsyncIOState::Started) {
      auto error = TIODispatcher::Get()->AsyncUnregister(MakeStrong(this));
      error.Subscribe(
        BIND(&TAsyncIOBase::OnUnregister, MakeStrong(this)));
    } else {
        State_ = EAsyncIOState::StartAborted;
    }
}

void TAsyncIOBase::Start(ev::dynamic_loop& eventLoop)
{
    TGuard<TSpinLock> guard(FlagsLock_);

    if (State_ == EAsyncIOState::StartAborted) {
        // We should FAIL the registration process.
        THROW_ERROR_EXCEPTION("Reader is already aborted");
    }

    YCHECK(State_ == EAsyncIOState::Created);

    DoStart(eventLoop);

    State_ = EAsyncIOState::Started;
}

void TAsyncIOBase::Stop()
{
    TGuard<TSpinLock> guard(FlagsLock_);

    YCHECK(State_ == EAsyncIOState::Started);

    DoStop();

    State_ = EAsyncIOState::Stopped;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
