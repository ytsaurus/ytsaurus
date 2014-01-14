#include "async_io.h"

#include "io_dispatcher.h"
#include "private.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TAsyncIOBase::TAsyncIOBase()
    : IsStartAborted_(false)
    , IsStarted_(false)
    , IsStopped_(false)
{ }

TAsyncIOBase::~TAsyncIOBase()
{
    YCHECK(IsStopped() || IsStartAborted());
}

void TAsyncIOBase::Register()
{
    TIODispatcher::Get()->AsyncRegister(MakeStrong(this)).Subscribe(
        BIND(&TAsyncIOBase::OnRegistered, MakeStrong(this)));
}

void TAsyncIOBase::Unregister()
{
    TGuard<TSpinLock> guard(FlagsLock);

    if (IsStarted()) {
        if (!IsStopped()) {
            auto error = TIODispatcher::Get()->AsyncUnregister(MakeStrong(this));
            error.Subscribe(
                BIND(&TAsyncIOBase::OnUnregister, MakeStrong(this)));
        }
    } else {
        IsStartAborted_ = true;
        // should I close a fd here????
    }
}

void TAsyncIOBase::Start(ev::dynamic_loop& eventLoop)
{
    TGuard<TSpinLock> guard(FlagsLock);

    if (IsStartAborted()) {
        // We should FAIL the registration process
        THROW_ERROR_EXCEPTION("Reader is already aborted");
    }

    YCHECK(!IsStarted());
    YCHECK(!IsStopped());

    DoStart(eventLoop);

    IsStarted_ = true;
}

void TAsyncIOBase::Stop()
{
    TGuard<TSpinLock> guard(FlagsLock);

    YCHECK(IsStarted());
    YCHECK(!IsStopped());

    DoStop();

    IsStopped_ = true;
}

bool TAsyncIOBase::IsStartAborted() const
{
    return IsStartAborted_;
}

bool TAsyncIOBase::IsStarted() const
{
    return IsStarted_;
}

bool TAsyncIOBase::IsStopped() const
{
    return IsStopped_;
}

////////////////////////////////////////////////////////////////////////////////

} // NPipes
} // NYT
