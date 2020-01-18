#include "future.h"
#include "invoker_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const TFuture<void> VoidFuture = MakeWellKnownFuture(TError());
const TFuture<bool> TrueFuture = MakeWellKnownFuture(TErrorOr<bool>(true));
const TFuture<bool> FalseFuture = MakeWellKnownFuture(TErrorOr<bool>(false));

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void TFutureStateBase::Subscribe(TVoidResultHandler handler)
{
    // Fast path.
    if (Set_) {
        RunNoExcept(handler);
        return;
    }

    // Slow path.
    {
        auto guard = Guard(SpinLock_);
        InstallAbandonedError();
        if (Set_) {
            guard.Release();
            RunNoExcept(handler);
        } else {
            VoidResultHandlers_.push_back(std::move(handler));
            HasHandlers_ = true;
        }
    }
}

bool TFutureStateBase::Cancel(const TError& error) noexcept
{
    // Calling subscribers may release the last reference to this.
    TIntrusivePtr<TFutureStateBase> this_(this);

    {
        auto guard = Guard(SpinLock_);
        if (Set_ || AbandonedUnset_ || Canceled_) {
            return false;
        }
        CancelationError_ = error;
        Canceled_ = true;
    }

    if (CancelHandlers_.empty()) {
        if (!DoTrySetCanceledError(error)) {
            return false;
        }
    } else {
        for (const auto& handler : CancelHandlers_) {
            RunNoExcept(handler, error);
        }
        CancelHandlers_.clear();
    }

    return true;
}

void TFutureStateBase::OnCanceled(TCancelHandler handler)
{
    // Fast path.
    if (Set_) {
        return;
    }
    if (Canceled_) {
        RunNoExcept(handler, CancelationError_);
        return;
    }

    // Slow path.
    {
        auto guard = Guard(SpinLock_);
        InstallAbandonedError();
        if (Canceled_) {
            guard.Release();
            RunNoExcept(handler, CancelationError_);
        } else if (!Set_) {
            CancelHandlers_.push_back(std::move(handler));
            HasHandlers_ = true;
        }
    }
}

bool TFutureStateBase::TimedWait(TDuration timeout) const
{
    // Fast path.
    if (Set_ || AbandonedUnset_) {
        return true;
    }

    // Slow path.
    {
        auto guard = Guard(SpinLock_);
        InstallAbandonedError();
        if (Set_) {
            return true;
        }
        if (!ReadyEvent_) {
            ReadyEvent_.reset(new NConcurrency::TEvent());
        }
    }

    return ReadyEvent_->Wait(timeout.ToDeadLine());
}

void TFutureStateBase::InstallAbandonedError() const
{
    const_cast<TFutureStateBase*>(this)->InstallAbandonedError();
}

void TFutureStateBase::InstallAbandonedError()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (AbandonedUnset_ && !Set_) {
        DoInstallAbandonedError();
    }
}

void TFutureStateBase::Dispose()
{
    // Check for fast path.
    if (Set_) {
        // Just kill the fake weak reference.
        UnrefFuture();
        return;
    }

    // Another fast path: no subscribers.
    {
        auto guard = Guard(SpinLock_);
        if (!HasHandlers_) {
            YT_ASSERT(!AbandonedUnset_);
            AbandonedUnset_ = true;
            // Cannot access this after UnrefFuture; in particular, cannot touch SpinLock_ in guard's dtor.
            guard.Release();
            UnrefFuture();
            return;
        }
    }

    // Slow path: notify the subscribers in a dedicated thread.
    GetFinalizerInvoker()->Invoke(BIND([=] () {
        // Set the promise if the value is still missing.
        if (!Set_) {
            DoTrySetAbandonedError();
        }
        // Kill the fake weak reference.
        UnrefFuture();
    }));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
