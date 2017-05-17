#include "periodic_executor.h"

#include <yt/core/actions/bind.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/utilex/random.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TDuration period,
    EPeriodicExecutorMode mode,
    TDuration splay)
    : Invoker_(invoker)
    , Callback_(callback)
    , Period_(period)
    , Mode_(mode)
    , Splay_(splay)
    , IdlePromise_(MakePromise<void>(TError()))
{
    YCHECK(Invoker_);
    YCHECK(Callback_);
}

void TPeriodicExecutor::Start()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Started_)
        return;
    Started_ = true;
    PostDelayedCallback(RandomDuration(Splay_));
}

TFuture<void> TPeriodicExecutor::Stop()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Started_) {
        Started_ = false;
        OutOfBandRequested_ = false;
        TDelayedExecutor::CancelAndClear(Cookie_);
    }
    return IdlePromise_;
}

void TPeriodicExecutor::ScheduleOutOfBand()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (!Started_)
        return;

    if (Busy_) {
        OutOfBandRequested_ = true;
    } else {
        guard.Release();
        PostCallback();
    }
}

void TPeriodicExecutor::ScheduleNext()
{
    TGuard<TSpinLock> guard(SpinLock_);
    
    // There several reasons why this may fail:
    // 1) Calling ScheduleNext outside of the periodic action
    // 2) Calling ScheduleNext more than once
    // 3) Calling ScheduleNext for an executor in automatic mode
    YCHECK(Busy_);
    Busy_ = false;

    if (!Started_)
        return;

    if (OutOfBandRequested_) {
        OutOfBandRequested_ = false;
        guard.Release();
        PostCallback();
    } else {
        PostDelayedCallback(Period_);
    }
}

void TPeriodicExecutor::PostDelayedCallback(TDuration delay)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    TDelayedExecutor::CancelAndClear(Cookie_);
    Cookie_ = TDelayedExecutor::Submit(
        BIND(&TPeriodicExecutor::OnTimer, MakeWeak(this)),
        delay); 
}

void TPeriodicExecutor::PostCallback()
{
    auto this_ = MakeWeak(this);
    GuardedInvoke(
        Invoker_,
        BIND(&TPeriodicExecutor::OnCallbackSuccess, this_),
        BIND(&TPeriodicExecutor::OnCallbackFailure, this_));
}

void TPeriodicExecutor::OnTimer(bool aborted)
{
    if (aborted) {
        return;
    }
    PostCallback();
}

void TPeriodicExecutor::OnCallbackSuccess()
{
    TPromise<void> idlePromise;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!Started_ || Busy_)
            return;
        Busy_ = true;
        TDelayedExecutor::CancelAndClear(Cookie_);
        idlePromise = IdlePromise_ = NewPromise<void>();
    }

    Callback_.Run();
    idlePromise.Set();
    
    if (Mode_ == EPeriodicExecutorMode::Automatic) {
        ScheduleNext();
    }
}

void TPeriodicExecutor::OnCallbackFailure()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (!Started_)
        return;
    PostDelayedCallback(Period_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
