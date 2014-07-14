#include "stdafx.h"
#include "periodic_executor.h"

#include <core/actions/invoker_util.h>
#include <core/actions/bind.h>

#include <core/concurrency/thread_affinity.h>

#include <util/random/random.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TDuration period,
    EPeriodicExecutorMode mode,
    TDuration splay)
    : Invoker(invoker)
    , Callback(callback)
    , Period(period)
    , Mode(mode)
    , Splay(splay)
    , Started(false)
    , Busy(false)
    , OutOfBandRequested(false)
{ }

void TPeriodicExecutor::Start()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (Started)
        return;
    Started = true;
    PostDelayedCallback(RandomDuration(Splay));
}

void TPeriodicExecutor::Stop()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!Started)
        return;
    Started = false;
    TDelayedExecutor::CancelAndClear(Cookie);
}

void TPeriodicExecutor::ScheduleOutOfBand()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!Started)
        return;
    if (Busy) {
        OutOfBandRequested = true;
    } else {
        guard.Release();
        PostCallback();
    }
}

void TPeriodicExecutor::ScheduleNext()
{
    TGuard<TSpinLock> guard(SpinLock);    
    if (!Started)
        return;

    // There several reasons why this may fail:
    // 1) Calling ScheduleNext outside of the periodic action
    // 2) Calling ScheduleNext more than once
    // 3) Calling ScheduleNext for an invoker in automatic mode
    YCHECK(Busy);
    Busy = false;

    if (OutOfBandRequested) {
        OutOfBandRequested = false;
        guard.Release();
        PostCallback();
    } else {
        PostDelayedCallback(Period);
    }
}

void TPeriodicExecutor::PostDelayedCallback(TDuration delay)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);
    TDelayedExecutor::CancelAndClear(Cookie);
    Cookie = TDelayedExecutor::Submit(
        BIND(&TPeriodicExecutor::PostCallback, MakeWeak(this)),
        delay);
}

void TPeriodicExecutor::PostCallback()
{
    auto this_ = MakeWeak(this);
    GuardedInvoke(
        Invoker,
        BIND(&TPeriodicExecutor::OnCallbackSuccess, this_),
        BIND(&TPeriodicExecutor::OnCallbackFailure, this_));
}

void TPeriodicExecutor::OnCallbackSuccess()
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (!Started || Busy)
            return;
        Busy = true;
        TDelayedExecutor::CancelAndClear(Cookie);                
    }

    Callback.Run();
    
    if (Mode == EPeriodicExecutorMode::Automatic) {
        ScheduleNext();
    }
}

void TPeriodicExecutor::OnCallbackFailure()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!Started)
        return;
    PostDelayedCallback(Period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
