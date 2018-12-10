#include "periodic_executor.h"

#include <yt/core/actions/bind.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/utilex/random.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TDuration period,
    EPeriodicExecutorMode mode,
    TDuration splay)
    : Invoker_(std::move(invoker))
    , Callback_(std::move(callback))
    , Period_(period)
    , Mode_(mode)
    , Splay_(splay)
{
    YCHECK(Invoker_);
    YCHECK(Callback_);
}

void TPeriodicExecutor::Start()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Started_)
        return;
    ExecutedPromise_ = TPromise<void>();
    IdlePromise_ = TPromise<void>();
    Started_ = true;
    PostDelayedCallback(RandomDuration(Splay_));
}

void TPeriodicExecutor::DoStop()
{
    if (Started_) {
        Started_ = false;
        OutOfBandRequested_ = false;
        if (ExecutedPromise_ && !ExecutedPromise_.IsSet()) {
            TInverseGuard<TSpinLock> guard(SpinLock_);
            ExecutedPromise_.Set(GetStoppedError());
        }
        TDelayedExecutor::CancelAndClear(Cookie_);
    }
}

TFuture<void> TPeriodicExecutor::Stop()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (ExecutingCallback_) {
        InitIdlePromise();
        DoStop();
        return IdlePromise_;
    } else {
        DoStop();
        return VoidFuture;
    }
}

TError TPeriodicExecutor::GetStoppedError()
{
    return TError("Periodic executor is stopped");
}

void TPeriodicExecutor::InitIdlePromise()
{
    if (IdlePromise_) {
        return;
    }

    if (Started_) {
        IdlePromise_ = NewPromise<void>();
    } else {
        IdlePromise_ = MakePromise<void>(TError());
    }
}

void TPeriodicExecutor::InitExecutedPromise()
{
    if (ExecutedPromise_) {
        return;
    }

    if (Started_) {
        ExecutedPromise_ = NewPromise<void>();
    } else {
        ExecutedPromise_ = MakePromise<void>(GetStoppedError());
    }
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
    
    if (IdlePromise_ && IdlePromise_.IsSet()) {
        IdlePromise_ = TPromise<void>();
    }

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
    TPromise<void> executedPromise;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!Started_ || Busy_)
            return;
        Busy_ = true;
        ExecutingCallback_ = true;
        TDelayedExecutor::CancelAndClear(Cookie_);
        if (ExecutedPromise_) {
            executedPromise = ExecutedPromise_;
            ExecutedPromise_ = TPromise<void>();
        }
        if (IdlePromise_) {
            IdlePromise_ = NewPromise<void>();
        }
    }

    Callback_.Run();

    TPromise<void> idlePromise;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        idlePromise = IdlePromise_;
        ExecutingCallback_ = false;
    }

    if (idlePromise && !idlePromise.IsSet()) {
        idlePromise.Set();
    }
    if (executedPromise) {
        executedPromise.Set();
    }
    
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

void TPeriodicExecutor::SetPeriod(TDuration period)
{
    TGuard<TSpinLock> guard(SpinLock_);
    Period_ = period;
}

TFuture<void> TPeriodicExecutor::GetExecutedEvent()
{
    TGuard<TSpinLock> guard(SpinLock_);
    InitExecutedPromise();
    return ExecutedPromise_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
