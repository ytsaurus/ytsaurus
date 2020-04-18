#include "periodic_executor.h"
#include "scheduler.h"

#include <yt/core/actions/bind.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/utilex/random.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutorOptions TPeriodicExecutorOptions::WithJitter(TDuration period)
{
    TPeriodicExecutorOptions options;
    options.Period = period;
    options.Jitter = DefaultJitter;
    return options;
}

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    std::optional<TDuration> period,
    EPeriodicExecutorMode mode,
    TDuration splay)
    : TPeriodicExecutor(
        std::move(invoker),
        std::move(callback),
        {
            period,
            mode,
            splay,
            0.0,
        })
{
    YT_VERIFY(Invoker_);
    YT_VERIFY(Callback_);
}

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TPeriodicExecutorOptions options)
    : Invoker_(std::move(invoker))
    , Callback_(std::move(callback))
    , Period_(options.Period)
    , Mode_(options.Mode)
    , Splay_(options.Splay)
    , Jitter_(options.Jitter)
{
    YT_VERIFY(Invoker_);
    YT_VERIFY(Callback_);
}

void TPeriodicExecutor::Start()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (Started_) {
        return;
    }

    ExecutedPromise_ = TPromise<void>();
    IdlePromise_ = TPromise<void>();
    Started_ = true;
    if (Period_) {
        PostDelayedCallback(RandomDuration(Splay_));
    }
}

void TPeriodicExecutor::DoStop(TGuard<TSpinLock>& guard)
{
    if (!Started_) {
        return;
    }

    Started_ = false;
    OutOfBandRequested_ = false;
    auto executedPromise = ExecutedPromise_;
    auto executionCanceler = ExecutionCanceler_;
    TDelayedExecutor::CancelAndClear(Cookie_);

    guard.Release();

    if (executedPromise) {
        executedPromise.TrySet(MakeStoppedError());
    }

    if (executionCanceler) {
        executionCanceler.Run(MakeStoppedError());
    }
}

TFuture<void> TPeriodicExecutor::Stop()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (ExecutingCallback_) {
        InitIdlePromise();
        auto idlePromise = IdlePromise_;
        DoStop(guard);
        return idlePromise;
    } else {
        DoStop(guard);
        return VoidFuture;
    }
}

TError TPeriodicExecutor::MakeStoppedError()
{
    return TError(NYT::EErrorCode::Canceled, "Periodic executor is stopped");
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
        ExecutedPromise_ = MakePromise<void>(MakeStoppedError());
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
    YT_VERIFY(Busy_);
    Busy_ = false;

    if (!Started_) {
        return;
    }

    if (IdlePromise_ && IdlePromise_.IsSet()) {
        IdlePromise_ = TPromise<void>();
    }

    if (OutOfBandRequested_) {
        OutOfBandRequested_ = false;
        guard.Release();
        PostCallback();
    } else if (Period_) {
        PostDelayedCallback(NextDelay());
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
        if (!Started_ || Busy_) {
            return;
        }
        Busy_ = true;
        ExecutingCallback_ = true;
        ExecutionCanceler_ = GetCurrentFiberCanceler();
        TDelayedExecutor::CancelAndClear(Cookie_);
        if (ExecutedPromise_) {
            executedPromise = ExecutedPromise_;
            ExecutedPromise_ = TPromise<void>();
        }
        if (IdlePromise_) {
            IdlePromise_ = NewPromise<void>();
        }
    }

    auto cleanup = [=] {
        TPromise<void> idlePromise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            idlePromise = IdlePromise_;
            ExecutingCallback_ = false;
            ExecutionCanceler_.Reset();
        }

        if (idlePromise) {
            idlePromise.TrySet();
        }

        if (executedPromise) {
            executedPromise.TrySet();
        }

        if (Mode_ == EPeriodicExecutorMode::Automatic) {
            ScheduleNext();
        }
    };

    try {
        Callback_.Run();
    } catch (const TFiberCanceledException&) {
        // There's very little we can do here safely;
        // in particular, we should refrain from setting promises;
        // let's forward the call to the delayed executor.
        TDelayedExecutor::Submit(
            BIND([this_ = MakeStrong(this), cleanup = std::move(cleanup)] () mutable { cleanup(); }),
            TDuration::Zero());
        throw;
    }

    cleanup();
}

void TPeriodicExecutor::OnCallbackFailure()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (!Started_) {
        return;
    }

    if (Period_) {
        PostDelayedCallback(NextDelay());
    }
}

void TPeriodicExecutor::SetPeriod(std::optional<TDuration> period)
{
    TGuard<TSpinLock> guard(SpinLock_);

    // Kick-start invocations, if needed.
    if (Started_ && period && (!Period_ || *period < *Period_) && !Busy_) {
        PostDelayedCallback(RandomDuration(Splay_));
    }

    Period_ = period;
}

TFuture<void> TPeriodicExecutor::GetExecutedEvent()
{
    TGuard<TSpinLock> guard(SpinLock_);
    InitExecutedPromise();
    return ExecutedPromise_;
}

TDuration TPeriodicExecutor::NextDelay()
{
    if (Jitter_ == 0.0) {
        return *Period_;
    } else {
        auto period = *Period_;
        period += RandomDuration(period) * Jitter_ - period * Jitter_ / 2.;
        return period;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
